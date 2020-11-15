#copied from https://gist.github.com/wiseman/8547eee19421d69a838dc951e432071a

"""Does utterance segmentation and speech recognition on an mp3 stream
(like from broadcastify...)
Requires that ffmpeg be installed, and a Microsoft cognitive speech
services API key
(see https://azure.microsoft.com/en-us/try/cognitive-services/?api=speech-services).
"""

import audioop
import collections
import datetime
import json
import os
import os.path
import queue
import subprocess
import sys
import threading
import time
import wave

import azure.cognitiveservices.speech as speechsdk
import ffmpeg
import gflags
import lemapp
import pyaudio
import termcolor
import webrtcvad

FLAGS = gflags.FLAGS

gflags.DEFINE_string(
    'speech_key',
    '3bebb1f9c2294ef4a90e8a2c9b3f2656',
    'The Azure speech-to-text subscription key.')
gflags.DEFINE_string(
    'speech_region',
    'westus',
    'The Azure speech-to-text region.')
gflags.DEFINE_boolean(
    'show_ffmpeg_output',
    False,
    'Show ffmpeg\'s output (for debugging).')
gflags.DEFINE_string(
    'wav_output_dir',
    'wavs',
    'The directory in which to store wavs.')


class Error(Exception):
    pass


def print_status(fmt, *args):
    sys.stdout.write('\r%s\r' % (75 * ' '))
    print(fmt % args)


def coroutine(func):
    def start(*args,**kwargs):
        cr = func(*args,**kwargs)
        next(cr)
        return cr
    return start


@coroutine
def broadcast(targets):
    while True:
        item = (yield)
        for target in targets:
            target.send(item)


TS_FORMAT = '%Y%m%d-%H%M%S.%f'

def timestamp_str(dt):
    return dt.strftime(TS_FORMAT)[:-3]


class BingRecognizer(object):
    def __init__(self, key=None, region=None):
        speech_config = speechsdk.SpeechConfig(subscription=key, region=region)
        speech_config.set_profanity(speechsdk.ProfanityOption.Raw)
        # Not sure if this does anything.
        speech_config.output_format = speechsdk.OutputFormat.Detailed
        self.speech_config = speech_config

    def recognize_file(self, path):
        audio_config = speechsdk.audio.AudioConfig(filename=path)
        speech_recognizer = speechsdk.SpeechRecognizer(
            speech_config=self.speech_config, audio_config=audio_config)
        result = speech_recognizer.recognize_once()
        return result


def start_daemon(callable, *args):
    t = threading.Thread(target=callable, args=args)
    t.daemon = True
    t.start()


def queue_get_nowait(q):
    value = None
    try:
        value = q.get_nowait()
    except queue.Empty:
        pass
    return value


class AudioFrame(object):
    def __init__(self, data=None, timestamp=None, sample_rate=None):
        self.data = data
        self.timestamp = timestamp
        self.sample_rate = sample_rate

    def __str__(self):
        return '<AudioFrame %s bytes (%s s)>' % (
            len(self.data),
            self.duration())

    def __repr__(self):
        return str(self)

    def duration(self):
        return len(self.data) / (2.0 * self.sample_rate)

    @staticmethod
    def coalesce(frames):
        "Coalesces multiple frames into one frame. Order is important."
        if not frames:
            return None
        # Use the earliest timestamp; timestamp is always when the
        # audio *started*.
        frame = AudioFrame(
            b''.join([f.data for f in frames]),
            sample_rate=frames[0].sample_rate,
            timestamp=frames[0].timestamp)
        return frame


# Takes AudioFrames as input and plays them to the default audio
# output device via PyAudio.

@coroutine
def play_audio_co():
    pa = pyaudio.PyAudio()
    audio_stream = pa.open(format=pa.get_format_from_width(2),
                           channels=1,
                           rate=16000,
                           output=True)
    while True:
        audio_frame = (yield)
        audio_stream.write(audio_frame.data)


# Takes AudioFrames as input and displays a console-based VU meter
# reflecting the RMS power of the audio in realtime.

@coroutine
def vu_meter_co():
    max_rms = 1.0
    num_cols = 70
    count = 0
    if FLAGS.show_ffmpeg_output:
        # Disable the VU meter if we're debugging ffmpeg.
        return
    while True:
        max_rms = max(max_rms * 0.99, 1.0)
        audio_frame = (yield)
        data = audio_frame.data
        ts = audio_frame.timestamp
        rms = audioop.rms(data, 2)
        norm_rms = rms / len(data)
        if norm_rms > max_rms:
            max_rms = norm_rms
        bar_cols = int(num_cols * (norm_rms / max_rms))
        sys.stdout.write('\r[' + ('*' * bar_cols) +
                         (' ' * (num_cols - bar_cols)) +
                         '] %4.1f %s' % (norm_rms, count))
        count += 1
        sys.stdout.flush()


# Takes as input AudioFrames of arbitrary durations and sends as output
# a new series of AudioFrames containing the same data, but all having
# the desired new frame duration.
#
# E.g. you can take frames that are 1 second long and convert them
# into frames that are 30 ms long.
#
# We do this because webrtc vad requires frames that are 10, 20, or 30
# ms ONLY.

@coroutine
def reframer_co(target, desired_frame_duration_ms=None):
    sample_rate = None
    data = bytes()
    ts = None
    total_bytes = 0
    while True:
        frame = (yield)
        if not sample_rate:
            sample_rate = frame.sample_rate
            num_bytes = 2 * sample_rate * desired_frame_duration_ms // 1000
        if not ts:
            ts = frame.timestamp
        data += frame.data
        while len(data) > num_bytes:
            buf = data[:num_bytes]
            total_bytes += num_bytes
            frame = AudioFrame(
                data=buf, timestamp=ts, sample_rate=sample_rate)
            if target:
                target.send(frame)
            ts += datetime.timedelta(milliseconds=desired_frame_duration_ms)
            data = data[num_bytes:]


# Takes AudioFrames as input, and uses VAD as a gate to pass through
# only voiced frames. The output is actually VAD trigger events, which
# are tuples of the following form:
#
#  ('triggered', datetime)    - Utterance started
#  ('audio', AudioFrame)      - Utterance in progress
#  ('detriggered', datetime)  - Utterance finished

@coroutine
def vad_trigger_co(target, sample_rate=None, frame_duration_ms=None,
                   padding_duration_ms=None):
    """Filters out non-voiced audio frames.
    Given a webrtcvad.Vad and a source of audio frames, yields only
    the voiced audio.
    Uses a padded, sliding window algorithm over the audio frames.
    When more than 90% of the frames in the window are voiced (as
    reported by the VAD), the collector triggers and begins yielding
    audio frames. Then the collector waits until 90% of the frames in
    the window are unvoiced to detrigger. The window is padded at the
    front and back to provide a small amount of silence or the
    beginnings/endings of speech around the voiced frames.
    Arguments:
    sample_rate - The audio sample rate, in Hz.
    frame_duration_ms - The frame duration in milliseconds.
    padding_duration_ms - The amount to pad the window, in milliseconds.
    Returns: A generator that yields PCM audio data.
    """
    num_padding_frames = int(padding_duration_ms / frame_duration_ms)
    # We use a deque for our sliding window/ring buffer.
    ring_buffer = collections.deque(maxlen=num_padding_frames)
    # We have two states: TRIGGERED and NOTTRIGGERED. We start in the
    # NOTTRIGGERED state.
    triggered = False
    triggered_ts = None
    voiced_frames = []
    vad = webrtcvad.Vad()
    while True:
        frame = (yield)
        is_speech = vad.is_speech(frame.data, sample_rate)
        if not triggered:
            ring_buffer.append((frame, is_speech))
            num_voiced = len([f for f, speech in ring_buffer if speech])
            # If we're NOTTRIGGERED and more than 90% of the frames in
            # the ring buffer are voiced frames, then enter the
            # TRIGGERED state.
            if num_voiced > 0.9 * ring_buffer.maxlen:
                triggered = True
                # We want to yield all the audio we see from now until
                # we are NOTTRIGGERED, but we have to start with the
                # audio that's already in the ring buffer.
                for f, s in ring_buffer:
                    if not triggered_ts:
                        triggered_ts = f.timestamp
                target.send(('triggered', triggered_ts))
                for f, s in ring_buffer:
                    target.send(('audio', f))
                ring_buffer.clear()
        else:
            # We're in the TRIGGERED state, so collect the audio data
            # and add it to the ring buffer.
            target.send(('audio', frame))
            ring_buffer.append((frame, is_speech))
            num_unvoiced = len([f for f, speech in ring_buffer if not speech])
            # If more than 90% of the frames in the ring buffer are
            # unvoiced, then enter NOTTRIGGERED and yield whatever
            # audio we've collected.
            if num_unvoiced > 0.9 * ring_buffer.maxlen:
                target.send(('detriggered', frame.timestamp))
                triggered = False
                triggered_ts = None
                ring_buffer.clear()


# Takes VAD trigger events as input. Collects voiced audio frames,
# waits until an utterance is complete, then puts a coalesced audio
# frame into the utterance queue.

@coroutine
def vad_collector_co(utterance_queue=None):
    frames = []
    while True:
        event, data = (yield)
        if event == 'triggered':
            frames = []
        elif event == 'detriggered':
            utterance_queue.put(AudioFrame.coalesce(frames))
        else:
            frames.append(data)


def enqueue_stream_input(f, queue, bufsiz):
    while True:
        ts = datetime.datetime.now()
        data = f.read(bufsiz)
        queue.put((ts, data))


def start_queueing_stream_input(f, queue, bufsiz):
    start_daemon(enqueue_stream_input, f, queue, bufsiz)


@coroutine
def print_co():
    while True:
        print((yield))


def coalesce_stream_input_queue(q):
    data = b''
    max_count = 100
    count = 0
    while not q.empty() and count < max_count:
        ts, buf = q.get()
        data += buf
        count += 1
    return data


# Takes audio out of the utterance queue, writes it to a wav and sends
# it to the speech recognizer.

def process_utterance_queue(queue):
    recognizer = BingRecognizer(FLAGS.speech_key, FLAGS.speech_region)
    while True:
        audio = queue.get()
        wav_filename = '%s.wav' % (timestamp_str(audio.timestamp))
        wav_path = os.path.join(FLAGS.wav_output_dir, wav_filename)
        wav_file = wave.open(wav_path, 'wb')
        wav_file.setnchannels(1)
        wav_file.setsampwidth(2)
        wav_file.setframerate(16000)
        wav_file.writeframes(audio.data)
        wav_file.close()
        result = recognizer.recognize_file(wav_path)
        print_recog_result(audio.timestamp, result)


def print_recog_result(ts, result):
    result_j = json.loads(result.json)
    if result_j['RecognitionStatus'] == 'Success':
        nbest = result_j['NBest']
        if nbest:
            print_status('%s[%4.2f] %s',
                         ts,
                         nbest[0]['Confidence'],
                         termcolor.colored(nbest[0]['Display'], attrs=['bold']))
            return
    print_status('%s', result)
    print(result.json)


def main(args):
    if len(args) != 2:
        raise lemapp.UsageError('Must supply one argument: The streaming mp3 URL.')
    try:
        os.makedirs(FLAGS.wav_output_dir)
    except FileExistsError:
        pass
    # Setup ffmpeg subprocess and threads that read its stdout and
    # stderr and put contents into queues.
    ffmpeg_cmd = ffmpeg.input(args[1]).output(
        '-', format='s16le', acodec='pcm_s16le', ac=1, ar='16k').compile()
    print(ffmpeg_cmd)
    ffmpeg_proc = subprocess.Popen(
        ffmpeg_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=False)
    ffmpeg_stdout_q = queue.Queue()
    ffmpeg_stderr_q = queue.Queue()
    start_queueing_stream_input(ffmpeg_proc.stdout, ffmpeg_stdout_q, 1600)
    start_queueing_stream_input(ffmpeg_proc.stderr, ffmpeg_stderr_q, 1)
    # Create the recognizer thread, which processes the utterance
    # queue.
    utterance_queue = queue.Queue()
    start_daemon(process_utterance_queue, utterance_queue)
    # Create the graph of coroutines.
    play_audio = play_audio_co()
    vu_meter = vu_meter_co()
    vad_collector = vad_collector_co(utterance_queue=utterance_queue)
    vad_trigger = vad_trigger_co(
        vad_collector,
        sample_rate=16000, frame_duration_ms=30, padding_duration_ms=500)
    reframer = reframer_co(vad_trigger, desired_frame_duration_ms=30)
    audio_pipeline_head = broadcast([reframer, vu_meter, play_audio])
    while True:
        got_item = False
        # Process ffmpeg stderr. Need to do this so it doesn't block.
        if not ffmpeg_stderr_q.empty():
            got_item = True
            stderr_buf = coalesce_stream_input_queue(ffmpeg_stderr_q)
            if FLAGS.show_ffmpeg_output:
                sys.stderr.write(stderr_buf.decode('utf8'))
                sys.stderr.flush()
        # Process ffmpeg stdout, containing decoded PCM audio.
        audio_item = queue_get_nowait(ffmpeg_stdout_q)
        if audio_item:
            got_item = True
            ts, audio_buf = audio_item
            audio_frame = AudioFrame(data=audio_buf, timestamp=ts, sample_rate=16000)
            audio_pipeline_head.send(audio_frame)
        if not got_item:
            time.sleep(0)

if __name__ == '__main__':
    lemapp.App().run()