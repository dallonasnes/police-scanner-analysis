"""
This class handles a single broadcast url

Input Args:
    1: url to the broadcast
    2: dept_name
    3: zone

Goal: Listen to specified broadcastify audio input stream for period of time specified in thread.join() call
      Then upload the saved stream (mp3) to S3 and kick off a transcription job. Lambda handler will continue the pipeline.join()
"""
from aws_utils import upload_to_aws, get_transcriber, amazon_transcribe

import threading
import requests
from requests.auth import HTTPBasicAuth 
import os
import sys
from shutil import rmtree
import time
from datetime import date, datetime
from secrets import BROADCASTIFY_USERNAME, BROADCASTIFY_PW

class Audio:
    def __init__(self, url, event, dept_name, zone_name):
        self.url = url
        self.event = event
        self.dept_name = dept_name
        self.zone_name = zone_name
    
    def set_event(self):
        self.event.set()

    def download_file(self):
        today = datetime.now()
        hour = str(today.hour)
        if len(hour) != 2: hour = '0' + hour
        minute = str(today.minute)
        if len(minute) != 2: minute = '0' + minute
        self.local_filename = str(today.day) + str(today.month) + str(today.year) + hour + minute + "00." + self.dept_name + "." + self.zone_name + "." + str(time.time()) + ".mp3"
        # NOTE the stream=True parameter below
        with requests.get(self.url, stream=True, auth=HTTPBasicAuth(BROADCASTIFY_USERNAME, BROADCASTIFY_PW)) as r:
            #TODO: how to handle request failures here?
            r.raise_for_status()
            with open(self.local_filename, 'wb') as f:
                for chunk in r.iter_content(chunk_size=4096): # used to be 8192, so let's see if reducing it fixes long silence at end
                    f.write(chunk)
                    if self.event.is_set():
                        return


if __name__ == "__main__":
    args = sys.argv
    if len(args) != 4:
        print("usage: (1) broadcastify url (2) department name (3) zone name/number")
    else:
        target_url = args[1] # "https://audio.broadcastify.com/32936.mp3"
        dept_name = args[2]  # "cpd"
        zone_name = args[3]  # "zone1"
        
        aud = Audio(target_url, threading.Event(), dept_name, zone_name)
        t = threading.Thread(target=aud.download_file,)
        t.daemon = True
        t.start()
        print(aud.local_filename)
        # wait 60 seconds for the thread to finish its work
        t.join(10)
        aud.set_event()
        print("uploading file to aws bucket")
        upload_to_aws(aud.local_filename, "dasnes-mpcs53014", aud.local_filename)
        os.remove(aud.local_filename)
        #now kick off transcriber job
        print("now kicking off transcriber job")
        transcriber = get_transcriber()
        possible_data = amazon_transcribe(transcriber, aud.local_filename)
        print("finished. the lambda will take it from here.")


