"""
This class handles a single broadcast url

Input Args:
    1: url to the broadcast
    2: Name
    3: Output path directory

Steps:
    1: create output dir, if exists then clear it
    2: set a timer
    3: set a daemon thread and open a wget process, passing in the url, pipe 
"""
from aws_utils import upload_to_aws

import threading
import requests
from requests.auth import HTTPBasicAuth 
import os
from shutil import rmtree
import time

from secrets import BROADCASTIFY_USERNAME, BROADCASTIFY_PW

class Audio:
    def __init__(self, url, event):
        self.url = url
        self.event = event
    
    def set_event(self):
        self.event.set()

    def download_file(self):
        self.local_filename = self.url.split('/')[-1].split('.')[0] + "-" + str(time.time()) + ".mp3"
        # NOTE the stream=True parameter below
        with requests.get(self.url, stream=True, auth=HTTPBasicAuth(BROADCASTIFY_USERNAME, BROADCASTIFY_PW)) as r:
            #TODO: how to handle request failures here?
            r.raise_for_status()
            with open(self.local_filename, 'wb') as f:
                for chunk in r.iter_content(chunk_size=4096): # used to be 8192, so let's see if reducing it fixes long silence at end
                    f.write(chunk)
                    if self.event.is_set():
                        return

target_url = "https://audio.broadcastify.com/32936.mp3"
aud = Audio(target_url, threading.Event())
t = threading.Thread(target=aud.download_file,)
t.daemon = True
t.start()

# wait 30 seconds for the thread to finish its work
t.join(30)
aud.set_event()
print("uploading file to aws bucket")
#upload_to_aws(aud.local_filename, "dasnes-mpcs53014", aud.local_filename)
#rmtree(aud.my_dir)
#os.remove(aud.local_filename)
#now kick off transcriber job

#TODO: once the transcriber finishes, a lambda sends the transcription URI to kafka consumer
