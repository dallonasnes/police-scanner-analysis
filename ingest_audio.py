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

import threading
import requests
from requests.auth import HTTPBasicAuth 
import os
import time

class Audio:
    def __init__(self, url, event):
        self.url = url
        self.event = event
    
    def set_event(self):
        self.event.set()

    def download_file(self):
        my_dir = str(time.time())
        self.local_filename = my_dir + "/" + self.url.split('/')[-1]
        # NOTE the stream=True parameter below
        with requests.get(self.url, stream=True, auth=HTTPBasicAuth("sensad", "imUsingThisUniquePWH3R3")) as r:
            r.raise_for_status()
            if not os.path.exists(my_dir):
                os.makedirs(my_dir)
            with open(self.local_filename, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
                    if self.event.is_set():
                        print("just finished")
                        return

target_url = "https://audio.broadcastify.com/32936.mp3"
aud = Audio(target_url, threading.Event())
t = threading.Thread(target=aud.download_file,)
t.daemon = True
t.start()

# wait 10 seconds for the thread to finish its work
t.join(10)
aud.set_event()

#now upload the file to AWS bucket