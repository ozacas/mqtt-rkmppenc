#!/usr/bin/python3
# usage:
#   sudo pip3 install paho-mqtt 
#   sudo apt install sqllite3
#   python3 video-server-job-publisher.py after tweaking settings below...
import os
import sys
import json
import re
import paho.mqtt.client as mqtt
from paho.mqtt.enums import MQTTErrorCode
import traceback
import subprocess
from time import sleep
from queue import Queue, Empty # note: must be thread safe
import sqlite3

done = False
work_queue = Queue()
processed_jobs_db = sqlite3.connect("tvheadend-recordings.db", check_same_thread=False)

def ok_recording(d:dict) -> bool:
   try:
      assert isinstance(d, dict) 
      assert 'uuid' in d.keys()
      assert 'status' in d.keys()
      assert 'filename' in d.keys()
      assert 'title' in d.keys() and 'eng' in d['title']
      assert 'channelname' in d.keys() and len(d['channelname']) > 0
      assert len(d['filename']) > 0 and d['filename'].startswith('/data')
      assert 'complete' in d['status'].lower()
      return True
   except AssertionError as ae:
      traceback.printStackTrace(ae)
      print(f"Rejecting recording as does not satisfy required metadata for operations: {d}")
      return False

def fetch_recording(recording:dict) -> str:
   exit_status = subprocess.call(["scp", f"acas@opi2.lan:{recording['filename']}", "/tmp/recording.ts"])
   assert exit_status == 0
   return "/tmp/recording.ts" 

def deduce_crop_settings(recording:dict) -> tuple:
   # if the recording is ER we know ABC will have no black borders, so no crop needed
   if recording['title']['eng'].startswith('ER'): 
       return (0, 0, 0, 0)
   # else we block and run handbrake locally and wait for the user to enter the data manually
   exit_status = subprocess.call(["flatpak", "run", "--filesystem=/tmp", "fr.handbrake.ghb", "--device=/tmp/recording.ts"])
   print(f"Handbrake run: {exit_status}")  
   if exit_status > 0:
      return None 
   left_crop = int(input('Left crop? (0 means no crop, -1 to shortcut zero crop) '))
   if left_crop < 0:
      return None # omit crop settings in this case during rkmppenc run
   top_crop = int(input('Top crop? (0 means no crop) '))
   if top_crop < 0:
      return (0, 0, 0, 0)
   right_crop = int(input('Right crop? (0 means no crop) '))
   bottom_crop = int(input('Bottom crop? (0 means no crop) '))
   return (left_crop, top_crop, right_crop, bottom_crop)

def deduce_interlace_settings(recording:dict) -> tuple:
   channel_name = recording['channelname'].lower() 
   if '9gem' in channel_name or '9go' in channel_name:
       return ("--vpp-yadif", "--interlace", "tff") 
   else:
       return None

def deduce_output_res(recording:dict) -> tuple:
   # always native resolution, except ER which due to ABC we need to specify a 16:9 ratio for correct output ratio
   if recording['title']['eng'].startswith('ER'):
       return (1280, 720)
   return None

def send_message(client, topic:str, payload: dict) -> None:
   json_str = json.dumps(payload, sort_keys=True)
   print(f"Sending to {topic} message {json_str}")
   ret = client.publish(topic, json_str)
   assert ret[0] == MQTTErrorCode.MQTT_ERR_SUCCESS

def deduce_output_filename(recording:dict)-> str:
   fname = os.path.basename(recording['filename'])
   if fname.endswith('.ts'): # strip well known extensions?
      fname = os.path.splitext(fname)[0]

   # eg. 'episode_disp': 'Season 2.Episode 13'
   if 'episode_disp' in recording.keys():
      series_episode = re.match(r'^Season\s+(\d+)[\s\.-_]+Episode\s+(\d+)\s*$', recording['episode_disp'])
   else:
      series_episode = None
   if not series_episode:
      copyright_year = recording['copyright_year']
      if copyright_year >= 1900:
          initial_out_fname = f"{fname}.{copyright_year}.mkv"  # add year to help kodi find correct metadata for most film titles
      else:
          initial_out_fname = f"{fname}.mkv"
   else:
      eng_title = recording['title']['eng']
      season = series_episode.group(1)
      episode_idx = series_episode.group(2)
      initial_out_fname = f"{eng_title}.s{season}e{episode_idx}.mkv"
   # strip special chars (if any) and replace with underscore
   out_fname = re.sub(r'[^A-Za-z0-9_.,-]', '_', initial_out_fname).lower()
   print(f"Proposed output filename for transcoded recording is {out_fname}")
   return out_fname
 
def run_work(e:dict) -> None: 
   uuid = e['uuid']
   assert len(uuid) > 16
   print(f"Downloading {e['title']} (uuid {uuid}) to local computer... please wait")
   local_file    = fetch_recording(e)
   print(f"Determining crop settings for {e['title']}")
   crop_settings = deduce_crop_settings(e)
   print(f"Crop settings are {crop_settings}")
   print(f"Determining interlace settings for {e['title']}")
   interlace_settings = deduce_interlace_settings(e) 
   print(f"Interlace settings are {interlace_settings}")
   print(f"Determining output resolution settings for {e['title']}")
   output_res    = deduce_output_res(e) 
   print(f"Output resolution explicitly set to {output_res}")
   # recording is ok so we request it be transcoded with the specified settings (dont care who does it)
   send_message(client, "rkmppenc", { "recording_file": e['filename'], 
                                      "crop_settings": crop_settings, 
                                      "interlace_settings": interlace_settings, 
                                      "output_res": output_res,
                                      "preferred_output_filename": deduce_output_filename(e) })
   os.unlink(local_file)
   # if we get here dont process this uuid again since it has been marked as to be transcoded
   processed_jobs_db.execute(f'insert into uuid_recordings values ("{uuid}");')
   print(f"Finished processing {e['title']} (uuid {uuid})")


def done_before(uuid:str) -> bool:
   results = processed_jobs_db.execute(f'select count(*) from uuid_recordings where uuid = "{uuid}";')
   t = results.fetchone()
   assert isinstance(t, tuple)
   if t[0] > 0:
      print(f"Skipping recording with UUID {uuid} since it has already been processed.")
   return (t[0] > 0)

def on_message(client, userdata, message):
   json_str = message.payload.decode('utf-8') 
   assert isinstance(json_str, str)
   d = json.loads(json_str) 
   print(d)
   assert isinstance(d, dict)
   assert 'entries' in d.keys()
   l = d['entries']
   print(f'Finished recording message: found {len(l)} completed recordings')
   done_recordings = 0
   for idx, e in enumerate(l):
      if ok_recording(e) and not done_before(e['uuid']):
         work_queue.put(e)
         done_recordings = done_recordings + 1
   print(f"Processed {done_recordings} recordings which were submitted to rkmppenc")

def on_disconnect(client, userdata,rc=0):
   done = True  # trigger main thread to go away eventually once work is done

if __name__ == "__main__":
   client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
   client.on_message = on_message
   client.on_disconnect = on_disconnect
   client.tls_set(ca_certs="ca.crt", certfile="hplappie.lan.crt", keyfile="hplappie.lan.key")
   client.connect("opi2.lan", port=8883)
   client.loop_start()
   client.subscribe("tvheadend/finished")
   print("Subscribed to tvheadend finished recordings topic... now waiting for recordings...")
   while not done:
      # no hurry here its a user-driven interactive workload
      sleep(10)
      # slow process things in main thread one-at-a-time ie. user interaction since not permitted in callback thread
      try:
         while True:
             r = work_queue.get(block=True, timeout=10) 
             print(f"Analysing recording {r}")
             run_work(r)
      except Empty:
         # not done, just nothing reported for now, keep going
         pass
   client.loop_stop()
   exit(0)
