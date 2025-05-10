#!/usr/bin/python3
# usage:
#   sudo pip3 install paho-mqtt 
#   sudo apt install sqllite3
#   python3 video-server-job-publisher.py after tweaking settings below...
import os
import sys
import json
import re
import traceback
import subprocess
from time import sleep
import argparse
import paho.mqtt.client as mqtt
from paho.mqtt.enums import MQTTErrorCode
from queue import Queue, Empty # note: must be thread safe
import sqlite3

class SkipJob(Exception):
  def __init__(self, message):
      super().__init__(message)

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
      assert len(d['filename']) > 0 
      assert 'complete' in d['status'].lower()
      return True
   except AssertionError as ae:
      traceback.printStackTrace(ae)
      print(f"Rejecting recording as does not satisfy required metadata for operations: {d}")
      return False

def fetch_recording(recording:dict, ssh_user: str, ssh_host:str, folder_prefix:str) -> str:
   assert len(ssh_user) > 0
   assert len(ssh_host) > 0
   assert folder_prefix is not None
   # we DO NOT use the filename field as given, instead we use a relative fetch for ~hts/recordings usually
   base_filename = os.path.basename(recording['filename'])
   ssh_args = ["scp", f"{ssh_user}@{ssh_host}:{folder_prefix}/{base_filename}", "/tmp/recording.ts"]
   print(f"Fetching recording using: {ssh_args}")
   exit_status = subprocess.call(ssh_args)
   assert exit_status == 0
   return "/tmp/recording.ts" 

def get_int(prompt:str) -> int:
   ok = False
   while not ok:
       input_str = input(prompt or 'Input integer number: ')
       if input_str.lower().startswith('s'):
          raise SkipJob(f'User requested skipping of job')
       try:
          input_crop = int(input_str)
          return input_crop
       except ValueError:
          print(f'You must supply a valid integer - not {input_str}')
          pass # try again
   
def deduce_crop_settings(recording:dict) -> tuple:
   # if the recording is ER we know ABC will have no black borders, so no crop needed
   if recording['title']['eng'].startswith('ER'): 
       return None # no crop arguments given to rkmppenc
   # else we block and run handbrake locally and wait for the user to enter the data manually
   exit_status = subprocess.call(["flatpak", "run", "--filesystem=/tmp", "fr.handbrake.ghb", "--device=/tmp/recording.ts"])
   print(f"Handbrake run: {exit_status}")  
   if exit_status > 0:
      return None 
   left_crop = get_int("Left crop? (0 means no pixels cropped from left, -1 to omit crop altogether, 's' to skip job) ")
   if left_crop < 0:
      return None # omit crop settings in this case during rkmppenc run
   top_crop = get_int("Top crop? (0 means no crop, -1 to omit crop altogether, 's' to skip job) ")
   if top_crop < 0:
      return None
   right_crop = get_int('Right crop? (0 means no crop) ')
   bottom_crop = get_int('Bottom crop? (0 means no crop) ')
   return (left_crop, top_crop, right_crop, bottom_crop)

def deduce_interlace_settings(recording:dict, local_file:str) -> tuple:
   # per https://stackoverflow.com/questions/24945378/progressive-or-interlace-detection-in-ffmpeg
   ffprobe_results = subprocess.run(["ffprobe", "-v", "quiet", "-select_streams", "v", "-show_entries", "stream=codec_name,height,width,pix_fmt,field_order", "-of", "csv=p=0", local_file], capture_output=True)
   if ffprobe_results.returncode == 0:
      for line in ffprobe_results.stdout.decode('utf-8').split("\n"):
         trimmed_line = line.rstrip()
         if ",tb" in trimmed_line:
            return ("--vpp-yadif", "--interlace", "tff")
         elif ",bt" in trimmed_line:
            return ("--vpp-yadif", "--interlace", "bff")
      # DONT FALLTHRU... assume progressive to avoid performance hit in unknown case
      return None
   else:
      print(f"Failed to run ffprobe... using channel heuristic for deinterlace detection instead")
      # FALLTHRU
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
   out_fname = re.sub(r'[^A-Za-z0-9_.,-]', '-', initial_out_fname).lower()
   print(f"Proposed output filename for transcoded recording is {out_fname}")
   return out_fname
 
def run_work(e:dict, ssh_user:str, ssh_host:str, folder_prefix:str, topic_rkmppenc:str='rkmppenc', vbr:int=700) -> None: 
   uuid = e['uuid']
   assert len(uuid) > 16
   print(f"Downloading {e['title']} (uuid {uuid}) to local computer... please wait")
   local_file    = fetch_recording(e, ssh_user, ssh_host, folder_prefix)
   try:
      print(f"Determining crop settings for {e['title']}")
      crop_settings = deduce_crop_settings(e)
      print(f"Crop settings are {crop_settings}")
      print(f"Determining interlace settings for {e['title']}")
      interlace_settings = deduce_interlace_settings(e, local_file) 
      print(f"Interlace settings are {interlace_settings}")
      print(f"Determining output resolution settings for {e['title']}")
      output_res    = deduce_output_res(e) 
      print(f"Output resolution explicitly set to {output_res}")
      # recording is ok so we request it be transcoded with the specified settings (dont care who does it)
      send_message(client, topic_rkmppenc, { 
         # target filename
         "recording_file": e['filename'], 
         # rkmppenc settings as required
         "crop_settings": crop_settings, 
         "interlace_settings": interlace_settings, 
         "output_res": output_res,
         "preferred_output_filename": deduce_output_filename(e),
         # since we have multiple servers the worker needs to know how to fetch the recording - trusted?
         "ssh_user": ssh_user,
         "ssh_host": ssh_host,
         "ssh_folder_prefix": folder_prefix,
         "vbr": vbr
      })
   except SkipJob:
      pass
   finally:
      os.unlink(local_file)
      # if we get here dont process this uuid again since it has been marked as to be transcoded
      with processed_jobs_db as con:
         cursor = con.cursor()
         result = cursor.execute(f'INSERT INTO uuid_recordings VALUES (:uuid);', { "uuid": uuid })
         con.commit()
         print(result)
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

def on_disconnect(client, userdata, flags, rc, properties):
   done = True  # trigger main thread to go away eventually once work is done

if __name__ == "__main__":
   a = argparse.ArgumentParser(description='Read finished recordings from MQTT and submit work for rkmppenc to run')
   a.add_argument('--ssh-user', help='SSH User to fetch recordings from [hts]', type=str, default='hts')
   a.add_argument('--ssh-host', help='SSH hostname/IP to fetch recordings from [opi2.lan]', type=str, default='opi2.lan')
   a.add_argument('--ssh-folder-prefix', help='Subfolder to fetch recordings from [recordings] ', type=str, default='recordings')
   a.add_argument('--mqtt-broker', help='MQTT Broker hostname to use [opi2.lan] ', type=str, default='opi2.lan')
   a.add_argument('--mqtt-port', help='TCP port to use on broker [8883] ', type=int, default=8883)
   a.add_argument('--topic-finished', help='Input recordings from tvheadend are posted to this topic [tvheadend/finished] ', type=str, default='tvheadend/finished')
   a.add_argument('--topic-transcode', help='Transcode jobs are submitted to this topic [$share/video/mpp/#] ', type=str, default='$share/video/mpp/#')
   a.add_argument('--cafile', help='Certificate Authority certificate [ca.crt] ', type=str, default='ca.crt')
   a.add_argument('--cert', help='Host certificate to provide to MQTT Broker [hplappie.lan.crt] ', type=str, default='hplappie.lan.crt')
   a.add_argument('--key', help='Host private key [hplappie.lan.key] ', type=str, default='hplappie.lan.key')
   a.add_argument('--vbr', help='Variable bitrate (kb/s) to use [700] ', type=int, default=700) 
   args = a.parse_args()
   client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
   client.on_message = on_message
   client.on_disconnect = on_disconnect
   client.tls_set(ca_certs=args.cafile, certfile=args.cert, keyfile=args.key)
   client.connect(args.mqtt_broker, port=args.mqtt_port)
   client.loop_start()
   client.subscribe(args.topic_finished)
   print("Subscribed to tvheadend finished recordings topic... now waiting for recordings...")
   while not done:
      # no hurry here its a user-driven interactive workload
      sleep(10)
      # slow process things in main thread one-at-a-time ie. user interaction since not permitted in callback thread
      try:
         while True:
             r = work_queue.get(block=True, timeout=10) 
             print(f"Analysing recording {r}")
             run_work(r, ssh_user=args.ssh_user, ssh_host=args.ssh_host, folder_prefix=args.ssh_folder_prefix, topic_rkmppenc=args.topic_transcode, vbr=args.vbr)
      except Empty:
         # not done, just nothing reported for now, keep going
         pass
   client.loop_stop()
   exit(0)
