#!/usr/bin/python3

import os
import json
import argparse
from time import sleep
import paho.mqtt.client as mqtt
from queue import Queue, Empty # note: must be thread safe
import subprocess

done = False
work_queue = Queue()

def on_message(client, userdata, message):
   work_queue.put(json.loads(message.payload))

def on_disconnect(client, userdata,rc=0):
   done = True

def fetch_recording(recording:dict, ssh_user:str, ssh_host:str, folder_prefix:str) -> str:
   assert 'recording_file' in recording.keys()
   rec_basename = os.path.basename(recording['recording_file'])
   ssh_args = ["scp", f"{ssh_user}@{ssh_host}:{folder_prefix}/{rec_basename}", "recording.ts"]
   print(f"Fetching recording using: {ssh_args}")
   ssh_exit_status = subprocess.call(ssh_args)
   if ssh_exit_status == 0:
       return "recording.ts"
   return None

def run_transcode(transcode_settings:dict, input_recording_fname=str, dest_folder='/nfs') -> None:
   assert isinstance(transcode_settings, dict)
   crop_settings = []
   print(transcode_settings)
   ts_keys = transcode_settings.keys()
   if 'crop_settings' in ts_keys and transcode_settings['crop_settings'] is not None:
       crop_settings = ["--crop", ':'.join([str(i) for i in transcode_settings['crop_settings']])]
   interlace_settings = []
   if 'interlace_settings' in ts_keys and transcode_settings['interlace_settings'] is not None:
       assert isinstance(transcode_settings['interlace_settings'], list)
       interlace_settings = transcode_settings['interlace_settings']
   output_settings = []
   if 'output_res' in ts_keys and transcode_settings['output_res'] is not None:
       res = transcode_settings['output_res']
       assert isinstance(res, list)
       assert len(res) == 2
       output_settings = ['--output-res', ':'.join([str(i) for i in res])]
   # HEVC output with de-interlacing and cropping is not supported currently, so we ensure interlacing is dropped if this is the case
   if any(crop_settings) and interlace_settings is not None and len(interlace_settings) > 0:
       interlace_settings = []

   # now do the run..
   final_args = ["rkmppenc", "-c", "hevc", "--preset", "best", "--audio-codec", "aac", "--vbr", "700"] + ["-i", input_recording_fname, "-o", f"{dest_folder}/{transcode_settings['preferred_output_filename']}"] + crop_settings + interlace_settings + output_settings
   print(final_args)
   exit_status = subprocess.call(final_args)
   print(f"{final_args} finished with exit status {exit_status}")
    
if __name__ == "__main__":
   a = argparse.ArgumentParser(description="Run transcoding jobs via rkmppenc from MQTT topic hosted on a broker")
   a.add_argument("--mqtt-broker", help="Hostname of MQTT broker [opi2.lan] ", type=str, default="opi2.lan")
   a.add_argument('--mqtt-port', help="Port of MQTT broker to user [8883] ", type=int, default=8883)
   a.add_argument('--mqtt-topic', help="Topic to read jobs from [rkmppenc] ", type=str, default="rkmppenc")
   a.add_argument("--cafile", help="Certificate Authority Certificate filename [ca.crt] ", type=str, default="ca.crt")
   a.add_argument("--cert", help="Host certificate filename [host.crt] ", type=str, default="host.crt")
   a.add_argument("--key", help="Host private key filename [host.key] ", type=str, default="host.key")
   args = a.parse_args()
   client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="run-rkmppenc", clean_session=False)
   client.on_message = on_message
   client.on_disconnect = on_disconnect
   client.tls_set(ca_certs=args.cafile, certfile=args.cert, keyfile=args.key)
   client.connect(args.mqtt_broker, port=args.mqtt_port)
   client.loop_start()
   client.subscribe(args.mqtt_topic)
   print("Subscribed to rkmppenc work topic... now waiting for transcode messages to arrive (indefinately)...")
   while not done:
      # slow process things in main thread one-at-a-time ie. user interaction since not permitted in callback thread
      try:
         while True:
             r = work_queue.get(block=True, timeout=10)
             print(f"Transcoding recording {r}")
             input_recording_fname = fetch_recording(r, r.get('ssh_user', 'hts'), r.get('ssh_host', 'opi2.lan'), r.get('ssh_folder_prefix', 'recordings'))
             if input_recording_fname:
                run_transcode(r, input_recording_fname)
             else:
                print(f"Unable to fetch recording {r}... ignoring but continuing to process others")
      except Empty:
         # not done, just nothing reported for now, keep going
         pass
   client.loop_stop()
   exit(0)
