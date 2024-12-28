#!/usr/bin/python3

import os
import json
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

def fetch_recording(recording:dict, ssh_host:str) -> str:
   ssh_exit_status = subprocess.call(["scp", f"{ssh_host}:{recording['recording_file']}", "recording.ts"])
   if ssh_exit_status == 0:
       return "recording.ts"
   return None

def run_transcode(transcode_settings:dict, input_recording_fname=str, dest_folder='/nfs') -> None:
   assert isinstance(transcode_settings, dict)
   crop_settings = []
   print(transcode_settings)
   if 'crop_settings' in transcode_settings.keys() and transcode_settings['crop_settings'] is not None:
       crop_settings = ["--crop", ':'.join([str(i) for i in transcode_settings['crop_settings']])]
   interlace_settings = []
   if 'interlace_settings' in transcode_settings.keys() and transcode_settings['interlace_settings'] is not None:
       assert isinstance(transcode_settings['interlace_settings'], list)
       interlace_settings = transcode_settings['interlace_settings']
   output_settings = []
   if 'output_settings' in transcode_settings.keys() and transcode_settings['output_settings'] is not None:
       assert isinstance(transcode_settings['output_settings'], list)
       output_settings = ['--output-res', ':'.join([str(i) for i in transcode_settings['output_settings']])]
   # HEVC output with de-interlacing and cropping is not supported currently, so we ensure interlacing is dropped if this is the case
   if any(crop_settings) and interlace_settings is not None and len(interlace_settings) > 0:
       interlace_settings = []

   # now do the run..
   final_args = ["rkmppenc", "-c", "hevc", "--preset", "best", "--audio-codec", "aac", "--vbr", "700"] + ["-i", input_recording_fname, "-o", f"{dest_folder}/{transcode_settings['preferred_output_filename']}"] + crop_settings + interlace_settings + output_settings
   print(final_args)
   exit_status = subprocess.call(final_args)
   print(f"{final_args} finished with exit status {exit_status}")
    
if __name__ == "__main__":
   client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="run-rkmppenc", clean_session=False)
   client.on_message = on_message
   client.on_disconnect = on_disconnect
   client.tls_set(ca_certs="ca.crt", certfile="t6.lan.crt", keyfile="t6.lan.key")
   client.connect("opi2.lan", port=8883)
   client.loop_start()
   client.subscribe("rkmppenc")
   print("Subscribed to rkmppenc work topic... now waiting for transcode messages to arrive (indefinately)...")
   while not done:
      # slow process things in main thread one-at-a-time ie. user interaction since not permitted in callback thread
      try:
         while True:
             r = work_queue.get(block=True, timeout=10)
             print(f"Transcoding recording {r}")
             input_recording_fname = fetch_recording(r, 'opi2.lan')
             if input_recording_fname:
                run_transcode(r, input_recording_fname)
             else:
                print(f"Unable to fetch recording {r}... ignoring but continuing to process others")
      except Empty:
         # not done, just nothing reported for now, keep going
         pass
   client.loop_stop()
   exit(0)
