#!/usr/bin/python3

import os
import json
import argparse
from time import sleep
import paho.mqtt.client as mqtt
from paho.mqtt.enums import MQTTProtocolVersion, MQTTErrorCode
from queue import Queue, Empty # note: must be thread safe
import subprocess
import re

# list of resolutions to perform upscaling on along with corresponding rkmppenc options (if not provided by server-side)
UPSCALE_RES = {
  "720x576": ["--output-res", "1280:720,preserve_aspect_ratio=increase"],
  "720x424": ["--output-res", "1222:720,preserve_aspect_ratio=increase"]
}

done = False
work_queue = Queue()
share_topic = None

def on_message(client, userdata, message):
   if not message.topic.startswith("$SYS"):
      print(message.topic)
   try:
      if 'mpp' in message.topic:
         work_queue.put(json.loads(message.payload, strict=False))
   except json.decoder.JSONDecodeError:
      print(f"Encountered invalid JSON: {message} ... ignoring")

def on_connect(client, userdata, flags, reason_code, properties):
    print(f"Connected with result code {reason_code}")
    client.subscribe("$SYS/#")
    if share_topic:
       print(f"Subscribing to share topic: {share_topic}")
       t = client.subscribe(share_topic)
       assert t[0] == MQTTErrorCode.MQTT_ERR_SUCCESS
       if share_topic.startswith('$'): # shared subscriptions not supported by paho mqtt client
          non_shared_topic_filter = share_topic.split('/', maxsplit=1)[-1]
          print(f"Registering message callback filter for {share_topic} as {non_shared_topic_filter}")
          client.message_callback_add(non_shared_topic_filter, on_message)

def on_disconnect(client, userdata, flags, rc, props):
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

def compute_upscale_settings(local_file: str) -> list:
   ffprobe_results = subprocess.run(["ffprobe", "-v", "quiet", "-select_streams", "v", "-show_entries", "stream=codec_name,height,width,pix_fmt,field_order", "-of", "csv=p=0", local_file], capture_output=True)
   if ffprobe_results.returncode == 0:
      for line in ffprobe_results.stdout.decode('utf-8').split("\n"):
         m = re.match(r"^(\S+),(\d+),(\d+),",  line)
         if m:
            key = f"{m.group(2)}x{m.group(3)}"
            if key in UPSCALE_RES:
               print(f"Upscaling video output as requested for {key}")
               return UPSCALE_RES[key]
   return []
 
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
   upscale_settings = []
   if 'output_res' in ts_keys and transcode_settings['output_res'] is not None:
       res = transcode_settings['output_res']
       assert isinstance(res, list)
       assert len(res) == 2
       output_settings = ['--output-res', ':'.join([str(i) for i in res])]
   else:
       # upscale_settings must only be set if output_settings is empty
       upscale_settings.extend(compute_upscale_settings(input_recording_fname))
        
   # HEVC output with de-interlacing and cropping is not supported currently, so we ensure interlacing is dropped if this is the case
   if any(crop_settings) and interlace_settings is not None and len(interlace_settings) > 0:
       interlace_settings = []

   # now do the run..
   final_args = ["rkmppenc", "-c", "hevc", "--preset", "best", "--audio-codec", "aac", "--vbr", "700"] + ["-i", input_recording_fname, "-o", f"{dest_folder}/{transcode_settings['preferred_output_filename']}"] + crop_settings + interlace_settings + output_settings + upscale_settings
   print(final_args)
   exit_status = subprocess.call(final_args)
   print(f"{final_args} finished with exit status {exit_status}")
   os.unlink(input_recording_fname) 
    
if __name__ == "__main__":
   a = argparse.ArgumentParser(description="Run transcoding jobs via rkmppenc from MQTT topic hosted on a broker")
   a.add_argument("--mqtt-broker", help="Hostname of MQTT broker [opi2.lan] ", type=str, default="opi2.lan")
   a.add_argument('--mqtt-port', help="Port of MQTT broker to user [8883] ", type=int, default=8883)
   a.add_argument('--mqtt-topic', help="Topic to read jobs from [$share/rkmppenc/video/mpp] ", type=str, default='$share/rkmppenc/video/mpp')
   a.add_argument("--cafile", help="Certificate Authority Certificate filename [ca.crt] ", type=str, default="ca.crt")
   a.add_argument("--cert", help="Host certificate filename [host.crt] ", type=str, default="host.crt")
   a.add_argument("--key", help="Host private key filename [host.key] ", type=str, default="host.key")
   args = a.parse_args()
   share_topic = args.mqtt_topic
   client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=f"rkmppenc", protocol=mqtt.MQTTv5)
      # FALLTHRU
   client.on_message = on_message
   client.on_connect = on_connect
   client.on_disconnect = on_disconnect
   client.tls_set(ca_certs=args.cafile, certfile=args.cert, keyfile=args.key)
   client.connect(args.mqtt_broker, port=args.mqtt_port)
   client.loop_start()
   print(f"Subscribed to {args.mqtt_topic}... now waiting for transcode jobs (indefinately)...")
   while not done:
      # slow process things in main thread one-at-a-time ie. user interaction since not permitted in callback thread
      try:
         while True:
             r = work_queue.get(block=True, timeout=10)
             if not isinstance(r, dict):
                 print(f"ERROR: got recording {r} but not expected JSON type... skipping")
                 continue
             print(f"Transcoding recording {r}")
             input_recording_fname = fetch_recording(r, r.get('ssh_user', 'hts'), r.get('ssh_host', 'opi2.lan'), r.get('ssh_folder_prefix', 'recordings'))
             if input_recording_fname:
                run_transcode(r, input_recording_fname)
             else:
                print(f"Unable to fetch recording {r}... ignoring but continuing to process remaining recordings")
      except Empty:
         # not done, just nothing reported for now, keep going
         pass
   client.loop_stop()
   exit(0)
