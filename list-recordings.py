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

done = False
work_queue = Queue()

def run_work(e:dict) -> None: 
   uuid = e['uuid']
   assert len(uuid) > 16
   print(f"{uuid} {e['title']} {e['filename']}")

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
      work_queue.put(e)
      done_recordings = done_recordings + 1
   print(f"Processed {done_recordings} recordings which were submitted to rkmppenc")

def on_disconnect(client, userdata, rc=0):
   done = True  # trigger main thread to go away eventually once work is done

if __name__ == "__main__":
   a = argparse.ArgumentParser(description='Read finished recordings from MQTT and submit work for rkmppenc to run')
   a.add_argument('--mqtt-broker', help='MQTT Broker hostname to use [opi2.lan] ', type=str, default='opi2.lan')
   a.add_argument('--mqtt-port', help='TCP port to use on broker [8883] ', type=int, default=8883)
   a.add_argument('--topic-finished', help='Input recordings from tvheadend are posted to this topic [tvheadend/finished] ', type=str, default='tvheadend/finished')
   a.add_argument('--topic-transcode', help='Transcode jobs are submitted to this topic [rkmppenc] ', type=str, default='rkmppenc')
   a.add_argument('--cafile', help='Certificate Authority certificate [ca.crt] ', type=str, default='ca.crt')
   a.add_argument('--cert', help='Host certificate to provide to MQTT Broker [hplappie.lan.crt] ', type=str, default='hplappie.lan.crt')
   a.add_argument('--key', help='Host private key [hplappie.lan.key] ', type=str, default='hplappie.lan.key')
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
             run_work(r)
      except Empty:
         # not done, just nothing reported for now, keep going
         pass
   client.loop_stop()
   exit(0)
