# mqtt-rkmppenc

Building on rkmppenc and rockchip SBC's - this gross hack builds a basic publish-subscribe system using scp, rkmppenc, RockChip 35xx SBC's and a video source. On the video server, a python script is run to submit transcoding jobs to an MQTT v5 broker. A corresponding client script reads the messages and manages the jobs using MPP-based hardware acceleration 
present on the RockChip SBC. Basic, but it gets the job done.
YMMV.

This code relies on numerous other open-source projects including: mqtt-tvheadend, Armbian with Rockchip vendor kernel, rkmppenc, sqllite3, linux and openssh to achieve its results. Without
their high-quality and invaluable work, it would not happen.

## Requirements

* an MQTT broker, I use mosquitto 2.0.18 but others may work too (TLS support with self-signed certs)

* SBC with vendor (rockchip) kernel installed, MPP and rkmppenc installed and working already

* some video source

* a workstation to download from the video source, determine cropping and other settings (via handbrake) and then submit the job to the work queue for the SBC to transcode

## Future Work

* implement a shared subscription to the MQTT broker (v5 or later only support this capability). This would enable multiple SBC's to transcode from the same job queue and
  parallel process jobs to the target destination


## Sample Run

In this scenario, we use a tvheadend server, local workstation to analyse the finished recordings (runs handbrake) and deduce crop settings and then submit
the job to a queue for the rockchip SBC to process via the external MQTT broker (unseen). All this must be setup:

* the local workstation must have a working `flatpak run fr.handbrake.ghb`
* the local workstation must have password free ssh access to download tvheadend recordings from the designated server (opi2.lan) 
* the local workstation must have TLS to secure access to the MQTT broker installed and the script in the same folder
* the rockchip SBC must have the vendor kernel (linux-rockchip), rkmppenc, python3, mali firmware and GPU enabled and also ssh access to the tvheadend server to fetch recordings
* the MQTT broker must permit access to prefined topics `tvheadend/#` and `rkmppenc` to each client
 

First, start the script which will analyse the video file(s) on your workstation from tvheadend -  (note how i've setup MQTT TLS to correspond to the script)

~~~~
my-workstation:~/mosquitto$ ls
ca.crt  grok-encode-settings.py  hplappie.lan.crt  hplappie.lan.csr  hplappie.lan.key  tvheadend-recordings.db
my-workstation:~/mosquitto$ python3 grok-encode-settings.py 
Subscribed to tvheadend finished recordings topic... now waiting for recordings...
~~~~

(the database is used to record previously recorded programs so they wont be processed again. It is a single table, `uuid_recordings`, with a single column `uuid` to hold the program ID)

Next, have tvheadend report the finished recordings using tvheadend-mqtt. For me, I use a docker container so can obtain the list of completed recordings via:

~~~~
my-workstation:~$ docker exec tvheadend-mqtt /app/bin/main publish finished
No plugins found.
2024-12-28 03:59:39 publish finished
~~~~

This will trigger the above python script to start downloading and processing videos from tvheadend using the server's API (with supplied credentials) and send job messages to the configured MQTT broker for the rockchip SBC to process:

Finally, we want our rockchip SBC to start listening to these messages and using the MPP driver and hardware GPU to transcode rapidly - 

~~~~
my-rockchip-sbc$ uname -a
Linux XXXX.lan 6.1.75-vendor-rk35xx #1 SMP Tue Nov 12 08:48:32 UTC 2024 aarch64 aarch64 aarch64 GNU/Linux
my-rockchip-sbc$ ls -l /dev/rga
crw-rw---- 1 root video 10, 120 Dec 28 13:30 /dev/rga
my-rockchip-sbc$ python3 run-rkmppenc.py 
Subscribed to rkmppenc work topic... now waiting for transcode messages to arrive (indefinately)...

~~~~

This will run `rkmppenc` with jobs submitted to the `rkmppenc` topic on the specified broker.
