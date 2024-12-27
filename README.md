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
