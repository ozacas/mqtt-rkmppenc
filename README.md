# mqtt-rkmppenc

Building on rkmppenc and rockchip SBC's - this gross hack builds a basic publish-subscribe system using scp, rkmppenc, RockChip 35xx SBC's and a video source. On the video server, a python script is run to submit transcoding jobs to an MQTT v5 broker. A corresponding client
script reads the messages and manages the jobs using MPP-based hardware acceleration present on the RockChip SBC. Basic, but it gets the job done.

YMMV.
