mqtt
=============
[![Build Status](https://travis-ci.org/atilaneves/mqtt.png?branch=master)](https://travis-ci.org/atilaneves/mqtt)

MQTT broker written in D, using [vibe.d](https://github.com/rejectedsoftware/vibe.d).

Doesn't yet implement all of MQTT. There is no authenticatin nor QOS levels other than 0.
It can be used for testing however and does correctly subscribe, unsubscribe and
dispatches messages.

The unit tests in the [tests directory](tests) need
[unit-threaded](https://github.com/atilaneves/unit-threaded) to run.

Depends on [vibe.d](https://github.com/rejectedsoftware/vibe.d) and on
[cerealed](https://github.com/atilaneves/cerealed).
The easiest way to build is by using
[dub](https://github.com/rejectedsoftware/dub). Simply typing dub will build and run.

Running the executable makes the server listen on port 1883.