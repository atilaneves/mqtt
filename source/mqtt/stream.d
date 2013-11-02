module mqtt.stream;

import mqtt.message;
import mqtt.factory;
import std.stdio;
import std.conv;

struct MqttStream {
    void opOpAssign(string op: "~")(ubyte[] bytes) {
        _bytes ~= bytes;
        updateBytes();
    }

    bool hasMessages() const {
        return _bytes.length >= _remaining + MqttFixedHeader.SIZE;
    }

    bool empty() {
        return _bytes.length == 0;
    }

    MqttMessage createMessage() {
        if(!hasMessages()) return null;

        const slice = slice();
        auto msg = MqttFactory.create(slice);

        _remaining = 0; //reset
        if(slice.length < _bytes.length) {
            _bytes = _bytes[slice.length..$]; //next msg
        } else {
            _bytes = []; //no more msgs
        }

        return msg;
    }

private:

    const(ubyte)[] _bytes;
    int _remaining;

    void updateBytes() {
        if(!_remaining && _bytes.length >= MqttFixedHeader.SIZE) {
            _remaining = MqttFixedHeader(slice()).remaining;
        }
    }

    const (ubyte[]) slice() const {
        immutable msgSize = _remaining + MqttFixedHeader.SIZE;
        return _bytes[0..msgSize];
    }
}
