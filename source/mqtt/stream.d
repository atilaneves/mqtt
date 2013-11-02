module mqtt.stream;

import mqtt.message;
import mqtt.factory;
import std.stdio;
import std.conv;

struct MqttStream {
    this(ubyte[] bytes) {
        _bytes = bytes;
        _header = MqttFixedHeader(bytes);
    }

    void opOpAssign(string op: "~")(ubyte[] bytes) {
        if(empty()) _header = MqttFixedHeader(bytes);
        _bytes ~= bytes;
    }

    bool isDone() {
        //+2 for the fixed header itself
        return _bytes.length >= _header.remaining + 2;
    }

    bool empty() {
        return _bytes.length == 0;
    }

    MqttMessage createMessage() {
        if(!isDone()) return null;
        auto msg = MqttFactory.create(_bytes);
        reset();
        return msg;
    }

private:

    const(ubyte)[] _bytes;
    MqttFixedHeader _header;

    void reset() {
        _bytes = [];
        _header = MqttFixedHeader();
    }
}
