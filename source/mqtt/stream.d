module mqtt.stream;

import mqtt.message;
import mqtt.factory;

struct MqttStream {
    this(ubyte[] bytes) {
        _bytes = bytes;
        _header = MqttFixedHeader(bytes);
    }

    void opOpAssign(string op: "~")(ubyte[] bytes) {
        _bytes ~= bytes;
    }

    bool isDone() {
        //+2 for the fixed header itself
        return _bytes.length >= _header.remaining + 2;
    }

    MqttMessage createMessage() {
        if(!isDone()) return null;
        return MqttFactory.create(_bytes);
    }

private:

    ubyte[] _bytes;
    MqttFixedHeader _header;
}
