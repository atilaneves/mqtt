module mqttd.stream;

import mqttd.server;
import mqttd.message;
import mqttd.factory;
import cerealed.decerealiser;
import std.stdio;
import std.conv;


struct MqttStream {
    void opOpAssign(string op: "~")(ubyte[] bytes) {
        _bytes ~= bytes;
        updateRemaining();
    }

    bool hasMessages() const {
        return _bytes.length >= _remaining + MqttFixedHeader.SIZE;
    }

    bool empty() const {
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

        updateRemaining();

        return msg;
    }

    auto read(MqttServer server, MqttConnection connection, int size) {
        auto bytes = new ubyte[size];
        connection.read(bytes);
        this ~= bytes;
        while(hasMessages()) {
            createMessage().handle(server, connection);
        }
    }

private:

    const(ubyte)[] _bytes;
    int _remaining;

    void updateRemaining() {
        if(!_remaining && _bytes.length >= MqttFixedHeader.SIZE) {
            auto cereal = new Decerealiser(slice());
            _remaining = cereal.value!MqttFixedHeader.remaining;
        }
    }

    const (ubyte[]) slice() const {
        immutable msgSize = _remaining + MqttFixedHeader.SIZE;
        return _bytes[0..msgSize];
    }
}
