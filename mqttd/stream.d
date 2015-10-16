module mqttd.stream;

import mqttd.server;
import mqttd.message;
import mqttd.factory;
import cerealed.decerealiser;
import std.stdio;
import std.conv;
import std.algorithm;
import std.exception;

version(Win32) {
    alias unsigned = uint;
} else {
    alias unsigned = ulong;
}

struct MqttStream {
    this(unsigned bufferSize) {
        allocate(bufferSize);
    }

    void opOpAssign(string op: "~")(ubyte[] bytes) {
        class Input : MqttInput {
            override void read(ubyte[] buf) {
                copy(bytes, buf);
            }
        }
        read(new Input, bytes.length);
    }

    auto read(MqttInput input, unsigned size) {
        checkRealloc(size);
        immutable end = _bytesRead + size;

        input.read(_buffer[_bytesRead .. end]);

        _bytes = _buffer[_bytesStart .. end];
        _bytesRead += size;
        updateRemaining();
    }

    void handleMessages(T)(MqttServer!T server, MqttConnection connection) {
        while(hasMessages()) handleMessage(server, connection);
    }

    bool hasMessages() const {
        return _bytes.length >= _remaining + MqttFixedHeader.SIZE;
    }

    bool empty() const {
        return _bytes.length == 0;
    }

    void handleMessage(T)(MqttServer!T server, MqttConnection connection) {
        if(!hasMessages()) return;

        const slice = slice();
        _bytesStart += slice.length;
        _bytes = _buffer[_bytesStart .. _bytesRead];

        MqttFactory.handleMessage(slice, server, connection);

        _remaining = 0; //reset
        updateRemaining();
    }


private:

    ubyte[] _buffer;
    ubyte[] _bytes;
    int _remaining;
    unsigned _bytesRead;
    unsigned _bytesStart;

    void allocate(unsigned bufferSize = 128) {
        enforce(bufferSize > 10, "bufferSize too small");
        _buffer = new ubyte[bufferSize];
    }

    void checkRealloc(unsigned numBytes) {
        if(!_buffer) allocate();

        immutable limit = (9 * _buffer.length) / 10;
        if(_bytesRead + numBytes > limit) {
            resetBuffer;
        }
    }

    void resetBuffer() {
        copy(_bytes, _buffer);
        _bytesStart = 0;
        _bytesRead = _bytes.length;
        _bytes = _buffer[_bytesStart .. _bytesRead];
    }

    void updateRemaining() {
        if(!_remaining && _bytes.length >= MqttFixedHeader.SIZE) {
            auto cereal = Decerealiser(slice());
            _remaining = cereal.value!MqttFixedHeader.remaining;
        }
    }

    const(ubyte[]) slice() const {
        immutable msgSize = _remaining + MqttFixedHeader.SIZE;
        return  _bytes[0..msgSize];
    }
}
