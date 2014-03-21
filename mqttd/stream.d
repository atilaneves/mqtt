module mqttd.stream;

import mqttd.server;
import mqttd.message;
import mqttd.factory;
import cerealed.decerealiser;
import std.stdio;
import std.conv;
import std.algorithm;
import std.exception;


struct MqttStream {
    this(ulong bufferSize) {
        _cereal = new Decerealiser;
        allocate(bufferSize);
    }

    void opOpAssign(string op: "~")(ubyte[] bytes) {
        checkRealloc(bytes.length);
        immutable end = _bytesRead + bytes.length;

        auto buf = _buffer[_bytesRead .. end];
        copy(bytes, buf);

        _bytes = _buffer[_bytesStart .. end];
        _bytesRead += bytes.length;
        updateRemaining();
    }

    auto read(MqttServer server, MqttConnection connection, ulong size) {
        checkRealloc(size);
        immutable end = _bytesRead + size;

        connection.read(_buffer[_bytesRead .. end]);

        _bytes = _buffer[_bytesStart .. end];
        _bytesRead += size;
        updateRemaining();

        while(hasMessages()) {
            createMessage().handle(server, connection);
        }
    }

    auto read(in ubyte[] bytes) {
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
        _bytesStart += slice.length;
        _bytes = _buffer[_bytesStart .. _bytesRead];

        auto msg = MqttFactory.create(slice);

        _remaining = 0; //reset
        updateRemaining();

        return msg;
    }


private:

    ubyte[] _buffer;
    ubyte[] _bytes;
    int _remaining;
    ulong _bytesRead;
    ulong _bytesStart;
    Decerealiser _cereal;

    void allocate(ulong bufferSize = 128) {
        enforce(bufferSize > 10, "bufferSize too small");
        _buffer = new ubyte[bufferSize];
    }

    void checkRealloc(ulong numBytes) {
        if(!_buffer) {
            allocate();
        }

        if(_bytesRead + numBytes > _buffer.length) {
            copy(_bytes, _buffer);
            _bytesStart = 0;
            _bytesRead = _bytes.length;
            _bytes = _buffer[0.._bytesRead];
        }
    }

    void updateRemaining() {
        if(!_remaining && _bytes.length >= MqttFixedHeader.SIZE) {
            _cereal.reset(slice());
            _remaining = _cereal.value!MqttFixedHeader.remaining;
        }
    }

    const(ubyte[]) slice() const {
        immutable msgSize = _remaining + MqttFixedHeader.SIZE;
        return  _bytes[0..msgSize];
    }
}
