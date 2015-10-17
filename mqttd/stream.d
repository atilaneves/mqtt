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
        struct Input {
            void read(ubyte[] buf) {
                copy(bytes, buf);
            }
            static assert(isMqttInput!Input);
        }
        read(new Input, bytes.length);
    }

    void read(T)(T input, unsigned size) if(isMqttInput!T) {
        checkRealloc(size);
        immutable end = _bytesRead + size;

        input.read(_buffer[_bytesRead .. end]);

        _bytes = _buffer[_bytesStart .. end];
        _bytesRead += size;
        updateRemaining();
    }

    void handleMessages(T)(MqttServer!T server, T connection) if(isMqttConnection!T) {
        while(hasMessages()) handleMessage(server, connection);
    }

    bool hasMessages() @safe const pure nothrow {
        return _bytes.length >= _remaining + MqttFixedHeader.SIZE;
    }

    bool empty() const {
        return _bytes.length == 0;
    }

    void handleMessage(T)(MqttServer!T server, T connection) if(isMqttConnection!T) {
        if(!hasMessages()) return;

        const slice = nextMessageBytes();
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

    const(ubyte[]) nextMessageBytes() const {
        immutable msgSize = _remaining + MqttFixedHeader.SIZE;
        return _bytes[0 .. msgSize];
    }

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
            auto cereal = Decerealiser(nextMessageBytes());
            _remaining = cereal.value!MqttFixedHeader.remaining;
        }
    }
}


@safe:

struct MqttStream2 {

    this(int bufferSize) pure nothrow {
        _buffer = new ubyte[bufferSize];
        _bytes = _buffer[0..0];
    }

    void opOpAssign(string op: "~")(ubyte[] bytes) {
        struct Input {
            void read(ubyte[] buf) {
                copy(bytes, buf);
            }
            static assert(isMqttInput!Input);
        }
        read(new Input, bytes.length);
    }

    void read(T)(auto ref T input, unsigned size) if(isMqttInput!T) {
        immutable end = _bytesRead + size;
        input.read(_buffer[_bytesRead .. end]);

        _bytesRead += size;
        _bytes = _buffer[0 .. _bytesRead];

        updateLastMessageSize;
    }

    bool hasMessages() const pure nothrow {
        return _bytes.length >= _lastMessageSize;
    }

    const(ubyte)[][] allMessageBytes() pure nothrow {
        const(ubyte)[][] result;
        auto origBytes = _bytes;
        while(hasMessages) {
            result ~= nextMessageBytes;
            _bytes = _bytes[result[$-1].length .. $];
        }
        _bytes = origBytes;
        return result;
    }

//private:

    ubyte[] _buffer; //the underlying storage
    ubyte[] _bytes; //the current bytes held
    int _lastMessageSize;
    int _bytesStart; //the starting position
    int _bytesRead; //what it says

    void updateLastMessageSize() {
        auto next = nextMessageBytes;
        if(!_lastMessageSize && nextMessageBytes.length >= MqttFixedHeader.SIZE) {
            auto dec = Decerealiser(next);
            _lastMessageSize = dec.value!MqttFixedHeader.remaining + MqttFixedHeader.SIZE;
        }
    }

    const(ubyte)[] nextMessageBytes() const pure nothrow {
        return _bytes;
    }
}
