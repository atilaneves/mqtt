module mqttd.stream;

import mqttd.server;
import mqttd.message;
import mqttd.broker;
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


enum isMqttInput(T) = is(typeof(() {
    ubyte[] bytes;
    auto t = T.init;
    t.read(bytes);
}));

@safe:


struct MqttStream {

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

    void read(T)(auto ref T input, unsigned size) @trusted if(isMqttInput!T) {
        resetBuffer;

        immutable end = _bytesRead + size;
        input.read(_buffer[_bytesRead .. end]);

        _bytesRead += size;
        _bytes = _buffer[0 .. _bytesRead];

        updateLastMessageSize;
    }


    bool hasMessages() pure nothrow {
        return _lastMessageSize >= MqttFixedHeader.SIZE && _bytes.length >= _lastMessageSize;
    }

    const(ubyte)[] popNextMessageBytes() {
        if(!hasMessages) return [];

        auto ret = nextMessageBytes;
        _bytes = _bytes[ret.length .. $];

        updateLastMessageSize;
        return ret;
    }

    void handleMessages(T)(ref MqttServer!T server, ref T connection) @trusted if(isMqttSubscriber!T) {
        while(hasMessages) server.newMessage(connection, popNextMessageBytes);
    }

    auto bufferSize() const pure nothrow @safe {
        return _buffer.length;
    }

private:

    ubyte[] _buffer; //the underlying storage
    ubyte[] _bytes; //the current bytes held
    int _lastMessageSize;
    int _bytesStart; //the starting position
    ulong _bytesRead; //what it says

    void updateLastMessageSize() {
        _lastMessageSize = nextMessageSize;
    }

    const(ubyte)[] nextMessageBytes() const {
        return _bytes[0 .. nextMessageSize];
    }

    int nextMessageSize() const {
        if(_bytes.length < MqttFixedHeader.SIZE) return 0;

        auto dec = Decerealiser(_bytes);
        return dec.value!MqttFixedHeader.remaining + MqttFixedHeader.SIZE;
    }

    //@trusted because of copy
    void resetBuffer() @trusted pure nothrow {
        copy(_bytes, _buffer);
        _bytesRead = _bytes.length;
        _bytes = _buffer[0 .. _bytesRead];
    }
}
