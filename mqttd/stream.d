module mqttd.stream;


enum isMqttInput(T) = is(typeof(() {
    ubyte[] bytes;
    auto t = T.init;
    t.read(bytes);
}));

@safe:


/**
   Abstracts a stream protocol such as TCP so that we can get discrete
   MQTT messages out of it.
 */
struct MqttStream {

    import mqttd.server: MqttServer;
    import mqttd.broker: isMqttSubscriber;

    this(int bufferSize) pure nothrow {
        _buffer = new ubyte[bufferSize];
        _bytes = _buffer[0..0];
    }

    void opOpAssign(string op: "~")(ubyte[] bytes) {
        struct Input {
            void read(ubyte[] buf) {
                import std.algorithm: copy;
                copy(bytes, buf);
            }
            static assert(isMqttInput!Input);
        }
        read(new Input, bytes.length);
    }

    void read(T)(auto ref T input, size_t size) @trusted if(isMqttInput!T) {
        resetBuffer;

        immutable end = _bytesRead + size;
        input.read(_buffer[_bytesRead .. end]);

        _bytesRead += size;
        _bytes = _buffer[0 .. _bytesRead];

        updateLastMessageSize;
    }


    bool hasMessages() pure nothrow {
        import mqttd.message: MqttFixedHeader;
        return
            _lastMessageSize >= MqttFixedHeader.SIZE
            && _bytes.length >= _lastMessageSize
            ;
    }

    const(ubyte)[] popNextMessageBytes() {
        if(!hasMessages) return [];

        auto ret = nextMessageBytes;
        _bytes = _bytes[ret.length .. $];

        updateLastMessageSize;
        return ret;
    }

    void handleMessages(T)(ref MqttServer!T server, ref T connection) @trusted if(isMqttSubscriber!T) {
        while(hasMessages) server.send(connection, popNextMessageBytes);
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
        import mqttd.message: MqttFixedHeader;
        import cerealed: Decerealiser;

        if(_bytes.length < MqttFixedHeader.SIZE) return 0;

        auto dec = Decerealiser(_bytes);
        return dec.value!MqttFixedHeader.remaining + MqttFixedHeader.SIZE;
    }

    //@trusted because of copy
    void resetBuffer() @trusted pure nothrow {
        import std.algorithm: copy;

        copy(_bytes, _buffer);
        _bytesRead = _bytes.length;
        _bytes = _buffer[0 .. _bytesRead];
    }
}


/**
   Satisfies the `isMqttConnection` interface for streams (e.g. TCP)
   Delegates actually reading or writing data to a templated channel.
 */
struct MqttStreamConnection(Channel) {

    import mqttd.server: MqttServer, isMqttConnection;
    import mqttd.stream: MqttStream, isMqttInput;

    this(Channel channel) {
        _channel = channel;
        _connected = true;
        enum bufferSize = 1024 * 512;
        _stream = MqttStream(bufferSize);
    }

    void read(ubyte[] bytes) {
        _channel.read(bytes);
    }

    void send(in ubyte[] bytes) {
        if(connected) {
            _channel.write(bytes);
        }
    }

    void run(ref MqttServer!(typeof(this)) server) {
        import mqttd.log: error;
        import std.datetime: seconds;

        while(connected) {
            if(!_channel.waitForData(60.seconds) ) {
                error("Persistent connection timeout!");
                _connected = false;
                break;
            }

            read(server);
        }

        _connected = false;
    }

    @property bool connected() const {
        return _channel.connected && _connected;
    }

    void disconnect() {
        _connected = false;
        _channel.close();
    }

private:

    Channel _channel;
    bool _connected;
    MqttStream _stream;

    void read(ref MqttServer!(typeof(this)) server) {
        import std.conv: text;

        while(connected && !_channel.empty) {
            if(_channel.leastSize > _stream.bufferSize) {
                throw new Exception(
                    text("Too many bytes (", _channel.leastSize,
                         " for puny stream buffer (", _stream.bufferSize, ")"));
            }
            _stream.read(this, _channel.leastSize);
            _stream.handleMessages(server, this);
        }
    }

    static assert(isMqttConnection!(typeof(this)));
    static assert(isMqttInput!(typeof(this)));
}
