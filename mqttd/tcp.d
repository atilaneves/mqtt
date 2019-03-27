module mqttd.tcp;


struct MqttTcpConnection {

    import mqttd.server: MqttServer, isMqttConnection;
    import mqttd.stream: MqttStream, isMqttInput;
    import vibe.d: TCPConnection;

    this(TCPConnection tcpConnection) @safe nothrow {
        _tcpConnection = tcpConnection;
        _connected = true;
        enum bufferSize = 1024 * 512;
        _stream = MqttStream(bufferSize);
    }

    void read(ubyte[] bytes) @safe {
        _tcpConnection.read(bytes);
    }

    void newMessage(in ubyte[] bytes) @safe {
        if(connected) {
            _tcpConnection.write(bytes);
        }
    }

    void run(ref MqttServer!MqttTcpConnection server) @safe {
        import mqttd.log: error;
        import std.datetime: seconds;

        while(connected) {
            if(!_tcpConnection.waitForData(60.seconds) ) {
                error("Persistent connection timeout!");
                _connected = false;
                break;
            }

            read(server);
        }

        _connected = false;
    }

    @property bool connected() @safe const {
        return _tcpConnection.connected && _connected;
    }

    void disconnect() @safe {
        _connected = false;
        _tcpConnection.close();
    }

private:

    TCPConnection _tcpConnection;
    bool _connected;
    MqttStream _stream;

    void read(ref MqttServer!MqttTcpConnection server) @safe {
        import std.conv: text;

        while(connected && !_tcpConnection.empty) {
            if(_tcpConnection.leastSize > _stream.bufferSize) {
                throw new Exception(
                    text("Too many bytes (", _tcpConnection.leastSize,
                         " for puny stream buffer (", _stream.bufferSize, ")"));
            }
            _stream.read(this, _tcpConnection.leastSize);
            _stream.handleMessages(server, this);
        }
    }

    static assert(isMqttConnection!MqttTcpConnection);
    static assert(isMqttInput!MqttTcpConnection);
}
