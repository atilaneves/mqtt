module mqttd.tcp;

import mqttd.server;
import mqttd.message;
import mqttd.stream;
import mqttd.message;
import vibe.d;
import std.stdio;
import std.conv;

struct MqttTcpConnection {

    this(TCPConnection tcpConnection) {
        _tcpConnection = tcpConnection;
        _connected = true;
        enum bufferSize = 1024 * 512;
        _stream = MqttStream(bufferSize);
    }

    void read(ubyte[] bytes) {
        _tcpConnection.read(bytes);
    }

    void newMessage(in ubyte[] bytes) {
        if(connected) {
            _tcpConnection.write(bytes);
        }
    }

    void run(ref MqttServer!MqttTcpConnection server) {
        while(connected) {
            if(!_tcpConnection.waitForData(60.seconds) ) {
                stderr.writeln("Persistent connection timeout!");
                _connected = false;
                break;
            }

            read(server);
        }
        _connected = false;
    }

    @property bool connected() const {
        return _tcpConnection.connected && _connected;
    }

    void disconnect() {
        _connected = false;
    }

private:

    TCPConnection _tcpConnection;
    bool _connected;
    MqttStream _stream;

    static void foo() {
        ubyte[] bytes;
        MqttTcpConnection.init.read(bytes);
    }

    void read(ref MqttServer!MqttTcpConnection server) {
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
