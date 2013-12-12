module mqttd.tcp;

import mqttd.server;
import mqttd.factory;
import mqttd.message;
import mqttd.stream;
import mqttd.message;
import vibe.d;
import std.stdio;


class MqttTcpConnection: MqttConnection {
    this(MqttServer server, TCPConnection tcpConnection) {
        _server = server;
        _tcpConnection = tcpConnection;
        _connected = true;
        enum bufferSize = 1024 * 16;
        _stream = MqttStream(bufferSize);
    }

    override void read(ubyte[] bytes) {
        _tcpConnection.read(bytes);
    }

    override void write(in ubyte[] bytes) {
        if(connected) {
            _tcpConnection.write(bytes);
        }
    }

    void run() {
        while(connected) {
            if(!_tcpConnection.waitForData(60.seconds) ) {
                stderr.writeln("Persistent connection timeout!");
                _connected = false;
                break;
            }

            read();
        }
        _connected = false;
    }

    @property bool connected() const {
        return _tcpConnection.connected && _connected;
    }

    override void disconnect() {
        _connected = false;
    }

private:

    MqttServer _server;
    TCPConnection _tcpConnection;
    bool _connected;
    MqttStream _stream;

    auto read() {
        while(connected && !_tcpConnection.empty) {
            _stream.read(_server, this, _tcpConnection.leastSize);
        }
    }
}
