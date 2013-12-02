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
        _stream = MqttStream();
    }

    override void write(in ubyte[] bytes) {
        if(connected) {
            runTask({
                _tcpConnection.write(bytes);
            });
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

    auto readConnect() {
        auto bytes = new ubyte[_tcpConnection.leastSize];
    }

    auto read() {
        while(connected && !_tcpConnection.empty) {
            auto bytes = new ubyte[_tcpConnection.leastSize];
            _tcpConnection.read(bytes);
            _stream ~= bytes;
            while(_stream.hasMessages()) {
                _stream.createMessage().handle(_server, this);
            }
        }
    }
}
