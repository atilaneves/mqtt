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
        if(_connected) {
            _tcpConnection.write(bytes);
        }
    }

    void run() {
        auto rtask = runTask({
            while(connected) {
                if(!_tcpConnection.waitForData(60.seconds) ) {
                    stderr.writeln("Persistent connection timeout!");
                    _connected = false;
                    break;
                }

                read();
            }
            _connected = false;
        });

        rtask.join();
    }

    @property bool connected() const {
        return _connected && _tcpConnection.connected;
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
        while(!_tcpConnection.empty) {
            auto bytes = new ubyte[_tcpConnection.leastSize];
            _tcpConnection.read(bytes);
            _stream ~= bytes;
            while(_stream.hasMessages() && connected) {
                const msg = _stream.createMessage();
                if(msg) msg.handle(_server, this);
            }
        }
    }
}
