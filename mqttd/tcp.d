module mqttd.tcp;

import mqttd.server;
import mqttd.factory;
import mqttd.message;
import mqttd.stream;
import mqttd.message;
import vibe.d;
import std.stdio;


class CMqttTcpConnection {
    mixin MqttConnection;

    this(CMqttServer!(typeof(this)) server, TCPConnection tcpConnection) {
        _server = server;
        _tcpConnection = tcpConnection;
        _connected = true;
        enum bufferSize = 1024 * 16;
        _stream = MqttStream(bufferSize);
    }

    final void read(ubyte[] bytes) {
        _tcpConnection.read(bytes);
    }

    final void write(in ubyte[] bytes) {
        if(connected) {
            _tcpConnection.write(bytes);
        }
    }

    final void run() {
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

    final @property bool connected() const {
        return _tcpConnection.connected && _connected;
    }

    final void disconnect() {
        _connected = false;
    }

private:

    CMqttServer!(typeof(this)) _server;
    TCPConnection _tcpConnection;
    bool _connected;
    MqttStream _stream;

    final void read() {
        while(connected && !_tcpConnection.empty) {
            _stream.read(this, _tcpConnection.leastSize);
            while(_stream.hasMessages)
                MqttFactory.handleMessage(_stream.popNextMessageBytes, _server, this);
        }
    }

    static assert(isMqttConnection!CMqttTcpConnection);
}
