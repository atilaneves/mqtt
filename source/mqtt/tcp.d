module mqtt.tcp;


import mqtt.server;
import mqtt.factory;
import mqtt.message;
import mqtt.stream;
import std.stdio;
import vibe.d;

class MqttTcpConnection: MqttConnection {
    this(MqttServer server, TCPConnection tcpConnection) {
        _server = server;
        _tcpConnection = tcpConnection;
        _connected = true;

        logDebug("MqttTcpConnection reading ", _tcpConnection.leastSize, " bytes");
        super(read());
    }

    override void write(in ubyte[] bytes) {
        _tcpConnection.write(bytes);
    }

    void run() {
        auto stream = MqttStream();
        do {
            if(!_tcpConnection.waitForData(60.seconds) ) {
                logDebug("persistent connection timeout!");
                break;
            }
            auto bytes = read();
            stream ~= bytes;
            if(stream.isDone()) {
                logDebug("Creating mqttdata");
                const mqttMsg = stream.createMessage();
                if(mqttMsg) mqttMsg.handle(_server, this);
            }
        } while(_tcpConnection.connected && _connected);
    }

    override void disconnect() {
        _connected = false;
    }

private:

    MqttServer _server;
    TCPConnection _tcpConnection;
    bool _connected;

    auto read() {
        auto bytes = new ubyte[_tcpConnection.leastSize];
        _tcpConnection.read(bytes);
        return bytes;
    }
}
