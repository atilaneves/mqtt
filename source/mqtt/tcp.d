module mqtt.tcp;


import mqtt.server;
import mqtt.factory;
import mqtt.message;
import vibe.d;
import std.stdio;


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
        do {
            if(!_tcpConnection.waitForData(60.seconds) ) {
                logDebug("persistent connection timeout!");
                break;
            }
            auto bytes = read();
            logDebug("Creating mqttdata");
            const mqttData = MqttFactory.create(bytes);
            if(mqttData) mqttData.handle(_server, this);
        } while(_tcpConnection.connected && _connected);
    }

    void newMessage(in string topic, in string payload) {
        const publish = new MqttPublish(topic, payload);
        logDebug("TCP connection sending back to client");
        _tcpConnection.write(publish.encode());
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
