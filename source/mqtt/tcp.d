module mqtt.tcp;


import mqtt.server;
import mqtt.factory;
import vibe.d;
import std.stdio;


class MqttTcpConnection: MqttConnection {
    this(MqttServer server, TCPConnection tcpConnection) {
        _server = server;
        _tcpConnection = tcpConnection;

        writeln("MqttTcpConnection reading ", _tcpConnection.leastSize, " bytes");
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
            writeln("Creating mqttdata");
            const mqttData = MqttFactory.create(bytes);
            mqttData.handle(_server, this);
        } while(_tcpConnection.connected);
    }

    void newMessage(in string topic, in string payload) {

    }


private:

    MqttServer _server;
    TCPConnection _tcpConnection;

    auto read() {
        auto bytes = new ubyte[_tcpConnection.leastSize];
        _tcpConnection.read(bytes);
        return bytes;
    }
}
