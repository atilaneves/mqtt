import vibe.d;
import mqtt.server;
import mqtt.message;
import std.stdio;


class MqttTcpConnection: MqttConnection {
    this(TCPConnection tcpConnection) {
        writeln("new TCP connection");
        _tcpConnection = tcpConnection;
        writeln("Reading ", _tcpConnection.leastSize, " bytes");
        auto bytes = new ubyte[_tcpConnection.leastSize];
        _tcpConnection.read(bytes);
        super(bytes);
    }

    override void write(in ubyte[] bytes) {
        _tcpConnection.write(bytes);
    }

    void newMessage(in string topic, in string payload) {

    }


private:

    TCPConnection _tcpConnection;
}


shared static this() {
    auto server = new MqttServer();
    writeln("About to listen");
    listenTCP(1883, (conn){server.newConnection(new MqttTcpConnection(conn)); });
}
