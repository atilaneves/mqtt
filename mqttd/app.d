import vibe.d;
import mqttd.server;
import mqttd.tcp;

import std.stdio;


private __gshared MqttServer gServer;

shared static this() {
    setLogLevel(LogLevel.none);
    //setLogLevel(LogLevel.debugV);
    gServer = new MqttServer();
    listenTCP_s(1883, &accept);
}


void accept(TCPConnection tcpConnection) {
    if (!tcpConnection.waitForData(10.seconds())) {
        stderr.writeln("Client didn't send the initial request in a timely manner. Closing connection.");
    }

    auto mqttConnection = new MqttTcpConnection(gServer, tcpConnection);
    mqttConnection.run();
    if(tcpConnection.connected) tcpConnection.close();
}
