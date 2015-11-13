import vibe.d;
import mqttd.server;
import mqttd.tcp;
import std.stdio;

private __gshared MqttServer!(CMqttTcpConnection) gServer;

shared static this() {
    // debug {
    //     setLogLevel(LogLevel.debugV);
    // }
    gServer = new typeof(gServer);
    gServer.useCache = true;
    listenTCP_s(1883, &accept);
}


void accept(TCPConnection tcpConnection) {
    if (!tcpConnection.waitForData(10.seconds())) {
        stderr.writeln("Client didn't send the initial request in a timely manner. Closing connection.");
    }

    auto mqttConnection = new CMqttTcpConnection(gServer, tcpConnection);
    mqttConnection.run();
    if(tcpConnection.connected) tcpConnection.close();
}
