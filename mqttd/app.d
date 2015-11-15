import vibe.d;
import mqttd.server;
import mqttd.tcp;
import std.stdio;
import std.typecons;

private __gshared MqttServer!(MqttTcpConnection) gServer;

shared static this() {
    // debug {
    //     setLogLevel(LogLevel.debugV);
    // }
    gServer = typeof(gServer)(No.useCache);
    listenTCP_s(1883, &accept);
}


void accept(TCPConnection tcpConnection) {
    if (!tcpConnection.waitForData(10.seconds())) {
        stderr.writeln("Client didn't send the initial request in a timely manner. Closing connection.");
    }

    auto mqttConnection = MqttTcpConnection(tcpConnection);
    mqttConnection.run(gServer);
    if(tcpConnection.connected) tcpConnection.close();
}
