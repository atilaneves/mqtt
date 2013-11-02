import vibe.d;
import mqtt.server;
import mqtt.tcp;

import std.stdio;


private MqttServer gServer;

shared static this() {
    setLogLevel(LogLevel.debugV);
    gServer = new MqttServer();
    logDebug("About to listen");
    listenTCP_s(1883, &accept);
}


void accept(TCPConnection tcpConnection) {
    logDebug("New TCP connection");
    if (!tcpConnection.waitForData(10.seconds())) {
        logDebug("Client didn't send the initial request in a timely manner. Closing connection.");
    }

    auto mqttConnection = new MqttTcpConnection(gServer, tcpConnection);
    gServer.newConnection(mqttConnection);
    mqttConnection.run();
}
