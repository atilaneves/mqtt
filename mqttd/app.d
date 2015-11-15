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


int main(string[] args) {
    if(args.length > 1) {
        writeln("Using the cache");
        gServer.useCache = Yes.useCache;
    }
    return vibemain();
}

int vibemain() {
    import vibe.core.core : runEventLoop, lowerPrivileges;
    import vibe.core.log;
    import std.encoding : sanitize;

    lowerPrivileges();

    logDiagnostic("Running event loop...");
    int status;
    version (VibeDebugCatchAll) {
        try {
            status = runEventLoop();
        } catch( Throwable th ){
            logError("Unhandled exception in event loop: %s", th.msg);
            logDiagnostic("Full exception: %s", th.toString().sanitize());
            return 1;
        }
    } else {
        status = runEventLoop();
    }

    logDiagnostic("Event loop exited with status %d.", status);
    return status;
}
