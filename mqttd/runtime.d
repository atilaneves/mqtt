module mqttd.runtime;


shared static this() nothrow {
    try
        mainLoop;
    catch(Exception e) {
        import std.experimental.logger: error;
        try
            error("static ctor ERROR: ", e.msg);
        catch(Exception _)
            assert(0, "Could not long error");
    }
}

void mainLoop() {

    import mqttd.log: error;
    import mqttd.server: MqttServer;
    import mqttd.tcp: MqttTcpConnection;
    import vibe.d: listenTCP, TCPConnection;
    import std.stdio: writeln;
    import std.typecons: Yes, No;
    import std.datetime: seconds;
    import core.runtime: Runtime;

    const useCache = Runtime.args.length > 1 ? Yes.useCache : No.useCache;
    if(useCache) writeln("Enabling the cache");

    // debug {
    //     setLogLevel(LogLevel.debugV);
    // }

    auto server = MqttServer!(MqttTcpConnection)(useCache);

    listenTCP(
        1883,
        (TCPConnection tcpConnection) {
            try {
                if (!tcpConnection.waitForData(10.seconds())) {
                    error("Client didn't send the initial request in a timely manner. Closing connection.");
                }

                auto mqttConnection = MqttTcpConnection(tcpConnection);
                mqttConnection.run(server);
                if(tcpConnection.connected) tcpConnection.close();
            } catch(Exception e)
                error("Fatal error: ", e.msg);

        }
    );
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
