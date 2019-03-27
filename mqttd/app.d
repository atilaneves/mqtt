import vibe.d;
import mqttd.server;
import mqttd.tcp;
import std.stdio;
import std.typecons;



shared static this() {

    import mqttd.log: error;
    import std.stdio: writeln;
    import std.typecons: Yes, No;
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



int main(string[] args) {
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
