import vibe.d;

shared static this() {
    listenTCP(1883, (conn){ conn.write(conn); });
}
