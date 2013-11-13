module tests.server;

import unit_threaded.check;
import mqttd.server;
import mqttd.message;
import mqttd.factory;
import std.stdio;


class TestMqttConnection: MqttConnection {
    this(in ubyte[] bytes) {
        super(bytes);
        connected = true;
    }

    override void write(in ubyte[] bytes) {
        writeln("TestMqttConnection got a message from the server");
        lastMsg = MqttFactory.create(bytes);
    }

    override void newMessage(in string topic, in ubyte[] payload) {
        payloads ~= cast(string)payload;
    }

    override void disconnect() { connected = false; }

    MqttMessage lastMsg;
    string[] payloads;
    bool connected;
}

void testConnect() {
    auto server = new MqttServer();
    ubyte[] bytes = [ 0x10, 0x2a, //fixed header
                      0x00, 0x06, 'M', 'Q', 'I', 's', 'd', 'p', //protocol name
                      0x03, //protocol version
                      0xcc, //connection flags 1100111x username, pw, !wr, w(01), w, !c, x
                      0x00, 0x0a, //keepalive of 10
                      0x00, 0x03, 'c', 'i', 'd', //client ID
                      0x00, 0x04, 'w', 'i', 'l', 'l', //will topic
                      0x00, 0x04, 'w', 'm', 's', 'g', //will msg
                      0x00, 0x07, 'g', 'l', 'i', 'f', 't', 'e', 'l', //username
                      0x00, 0x02, 'p', 'w', //password
        ];

    auto connection = new TestMqttConnection(bytes);
    server.newConnection(connection);
    const connack = cast(MqttConnack)connection.lastMsg;
    checkNotNull(connack);
    checkEqual(connack.code, MqttConnack.Code.ACCEPTED);
}


void testConnectBigId() {
   auto server = new MqttServer();
    ubyte[] bytes = [ 0x10, 0x3f, //fixed header
                      0x00, 0x06, 'M', 'Q', 'I', 's', 'd', 'p', //protocol name
                      0x03, //protocol version
                      0xcc, //connection flags 1100111x username, pw, !wr, w(01), w, !c, x
                      0x00, 0x0a, //keepalive of 10
                      0x00, 0x18, 'c', 'i', 'd', 'd', 'd', 'd', 'd', 'd', 'd', 'd', 'd', 'd',
                                  'd', 'd', 'd', 'd', 'd', 'd', 'd', 'd', 'd', 'd', 'd', 'd', //24 char client id
                      0x00, 0x04, 'w', 'i', 'l', 'l', //will topic
                      0x00, 0x04, 'w', 'm', 's', 'g', //will msg
                      0x00, 0x07, 'g', 'l', 'i', 'f', 't', 'e', 'l', //username
                      0x00, 0x02, 'p', 'w', //password
        ];

    auto connection = new TestMqttConnection(bytes);
    server.newConnection(connection);
    const connack = cast(MqttConnack)connection.lastMsg;
    checkNotNull(connack);
    checkEqual(connack.code, MqttConnack.Code.BAD_ID);
}

void testConnectSmallId() {
   auto server = new MqttServer();
    ubyte[] bytes = [ 0x10, 0x27, //fixed header
                      0x00, 0x06, 'M', 'Q', 'I', 's', 'd', 'p', //protocol name
                      0x03, //protocol version
                      0xcc, //connection flags 1100111x username, pw, !wr, w(01), w, !c, x
                      0x00, 0x0a, //keepalive of 10
                      0x00, 0x00, //no client id
                      0x00, 0x04, 'w', 'i', 'l', 'l', //will topic
                      0x00, 0x04, 'w', 'm', 's', 'g', //will msg
                      0x00, 0x07, 'g', 'l', 'i', 'f', 't', 'e', 'l', //username
                      0x00, 0x02, 'p', 'w', //password
        ];

    auto connection = new TestMqttConnection(bytes);
    server.newConnection(connection);
    const connack = cast(MqttConnack)connection.lastMsg;
    checkNotNull(connack);
    checkEqual(connack.code, MqttConnack.Code.BAD_ID);
}

void testSubscribe() {
    auto server = new MqttServer();
    ubyte[] bytes = [ 0x10, 0x2a, //fixed header
                      0x00, 0x06, 'M', 'Q', 'I', 's', 'd', 'p', //protocol name
                      0x03, //protocol version
                      0xcc, //connection flags 1100111x username, pw, !wr, w(01), w, !c, x
                      0x00, 0x0a, //keepalive of 10
                      0x00, 0x03, 'c', 'i', 'd', //client ID
                      0x00, 0x04, 'w', 'i', 'l', 'l', //will topic
                      0x00, 0x04, 'w', 'm', 's', 'g', //will msg
                      0x00, 0x07, 'g', 'l', 'i', 'f', 't', 'e', 'l', //username
                      0x00, 0x02, 'p', 'w', //password
        ];

    auto connection = new TestMqttConnection(bytes);
    server.newConnection(connection);

    server.publish("foo/bar/baz", "interesting stuff");
    checkEqual(connection.payloads, []);

    server.subscribe(connection, 42, ["foo/bar/+"]);
    const suback = cast(MqttSuback)connection.lastMsg;
    checkNotNull(suback);
    checkEqual(suback.msgId, 42);
    checkEqual(suback.qos, [0]);

    server.publish("foo/bar/baz", "interesting stuff");
    server.publish("foo/boogagoo", "oh noes!!!");
    checkEqual(connection.payloads, ["interesting stuff"]);
}


void testSubscribeWithMessage() {
    auto server = new MqttServer();
    ubyte[] bytes = [ 0x10, 0x2a, //fixed header
                      0x00, 0x06, 'M', 'Q', 'I', 's', 'd', 'p', //protocol name
                      0x03, //protocol version
                      0xcc, //connection flags 1100111x username, pw, !wr, w(01), w, !c, x
                      0x00, 0x0a, //keepalive of 10
                      0x00, 0x03, 'c', 'i', 'd', //client ID
                      0x00, 0x04, 'w', 'i', 'l', 'l', //will topic
                      0x00, 0x04, 'w', 'm', 's', 'g', //will msg
                      0x00, 0x07, 'g', 'l', 'i', 'f', 't', 'e', 'l', //username
                      0x00, 0x02, 'p', 'w', //password
        ];

    auto connection = new TestMqttConnection(bytes);
    server.newConnection(connection);

    server.publish("foo/bar/baz", "interesting stuff");
    checkEqual(connection.payloads, []);

    bytes = [ 0x8c, 0x13, //fixed header
              0x00, 0x21, //message ID
              0x00, 0x05, 'f', 'i', 'r', 's', 't',
              0x01, //qos
              0x00, 0x06, 's', 'e', 'c', 'o', 'n', 'd',
              0x02, //qos
        ];

    MqttFactory.create(bytes).handle(server, connection); //subscribe
    const suback = cast(MqttSuback)connection.lastMsg;
    checkNotNull(suback);
    checkEqual(suback.msgId, 33);
    checkEqual(suback.qos, [1, 2]);

    bytes = [ 0x3c, 0x0d, //fixed header
              0x00, 0x05, 'f', 'i', 'r', 's', 't',//topic name
              0x00, 0x21, //message ID
              'b', 'o', 'r', 'g', //payload
        ];
    MqttFactory.create(bytes).handle(server, connection); //publish

    bytes = [ 0x3c, 0x0d, //fixed header
              0x00, 0x06, 's', 'e', 'c', 'o', 'n', 'd',//topic name
              0x00, 0x21, //message ID
              'f', 'o', 'o',//payload
        ];
    MqttFactory.create(bytes).handle(server, connection); //publish

    bytes = [ 0x3c, 0x0c, //fixed header
              0x00, 0x05, 't', 'h', 'i', 'r', 'd',//topic name
              0x00, 0x21, //message ID
              'f', 'o', 'o',//payload
        ];
    MqttFactory.create(bytes).handle(server, connection); //publish


    checkEqual(connection.payloads, ["borg", "foo"]);
}

void testStressPure() {
    import std.conv;
    import std.parallelism;

    auto server = new MqttServer();
    ubyte[] bytes = [ 0x10, 0x2a, //fixed header
                      0x00, 0x06, 'M', 'Q', 'I', 's', 'd', 'p', //protocol name
                      0x03, //protocol version
                      0xcc, //connection flags 1100111x username, pw, !wr, w(01), w, !c, x
                      0x00, 0x0a, //keepalive of 10
                      0x00, 0x03, 'c', 'i', 'd', //client ID
                      0x00, 0x04, 'w', 'i', 'l', 'l', //will topic
                      0x00, 0x04, 'w', 'm', 's', 'g', //will msg
                      0x00, 0x07, 'g', 'l', 'i', 'f', 't', 'e', 'l', //username
                      0x00, 0x02, 'p', 'w', //password
        ];

    string[] clients;
    for(int i = 0; i < 100; ++i) clients ~= "client" ~ to!string(i);

    foreach(client; parallel(clients)) {
        auto connection = new TestMqttConnection(bytes);
        server.newConnection(connection);

        server.publish("foo/bar/baz", "interesting stuff");
        checkEqual(connection.payloads, []);

        server.subscribe(connection, 42, [client ~ "/+"]);
        const suback = cast(MqttSuback)connection.lastMsg;
        checkNotNull(suback);
        checkEqual(suback.msgId, 42);
        checkEqual(suback.qos, [0]);

        server.publish(client ~ "/foo", "interesting stuff");
        server.publish(client ~ "/foo/bar", "oh noes!!!");
        checkEqual(connection.payloads, ["interesting stuff"]);
    }
}

void testPing() {
    auto server = new MqttServer();
    ubyte[] bytes = [ 0x10, 0x2a, //fixed header
                      0x00, 0x06, 'M', 'Q', 'I', 's', 'd', 'p', //protocol name
                      0x03, //protocol version
                      0xcc, //connection flags 1100111x username, pw, !wr, w(01), w, !c, x
                      0x00, 0x0a, //keepalive of 10
                      0x00, 0x03, 'c', 'i', 'd', //client ID
                      0x00, 0x04, 'w', 'i', 'l', 'l', //will topic
                      0x00, 0x04, 'w', 'm', 's', 'g', //will msg
                      0x00, 0x07, 'g', 'l', 'i', 'f', 't', 'e', 'l', //username
                      0x00, 0x02, 'p', 'w', //password
        ];

    auto connection = new TestMqttConnection(bytes);
    server.newConnection(connection);

    server.ping(connection);
    const pingResp = cast(MqttPingResp)connection.lastMsg;
    checkNotNull(pingResp);
}


void testPingWithMessage() {
    auto server = new MqttServer();
    ubyte[] bytes = [ 0x10, 0x2a, //fixed header
                      0x00, 0x06, 'M', 'Q', 'I', 's', 'd', 'p', //protocol name
                      0x03, //protocol version
                      0xcc, //connection flags 1100111x username, pw, !wr, w(01), w, !c, x
                      0x00, 0x0a, //keepalive of 10
                      0x00, 0x03, 'c', 'i', 'd', //client ID
                      0x00, 0x04, 'w', 'i', 'l', 'l', //will topic
                      0x00, 0x04, 'w', 'm', 's', 'g', //will msg
                      0x00, 0x07, 'g', 'l', 'i', 'f', 't', 'e', 'l', //username
                      0x00, 0x02, 'p', 'w', //password
        ];

    auto connection = new TestMqttConnection(bytes);
    server.newConnection(connection);

    const msg = MqttFactory.create([0xc0, 0x00]); //ping request
    msg.handle(server, connection);
    const pingResp = cast(MqttPingResp)connection.lastMsg;
    checkNotNull(pingResp);
}
