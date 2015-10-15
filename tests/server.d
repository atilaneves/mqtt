module tests.server;

import unit_threaded;
import mqttd.server;
import mqttd.message;
import mqttd.factory;
import std.stdio;


class TestMqttConnection: MqttConnection {
    this(in ubyte[] bytes) {
        connect = cast(MqttConnect)MqttFactory.create(bytes);
    }

    this(MqttConnect connect) {
        connected = true;
        this.connect = connect;
    }

    override void write(in ubyte[] bytes) {
        lastMsg = MqttFactory.create(bytes);
        writelnUt("TestMqttConnection got a message from the server:\n", lastMsg, "\n");
    }

    override void newMessage(in string topic, in ubyte[] payload) {
        payloads ~= cast(string)payload;
    }

    override void disconnect() { connected = false; }

    MqttMessage lastMsg;
    string[] payloads;
    bool connected;
    MqttConnect connect;
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
    server.newConnection(connection, connection.connect);
    const connack = cast(MqttConnack)connection.lastMsg;
    shouldNotBeNull(connack);
    shouldEqual(connack.code, MqttConnack.Code.ACCEPTED);
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
    server.newConnection(connection, connection.connect);
    const connack = cast(MqttConnack)connection.lastMsg;
    shouldNotBeNull(connack);
    shouldEqual(connack.code, MqttConnack.Code.BAD_ID);
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
    server.newConnection(connection, connection.connect);
    const connack = cast(MqttConnack)connection.lastMsg;
    shouldNotBeNull(connack);
    shouldEqual(connack.code, MqttConnack.Code.BAD_ID);
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
    server.newConnection(connection, connection.connect);

    server.publish("foo/bar/baz", "interesting stuff");
    shouldEqual(connection.payloads, []);

    server.subscribe(connection, 42, ["foo/bar/+"]);
    const suback = cast(MqttSuback)connection.lastMsg;
    shouldNotBeNull(suback);
    shouldEqual(suback.msgId, 42);
    shouldEqual(suback.qos, [0]);

    server.publish("foo/bar/baz", "interesting stuff");
    server.publish("foo/boogagoo", "oh noes!!!");
    shouldEqual(connection.payloads, ["interesting stuff"]);

    server.unsubscribe(connection);
    server.publish("foo/bar/baz", "interesting stuff");
    server.publish("foo/boogagoo", "oh noes!!!");
    shouldEqual(connection.payloads, ["interesting stuff"]); //shouldn't have changed
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
    server.newConnection(connection, connection.connect);

    server.publish("foo/bar/baz", "interesting stuff");
    shouldEqual(connection.payloads, []);

    bytes = [ 0x8c, 0x13, //fixed header
              0x00, 0x21, //message ID
              0x00, 0x05, 'f', 'i', 'r', 's', 't',
              0x01, //qos
              0x00, 0x06, 's', 'e', 'c', 'o', 'n', 'd',
              0x02, //qos
        ];

    const msg = MqttFactory.create(bytes);
    shouldNotBeNull(msg);
    msg.handle(server, connection); //subscribe
    const suback = cast(MqttSuback)connection.lastMsg;
    shouldNotBeNull(suback);
    shouldEqual(suback.msgId, 33);
    shouldEqual(suback.qos, [1, 2]);

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


    shouldEqual(connection.payloads, ["borg", "foo"]);
}

void testUnsubscribe() {
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
    server.newConnection(connection, connection.connect);

    server.subscribe(connection, 42, ["foo/bar/+"]);
    const suback = cast(MqttSuback)connection.lastMsg;
    shouldNotBeNull(suback);

    server.publish("foo/bar/baz", "interesting stuff");
    server.publish("foo/boogagoo", "oh noes!!!");
    shouldEqual(connection.payloads, ["interesting stuff"]);

    server.unsubscribe(connection, 2, ["boo"]); //doesn't exist, so no effect
    const unsuback1 = cast(MqttUnsuback)connection.lastMsg;
    shouldNotBeNull(unsuback1);
    shouldEqual(unsuback1.msgId, 2);

    server.publish("foo/bar/baz", "interesting stuff");
    server.publish("foo/boogagoo", "oh noes!!!");
    shouldEqual(connection.payloads, ["interesting stuff", "interesting stuff"]);

    server.unsubscribe(connection, 3, ["foo/bar/+"]);
    const unsuback2 = cast(MqttUnsuback)connection.lastMsg;
    shouldNotBeNull(unsuback2);
    shouldEqual(unsuback2.msgId, 3);

    server.publish("foo/bar/baz", "interesting stuff");
    server.publish("foo/boogagoo", "oh noes!!!");
    shouldEqual(connection.payloads, ["interesting stuff", "interesting stuff"]); //shouldn't have changed
}


void testUnsubscribeHandle() {
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
    server.newConnection(connection, connection.connect);
    server.subscribe(connection, 42, ["foo/bar/+"]);

    server.publish("foo/bar/baz", "interesting stuff");
    server.publish("foo/boogagoo", "oh noes!!!");
    shouldEqual(connection.payloads, ["interesting stuff"]);

    bytes = [ 0xa2, 0x0d, //fixed header
              0x00, 0x21, //message ID
              0x00, 0x09, 'f', 'o', 'o', '/', 'b', 'a', 'r', '/', '+',
        ];

    MqttMessage msg = MqttFactory.create(bytes);
    shouldNotBeNull(msg);
    msg.handle(server, connection); //unsubscribe
    const unsuback = cast(MqttUnsuback)connection.lastMsg;
    shouldNotBeNull(unsuback);
    shouldEqual(unsuback.msgId, 33);

    server.publish("foo/bar/baz", "interesting stuff");
    server.publish("foo/boogagoo", "oh noes!!!");
    shouldEqual(connection.payloads, ["interesting stuff"]); //shouldn't have changed
}

void testSubscribeWildCard() {
    import std.conv;
    auto server = new MqttServer;
    TestMqttConnection[] reqs;
    TestMqttConnection[] reps;
    TestMqttConnection[] wlds;
    immutable numPairs = 2;
    immutable numWilds = 2;

    foreach(i; 0..numWilds) {
        wlds ~= new TestMqttConnection(new MqttConnect(MqttFixedHeader()));
        server.newConnection(wlds[$ - 1], wlds[$ - 1].connect);
        server.subscribe(wlds[$ - 1], cast(ushort)(i * 20 + 1), [text("pingtest/0/#")]);
    }

    foreach(i; 0..numPairs) {
        reqs ~= new TestMqttConnection(new MqttConnect(MqttFixedHeader()));
        server.newConnection(reqs[$ - 1], reqs[$ - 1].connect);
        server.subscribe(reqs[$ - 1], cast(ushort)(i * 2), [text("pingtest/", i, "/request")]);
    }

    foreach(i; 0..numPairs) {
        reps ~= new TestMqttConnection(new MqttConnect(MqttFixedHeader()));
        server.newConnection(reps[$ - 1], reps[$ - 1].connect);
        server.subscribe(reps[$ - 1], cast(ushort)(i * 2 + 1), [text("pingtest/", i, "/reply")]);
    }

    //reset all payloads from connack and suback
    foreach(c; reqs) c.payloads = [];
    foreach(c; reps) c.payloads = [];
    foreach(c; wlds) c.payloads = [];

    immutable numMessages = 2;
    foreach(i; 0..numPairs) {
        foreach(j; 0..numMessages) {
            server.publish(text("pingtest/", i, "/request"), "pingawing");
            server.publish(text("pingtest/", i, "/reply"), "pongpongpong");
        }
    }

    foreach(i; 0..numPairs) {
        shouldEqual(reqs[i].payloads.length, numMessages);
        foreach(p; reqs[i].payloads) shouldEqual(cast(string)p, "pingawing");
        shouldEqual(reps[i].payloads.length, numMessages);
        foreach(p; reps[i].payloads) shouldEqual(cast(string)p, "pongpongpong");
    }

    foreach(i, c; wlds) {
        shouldEqual(c.payloads.length, numMessages * 2);
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
    server.newConnection(connection, connection.connect);

    server.ping(connection);
    const pingResp = cast(MqttPingResp)connection.lastMsg;
    shouldNotBeNull(pingResp);
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
    server.newConnection(connection, connection.connect);

    const msg = MqttFactory.create([0xc0, 0x00]); //ping request
    msg.handle(server, connection);
    const pingResp = cast(MqttPingResp)connection.lastMsg;
    shouldNotBeNull(pingResp);
}

const (ubyte)[] connectionMsgBytes() pure nothrow {
    return [ 0x10, 0x2a, //fixed header
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
}

void testPingWithMessageNoCreate() {
    auto server = new MqttServer();
    auto connection = new TestMqttConnection(connectionMsgBytes);
    server.newConnection(connection, connection.connect);

    MqttFactory.handleMessage([0xc0, 0x00], server, connection); //ping request
    const pingResp = cast(MqttPingResp)connection.lastMsg;
    shouldNotBeNull(pingResp);
}
