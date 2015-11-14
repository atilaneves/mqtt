module tests.server;

import unit_threaded;
import mqttd.server;
import mqttd.message;
import mqttd.factory;
import mqttd.broker;
import std.stdio, std.conv, std.algorithm, std.array, std.range;
import cerealed;


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

struct TestMqttConnection {
    void newMessage(in ubyte[] payload) {
        writeln(&this, " New message: ", payload);
        auto dec = Decerealiser(payload);
        immutable fixedHeader = dec.value!MqttFixedHeader;
        dec.reset;
        switch(fixedHeader.type) with(MqttType) {
            case CONNACK:
                code = dec.value!MqttConnack.code;
                break;
            case PUBLISH:
                auto msg = dec.value!MqttPublish;
                payloads ~= msg.payload.dup;
                break;

            default:
                break;
        }

        lastBytes = payload.dup;
    }

    void disconnect() { connected = false; }

    T lastMsg(T)() {
        auto dec = Decerealiser(lastBytes);
        auto fixedHeader = dec.value!MqttFixedHeader;
        dec.reset;

        auto t = T(fixedHeader);
        dec.grain(t);
        return t;
    }
    alias Payload = ubyte[];

    const(ubyte)[] lastBytes;
    Payload[] payloads;
    bool connected = false;
    MqttConnect connect;
    MqttConnack.Code code = MqttConnack.Code.SERVER_UNAVAILABLE;

    static assert(isNewMqttSubscriber!TestMqttConnection);
}


void testConnect() {
    auto server = MqttServer!TestMqttConnection();
    auto connection = TestMqttConnection();
    server.newMessage(connection, connectionMsgBytes);
    connection.code.shouldEqual(MqttConnack.Code.ACCEPTED);
}

void testConnectBigId() {
   auto server = MqttServer!TestMqttConnection();
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

    auto connection = TestMqttConnection();
    server.newMessage(connection, bytes);
    connection.connect.isBadClientId.shouldBeTrue;
    connection.code.shouldEqual(MqttConnack.Code.BAD_ID);
}

void publish(S)(ref MqttServer!S server, ref S connection, in string topic, in ubyte[] payload) if(isNewMqttSubscriber!S) {
    MqttPublish(topic, payload).cerealise!(b => server.newMessage(connection, b));
}

void subscribe(S)(ref MqttServer!S server, ref S connection, in ushort msgId, in string[] topics) if(isNewMqttSubscriber!S) {
    MqttSubscribe(msgId, topics.map!(a => MqttSubscribe.Topic(a, 0)).array).cerealise!(b => server.newMessage(connection, b));
}

void unsubscribe(S)(ref MqttServer!S server, ref S connection, in ushort msgId, in string[] topics) if(isNewMqttSubscriber!S) {
    MqttUnsubscribe(msgId, topics).cerealise!(b => server.newMessage(connection, b));
}

void testConnectSmallId() {
   auto server = MqttServer!TestMqttConnection();
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

    auto connection = TestMqttConnection();
    server.newMessage(connection, bytes);
    connection.connect.isBadClientId.shouldBeTrue;
    connection.code.shouldEqual(MqttConnack.Code.BAD_ID);
}

void testSubscribeWithMessage() {
    auto server = MqttServer!TestMqttConnection();
    auto connection = TestMqttConnection();

    server.newMessage(connection, connectionMsgBytes);

    server.publish(connection, "foo/bar/baz", [1, 2, 3, 4, 5, 6]);
    shouldEqual(connection.payloads, []);

    ubyte[] bytes = [ 0x8b, 0x13, //fixed header
                      0x00, 0x21, //message ID
                      0x00, 0x05, 'f', 'i', 'r', 's', 't',
                      0x01, //qos
                      0x00, 0x06, 's', 'e', 'c', 'o', 'n', 'd',
                      0x02, //qos
        ];

    server.newMessage(connection, bytes);
    const suback = connection.lastMsg!MqttSuback;
    shouldEqual(suback.msgId, 0x21);
    shouldEqual(suback.qos, [1, 2]);

    bytes = [ 0x3c, 0x0d, //fixed header
              0x00, 0x05, 'f', 'i', 'r', 's', 't',//topic name
              0x00, 0x21, //message ID
              1, 2, 3, 4 //payload
        ];
    server.newMessage(connection, bytes);

    bytes = [ 0x3c, 0x0d, //fixed header
              0x00, 0x06, 's', 'e', 'c', 'o', 'n', 'd',//topic name
              0x00, 0x21, //message ID
              9, 8, 7//payload
        ];
    server.newMessage(connection, bytes); //publish

    bytes = [ 0x3c, 0x0c, //fixed header
              0x00, 0x05, 't', 'h', 'i', 'r', 'd',//topic name
              0x00, 0x21, //message ID
              2, 4, 6, //payload
        ];
    server.newMessage(connection, bytes); //publish


    shouldEqual(connection.payloads, [[1, 2, 3, 4], [9, 8, 7]]);
}


void testPingWithMessage() {
    auto server = MqttServer!TestMqttConnection();
    auto connection = TestMqttConnection();

    server.newMessage(connection, connectionMsgBytes);
    server.newMessage(connection, cast(ubyte[])[0xc0, 0x00]); //ping request
    const pingResp = connection.lastMsg!MqttPingResp; //shouldn't throw
}


void testUnsubscribe() {
    auto server = MqttServer!TestMqttConnection();
    auto connection = TestMqttConnection();

    server.newMessage(connection, connectionMsgBytes);

    server.subscribe(connection, 42, ["foo/bar/+"]);
    const suback = connection.lastMsg!MqttSuback;

    server.publish(connection, "foo/bar/baz", [1, 2, 3, 4]);
    server.publish(connection, "foo/boogagoo", [9, 8, 7]);
    shouldEqual(connection.payloads, [[1, 2, 3, 4]]);

    server.unsubscribe(connection, 2, ["boo"]); //doesn't exist, so no effect
    const unsuback1 = connection.lastMsg!MqttUnsuback;
    shouldEqual(unsuback1.msgId, 2);

    server.publish(connection, "foo/bar/baz", [1, 2, 3, 4]);
    server.publish(connection, "foo/boogagoo", [9, 8, 7]);
    shouldEqual(connection.payloads, [[1, 2, 3, 4], [1, 2, 3, 4]]);

    server.unsubscribe(connection, 3, ["foo/bar/+"]);
    const unsuback2 = connection.lastMsg!MqttUnsuback;
    shouldEqual(unsuback2.msgId, 3);

    server.publish(connection, "foo/bar/baz", [1, 2, 3, 4]);
    server.publish(connection, "foo/boogagoo", [9, 8, 7]);
    shouldEqual(connection.payloads, [[1, 2, 3, 4], [1, 2, 3, 4]]); //shouldn't have changed
}

void testUnsubscribeHandle() {
    auto server = MqttServer!TestMqttConnection();
    auto connection = TestMqttConnection();
    server.newMessage(connection, connectionMsgBytes);
    server.subscribe(connection, 42, ["foo/bar/+"]);

    server.publish(connection, "foo/bar/baz", [1, 2, 3, 4]);
    server.publish(connection, "foo/boogagoo", [9, 8, 7]);
    shouldEqual(connection.payloads, [[1, 2, 3, 4]]);

    ubyte[] bytes = [ 0xa2, 0x0d, //fixed header
                      0x00, 0x21, //message ID
                      0x00, 0x09, 'f', 'o', 'o', '/', 'b', 'a', 'r', '/', '+',
        ];

    server.newMessage(connection, bytes);
    const unsuback = connection.lastMsg!MqttUnsuback;
    shouldEqual(unsuback.msgId, 33);

    server.publish(connection, "foo/bar/baz", [1, 2, 3, 4]);
    server.publish(connection, "foo/boogagoo", [9, 8, 7]);
    shouldEqual(connection.payloads, [[1, 2, 3, 4]]); //shouldn't have changed
}


void testSubscribeWildCard() {
    import std.conv;
    auto server = MqttServer!TestMqttConnection();
    immutable numPairs = 2;
    immutable numWilds = 2;
    TestMqttConnection[numPairs] reqs;
    TestMqttConnection[numPairs] reps;
    TestMqttConnection[numWilds] wlds;

    foreach(i, ref wld; wlds)
        server.subscribe(wld, cast(ushort)(i * 20 + 1), ["pingtest/0/#"]);

    foreach(i, ref req; reqs)
        server.subscribe(req, cast(ushort)(i * 2), [text("pingtest/", i, "/request")]);

    foreach(i, ref rep; reps)
        server.subscribe(rep, cast(ushort)(i * 2 + 1), [text("pingtest/", i, "/reply")]);

    foreach(ref c; reqs) c.payloads = [];
    foreach(ref c; reps) c.payloads = [];
    foreach(ref c; wlds) c.payloads = [];

    immutable numMessages = 2;
    foreach(i; 0..numPairs) {
        foreach(j; 0..numMessages) {
            server.publish(reqs[0], text("pingtest/", i, "/request"), [0, 1, 2, 3]);
            server.publish(reqs[0], text("pingtest/", i, "/reply"), [9, 8, 7]);
        }
    }

    foreach(ref req; reqs) {
        writeln("checking payloads of ", &req);
        req.payloads.shouldEqual([0, 1, 2, 3].repeat.take(numMessages));
    }

    foreach(rep; reps)
        rep.payloads.shouldEqual([9, 8, 7].repeat.take(numMessages));

    foreach(w; wlds)
        shouldEqual(w.payloads.length, numMessages * 2);
}
