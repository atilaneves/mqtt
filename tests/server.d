import unit_threaded.check;
import mqtt.server;
import mqtt.message;
import mqtt.factory;


class TestMqttConnection: MqttConnection {
    this(in ubyte[] bytes) {
        super(bytes);
    }


    override void write(in ubyte[] bytes) {
        lastMsg = MqttFactory.create(bytes);
    }

    void newMessage(in string topic, in string payload) {
        payloads ~= payload;
    }

    MqttMessage lastMsg;
    string[] payloads;
}

void testConnect() {
    auto server = new MqttServer();
    ubyte[] bytes = [ 0x10, 0x29, //fixed header
                      0x00, 0x06, 'M', 'Q', 'I', 's', 'd', 'p', //protocol name
                      0x03, //protocol version
                      0xcc, //connection flags 1100111x username, pw, !wr, w(01), w, !c, x
                      0x00, 0x0a, //keepalive of 10
                      0x00, 0x03, 'c', 'i', 'd', //client ID
                      0x00, 0x04, 'w', 'i', 'l', 'l', //will topic
                      0x00, 0x04, 'w', 'm', 's', 'g', //will msg
                      0x00, 0x07, 'g', 'l', 'i', 'f', 't', 'e', 'l', //username
                      0x00, 0x01, 'p', 'w', //password
        ];

    auto connection = new TestMqttConnection(bytes);
    server.newConnection(connection);
    const connack = cast(MqttConnack)connection.lastMsg;
    checkNotNull(connack);
    checkEqual(connack.code, MqttConnack.Code.ACCEPTED);
}


void testConnectBigId() {
   auto server = new MqttServer();
    ubyte[] bytes = [ 0x10, 0x29, //fixed header
                      0x00, 0x06, 'M', 'Q', 'I', 's', 'd', 'p', //protocol name
                      0x03, //protocol version
                      0xcc, //connection flags 1100111x username, pw, !wr, w(01), w, !c, x
                      0x00, 0x0a, //keepalive of 10
                      0x00, 0x18, 'c', 'i', 'd', 'd', 'd', 'd', 'd', 'd', 'd', 'd', 'd', 'd',
                                  'd', 'd', 'd', 'd', 'd', 'd', 'd', 'd', 'd', 'd', 'd', 'd', //24 char client id
                      0x00, 0x04, 'w', 'i', 'l', 'l', //will topic
                      0x00, 0x04, 'w', 'm', 's', 'g', //will msg
                      0x00, 0x07, 'g', 'l', 'i', 'f', 't', 'e', 'l', //username
                      0x00, 0x01, 'p', 'w', //password
        ];

    auto connection = new TestMqttConnection(bytes);
    server.newConnection(connection);
    const connack = cast(MqttConnack)connection.lastMsg;
    checkNotNull(connack);
    checkEqual(connack.code, MqttConnack.Code.BAD_ID);
}

void testConnectSmallId() {
   auto server = new MqttServer();
    ubyte[] bytes = [ 0x10, 0x29, //fixed header
                      0x00, 0x06, 'M', 'Q', 'I', 's', 'd', 'p', //protocol name
                      0x03, //protocol version
                      0xcc, //connection flags 1100111x username, pw, !wr, w(01), w, !c, x
                      0x00, 0x0a, //keepalive of 10
                      0x00, 0x00, //no client id
                      0x00, 0x04, 'w', 'i', 'l', 'l', //will topic
                      0x00, 0x04, 'w', 'm', 's', 'g', //will msg
                      0x00, 0x07, 'g', 'l', 'i', 'f', 't', 'e', 'l', //username
                      0x00, 0x01, 'p', 'w', //password
        ];

    auto connection = new TestMqttConnection(bytes);
    server.newConnection(connection);
    const connack = cast(MqttConnack)connection.lastMsg;
    checkNotNull(connack);
    checkEqual(connack.code, MqttConnack.Code.BAD_ID);
}

void testSubscribe() {
    auto server = new MqttServer();
    ubyte[] bytes = [ 0x10, 0x29, //fixed header
                      0x00, 0x06, 'M', 'Q', 'I', 's', 'd', 'p', //protocol name
                      0x03, //protocol version
                      0xcc, //connection flags 1100111x username, pw, !wr, w(01), w, !c, x
                      0x00, 0x0a, //keepalive of 10
                      0x00, 0x03, 'c', 'i', 'd', //client ID
                      0x00, 0x04, 'w', 'i', 'l', 'l', //will topic
                      0x00, 0x04, 'w', 'm', 's', 'g', //will msg
                      0x00, 0x07, 'g', 'l', 'i', 'f', 't', 'e', 'l', //username
                      0x00, 0x01, 'p', 'w', //password
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
