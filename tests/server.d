import unit_threaded.check;
import mqtt.server;
import mqtt.message;


class TestMqttServer: MqttServer {
}

class TestMqttConnection: MqttConnection {
    this(in ubyte[] bytes) {
    }

    override void newMessage(MqttMessage msg) {
        lastMsg = msg;
    }
    MqttMessage lastMsg;
}

void testConnect() {
    auto server = new TestMqttServer();
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
