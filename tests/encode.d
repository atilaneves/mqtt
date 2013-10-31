import unit_threaded.check;
import mqtt.message;
import mqtt.factory;
import mqtt.utf8;


void testEncodeFixedHeader() {
    const msg = MqttFixedHeader(MqttType.PUBLISH, true, 2, false, 5);
    checkEqual(msg.type, MqttType.PUBLISH);
    checkEqual(msg.dup, true);
    checkEqual(msg.qos, 2);
    checkEqual(msg.retain, false);
    checkEqual(msg.remaining, 5);
    checkEqual(msg.encode(), [0x3c, 0x5]);
}

void testDecodeFixedHeader() {
    const msg = MqttFixedHeader([0x3c, 0x5]);
    checkEqual(msg.type, MqttType.PUBLISH);
    checkEqual(msg.dup, true);
    checkEqual(msg.qos, 2);
    checkEqual(msg.retain, false);
    checkEqual(msg.remaining, 5);
}

void testEncodeString() {
    const str = MqttString("foobarbaz");
    checkEqual(str.encode(), [ 0x00, 0x09, 'f', 'o', 'o', 'b', 'a', 'r', 'b', 'a', 'z']);
}

void testDecodeString() {
    const ubyte[] bytes = [0x00, 0x09, 'f', 'o', 'o', 'b', 'a', 'r', 'b', 'a', 'z'];
    const str = MqttString(bytes);
    checkEqual(str.str, "foobarbaz");
}

void testEncodeBigRemaining() {
    {
        const msg = MqttFixedHeader(MqttType.SUBSCRIBE, false, 1, true, 261);
        checkEqual(msg.type, MqttType.SUBSCRIBE);
        checkEqual(msg.dup, false);
        checkEqual(msg.qos, 1);
        checkEqual(msg.retain, true);
        checkEqual(msg.remaining, 261);
        checkEqual(msg.encode(), [0x83, 0x85, 0x02]);
    }
    {
        const msg = MqttFixedHeader(MqttType.SUBSCRIBE, false, 1, true, 321);
        checkEqual(msg.encode(), [0x83, 0xc1, 0x02]);
    }
    {
        const msg = MqttFixedHeader(MqttType.SUBSCRIBE, false, 1, true, 2_097_157);
        checkEqual(msg.encode(), [0x83, 0x85, 0x80, 0x80, 0x01]);
    }
}

void testDecodeBigRemaining() {
    {
        const msg = MqttFixedHeader([0x12, 0xc1, 0x02]);
        checkEqual(msg.remaining, 321);
    }
    {
        const msg = MqttFixedHeader([0x12, 0x83, 0x02]);
        checkEqual(msg.remaining, 259);
    }
    {
        const msg = MqttFixedHeader([0x12, 0x85, 0x80, 0x80, 0x01]);
        checkEqual(msg.remaining, 2_097_157);
    }
}

void testConnectMsg() {
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
    const msg = MqttFactory.create(bytes);
    checkNotNull(msg);
    checkEqual(msg.fixedHeader.remaining, 41);

    const connect = cast(MqttConnect)msg;
    checkNotNull(connect);

    checkEqual(connect.protoName, "MQIsdp");
    checkEqual(connect.protoVersion, 3);
    checkEqual(connect.keepAlive, 10);
    checkTrue(connect.hasUserName);
    checkTrue(connect.hasPassword);
    checkFalse(connect.hasWillRetain);
    checkEqual(connect.willQos, 1);
    checkTrue(connect.hasWill);
    checkFalse(connect.hasClear);
}

void testConnackMsg() {
    const connack = new MqttConnack(MqttConnack.Code.SERVER_UNAVAILABLE);
    checkEqual(connack.encode(),
               [0x20, 0x2, 0x0, 0x3]);
}

void testDecodePublishWithMsgId() {

    ubyte[] bytes = [ 0x3c, 0x07, //fixed header
                      0x00, 0x03, 't', 'o', 'p', //topic name
                      0x00, 0x21, //message ID
        ];

    const msg = MqttFactory.create(bytes);
    checkNotNull(msg);
    checkEqual(msg.fixedHeader.remaining, 7);

    const publish = cast(MqttPublish)msg;
    checkNotNull(publish);

    checkEqual(publish.topic, "top");
    checkEqual(publish.msgId, 33);
}

void testDecodePublishWithNoMsgId() {
    ubyte[] bytes = [ 0x30, 0x05, //fixed header
                      0x00, 0x03, 't', 'u', 'p', //topic name
        ];

    const msg = MqttFactory.create(bytes);
    checkNotNull(msg);
    checkEqual(msg.fixedHeader.remaining, 5);

    const publish = cast(MqttPublish)msg;
    checkNotNull(publish);

    checkEqual(publish.topic, "tup");
    checkEqual(publish.msgId, 0); //no message id
}

void testEncodePublish() {
    checkEqual((new MqttPublish(false, 2, true, "foo", "info", 12)).encode(),
               [0x35, 0x0d, //header
                0x00, 0x03, 'f', 'o', 'o', //topic
                0x00, 0x0c, //msgId
                0x00, 0x04, 'i', 'n', 'f', 'o',
               ]
              );

    checkEqual((new MqttPublish(true, 0, false, "bars", "boo")).encode(),
               [0x38, 0x0b, //header
                0x00, 0x04, 'b', 'a', 'r', 's',//topic
                0x00, 0x03, 'b', 'o', 'o',
               ]
        );
}


void testSubscribe() {
    ubyte[] bytes = [ 0x8c, 0x13, //fixed header
                      0x00, 0x21, //message ID
                      0x00, 0x05, 'f', 'i', 'r', 's', 't',
                      0x01, //qos
                      0x00, 0x06, 's', 'e', 'c', 'o', 'n', 'd',
                      0x02, //qos
        ];

    const msg = MqttFactory.create(bytes);
    checkNotNull(msg);
    checkEqual(msg.fixedHeader.remaining, 19);

    const subscribe = cast(MqttSubscribe)msg;
    checkNotNull(subscribe);

    checkEqual(subscribe.msgId, 33);
    checkEqual(subscribe.topics,
               [MqttSubscribe.Topic("first", 1), MqttSubscribe.Topic("second", 2)]);
}
