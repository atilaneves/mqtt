module tests.encode;

import unit_threaded.check;
import cerealed.cerealiser;
import cerealed.decerealiser;
import mqttd.message;
import mqttd.factory;

void testCerealiseFixedHeader() {
    auto cereal = new Cerealiser();
    cereal ~= MqttFixedHeader(MqttType.PUBLISH, true, 2, false, 5);
    checkEqual(cereal.bytes, [0x3c, 0x5]);
}

void testDecerealiseMqttHeader() {
     auto cereal = new Decerealiser([0x3c, 0x5]);
     const header = cereal.value!MqttFixedHeader;
     checkEqual(header.type, MqttType.PUBLISH);
     checkEqual(header.dup, true);
     checkEqual(header.qos, 2);
     checkEqual(header.retain, false);
     checkEqual(header.remaining, 5);
}

private auto encode(in MqttFixedHeader header) {
    auto cereal = new Cerealiser;
    cereal ~= cast(MqttFixedHeader) header;
    return cereal.bytes;
}

private auto headerFromBytes(in ubyte[] bytes) {
    auto cereal = new Decerealiser(bytes);
    return cereal.value!MqttFixedHeader;
}

void testEncodeFixedHeader() {
    const msg = MqttFixedHeader(MqttType.PUBLISH, true, 2, false, 5);
    checkEqual(msg.encode(), [0x3c, 0x5]);
}

void testDecodeFixedHeader() {
    const msg = headerFromBytes([0x3c, 0x5, 0, 0, 0, 0, 0]);
    checkEqual(msg.type, MqttType.PUBLISH);
    checkEqual(msg.dup, true);
    checkEqual(msg.qos, 2);
    checkEqual(msg.retain, false);
    checkEqual(msg.remaining, 5);
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
        ubyte[] bytes = [0x12, 0xc1, 0x02];
        bytes.length += 321;
        const hdr = headerFromBytes(bytes);
        checkEqual(hdr.remaining, 321);
    }
    {
        ubyte[] bytes = [0x12, 0x83, 0x02];
        bytes.length += 259;
        const hdr = headerFromBytes(bytes);
        checkEqual(hdr.remaining, 259);
    }
    {
        ubyte[] bytes = [0x12, 0x85, 0x80, 0x80, 0x01];
        bytes.length += 2_097_157;
        const hdr = headerFromBytes(bytes);
        checkEqual(hdr.remaining, 2_097_157);
    }
}

void testConnectMsg() {
    ubyte[] bytes = [ 0x10, 0x2a, //fixed header
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
    auto cereal = new Cerealiser();
    cereal ~= new MqttConnack(MqttConnack.Code.SERVER_UNAVAILABLE);
    checkEqual(cereal.bytes, [0x20, 0x2, 0x0, 0x3]);
}

void testDecodePublishWithMsgId() {

    ubyte[] bytes = [ 0x3c, 0x0b, //fixed header
                      0x00, 0x03, 't', 'o', 'p', //topic name
                      0x00, 0x21, //message ID
                      'b', 'o', 'r', 'g', //payload
        ];

    const msg = MqttFactory.create(bytes);
    checkNotNull(msg);

    const publish = cast(MqttPublish)msg;
    checkNotNull(publish);

    checkEqual(publish.topic, "top");
    checkEqual(publish.payload, "borg");
    checkEqual(publish.msgId, 33);
}

void testDecodePublishWithNoMsgId() {
    ubyte[] bytes = [ 0x30, 0x05, //fixed header
                      0x00, 0x03, 't', 'u', 'p', //topic name
        ];

    const msg = MqttFactory.create(bytes);
    checkNotNull(msg);

    const publish = cast(MqttPublish)msg;
    checkNotNull(publish);

    checkEqual(publish.topic, "tup");
    checkEqual(publish.msgId, 0); //no message id
}

void testDecodePublishWithBadSize() {
    ubyte[] bytes = [ 0x30, 0x60, //fixed header with wrong (too big) size
                      0x00, 0x03, 't', 'u', 'p', //topic name
                      'b', 'o', 'r', 'g', //payload
        ];

    const msg = MqttFactory.create(bytes);
    checkNull(msg);
}


void testEncodePublish() {
    checkEqual((new MqttPublish(false, 2, true, "foo", cast(ubyte[])"info", 12)).encode(),
               [0x35, 0x0b, //header
                0x00, 0x03, 'f', 'o', 'o', //topic
                0x00, 0x0c, //msgId
                'i', 'n', 'f', 'o',
               ]
              );

    checkEqual((new MqttPublish(true, 0, false, "bars", cast(ubyte[])"boo")).encode(),
               [0x38, 0x09, //header
                0x00, 0x04, 'b', 'a', 'r', 's',//topic
                'b', 'o', 'o',
               ]
        );

    checkEqual((new MqttPublish("topic", cast(ubyte[])"payload")).encode(),
               [0x30, 0x0e, //header
                0x00, 0x05, 't', 'o', 'p', 'i', 'c',
                'p', 'a', 'y', 'l', 'o', 'a', 'd']);
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

    const subscribe = cast(MqttSubscribe)msg;
    checkNotNull(subscribe);

    checkEqual(subscribe.msgId, 33);
    checkEqual(subscribe.topics,
               [MqttSubscribe.Topic("first", 1), MqttSubscribe.Topic("second", 2)]);
}

void testSuback() {
    checkEqual((new MqttSuback(12, [1, 2, 0, 2])).encode(),
                [0x90, 0x06, //fixed header
                 0x00, 0x0c, //msgId
                 0x01, 0x02, 0x00, 0x02, //qos
                 ]);
}

void testPingReq() {
    ubyte[] bytes = [ 0xc0, 0x00 ];
    const pingReq = MqttFactory.create(bytes);
    checkNotNull(pingReq);
}

void testPingResp() {
    checkEqual((new MqttPingResp()).encode(),
               [0xd0, 0x00]);
}
