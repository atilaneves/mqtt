module tests.encode;

import unit_threaded;
import cerealed.cerealiser;
import cerealed.decerealiser;
import mqttd.message;
import mqttd.factory;

void testCerealiseFixedHeader() {
    auto cereal = Cerealiser();
    cereal ~= MqttFixedHeader(MqttType.PUBLISH, true, 2, false, 5);
    shouldEqual(cereal.bytes, [0x3c, 0x5]);
}

void testDecerealiseMqttHeader() {
     auto cereal = Decerealiser([0x3c, 0x5]);
     const header = cereal.value!MqttFixedHeader;
     shouldEqual(header.type, MqttType.PUBLISH);
     shouldEqual(header.dup, true);
     shouldEqual(header.qos, 2);
     shouldEqual(header.retain, false);
     shouldEqual(header.remaining, 5);
}

private auto encode(in MqttFixedHeader header) {
    auto cereal = Cerealiser();
    cereal ~= cast(MqttFixedHeader) header;
    return cereal.bytes;
}

private auto headerFromBytes(in ubyte[] bytes) {
    auto cereal = Decerealiser(bytes);
    return cereal.value!MqttFixedHeader;
}

void testEncodeFixedHeader() {
    const msg = MqttFixedHeader(MqttType.PUBLISH, true, 2, false, 5);
    shouldEqual(msg.encode(), [0x3c, 0x5]);
}

void testDecodeFixedHeader() {
    const msg = headerFromBytes([0x3c, 0x5, 0, 0, 0, 0, 0]);
    shouldEqual(msg.type, MqttType.PUBLISH);
    shouldEqual(msg.dup, true);
    shouldEqual(msg.qos, 2);
    shouldEqual(msg.retain, false);
    shouldEqual(msg.remaining, 5);
}

void testEncodeBigRemaining() {
    {
        const msg = MqttFixedHeader(MqttType.SUBSCRIBE, false, 1, true, 261);
        shouldEqual(msg.type, MqttType.SUBSCRIBE);
        shouldEqual(msg.dup, false);
        shouldEqual(msg.qos, 1);
        shouldEqual(msg.retain, true);
        shouldEqual(msg.remaining, 261);
        shouldEqual(msg.encode(), [0x83, 0x85, 0x02]);
    }
    {
        const msg = MqttFixedHeader(MqttType.SUBSCRIBE, false, 1, true, 321);
        shouldEqual(msg.encode(), [0x83, 0xc1, 0x02]);
    }
    {
        const msg = MqttFixedHeader(MqttType.SUBSCRIBE, false, 1, true, 2_097_157);
        shouldEqual(msg.encode(), [0x83, 0x85, 0x80, 0x80, 0x01]);
    }
}

void testDecodeBigRemaining() {
    {
        ubyte[] bytes = [0x12, 0xc1, 0x02];
        bytes.length += 321;
        const hdr = headerFromBytes(bytes);
        shouldEqual(hdr.remaining, 321);
    }
    {
        ubyte[] bytes = [0x12, 0x83, 0x02];
        bytes.length += 259;
        const hdr = headerFromBytes(bytes);
        shouldEqual(hdr.remaining, 259);
    }
    {
        ubyte[] bytes = [0x12, 0x85, 0x80, 0x80, 0x01];
        bytes.length += 2_097_157;
        const hdr = headerFromBytes(bytes);
        shouldEqual(hdr.remaining, 2_097_157);
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
                      0x00, 0x02, 'p', 'w', //password
        ];
    const msg = MqttFactory.create(bytes);
    shouldNotBeNull(msg);

    const connect = cast(MqttConnect)msg;
    shouldNotBeNull(connect);

    shouldEqual(connect.protoName, "MQIsdp");
    shouldEqual(connect.protoVersion, 3);
    shouldBeTrue(connect.hasUserName);
    shouldBeTrue(connect.hasPassword);
    shouldBeFalse(connect.hasWillRetain);
    shouldEqual(connect.willQos, 1);
    shouldBeTrue(connect.hasWill);
    shouldBeFalse(connect.hasClear);
    shouldEqual(connect.keepAlive, 10);
    shouldEqual(connect.clientId, "cid");
    shouldEqual(connect.willTopic, "will");
    shouldEqual(connect.willMessage, "wmsg");
    shouldEqual(connect.userName, "gliftel");
    shouldEqual(connect.password, "pw");
}

void testConnackMsg() {
    auto cereal = Cerealiser();
    cereal ~= new MqttConnack(MqttConnack.Code.SERVER_UNAVAILABLE);
    shouldEqual(cereal.bytes, [0x20, 0x2, 0x0, 0x3]);
}

void testDecodePublishWithMsgId() {

    ubyte[] bytes = [ 0x3c, 0x0b, //fixed header
                      0x00, 0x03, 't', 'o', 'p', //topic name
                      0x00, 0x21, //message ID
                      'b', 'o', 'r', 'g', //payload
        ];

    const msg = MqttFactory.create(bytes);
    shouldNotBeNull(msg);

    const publish = cast(MqttPublish)msg;
    shouldNotBeNull(publish);

    shouldEqual(publish.topic, "top");
    shouldEqual(publish.payload, "borg");
    shouldEqual(publish.msgId, 33);
}

void testDecodePublishWithNoMsgId() {
    ubyte[] bytes = [ 0x30, 0x05, //fixed header
                      0x00, 0x03, 't', 'u', 'p', //topic name
        ];

    const msg = MqttFactory.create(bytes);
    shouldNotBeNull(msg);

    const publish = cast(MqttPublish)msg;
    shouldNotBeNull(publish);

    shouldEqual(publish.topic, "tup");
    shouldEqual(publish.msgId, 0); //no message id
}

void testDecodePublishWithBadSize() {
    ubyte[] bytes = [ 0x30, 0x60, //fixed header with wrong (too big) size
                      0x00, 0x03, 't', 'u', 'p', //topic name
                      'b', 'o', 'r', 'g', //payload
        ];

    const msg = MqttFactory.create(bytes);
    shouldBeNull(msg);
}

private auto encodeMsg(T)(T msg) {
    auto cereal = Cerealiser();
    cereal ~= msg;
    return cereal.bytes;
}


void testEncodePublish() {
    shouldEqual((new MqttPublish(false, 2, true, "foo", cast(ubyte[])"info", 12)).encodeMsg(),
               [0x35, 0x0b, //header
                0x00, 0x03, 'f', 'o', 'o', //topic
                0x00, 0x0c, //msgId
                'i', 'n', 'f', 'o',
               ]
              );

    shouldEqual((new MqttPublish(true, 0, false, "bars", cast(ubyte[])"boo")).encodeMsg(),
               [0x38, 0x09, //header
                0x00, 0x04, 'b', 'a', 'r', 's',//topic
                'b', 'o', 'o',
               ]
        );

    shouldEqual((new MqttPublish("topic", cast(ubyte[])"payload")).encodeMsg(),
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
    shouldNotBeNull(msg);

    const subscribe = cast(MqttSubscribe)msg;
    shouldNotBeNull(subscribe);

    shouldEqual(subscribe.msgId, 33);
    shouldEqual(subscribe.topics,
               [MqttSubscribe.Topic("first", 1), MqttSubscribe.Topic("second", 2)]);
}

void testSuback() {
    shouldEqual((new MqttSuback(12, [1, 2, 0, 2])).encodeMsg(),
                [0x90, 0x06, //fixed header
                 0x00, 0x0c, //msgId
                 0x01, 0x02, 0x00, 0x02, //qos
                 ]);
}

void testPingReq() {
    ubyte[] bytes = [ 0xc0, 0x00 ];
    const pingReq = MqttFactory.create(bytes);
    shouldNotBeNull(pingReq);
}

void testPingResp() {
    shouldEqual((new MqttPingResp()).encode(),
               [0xd0, 0x00]);
}


void testUnsubscribe() {
    ubyte[] bytes = [ 0xa2, 0x11, //fixed header
                      0x00, 0x2a, //message ID
                      0x00, 0x05, 'f', 'i', 'r', 's', 't',
                      0x00, 0x06, 's', 'e', 'c', 'o', 'n', 'd',
        ];

    const msg = MqttFactory.create(bytes);
    shouldNotBeNull(msg);

    const unsubscribe = cast(MqttUnsubscribe)msg;
    shouldNotBeNull(unsubscribe);

    shouldEqual(unsubscribe.msgId, 42);
    shouldEqual(unsubscribe.topics, ["first", "second"]);
}


void testUnsuback() {
    shouldEqual((new MqttUnsuback(13)).encodeMsg(),
               [0xb0, 0x02, //fixed header
                0x00, 0x0d ]); //msgId
}
