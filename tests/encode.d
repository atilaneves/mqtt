import unit_threaded.check;
import mqtt.message;
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
