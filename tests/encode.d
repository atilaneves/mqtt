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
