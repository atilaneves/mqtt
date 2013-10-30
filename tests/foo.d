import unit_threaded.check;
import mqtt.message;


void testEncode() {
    const msg = MqttMessage(3, true, 2, false, 5);
    checkEqual(msg.type, 3);
    checkEqual(msg.dup, true);
    checkEqual(msg.qos, 2);
    checkEqual(msg.retain, false);
    checkEqual(msg.remaining, 5);
    checkEqual(msg.encode(), [0x3c, 0x5]);
}

void testDecode() {
    auto msg = MqttMessage([0x3c, 0x5]);
    checkEqual(msg.type, 3);
    checkEqual(msg.dup, true);
    checkEqual(msg.qos, 2);
    checkEqual(msg.retain, false);
    checkEqual(msg.remaining, 5);
}
