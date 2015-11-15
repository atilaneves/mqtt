module tests.message;

import unit_threaded;
import cerealed;
import mqttd.message;


@HiddenTest
void testGetTopicThrows() {
    auto enc = Cerealiser();
    enc ~= MqttSuback(33, [1, 2]);
    getTopic(enc.bytes).shouldThrow;
}

void testGetTopic1() {
    auto enc = Cerealiser();
    enc ~= MqttPublish("/foo/bar", [1, 2, 3]);
    getTopic(enc.bytes).shouldEqual("/foo/bar");
}

void testGetTopic2() {
    auto enc = Cerealiser();
    enc ~= MqttPublish("/bar/foo", [1, 2, 3]);
    getTopic(enc.bytes).shouldEqual("/bar/foo");
}
