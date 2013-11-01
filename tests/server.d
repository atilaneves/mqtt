import unit_threaded.check;
import mqtt.server;


class TestMqttSubscriber: MqttSubscriber {
    override void newMessage(in string topic, in string payload) {
        messages ~= payload;
    }

    string[] messages;
}

class TestMqttServer: MqttServer {
}

void testSubscribe() {
    auto server = new TestMqttServer();

    auto subscriber = new TestMqttSubscriber();
    server.publish("topics/foo", "my foo is foo");
    checkEqual(subscriber.messages, []);

    server.subscribe(subscriber, ["topics/foo"]);
    server.publish("topics/foo", "my foo is foo");
    server.publish("topics/bar", "my bar is bar");
    checkEqual(subscriber.messages, ["my foo is foo"]);

    server.subscribe(subscriber, ["topics/bar"]);
    server.publish("topics/foo", "my foo is foo");
    server.publish("topics/bar", "my bar is bar");
    checkEqual(subscriber.messages, ["my foo is foo", "my foo is foo", "my bar is bar"]);
}
