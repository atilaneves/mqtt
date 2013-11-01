import unit_threaded.check;
import mqtt.server;


class TestMqttSubscriber: MqttSubscriber {
    this(in string addr, in int port) {
    }

    override void newMessage(in string topic, in string payload) {
        _messages ~= payload;
    }

    @property const(string[]) messages() const { return _messages; }

    string[] _messages;
}

class TestMqttServer: MqttServer {
    this(in int port) {
    }

}

void testSubscribe() {
    enum port = 1883;
    auto server = new TestMqttServer(port);

    auto subscriber = new TestMqttSubscriber("localhost", port);
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
