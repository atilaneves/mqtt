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


void testWildCards() {
   auto server = new TestMqttServer();
   checkTrue(server.matches("foo/bar/baz", "foo/bar/baz"));
   checkTrue(server.matches("foo/bar", "foo/+"));
   checkTrue(server.matches("foo/baz", "foo/+"));
   checkFalse(server.matches("foo/bar/baz", "foo/+"));
   checkTrue(server.matches("foo/bar", "foo/#"));
   checkTrue(server.matches("foo/bar/baz", "foo/#"));
   checkTrue(server.matches("foo/bar/baz/boo", "foo/#"));
   checkFalse(server.matches("foo/bar/baz", "foo/#/bar"));
   checkTrue(server.matches("foo/bla/bar/baz/boo/bogadog", "foo/+/bar/baz/#"));
   checkTrue(server.matches("foo/bla/blue/red/bar/baz", "foo/#/bar/baz"));
   checkFalse(server.matches("foo/bla/blue/red/bar/baz/black", "foo/#/bar/baz"));
}


void testSubscribeWithWildCards() {
    auto server = new TestMqttServer();
    auto subscriber1 = new TestMqttSubscriber();

    server.subscribe(subscriber1, ["topics/foo/+"]);
    server.publish("topics/foo/bar", "3");
    server.publish("topics/bar/baz/boo", "4"); //shouldn't get this one
    checkEqual(subscriber1.messages, ["3"]);

    auto subscriber2 = new TestMqttSubscriber();
    server.subscribe(subscriber2, ["topics/foo/#"]);
    server.publish("topics/foo/bar", "3");
    server.publish("topics/bar/baz/boo", "4");

    checkEqual(subscriber1.messages, ["3", "3"]);
    checkEqual(subscriber2.messages, ["3"]);

    auto subscriber3 = new TestMqttSubscriber();
    server.subscribe(subscriber3, ["topics/+/bar"]);
    auto subscriber4 = new TestMqttSubscriber();
    server.subscribe(subscriber4, ["topics/#"]);

    server.publish("topics/foo/bar", "3");
    server.publish("topics/bar/baz/boo", "4");
    server.publish("topics/boo/bar/zoo", "5");
    server.publish("topics/foo/bar/zoo", "6");
    server.publish("topics/bbobobobo/bar", "7");

    checkEqual(subscriber1.messages, ["3", "3", "3"]);
    checkEqual(subscriber2.messages, ["3", "3", "6"]);
    checkEqual(subscriber3.messages, ["3", "7"]);
    checkEqual(subscriber4.messages, ["3", "4", "5", "6", "7"]);
}
