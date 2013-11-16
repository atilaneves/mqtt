module tests.broker;

import unit_threaded.check;
import mqttd.broker;


class TestMqttSubscriber: MqttSubscriber {
    override void newMessage(in string topic, in ubyte[] payload) {
        messages ~= cast(string)payload;
    }
    string[] messages;
}

void testSubscribe() {
    auto broker = MqttBroker();

    auto subscriber = new TestMqttSubscriber();
    broker.publish("topics/foo", "my foo is foo");
    checkEqual(subscriber.messages, []);

    broker.subscribe(subscriber, ["topics/foo"]);
    broker.publish("topics/foo", "my foo is foo");
    broker.publish("topics/bar", "my bar is bar");
    checkEqual(subscriber.messages, ["my foo is foo"]);

    broker.subscribe(subscriber, ["topics/bar"]);
    broker.publish("topics/foo", "my foo is foo");
    broker.publish("topics/bar", "my bar is bar");
    checkEqual(subscriber.messages, ["my foo is foo", "my foo is foo", "my bar is bar"]);
}


void testUnsubscribeAll() {
    auto broker = MqttBroker();
    auto subscriber = new TestMqttSubscriber();

    broker.subscribe(subscriber, ["topics/foo"]);
    broker.publish("topics/foo", "my foo is foo");
    broker.publish("topics/bar", "my bar is bar");
    checkEqual(subscriber.messages, ["my foo is foo"]);

    broker.unsubscribe(subscriber);
    broker.publish("topics/foo", "my foo is foo");
    broker.publish("topics/bar", "my bar is bar");
    checkEqual(subscriber.messages, ["my foo is foo"]); //shouldn't have changed
}

void testUnsubscribe() {
    auto broker = MqttBroker();
    auto subscriber = new TestMqttSubscriber();

    broker.subscribe(subscriber, ["topics/foo", "topics/bar"]);
    broker.publish("topics/foo", "my foo is foo");
    broker.publish("topics/bar", "my bar is bar");
    broker.publish("topics/baz", "my baz is baz");
    checkEqual(subscriber.messages, ["my foo is foo", "my bar is bar"]);

    broker.unsubscribe(subscriber, ["topics/foo"]);
    broker.publish("topics/foo", "my foo is foo");
    broker.publish("topics/bar", "my bar is bar");
    broker.publish("topics/baz", "my baz is baz");
    checkEqual(subscriber.messages, ["my foo is foo", "my bar is bar", "my bar is bar"]);
}


void testWildCards() {
   auto broker = MqttBroker();
   checkTrue(broker.matches("foo/bar/baz", "foo/bar/baz"));
   checkTrue(broker.matches("foo/bar", "foo/+"));
   checkTrue(broker.matches("foo/baz", "foo/+"));
   checkFalse(broker.matches("foo/bar/baz", "foo/+"));
   checkTrue(broker.matches("foo/bar", "foo/#"));
   checkTrue(broker.matches("foo/bar/baz", "foo/#"));
   checkTrue(broker.matches("foo/bar/baz/boo", "foo/#"));
   checkTrue(broker.matches("foo/bla/bar/baz/boo/bogadog", "foo/+/bar/baz/#"));
   checkTrue(broker.matches("finance", "finance/#"));
   checkFalse(broker.matches("finance", "finance#"));
   checkTrue(broker.matches("finance", "#"));
   checkTrue(broker.matches("finance/stock", "#"));
   checkFalse(broker.matches("finance/stock", "finance/stock/ibm"));
   checkTrue(broker.matches("topics/foo/bar", "topics/foo/#"));
   checkFalse(broker.matches("topics/bar/baz/boo", "topics/foo/#"));
}


void testSubscribeWithWildCards() {
    auto broker = MqttBroker();
    auto subscriber1 = new TestMqttSubscriber();

    broker.subscribe(subscriber1, ["topics/foo/+"]);
    broker.publish("topics/foo/bar", "3");
    broker.publish("topics/bar/baz/boo", "4"); //shouldn't get this one
    checkEqual(subscriber1.messages, ["3"]);

    auto subscriber2 = new TestMqttSubscriber();
    broker.subscribe(subscriber2, ["topics/foo/#"]);
    broker.publish("topics/foo/bar", "3");
    broker.publish("topics/bar/baz/boo", "4");

    checkEqual(subscriber1.messages, ["3", "3"]);
    checkEqual(subscriber2.messages, ["3"]);

    auto subscriber3 = new TestMqttSubscriber();
    broker.subscribe(subscriber3, ["topics/+/bar"]);
    auto subscriber4 = new TestMqttSubscriber();
    broker.subscribe(subscriber4, ["topics/#"]);

    broker.publish("topics/foo/bar", "3");
    broker.publish("topics/bar/baz/boo", "4");
    broker.publish("topics/boo/bar/zoo", "5");
    broker.publish("topics/foo/bar/zoo", "6");
    broker.publish("topics/bbobobobo/bar", "7");

    checkEqual(subscriber1.messages, ["3", "3", "3"]);
    checkEqual(subscriber2.messages, ["3", "3", "6"]);
    checkEqual(subscriber3.messages, ["3", "7"]);
    checkEqual(subscriber4.messages, ["3", "4", "5", "6", "7"]);
}
