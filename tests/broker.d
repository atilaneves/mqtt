module tests.broker;

import unit_threaded;
import mqttd.broker;


struct TestMqttSubscriber {
    void newMessage(in string topic, in ubyte[] payload) {
        messages ~= cast(string)payload;
    }
    string[] messages;
    static assert(isMqttSubscriber!TestMqttSubscriber);
}

struct NewTestMqttSubscriber {
    alias Payload = ubyte[];
    void newMessage(in ubyte[] bytes) {
        messages ~= bytes;
    }

    const(Payload)[] messages;

    static assert(isNewMqttSubscriber!NewTestMqttSubscriber);
}

void testSubscribe() {
    auto broker = MqttBroker!TestMqttSubscriber();

    auto subscriber = TestMqttSubscriber();
    broker.publish("topics/foo", "my foo is foo");
    shouldEqual(subscriber.messages, []);

    broker.subscribe(subscriber, ["topics/foo"]);
    broker.publish("topics/foo", "my foo is foo");
    broker.publish("topics/bar", "my bar is bar");
    shouldEqual(subscriber.messages, ["my foo is foo"]);

    broker.subscribe(subscriber, ["topics/bar"]);
    broker.publish("topics/foo", "my foo is foo");
    broker.publish("topics/bar", "my bar is bar");
    shouldEqual(subscriber.messages, ["my foo is foo", "my foo is foo", "my bar is bar"]);
}


void testUnsubscribeAll() {
    auto broker = MqttBroker!TestMqttSubscriber();
    auto subscriber = TestMqttSubscriber();

    broker.subscribe(subscriber, ["topics/foo"]);
    broker.publish("topics/foo", "my foo is foo");
    broker.publish("topics/bar", "my bar is bar");
    shouldEqual(subscriber.messages, ["my foo is foo"]);

    broker.unsubscribe(subscriber);
    broker.publish("topics/foo", "my foo is foo");
    broker.publish("topics/bar", "my bar is bar");
    shouldEqual(subscriber.messages, ["my foo is foo"]); //shouldn't have changed
}


void testUnsubscribeOne() {
    auto broker = MqttBroker!TestMqttSubscriber();
    auto subscriber = TestMqttSubscriber();

    broker.subscribe(subscriber, ["topics/foo", "topics/bar"]);
    broker.publish("topics/foo", "my foo is foo");
    broker.publish("topics/bar", "my bar is bar");
    broker.publish("topics/baz", "my baz is baz");
    shouldEqual(subscriber.messages, ["my foo is foo", "my bar is bar"]);

    broker.unsubscribe(subscriber, ["topics/foo"]);
    broker.publish("topics/foo", "my foo is foo");
    broker.publish("topics/bar", "my bar is bar");
    broker.publish("topics/baz", "my baz is baz");
    shouldEqual(subscriber.messages, ["my foo is foo", "my bar is bar", "my bar is bar"]);
}


private void checkMatches(in string pubTopic, in string subTopic, bool matches) {
    auto broker = MqttBroker!TestMqttSubscriber();
    auto subscriber = TestMqttSubscriber();

    broker.subscribe(subscriber, [subTopic]);
    broker.publish(pubTopic, "payload");
    const expected = matches ? ["payload"] : [];
    writelnUt("checkMatches, subTopic is ", subTopic, " pubTopic is ", pubTopic,
              ", matches is ", matches);
    shouldEqual(subscriber.messages, expected);
}


void testWildCards() {
   checkMatches("foo/bar/baz", "foo/bar/baz", true);
   checkMatches("foo/bar", "foo/+", true);
   checkMatches("foo/baz", "foo/+", true);
   checkMatches("foo/bar/baz", "foo/+", false);
   checkMatches("foo/bar", "foo/#", true);
   checkMatches("foo/bar/baz", "foo/#", true);
   checkMatches("foo/bar/baz/boo", "foo/#", true);
   checkMatches("foo/bla/bar/baz/boo/bogadog", "foo/+/bar/baz/#", true);
   checkMatches("finance", "finance/#", true);
   checkMatches("finance", "finance#", false);
   checkMatches("finance", "#", true);
   checkMatches("finance/stock", "#", true);
   checkMatches("finance/stock", "finance/stock/ibm", false);
   checkMatches("topics/foo/bar", "topics/foo/#", true);
   checkMatches("topics/bar/baz/boo", "topics/foo/#", false);
}


void testSubscribeWithWildCards() {
    auto broker = MqttBroker!TestMqttSubscriber();
    auto subscriber1 = TestMqttSubscriber();

    broker.subscribe(subscriber1, ["topics/foo/+"]);
    broker.publish("topics/foo/bar", "3");
    broker.publish("topics/bar/baz/boo", "4"); //shouldn't get this one
    shouldEqual(subscriber1.messages, ["3"]);

    auto subscriber2 = TestMqttSubscriber();
    broker.subscribe(subscriber2, ["topics/foo/#"]);
    broker.publish("topics/foo/bar", "3");
    broker.publish("topics/bar/baz/boo", "4");

    shouldEqual(subscriber1.messages, ["3", "3"]);
    shouldEqual(subscriber2.messages, ["3"]);

    auto subscriber3 = TestMqttSubscriber();
    broker.subscribe(subscriber3, ["topics/+/bar"]);
    auto subscriber4 = TestMqttSubscriber();
    broker.subscribe(subscriber4, ["topics/#"]);

    broker.publish("topics/foo/bar", "3");
    broker.publish("topics/bar/baz/boo", "4");
    broker.publish("topics/boo/bar/zoo", "5");
    broker.publish("topics/foo/bar/zoo", "6");
    broker.publish("topics/bbobobobo/bar", "7");

    shouldEqual(subscriber1.messages, ["3", "3", "3"]);
    shouldEqual(subscriber2.messages, ["3", "3", "6"]);
    shouldEqual(subscriber3.messages, ["3", "7"]);
    shouldEqual(subscriber4.messages, ["3", "4", "5", "6", "7"]);
}
