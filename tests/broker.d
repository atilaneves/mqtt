module tests.broker;

import unit_threaded;
import mqttd.broker;
import mqttd.message;
import std.algorithm;
import std.typecons;

struct TestMqttSubscriber {
    alias Payload = ubyte[];
    void newMessage(in ubyte[] bytes) {
        import std.stdio;
        writeln("new message: ", bytes);
        messages ~= bytes;
    }

    const(Payload)[] messages;

    static assert(isNewMqttSubscriber!TestMqttSubscriber);
}


void testSubscribe() {

    foreach(useCache; [Yes.useCache, No.useCache]) {
        auto broker = NewMqttBroker!TestMqttSubscriber(useCache);

        auto subscriber = TestMqttSubscriber();
        broker.publish("topics/foo", [2, 4, 6]);
        shouldEqual(subscriber.messages, []);

        broker.subscribe(subscriber, ["topics/foo"]);
        broker.publish("topics/foo", [2, 4, 6]);
        broker.publish("topics/bar", [1, 3, 5, 7]);
        shouldEqual(subscriber.messages, [[2, 4, 6]]);

        broker.subscribe(subscriber, ["topics/bar"]);
        broker.publish("topics/foo", [2, 4, 6]);
        broker.publish("topics/bar", [1, 3, 5, 7]);
        shouldEqual(subscriber.messages, [[2, 4, 6], [2, 4, 6], [1, 3, 5, 7]]);
    }
}


void testUnsubscribeAll() {
    foreach(useCache; [Yes.useCache, No.useCache]) {
        auto broker = NewMqttBroker!TestMqttSubscriber(useCache);
        auto subscriber = TestMqttSubscriber();

        broker.subscribe(subscriber, ["topics/foo"]);
        broker.publish("topics/foo", [2, 4, 6]);
        broker.publish("topics/bar", [1, 3, 5, 7]);
        shouldEqual(subscriber.messages, [[2, 4, 6]]);

        broker.unsubscribe(subscriber);
        broker.publish("topics/foo", [2, 4, 6]);
        broker.publish("topics/bar", [1, 3, 5, 7]);
        shouldEqual(subscriber.messages, [[2, 4, 6]]); //shouldn't have changed
    }
}

void testUnsubscribeOne() {
    foreach(useCache; [Yes.useCache, No.useCache]) {
        auto broker = NewMqttBroker!TestMqttSubscriber(useCache);
        auto subscriber = TestMqttSubscriber();

        broker.subscribe(subscriber, ["topics/foo", "topics/bar"]);
        broker.publish("topics/foo", [2, 4, 6]);
        broker.publish("topics/bar", [1, 3, 5, 7]);
        broker.publish("topics/baz", [9, 8, 7, 6, 5]);
        shouldEqual(subscriber.messages, [[2, 4, 6], [1, 3, 5, 7]]);

        broker.unsubscribe(subscriber, ["topics/foo"]);
        broker.publish("topics/foo", [2, 4, 6]);
        broker.publish("topics/bar", [1, 3, 5, 7]);
        broker.publish("topics/baz", [9, 8, 7, 6, 5]);
        shouldEqual(subscriber.messages, [[2, 4, 6], [1, 3, 5, 7], [1, 3, 5, 7]]);
    }
}


private void checkMatches(in string pubTopic, in string subTopic, bool matches) {
    foreach(useCache; [Yes.useCache, No.useCache]) {
        auto broker = NewMqttBroker!TestMqttSubscriber(useCache);
        auto subscriber = TestMqttSubscriber();

        broker.subscribe(subscriber, [subTopic]);
        broker.publish(pubTopic, [1, 2, 3, 4]);
        const expected = matches ? [[1, 2, 3, 4]] : [];
        writelnUt("checkMatches, subTopic is ", subTopic, " pubTopic is ", pubTopic,
                  ", matches is ", matches);
        shouldEqual(subscriber.messages, expected);
    }
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
    foreach(useCache; [Yes.useCache, No.useCache]) {

        auto broker = NewMqttBroker!TestMqttSubscriber(useCache);
        auto subscriber1 = TestMqttSubscriber();

        broker.subscribe(subscriber1, ["topics/foo/+"]);
        broker.publish("topics/foo/bar", [3]);
        broker.publish("topics/bar/baz/boo", [4]); //shouldn't get this one
        shouldEqual(subscriber1.messages, [[3]]);

        auto subscriber2 = TestMqttSubscriber();
        broker.subscribe(subscriber2, ["topics/foo/#"]);
        broker.publish("topics/foo/bar", [3]);
        broker.publish("topics/bar/baz/boo", [4]);

        shouldEqual(subscriber1.messages, [[3], [3]]);
        shouldEqual(subscriber2.messages, [[3]]);

        auto subscriber3 = TestMqttSubscriber();
        broker.subscribe(subscriber3, ["topics/+/bar"]);
        auto subscriber4 = TestMqttSubscriber();
        broker.subscribe(subscriber4, ["topics/#"]);

        broker.publish("topics/foo/bar", [3]);
        broker.publish("topics/bar/baz/boo", [4]);
        broker.publish("topics/boo/bar/zoo", [5]);
        broker.publish("topics/foo/bar/zoo", [6]);
        broker.publish("topics/bbobobobo/bar", [7]);

        shouldEqual(subscriber1.messages, [[3], [3], [3]]);
        shouldEqual(subscriber2.messages, [[3], [3], [6]]);
        shouldEqual(subscriber3.messages, [[3], [7]]);
        shouldEqual(subscriber4.messages, [[3], [4], [5], [6], [7]]);
    }
}


void testPlus() {
    foreach(useCache; [Yes.useCache, No.useCache]) {
        auto broker = NewMqttBroker!TestMqttSubscriber(useCache);
        auto subscriber = TestMqttSubscriber();

        broker.publish("foo/bar/baz", [1, 2, 3, 4]);
        subscriber.messages.shouldBeEmpty;

        broker.subscribe(subscriber, [MqttSubscribe.Topic("foo/bar/+", 0)]);
        broker.publish("foo/bar/baz", [1, 2, 3, 4]);
        broker.publish("foo/boogagoo", [9, 8, 7]);
        subscriber.messages.shouldEqual([[1, 2, 3, 4]]);
    }
}
