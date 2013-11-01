module mqtt.broker;


import mqtt.message;
import std.algorithm;
import std.array;
import std.algorithm;
import std.range;
import std.regex;
import std.stdio;

interface MqttSubscriber {
    void newMessage(in string topic, in string payload);
}


struct MqttBroker {
    void publish(in string topic, in string payload) {
        foreach(s; _subscriptions) {
            foreach(t; s.topics) {
                if(matches(topic, t.topic)) {
                    s.newMessage(topic, payload);
                }
            }
        }
    }

    void subscribe(MqttSubscriber subscriber, in string[] topics) {
        subscribe(subscriber, array(map!(a => MqttSubscribe.Topic(a, 0))(topics)));
    }

    void subscribe(MqttSubscriber subscriber, in MqttSubscribe.Topic[] topics) {
        _subscriptions ~= Subscription(subscriber, topics);
    }

    bool matches(in string actualTopic, in string subscriptionTopic) const {
        if(subscriptionTopic == actualTopic) return true;
        if(subscriptionTopic.length > actualTopic.length) return false;

        const subParts = array(splitter(subscriptionTopic, "/"));
        const actParts = array(splitter(actualTopic, "/"));

        if(subParts.length < actParts.length && !find(subParts, "#")) return false;
        return !match(actualTopic, wildcardsToRegex(subscriptionTopic)).empty;
    }


private:

    struct Subscription {
        MqttSubscriber subscriber;
        const MqttSubscribe.Topic[] topics;
        void newMessage(in string topic, in string payload) {
            subscriber.newMessage(topic, payload);
        }
    }

    Subscription[] _subscriptions;

    auto wildcardsToRegex(in string topic) const {
        enum plusSlashRegex = ctRegex!r"\+/";
        enum plusEndRegex = ctRegex!r"\+$";
        enum hashRegex = ctRegex!r"#";

        const endPlus = replace(topic, plusEndRegex, r"[^/]+$$");
        const plus = replace(endPlus, plusSlashRegex, r"[^/]+/");
        return "^" ~ replace(plus, hashRegex, r"\w+(/\w+)*") ~ "$";
    }
}
