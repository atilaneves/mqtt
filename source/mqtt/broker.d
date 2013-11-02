module mqtt.broker;


import mqtt.message;
import std.algorithm;
import std.array;
import std.algorithm;
import std.range;
import std.parallelism;


interface MqttSubscriber {
    void newMessage(in string topic, in string payload);
}

bool revStrEquals(in string str1, in string str2) pure nothrow { //compare strings in reverse
    if(str1.length != str2.length) return false;
    for(auto i = cast(long)str1.length - 1; i >= 0; --i)
        if(str1[i] != str2[i]) return false;
    return true;
}

struct MqttBroker {
    void publish(in string topic, in string payload) {
        auto topParts = array(splitter(topic, "/"));
        foreach(s; _subscriptions) {
            s.handlePublish(topParts, topic, payload);
        }
    }

    void subscribe(MqttSubscriber subscriber, in string[] topics) {
        subscribe(subscriber, array(map!(a => MqttSubscribe.Topic(a, 0))(topics)));
    }

    void subscribe(MqttSubscriber subscriber, in MqttSubscribe.Topic[] topics) {
        _subscriptions ~= Subscription(subscriber, topics);
    }

    static bool matches(in string topic, in string pattern) {
        if(pattern.length > topic.length) return false;
        if(pattern == topic) return true;
        return matches(array(splitter(topic, "/")), array(splitter(pattern, "/")));
    }

    static bool matches(in string[] topParts, in string[] patParts) {
        if(patParts.length > topParts.length) return false;
        if(patParts.length != topParts.length && find(patParts, "#").empty) return false;

        //for(auto i = cast(long)topParts.length - 1; i >= 0; --i) {
        for(int i = 0; i < topParts.length; ++i) {
            if(patParts[i] == "#") return true; //so not right
            if(patParts[i] != "+" && !patParts[i].revStrEquals(topParts[i])) return false;
        }

        return true;
    }


private:

    struct Subscription {
        this(MqttSubscriber subscriber, in MqttSubscribe.Topic[] topics) {
            this._subscriber = subscriber;
            foreach(t; topics) {
                this._topics ~= TopicPattern(array(splitter(t.topic, "/")), t.qos);
            }
        }

        void newMessage(in string topic, in string payload) {
            _subscriber.newMessage(topic, payload);
        }

        void handlePublish(in string[] topParts, in string topic, in string payload) {
            foreach(t; _topics) {
                if(MqttBroker.matches(topParts, t.pattern)) {
                    _subscriber.newMessage(topic, payload);
                }
            }
        }

    private:
        MqttSubscriber _subscriber;
        static struct TopicPattern {
            string[] pattern;
            ubyte qos;
        }
        TopicPattern[] _topics;
    }

    Subscription[] _subscriptions;
}
