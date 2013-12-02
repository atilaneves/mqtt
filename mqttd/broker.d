module mqttd.broker;


import mqttd.message;
import std.algorithm;
import std.array;


interface MqttSubscriber {
    void newMessage(in string topic, in ubyte[] payload);
}

private bool revStrEquals(in string str1, in string str2) pure nothrow { //compare strings in reverse
    if(str1.length != str2.length) return false;
    for(auto i = cast(int)str1.length - 1; i >= 0; --i)
        if(str1[i] != str2[i]) return false;
    return true;
}

private bool equalOrPlus(in string pat, in string top) pure nothrow {
    return pat == "+" || pat.revStrEquals(top);
}


struct MqttBroker {
    alias subscriptions this;

    static bool matches(in string topic, in string pattern) {
        return matches(array(splitter(topic, "/")), pattern);
    }

    static bool matches(in string[] topParts, in string pattern) {
        return matches(topParts, array(splitter(pattern, "/")));
    }

    static bool matches(in string[] topParts, in string[] patParts) {
        immutable hasHash = patParts[$ - 1] == "#";
        if(hasHash) {
            //+1 here allows "finance/#" to match "finance"
            if(patParts.length > topParts.length + 1) return false;
        } else {
            if(patParts.length != topParts.length) return false;
        }

        immutable end = cast(int)(hasHash ? patParts.length - 2 : patParts.length - 1);
        for(int i = end; i >=0 ; --i) { //starts with same thing
            if(!patParts[i].equalOrPlus(topParts[i])) return false;
        }
        return true;
    }

    Subscriptions subscriptions;
}


private struct Subscriptions {
public:

    void subscribe(MqttSubscriber subscriber, in string[] topics) {
        subscribe(subscriber, array(map!(a => MqttSubscribe.Topic(a, 0))(topics)));
    }

    void subscribe(MqttSubscriber subscriber, in MqttSubscribe.Topic[] topics) {
        foreach(t; topics) _subscriptions ~= Subscription(subscriber, t);
    }

    void unsubscribe(MqttSubscriber subscriber) {
        _subscriptions = std.algorithm.remove!(s => s.isSubscriber(subscriber))(_subscriptions);
    }

    void unsubscribe(MqttSubscriber subscriber, in string[] topics) {
        _subscriptions = std.algorithm.remove!(s => s.isSubscription(subscriber, topics))(_subscriptions);
    }

    void publish(in string topic, in string payload) {
        publish(topic, cast(ubyte[])payload);
    }

    void publish(in string topic, in ubyte[] payload) {
        const topParts = array(splitter(topic, "/"));
        foreach(ref s; filter!(s => s.matches(topParts))(_subscriptions)) {
            s.newMessage(topic, payload);
        }
    }

private:

    Subscription[] _subscriptions;
}


private struct Subscription {
    this(MqttSubscriber subscriber, in MqttSubscribe.Topic topic) {
        _subscriber = subscriber;
        _topic = topic.topic;
        _pattern = array(splitter(topic.topic, "/"));
        _qos = topic.qos;
    }

    bool matches(in string[] topic) const {
        return MqttBroker.matches(topic, _pattern);
    }

    void newMessage(in string topic, in ubyte[] payload) {
        _subscriber.newMessage(topic, payload);
    }

    bool isSubscriber(MqttSubscriber subscriber) const {
        return _subscriber == subscriber;
    }

    bool isSubscription(MqttSubscriber subscriber, in string[] topics) const {
        return isSubscriber(subscriber) && isTopic(topics);
    }

    bool isTopic(in string[] topics) const {
        return !find(topics, _topic).empty;
    }

private:
    const string _topic;
    const string[] _pattern;
    MqttSubscriber _subscriber;
    ubyte _qos;
}
