module mqttd.broker;


import mqttd.message;
import std.algorithm;
import std.array;


interface MqttSubscriber {
    void newMessage(in string topic, in ubyte[] payload);
}

private bool revStrEquals(in string str1, in string str2) pure nothrow { //compare strings in reverse
    if(str1.length != str2.length) return false;
    for(long i = str1.length - 1; i >= 0; --i)
        if(str1[i] != str2[i]) return false;
    return true;
}

private bool equalOrPlus(in string pat, in string top) pure nothrow {
    return pat == "+" || pat.revStrEquals(top);
}


struct MqttBroker {
    void publish(in string topic, in string payload) {
        publish(topic, cast(ubyte[])payload);
    }

    void publish(in string topic, in ubyte[] payload) {
        const topParts = array(splitter(topic, "/"));
        foreach(ref s; filter!(a => a.matches(topParts))(_subscriptions)) {
            if(s.matches(topParts)) s.newMessage(topic, payload);
        }
    }

    void subscribe(MqttSubscriber subscriber, in string[] topics) {
        subscribe(subscriber, array(map!(a => MqttSubscribe.Topic(a, 0))(topics)));
    }

    void subscribe(MqttSubscriber subscriber, in MqttSubscribe.Topic[] topics) {
        foreach(topic; topics) {
            _subscriptions ~= Subscription(subscriber, topic);
        }
    }

    void unsubscribe(MqttSubscriber subscriber) {
        _subscriptions = std.algorithm.remove!(s => s.isSubscriber(subscriber))(_subscriptions);
    }

    void unsubscribe(MqttSubscriber subscriber, in string[] topics) {
        _subscriptions = std.algorithm.remove!(s => s.isSubscription(subscriber, topics))(_subscriptions);
    }

    static bool matches(in string topic, in string pattern) {
        return matches(array(splitter(topic, "/")), pattern);
    }

    static bool matches(in string[] topParts, in string pattern) {
        return PatternMatcherFactory.create(pattern).matches(topParts);
    }

private:

    Subscription[] _subscriptions;
}


private class PatternMatcher {
    this(in string topic, in string[] pattern) {
        _topic = topic;
        _pattern = pattern;
    }

    bool isTopic(in string[] topics) const {
        return !find(topics, _topic).empty;
    }

    abstract bool matches(in string[] topic) const;

    const string _topic;
    const string[] _pattern;
}

private class PlusMatcher: PatternMatcher {
    this(in string topic, in string[] pattern) { super(topic, pattern); }

    override bool matches(in string[] topic) const {
        if(_pattern.length != topic.length) return false;
        for(long i = topic.length - 1; i >= 0; --i) {
            if(!_pattern[i].equalOrPlus(topic[i])) return false;
        }
        return true;
    }
};

private class HashMatcher: PatternMatcher {
    this(in string topic, in string[] pattern) { super(topic, pattern); }

    override bool matches(in string[] topic) const {
        //+1 here allows "finance/#" to match "finance"
        if(_pattern.length > topic.length + 1) return false;
        for(long i = _pattern.length - 2; i >=0 ; --i) { //starts with same thing
            if(!_pattern[i].equalOrPlus(topic[i])) return false;
        }
        return true;
    }
}

private class PatternMatcherFactory {
    static PatternMatcher create(in string topic) {
        const pattern = array(splitter(topic, "/"));
        const index = countUntil(pattern, "#");
        if(index == -1) return new PlusMatcher(topic, pattern);
        return new HashMatcher(topic, pattern);
    }
}

private struct Subscription {
    this(MqttSubscriber subscriber, in MqttSubscribe.Topic topic) {
        _subscriber = subscriber;
        _matcher = PatternMatcherFactory.create(topic.topic);
        _qos = topic.qos;
    }

    void newMessage(in string topic, in ubyte[] payload) {
        _subscriber.newMessage(topic, payload);
    }

    bool isSubscriber(MqttSubscriber subscriber) const {
        return _subscriber == subscriber;
    }

    bool isSubscription(MqttSubscriber subscriber, in string[] topics) const {
        return isSubscriber(subscriber) && _matcher.isTopic(topics);
    }

private:
    const PatternMatcher _matcher;
    MqttSubscriber _subscriber;
    ubyte _qos;
    bool matches(in string[] topic) const { return _matcher.matches(topic); }
}
