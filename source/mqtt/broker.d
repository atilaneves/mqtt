module mqtt.broker;


import mqtt.message;
import std.algorithm;
import std.array;
import std.algorithm;
import std.range;
import std.parallelism;


interface MqttSubscriber {
    void newMessage(in string topic, in ubyte[] payload);
}

bool revStrEquals(in string str1, in string str2) pure nothrow { //compare strings in reverse
    if(str1.length != str2.length) return false;
    for(auto i = cast(long)str1.length - 1; i >= 0; --i)
        if(str1[i] != str2[i]) return false;
    return true;
}

bool equalOrPlus(in string pat, in string top) {
    return pat == "+" || pat.revStrEquals(top);
}

struct MqttBroker {
    void publish(in string topic, in string payload) {
        publish(topic, cast(ubyte[])payload);
    }

    void publish(in string topic, in ubyte[] payload) {
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
        return matches(array(splitter(topic, "/")), array(splitter(pattern, "/")));
    }

    static bool matches(in string[] topParts, in string[] patParts) {
        return PatternMatcherFactory.create(patParts).matches(topParts);
    }


private:

    static class PatternMatcher {
        this(in string[] pattern) { _pattern = pattern; }
        abstract bool matches(in string[] topic) const;
        const string[] _pattern;
    }

    static class ExactMatcher: PatternMatcher {
        this(in string[] pattern) { super(pattern); }
        override bool matches(in string[] topic) const {
            if(_pattern.length != topic.length) return false;
            for(long i = topic.length - 1; i >= 0; --i) {
                if(!_pattern[i].equalOrPlus(topic[i])) return false;
            }
            return true;
        }
    };

    static class OneHashMatcher: PatternMatcher {
        long _index; //index of the one hash
        this(in string[] pattern, long index) {
            super(pattern);
            _index = index;
        }
        override bool matches(in string[] topic) const {
            //+1 here allows "finance/#" to match "finance"
            if(_pattern.length > topic.length + 1) return false;
            for(long i = _index -1; i >=0 ; --i) { //starts with same thing
                if(!_pattern[i].equalOrPlus(topic[i])) return false;
            }
            for(long i = _pattern.length - 1, j = topic.length - 1; i > _index; --i, --j) {
                if(!_pattern[i].equalOrPlus(topic[j])) return false;
            }
            return true;
        }
    }

    static class MultipleHashMatcher: PatternMatcher {
        this(in string[] pattern) { super(pattern); }
        override bool matches(in string[] topic) const {
            //TODO: calculate match
            return false;
        }
    }

    static class PatternMatcherFactory {
        static PatternMatcher create(in string[] pattern) {
            const index = countUntil(pattern, "#");
            if(index == -1) return new ExactMatcher(pattern);
            return new OneHashMatcher(pattern, index);
        }
    }

    static struct Subscription {
        this(MqttSubscriber subscriber, in MqttSubscribe.Topic[] topics) {
            this._subscriber = subscriber;
            foreach(t; topics) {
                const matcher = PatternMatcherFactory.create(array(splitter(t.topic, "/")));
                this._topics ~= TopicPattern(matcher, t.qos);
            }
        }

        void newMessage(in string topic, in ubyte[] payload) {
            _subscriber.newMessage(topic, payload);
        }

        void handlePublish(in string[] topParts, in string topic, in ubyte[] payload) {
            foreach(t; _topics) {
                if(t.matches(topParts)) {
                    _subscriber.newMessage(topic, payload);
                }
            }
        }

    private:
        MqttSubscriber _subscriber;
        static struct TopicPattern {
            const PatternMatcher _matcher;
            ubyte qos;
            bool matches(in string[] topic) const { return _matcher.matches(topic); }
        }
        TopicPattern[] _topics;
    }

    Subscription[] _subscriptions;
}
