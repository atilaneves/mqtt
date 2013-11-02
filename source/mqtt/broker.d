module mqtt.broker;


import mqtt.message;
import std.algorithm;
import std.array;
import std.algorithm;
import std.range;


interface MqttSubscriber {
    void newMessage(in string topic, in string payload);
}


struct MqttBroker {
    void publish(in string topic, in string payload) {
        foreach(s; _subscriptions) s.handlePublish(topic, payload);
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
        return matches(topic, array(splitter(pattern, "/")));
    }

    static bool matches(in string topic, in string[] patParts) {
        const topParts = array(splitter(topic,  "/"));

        if(patParts.length > topParts.length) return false;
        if(patParts.length != topParts.length && find(patParts, "#").empty) return false;

        for(int i = 0; i < topParts.length; ++i) {
            if(patParts[i] == "#") return true; //so not right
            if(patParts[i] != "+" && patParts[i] != topParts[i]) return false;
        }

        return true;
    }


private:

    struct Subscription {
        this(MqttSubscriber subscriber, in MqttSubscribe.Topic[] topics) {
            this._subscriber = subscriber;
            this._topics = topics;
            // foreach(t; topics) {
            //      this._topics ~= TopicPattern(array(splitter(t.topic, "/")), t.qos);
            // }
        }

        void newMessage(in string topic, in string payload) {
            _subscriber.newMessage(topic, payload);
        }

        void handlePublish(in string topic, in string payload) {
            foreach(t; _topics) {
                if(MqttBroker.matches(topic, t.topic)) {
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
        const MqttSubscribe.Topic[] _topics;
        //TopicPattern[] _topics;
    }

    Subscription[] _subscriptions;
}
