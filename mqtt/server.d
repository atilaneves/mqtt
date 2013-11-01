module mqtt.server;


import mqtt.message;
import std.algorithm;
import std.array;
import std.algorithm;



interface MqttSubscriber {
    void newMessage(in string topic, in string payload);
}


abstract class MqttServer {
    void publish(in string topic, in string payload) {
        foreach(s; _subscriptions) {
            foreach(t; s.topics) {
                if(t.topic == topic) {
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


private:

    struct Subscription {
        MqttSubscriber subscriber;
        const MqttSubscribe.Topic[] topics;
        void newMessage(in string topic, in string payload) {
            subscriber.newMessage(topic, payload);
        }
    }

    Subscription[] _subscriptions;
}
