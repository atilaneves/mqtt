module mqtt.server;


import mqtt.message;
import std.algorithm;
import std.array;


interface MqttSubscriber {
    void newMessage(in string topic, in string payload);
}


abstract class MqttServer {
    void publish(in string topic, in string payload) pure const nothrow {
    }

    void subscribe(MqttSubscriber subscriber, in MqttSubscribe.Topic[] topics) {
    }

    void subscribe(MqttSubscriber subscriber, in string[] topics) {
        subscribe(subscriber, array(map!(a => MqttSubscribe.Topic(a, 0))(topics)));
    }
}

class MqttServerImpl: MqttServer {
    this(in int port) {
    }

}
