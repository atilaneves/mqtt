module mqttd.server;


import mqttd.message;
import mqttd.factory;
import mqttd.broker;
import cerealed.cerealiser;
import std.stdio;
import std.algorithm;
import std.array;


enum isMqttInput(T) = is(typeof(() {
    ubyte[] bytes;
    T.init.read(bytes);
}));


enum isMqttConnection(T) = isMqttSubscriber!T && isMqttInput!T && is(typeof(() {
    ubyte[] bytes;
    auto t = T.init;
    t.read(bytes);
    t.write(bytes);
    t.disconnect();
}));

class MqttServer(T) if(isMqttConnection!T) {

    alias Connection = RefType!T;

    void newConnection(Connection connection, in MqttConnect connect) {
        auto code = MqttConnack.Code.ACCEPTED;
        if(connect.isBadClientId) {
            code = MqttConnack.Code.BAD_ID;
        }

        MqttConnack(code).cerealise!(b => connection.write(b));
    }

    void subscribe(Connection connection, in ushort msgId, in string[] topics) {
        enum qos = 0;
        subscribe(connection, msgId, topics.map!(a => MqttSubscribe.Topic(a, qos)).array);
    }

    void subscribe(Connection connection, in ushort msgId, in MqttSubscribe.Topic[] topics) {
        const qos = topics.map!(a => a.qos).array;
        MqttSuback(msgId, qos).cerealise!(b => connection.write(b));
        _broker.subscribe(connection, topics);
    }

    void unsubscribe(Connection connection) {
        _broker.unsubscribe(connection);
    }

    void unsubscribe(Connection connection, in ushort msgId, in string[] topics) {
        MqttUnsuback(msgId).cerealise!(b => connection.write(b));
        _broker.unsubscribe(connection, topics);
    }

    void publish(in string topic, in string payload) {
        publish(topic, cast(ubyte[])payload);
    }

    void publish(in string topic, in ubyte[] payload) {
        _broker.publish(topic, payload);
    }

    void ping(Connection connection) const {
        static MqttPingResp resp;
        connection.write(resp.encode);
    }

    @property void useCache(bool u) {
        _broker.useCache = u;
    }


private:

    MqttBroker!T _broker;
}


mixin template MqttConnection() {
    void newMessage(in string topic, in ubyte[] payload) {
        import cerealed;
        MqttPublish(topic, payload).cerealise!(b => write(cast(immutable)b));
    }

    void newMessage(in ubyte[] bytes) {
        write(bytes);
    }

    void read(ubyte[] bytes) {}
}
