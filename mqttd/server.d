module mqttd.server;


import mqttd.message;
import mqttd.factory;
import mqttd.broker;
import cerealed.cerealiser;
import std.stdio;
import std.algorithm;
import std.array;


enum isMqttConnection(T) = isMqttSubscriber!T && is(typeof(() {
    ubyte[] bytes;
    auto t = T.init;
    t.read(bytes);
    t.write(bytes);
    t.disconnect();
}));

class MqttServer(T) if(isMqttConnection!T) {
    void newConnection(T connection, in MqttConnect connect) {
        auto code = MqttConnack.Code.ACCEPTED;
        if(connect.isBadClientId) {
            code = MqttConnack.Code.BAD_ID;
        }

        MqttConnack(code).cerealise!(b => connection.write(b));
    }

    void subscribe(T connection, in ushort msgId, in string[] topics) {
        enum qos = 0;
        subscribe(connection, msgId, topics.map!(a => MqttSubscribe.Topic(a, qos)).array);
    }

    void subscribe(T connection, in ushort msgId, in MqttSubscribe.Topic[] topics) {
        const qos = topics.map!(a => a.qos).array;
        MqttSuback(msgId, qos).cerealise!(b => connection.write(b));
        _broker.subscribe(connection, topics);
    }

    void unsubscribe(T connection) {
        _broker.unsubscribe(connection);
    }

    void unsubscribe(T connection, in ushort msgId, in string[] topics) {
        MqttUnsuback(msgId).cerealise!(b => connection.write(b));
        _broker.unsubscribe(connection, topics);
    }

    void publish(in string topic, in string payload) {
        publish(topic, cast(ubyte[])payload);
    }

    void publish(in string topic, in ubyte[] payload) {
        _broker.publish(topic, payload);
    }

    void ping(T connection) const {
        static MqttPingResp resp;
        connection.write(resp.encode);
    }

    @property void useCache(bool u) {
        _broker.useCache = u;
    }


private:

    MqttBroker!T _broker;
}

interface MqttInput {
    void read(ubyte[] bytes);
}

class MqttConnection: MqttInput {
    void newMessage(in string topic, in ubyte[] payload) {
        MqttPublish(topic, payload).cerealise!(b => write(cast(immutable)b));
    }

    override void read(ubyte[] bytes) {
    }
    abstract void write(in ubyte[] bytes);
    abstract void disconnect();

    final void newMessage(in ubyte[] bytes) {
        write(bytes);
    }

    static assert(isMqttSubscriber!MqttConnection);
}
