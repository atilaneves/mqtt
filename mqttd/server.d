module mqttd.server;


import mqttd.message;
import mqttd.factory;
import mqttd.broker;
import cerealed.cerealiser;
import std.stdio;
import std.algorithm;
import std.array;


class MqttServer {
    void newConnection(MqttConnection connection, const MqttConnect connect) {
        if(!connect) {
            stderr.writeln("Invalid connect message");
            return;
        }
        auto code = MqttConnack.Code.ACCEPTED;
        if(connect.isBadClientId) {
            code = MqttConnack.Code.BAD_ID;
        }

        new MqttConnack(code).cerealise!(b => connection.write(b));
    }

    void subscribe(MqttConnection connection, in ushort msgId, in string[] topics) {
        enum qos = 0;
        subscribe(connection, msgId, topics.map!(a => MqttSubscribe.Topic(a, qos)).array);
    }

    void subscribe(MqttConnection connection, in ushort msgId, in MqttSubscribe.Topic[] topics) {
        const qos = topics.map!(a => a.qos).array;
        new MqttSuback(msgId, qos).cerealise!(b => connection.write(b));
        _broker.subscribe(connection, topics);
    }

    void unsubscribe(MqttConnection connection) {
        _broker.unsubscribe(connection);
    }

    void unsubscribe(MqttConnection connection, in ushort msgId, in string[] topics) {
        new MqttUnsuback(msgId).cerealise!(b => connection.write(b));
        _broker.unsubscribe(connection, topics);
    }

    void publish(in string topic, in string payload) {
        publish(topic, cast(ubyte[])payload);
    }

    void publish(in string topic, in ubyte[] payload) {
        _broker.publish(topic, payload);
    }

    void ping(MqttConnection connection) const {
        connection.write((new MqttPingResp).encode);
    }

    @property void useCache(bool u) {
        _broker.useCache = u;
    }


private:

    MqttBroker _broker;
}

interface MqttInput {
    void read(ubyte[] bytes);
}

class MqttConnection: MqttSubscriber, MqttInput {
    override void newMessage(in string topic, in ubyte[] payload) {
        new MqttPublish(topic, payload).cerealise!(b => write(cast(immutable)b));
    }

    override void read(ubyte[] bytes) {
    }
    abstract void write(in ubyte[] bytes);
    abstract void disconnect();
}
