module mqttd.server;


import mqttd.message;
import mqttd.factory;
import mqttd.broker;
import cerealed.cerealiser;
import std.stdio;
import std.algorithm;
import std.array;


private auto encode(T)(T msg, Cerealiser enc) {
    enc.reset();
    enc ~= msg;
    return enc.bytes;
}


class MqttServer {
    this() {
        _cereal = new Cerealiser;
    }

    void newConnection(MqttConnection connection, const MqttConnect connect) {
        if(!connect) {
            stderr.writeln("Invalid connect message");
            return;
        }
        auto code = MqttConnack.Code.ACCEPTED;
        if(connect.isBadClientId) {
            code = MqttConnack.Code.BAD_ID;
        }

        connection.write(encode(new MqttConnack(code), _cereal));
    }

    void subscribe(MqttConnection connection, in ushort msgId, in string[] topics) {
        enum qos = 0;
        subscribe(connection, msgId, array(map!(a => MqttSubscribe.Topic(a, qos))(topics)));
    }

    void subscribe(MqttConnection connection, in ushort msgId, in MqttSubscribe.Topic[] topics) {
        const qos = array(map!(a => a.qos)(topics));
        connection.write(encode(new MqttSuback(msgId, qos), _cereal));
        _broker.subscribe(connection, topics);
    }

    void unsubscribe(MqttConnection connection) {
        _broker.unsubscribe(connection);
    }

    void unsubscribe(MqttConnection connection, in ushort msgId, in string[] topics) {
        connection.write(encode(new MqttUnsuback(msgId), _cereal));
        _broker.unsubscribe(connection, topics);
    }

    void publish(in string topic, in string payload) {
        publish(topic, cast(ubyte[])payload);
    }

    void publish(in string topic, in ubyte[] payload) {
        _broker.publish(topic, payload);
    }

    void ping(MqttConnection connection) const {
        connection.write(new MqttPingResp().encode());
    }

    @property void useCache(bool u) {
        _broker.useCache = u;
    }


private:

    MqttBroker _broker;
    Cerealiser _cereal;
}


class MqttConnection: MqttSubscriber {
    this() {
        _cereal = new Cerealiser;
    }

    override void newMessage(in string topic, in ubyte[] payload) {
        write(cast(immutable)(encode(new MqttPublish(topic, payload), _cereal)));
    }

    void read(ubyte[] bytes) {
    }
    abstract void write(in ubyte[] bytes);
    abstract void disconnect();

private:

    Cerealiser _cereal;
}
