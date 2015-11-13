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

enum isMqttServer(S, C) = isMqttConnection!C && is(typeof(() {
    auto server = S.init;
    auto connection = C.init;
    server.newConnection(connection, MqttConnect());
    ushort msgId;
    string[] topics;
    server.subscribe(connection, msgId, topics);
    MqttSubscribe.Topic[] topics2;
    server.subscribe(connection, msgId, topics);
    server.unsubscribe(connection);
    server.unsubscribe(connection, msgId, topics);
    string topic;
    ubyte[] payload;
    server.publish(topic, payload);
    server.ping(connection);
    server.useCache = false;
}));


class CMqttServer(T) if(isMqttConnection!T) {

    alias Connection = T;

    final void newConnection(Connection connection, in MqttConnect connect) {
        auto code = MqttConnack.Code.ACCEPTED;
        if(connect.isBadClientId) {
            code = MqttConnack.Code.BAD_ID;
        }

        MqttConnack(code).cerealise!(b => connection.write(b));
    }

    final void subscribe(Connection connection, in ushort msgId, in string[] topics) {
        enum qos = 0;
        subscribe(connection, msgId, topics.map!(a => MqttSubscribe.Topic(a, qos)).array);
    }

    final void subscribe(Connection connection, in ushort msgId, in MqttSubscribe.Topic[] topics) {
        const qos = topics.map!(a => a.qos).array;
        MqttSuback(msgId, qos).cerealise!(b => connection.write(b));
        _broker.subscribe(connection, topics);
    }

    final void unsubscribe(Connection connection) {
        _broker.unsubscribe(connection);
    }

    final void unsubscribe(Connection connection, in ushort msgId, in string[] topics) {
        MqttUnsuback(msgId).cerealise!(b => connection.write(b));
        _broker.unsubscribe(connection, topics);
    }

    final void publish(in string topic, in string payload) {
        publish(topic, cast(ubyte[])payload);
    }

    final void publish(in string topic, in ubyte[] payload) {
        _broker.publish(topic, payload);
    }

    final void ping(Connection connection) const {
        static MqttPingResp resp;
        connection.write(resp.encode);
    }

    final @property void useCache(bool u) {
        _broker.useCache = u;
    }

private:

    MqttBroker!T _broker;


    static assert(isMqttServer!(CMqttServer, T));
}


mixin template MqttConnection() {
    final void newMessage(in string topic, in ubyte[] payload) {
        import cerealed;
        MqttPublish(topic, payload).cerealise!(b => write(cast(immutable)b));
    }

    final void newMessage(in ubyte[] bytes) {
        write(bytes);
    }

    final void read(ubyte[] bytes) {}
}
