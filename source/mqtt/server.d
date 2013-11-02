module mqtt.server;


import mqtt.message;
import mqtt.factory;
import mqtt.broker;
import std.stdio;
import std.algorithm;
import std.array;

class MqttServer {
    void newConnection(MqttConnection connection) {
        const connect = connection.connectMessage;
        auto code = MqttConnack.Code.ACCEPTED;
        if(connect.isBadClientId) {
            code = MqttConnack.Code.BAD_ID;
        }

        connection.write((new MqttConnack(code)).encode());
        _connections ~= connection;
    }

    void subscribe(MqttConnection connection, in ushort msgId, in string[] topics) {
        subscribe(connection, msgId, array(map!(a => MqttSubscribe.Topic(a, 0))(topics)));
    }

    void subscribe(MqttConnection connection, in ushort msgId, in MqttSubscribe.Topic[] topics) {
        writeln("Subscribing to topic ", topics[0].topic, " with qos ", topics[0].qos);
        const qos = array(map!(a => a.qos)(topics));
        const suback = new MqttSuback(msgId, qos);
        connection.write(suback.encode());
        _broker.subscribe(connection, topics);
    }

    void publish(in string topic, in string payload) {
        writeln("Publishing ", topic, " : ", payload);
        _broker.publish(topic, payload);
    }

    void ping(MqttConnection connection) const {
        writeln("Ping");
        connection.write((new MqttPingResp()).encode());
    }


private:

    MqttBroker _broker;
    MqttConnection[] _connections;
}

class MqttConnection: MqttSubscriber {
    this(in ubyte[] bytes) {
        connectMessage = cast(MqttConnect)MqttFactory.create(bytes);
        if(connectMessage is null) {
            stderr.writeln("Invalid connect message");
        }
    }

    abstract void write(in ubyte[] bytes);
    abstract void disconnect();

    MqttConnect connectMessage;
}
