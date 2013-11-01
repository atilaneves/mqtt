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
        //const qos = array(map!(a => a.qos)(topics));
        //TODO: actually use QOS, for now just 0
        const qos = array(map!(a => cast(ubyte)0)(topics));
        const suback = new MqttSuback(msgId, qos);
        connection.write(suback.encode());
        _broker.subscribe(connection, topics);
    }

    void publish(in string topic, in string payload) {
        _broker.publish(topic, payload);
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
    void disconnect() {}

    MqttConnect connectMessage;
}
