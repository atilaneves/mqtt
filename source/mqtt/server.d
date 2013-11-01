module mqtt.server;


import mqtt.message;
import mqtt.factory;
import mqtt.broker;
import std.stdio;
import std.algorithm;
import std.array;

class MqttServer {
    void newConnection(MqttConnection connection) {
        writeln("MqttServer new connection");
        const connect = connection.connectMessage;
        auto code = MqttConnack.Code.ACCEPTED;
        if(connect.clientId.length < 1 || connect.clientId.length > 23) {
            code = MqttConnack.Code.BAD_ID;
        }

        connection.write((new MqttConnack(code)).encode());
    }

    void subscribe(MqttConnection connection, in ushort msgId, in string[] topics) {
        subscribe(connection, msgId, array(map!(a => MqttSubscribe.Topic(a, 0))(topics)));
    }

    void subscribe(MqttConnection connection, in ushort msgId, in MqttSubscribe.Topic[] topics) {
        const qos = array(map!(a => a.qos)(topics));
        const suback = new MqttSuback(msgId, qos);
        connection.write(suback.encode());
        _broker.subscribe(connection, topics);
    }

    void publish(in string topic, in string payload) {
        _broker.publish(topic, payload);
    }


private:

    MqttBroker _broker;
}

class MqttConnection: MqttSubscriber {
    this(in ubyte[] bytes) {
        connectMessage = cast(MqttConnect)MqttFactory.create(bytes);
        if(connectMessage is null) {
            stderr.writeln("Invalid connect message");
        }
        writeln("Received connect message from client id ", connectMessage.clientId);
    }

    abstract void write(in ubyte[] bytes);

    MqttConnect connectMessage;
}
