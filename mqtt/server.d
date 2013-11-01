module mqtt.server;


import mqtt.message;
import mqtt.factory;
import std.stdio;


abstract class MqttServer {
    void newConnection(MqttConnection connection) {
        const connect = connection.connectMessage;
        auto code = MqttConnack.Code.ACCEPTED;
        if(connect.clientId.length < 1 || connect.clientId.length > 23) {
            code = MqttConnack.Code.BAD_ID;
        }

        connection.write((new MqttConnack(code)).encode());
    }
}

abstract class MqttConnection {
    this(in ubyte[] bytes) {
        connectMessage = cast(MqttConnect)MqttFactory.create(bytes);
        if(connectMessage is null) {
            stderr.writeln("Invalid connect message");
        }
    }

    final void write(in ubyte[] bytes) {
        newMessage(MqttFactory.create(bytes));
    }

    void newMessage(MqttMessage msg);

    MqttConnect connectMessage;
}
