module mqtt.server;

import mqtt.message;
import mqtt.factory;

abstract class MqttServer {
    void newConnection(MqttConnection connection) {
        const connack = new MqttConnack(MqttConnack.Code.ACCEPTED);
        connection.write(connack.encode());
    }
}

interface MqttConnection {
    final void write(in ubyte[] bytes) {
        newMessage(MqttFactory.create(bytes));
    }

    void newMessage(MqttMessage msg);
}
