module mqtt.factory;


import mqtt.message;
import std.stdio;


struct MqttFactory {
    static MqttMessage create(in ubyte[] bytes) {
        const fixedHeader = MqttFixedHeader(bytes);
        switch(fixedHeader.type) {
        case MqttType.CONNECT:
            return new MqttConnect(fixedHeader);
        case MqttType.CONNACK:
            return new MqttConnack(fixedHeader);
        case MqttType.PUBLISH:
            return new MqttPublish(fixedHeader);
        case MqttType.SUBSCRIBE:
            return new MqttSubscribe(fixedHeader);
        default:
            stderr.writeln("Unknown MQTT message type ", fixedHeader.type);
            return null;
        }
    }
}
