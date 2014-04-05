module mqttd.factory;


import mqttd.message;
import cerealed.decerealiser;
import std.stdio;


struct MqttFactory {
    static MqttMessage create(in ubyte[] bytes) {

        MqttFixedHeader fixedHeader = void;

        try {
            fixedHeader = MqttFixedHeader(bytes);
        } catch(MqttPacketException ex) {
            stderr.writeln("Error receiving MQTT packet: ", ex.msg);
            return null;
        }

        switch(fixedHeader.type) with(MqttType) {
        case CONNECT:
            return fixedHeader.value!MqttConnect;
        case CONNACK:
            return fixedHeader.value!MqttConnack;
        case PUBLISH:
            return fixedHeader.value!MqttPublish;
        case SUBSCRIBE:
            return fixedHeader.value!MqttSubscribe;
        case SUBACK:
            return fixedHeader.value!MqttSuback;
        case UNSUBSCRIBE:
            return fixedHeader.value!MqttUnsubscribe;
        case UNSUBACK:
            return fixedHeader.value!MqttUnsuback;
        case PINGREQ:
            return fixedHeader.value!MqttPingReq;
        case PINGRESP:
            return fixedHeader.value!MqttPingResp;
        case DISCONNECT:
            return fixedHeader.value!MqttDisconnect;
        default:
            stderr.writeln("Unknown MQTT message type: ", fixedHeader.type);
            return null;
        }
    }
}
