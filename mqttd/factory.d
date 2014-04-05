module mqttd.factory;


import mqttd.message;
import cerealed.decerealiser;
import std.stdio;


struct MqttFactory {
    static MqttMessage create(in ubyte[] bytes) {

        auto cereal = Decerealiser(bytes);
        auto fixedHeader = cereal.value!MqttFixedHeader;

        if(!fixedHeader.check(bytes, cereal.bytes.length)) {
            return null;
        }

        cereal.reset(); //so that the created MqttMessage can re-read the header

        switch(fixedHeader.type) with(MqttType) {
        case CONNECT:
            return cereal.value!MqttConnect(fixedHeader);
        case CONNACK:
            return cereal.value!MqttConnack(fixedHeader);
        case PUBLISH:
            return cereal.value!MqttPublish(fixedHeader);
        case SUBSCRIBE:
            return cereal.value!MqttSubscribe(fixedHeader);
        case SUBACK:
            return cereal.value!MqttSuback(fixedHeader);
        case UNSUBSCRIBE:
            return cereal.value!MqttUnsubscribe(fixedHeader);
        case UNSUBACK:
            return cereal.value!MqttUnsuback(fixedHeader);
        case PINGREQ:
            return cereal.value!MqttPingReq(fixedHeader);
        case PINGRESP:
            return cereal.value!MqttPingResp(fixedHeader);
        case DISCONNECT:
            return cereal.value!MqttDisconnect(fixedHeader);
        default:
            stderr.writeln("Unknown MQTT message type: ", fixedHeader.type);
            return null;
        }
    }
}
