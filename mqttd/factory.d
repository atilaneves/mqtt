module mqttd.factory;


import mqttd.message;
import cerealed.decerealiser;
import std.stdio;


struct MqttFactory {
    static MqttMessage create(in ubyte[] bytes) {
        auto cereal = Decerealiser(bytes);
        auto fixedHeader = cereal.value!MqttFixedHeader;
        if(fixedHeader.remaining < cereal.bytes.length) {
            stderr.writeln("Wrong MQTT remaining size ", cast(int)fixedHeader.remaining,
                           ". Real remaining size: ", cereal.bytes.length);
        }

        const mqttSize = fixedHeader.remaining + MqttFixedHeader.SIZE;
        if(mqttSize != bytes.length) {
            stderr.writeln("Malformed packet. Actual size: ", bytes.length,
                           ". Advertised size: ", mqttSize, " (r ", fixedHeader.remaining ,")");
            stderr.writeln("Packet:");
            stderr.writefln("%(0x%x %)", bytes);
            return null;
        }

        cereal.reset(); //so the messages created below can re-read the header

        switch(fixedHeader.type) with(MqttType) {
        case CONNECT:
            return cereal.value!MqttConnect(fixedHeader);
        case CONNACK:
            return cereal.value!MqttConnack;
        case PUBLISH:
            return cereal.value!MqttPublish(fixedHeader);
        case SUBSCRIBE:
            if(fixedHeader.qos != 1) {
                stderr.writeln("SUBSCRIBE message with qos ", fixedHeader.qos, ", should be 1");
            }
            return cereal.value!MqttSubscribe(fixedHeader);
        case SUBACK:
            return cereal.value!MqttSuback(fixedHeader);
        case UNSUBSCRIBE:
            return cereal.value!MqttUnsubscribe(fixedHeader);
        case UNSUBACK:
            return cereal.value!MqttUnsuback(fixedHeader);
        case PINGREQ:
            return new MqttPingReq();
        case PINGRESP:
            return new MqttPingResp();
        case DISCONNECT:
            return new MqttDisconnect();
        default:
            stderr.writeln("Unknown MQTT message type: ", fixedHeader.type);
            return null;
        }
    }
}
