module mqttd.factory;


import mqttd.message;
import cerealed;
import std.stdio;


struct MqttFactory {
    static MqttMessage create(in ubyte[] bytes) {
        auto cereal = new Decerealiser(bytes);
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

        cereal.reset();

        switch(fixedHeader.type) {
        case MqttType.CONNECT:
            return cereal.value!MqttConnect(fixedHeader);
        case MqttType.CONNACK:
            return cereal.value!MqttConnack;
        case MqttType.PUBLISH:
            return cereal.value!MqttPublish(fixedHeader);
        case MqttType.SUBSCRIBE:
            if(fixedHeader.qos != 1) {
                stderr.writeln("SUBSCRIBE message with qos ", fixedHeader.qos, ", should be 1");
            }
            return cereal.value!MqttSubscribe(fixedHeader);
        case MqttType.SUBACK:
            return cereal.value!MqttSuback(fixedHeader);
        case MqttType.PINGREQ:
            return new MqttPingReq();
        case MqttType.PINGRESP:
            return new MqttPingResp();
        case MqttType.DISCONNECT:
            return new MqttDisconnect();
        default:
            stderr.writeln("Unknown MQTT message type: ", fixedHeader.type);
            return null;
        }
    }
}
