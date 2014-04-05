module mqttd.factory;


import mqttd.message;
import cerealed.decerealiser;
import std.stdio;


struct MqttFactory {
    static MqttMessage create(in ubyte[] bytes) {
        auto cereal = Decerealiser(bytes);
        auto fixedHeader = cereal.value!MqttFixedHeader;

        if(!headerChecks(fixedHeader, bytes, cereal.bytes.length)) {
            return null;
        }

        cereal.reset(); //so the messages created below can re-read the header

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

private:
    static bool headerChecks(in MqttFixedHeader fixedHeader, in ubyte[] bytes, in ulong remainingLength) {
        if(fixedHeader.remaining < remainingLength) {
            stderr.writeln("Wrong MQTT remaining size ", cast(int)fixedHeader.remaining,
                           ". Real remaining size: ", remainingLength);
        }

        const mqttSize = fixedHeader.remaining + MqttFixedHeader.SIZE;
        if(mqttSize != bytes.length) {
            stderr.writeln("Malformed packet. Actual size: ", bytes.length,
                           ". Advertised size: ", mqttSize, " (r ", fixedHeader.remaining ,")");
            stderr.writeln("Packet:");
            stderr.writefln("%(0x%x %)", bytes);
            return false;
        }

        return true;
    }
}
