module mqttd.factory;


import mqttd.message;
import std.stdio;


struct MqttFactory {
    static MqttMessage create(in ubyte[] bytes) {
        auto fixedHeader = MqttFixedHeader(bytes);
        const mqttSize = fixedHeader.remaining + MqttFixedHeader.SIZE;
        if(mqttSize != bytes.length) {
            stderr.writeln("Malformed packet. Actual size: ", bytes.length,
                           ". Advertised size: ", mqttSize, " (r ", fixedHeader.remaining ,")");
            stderr.writeln("Packet:");
            stderr.writefln("%(0x%x %)", bytes);
            return null;
        }
        switch(fixedHeader.type) {
        case MqttType.CONNECT:
            return new MqttConnect(fixedHeader);
        case MqttType.CONNACK:
            return new MqttConnack(fixedHeader);
        case MqttType.PUBLISH:
            return new MqttPublish(fixedHeader);
        case MqttType.SUBSCRIBE:
            return new MqttSubscribe(fixedHeader);
        case MqttType.SUBACK:
            return new MqttSuback(fixedHeader);
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
