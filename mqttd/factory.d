module mqttd.factory;


import mqttd.message;
import mqttd.server;
import cerealed.decerealiser;
import std.stdio;


struct MqttFactory {
    static this() {
        with(MqttType) {
            registerType!MqttConnect(CONNECT);
            registerType!MqttConnack(CONNACK);
            registerType!MqttPublish(PUBLISH);
            registerType!MqttSubscribe(SUBSCRIBE);
            registerType!MqttSuback(SUBACK);
            registerType!MqttUnsubscribe(UNSUBSCRIBE);
            registerType!MqttUnsuback(UNSUBACK);
            registerType!MqttPingReq(PINGREQ);
            registerType!MqttPingResp(PINGRESP);
            registerType!MqttDisconnect(DISCONNECT);
        }
    }

    static MqttMessage create(in ubyte[] bytes) {

        auto cereal = Decerealiser(bytes);
        auto fixedHeader = cereal.value!MqttFixedHeader;

        if(!fixedHeader.check(bytes, cereal.bytes.length)) {
            return null;
        }

        cereal.reset(); //so that the created MqttMessage can re-read the header

        if(fixedHeader.type !in _msgCreators) {
            stderr.writeln("Unknown MQTT message type: ", fixedHeader.type);
            return null;
        }

        return _msgCreators[fixedHeader.type](fixedHeader, cereal);
    }

    static void handleMessage(in ubyte[] bytes, MqttServer server, MqttConnection connection) {
    }

private:

    alias MessageCreator = MqttMessage function(MqttFixedHeader, Decerealiser);
    static MessageCreator[MqttType] _msgCreators;

    static void registerType(T)(MqttType type) {
        _msgCreators[type] = (header, cereal) {
            return cereal.value!T(header);
        };
    }
}
