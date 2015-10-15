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

        auto cereal = Decerealiser(bytes);
        auto fixedHeader = cereal.value!MqttFixedHeader;

        if(!fixedHeader.check(bytes, cereal.bytes.length)) {
            return;
        }

        cereal.reset(); //so that the created MqttMessage can re-read the header

        if(fixedHeader.type !in _msgCreators) {
            stderr.writeln("Unknown MQTT message type: ", fixedHeader.type);
            return;
        }

        auto msg = _msgCreators[fixedHeader.type](fixedHeader, cereal);
        msg.handle(server, connection);
        switch(fixedHeader.type) with(MqttType) {
            case CONNECT:
                handleM!MqttConnect(fixedHeader, cereal, server, connection);
                break;
            case CONNACK:
                handleM!MqttConnack(fixedHeader, cereal, server, connection);
                break;
            case PUBLISH:
                handleM!MqttPublish(fixedHeader, cereal, server, connection);
                break;
            case SUBSCRIBE:
                handleM!MqttSubscribe(fixedHeader, cereal, server, connection);
                break;
            case SUBACK:
                handleM!MqttSuback(fixedHeader, cereal, server, connection);
                break;
            case UNSUBSCRIBE:
                handleM!MqttUnsubscribe(fixedHeader, cereal, server, connection);
                break;
            case UNSUBACK:
                handleM!MqttUnsuback(fixedHeader, cereal, server, connection);
                break;
            case PINGREQ:
                handleM!MqttPingReq(fixedHeader, cereal, server, connection);
                break;
            case PINGRESP:
                handleM!MqttPingResp(fixedHeader, cereal, server, connection);
                break;
            case DISCONNECT:
                handleM!MqttDisconnect(fixedHeader, cereal, server, connection);
                break;
            default:
                import std.conv;
                throw new Exception(text("Unsupported MQTT type ", fixedHeader.type));
        }
    }

private:

    alias MessageCreator = MqttMessage function(MqttFixedHeader, Decerealiser);
    alias MessageHandler = void function(MqttFixedHeader, Decerealiser, MqttServer, MqttConnection);
    static MessageCreator[MqttType] _msgCreators;
    static MessageHandler[MqttType] _msgHandlers;

    static void registerType(T)(MqttType type) {
        _msgCreators[type] = (header, cereal) {
            return cereal.value!T(header);
        };
    }

    static void handleM(T)(MqttFixedHeader header, Decerealiser cereal,
                                   MqttServer server, MqttConnection connection) {
        auto msg = new T(header);
        msg.handle(server, connection);
    }
}
