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

        switch(fixedHeader.type) with(MqttType) {
            case CONNECT:
                handleMessage!MqttConnect(fixedHeader, cereal, server, connection);
                break;
            case CONNACK:
                handleMessage!MqttConnack(fixedHeader, cereal, server, connection);
                break;
            case PUBLISH:
                handleMessage!MqttPublish(fixedHeader, cereal, server, connection);
                break;
            case SUBSCRIBE:
                handleMessage!MqttSubscribe(fixedHeader, cereal, server, connection);
                break;
            case SUBACK:
                handleMessage!MqttSuback(fixedHeader, cereal, server, connection);
                break;
            case UNSUBSCRIBE:
                handleMessage!MqttUnsubscribe(fixedHeader, cereal, server, connection);
                break;
            case UNSUBACK:
                handleMessage!MqttUnsuback(fixedHeader, cereal, server, connection);
                break;
            case PINGREQ:
                handleMessage!MqttPingReq(fixedHeader, cereal, server, connection);
                break;
            case PINGRESP:
                handleMessage!MqttPingResp(fixedHeader, cereal, server, connection);
                break;
            case DISCONNECT:
                handleMessage!MqttDisconnect(fixedHeader, cereal, server, connection);
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

    static void handleMessage(T)(MqttFixedHeader header, Decerealiser cereal,
                                 MqttServer server, MqttConnection connection) {
        // auto msg = new T(header);
        // cereal.grain(msg);
        auto msg = cereal.value!T(header);
        msg.handle(server, connection);
    }
}
