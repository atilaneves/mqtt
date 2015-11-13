module mqttd.factory;


import mqttd.message;
import mqttd.server;
import cerealed.decerealiser;
import std.stdio;


struct MqttFactory {
    static void handleMessage(T)(in ubyte[] bytes, CMqttServer!T server, T connection) if(isMqttConnection!T) {

        auto cereal = Decerealiser(bytes);
        auto fixedHeader = cereal.value!MqttFixedHeader;

        if(!fixedHeader.check(bytes, cereal.bytes.length)) {
            return;
        }

        cereal.reset(); //so that the created MqttMessage can re-read the header

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

    static void handleMessage(M, T)(MqttFixedHeader header, Decerealiser cereal,
                                    CMqttServer!T server, T connection) if(isMqttConnection!T) {
        static if(__traits(hasMember, M, "handle")) {
            auto msg = M(header);
            cereal.grain(msg);

            msg.handle(server, connection);
        }
    }
}
