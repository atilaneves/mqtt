module mqtt.factory;


import mqtt.message;


struct MqttFactory {
    static MqttMessage create(ubyte[] bytes) {
        const fixedHeader = MqttFixedHeader(bytes);
        switch(fixedHeader.type) {
        case MqttType.CONNECT:
            return new MqttConnect(fixedHeader);
       case MqttType.CONNACK:
            return new MqttConnack(fixedHeader);
        default:
            return null;
        }
    }
}
