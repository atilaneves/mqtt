module mqttd.message;


import mqttd.server;
import cerealed;
import std.stdio;
import std.algorithm;

enum MqttType {
    RESERVED1   = 0,
    CONNECT     = 1,
    CONNACK     = 2,
    PUBLISH     = 3,
    PUBACK      = 4,
    PUBREC      = 5,
    PUBREL      = 6,
    PUBCOMP     = 7,
    SUBSCRIBE   = 8,
    SUBACK      = 9,
    UNSUBSCRIBE = 10,
    UNSUBACK    = 11,
    PINGREQ     = 12,
    PINGRESP    = 13,
    DISCONNECT  = 14,
    RESERVED2   = 15
}

struct MqttFixedHeader {
public:
    enum SIZE = 2;

    @Bits!4 MqttType type;
    @Bits!1 bool dup;
    @Bits!2 ubyte qos;
    @Bits!1 bool retain;
    @NoCereal uint remaining;


    void postBlit(Cereal)(ref Cereal cereal) if(isCerealiser!Cereal) {
        setRemainingSize(cereal);
    }

    void postBlit(Cereal)(ref Cereal cereal) if(isDecerealiser!Cereal) {
        remaining = getRemainingSize(cereal);
    }

    mixin assertHasPostBlit!MqttFixedHeader;

private:

    uint getRemainingSize(Cereal)(ref Cereal cereal) {
        //algorithm straight from the MQTT spec
        int multiplier = 1;
        uint value = 0;
        ubyte digit;
        do {
            cereal.grain(digit);
            value += (digit & 127) * multiplier;
            multiplier *= 128;
        } while((digit & 128) != 0);

        return value;
    }

    void setRemainingSize(Cereal)(ref Cereal cereal) const {
        //algorithm straight from the MQTT spec
        ubyte[] digits;
        uint x = remaining;
        do {
            ubyte digit = x % 128;
            x /= 128;
            if(x > 0) {
                digit = digit | 0x80;
            }
            digits ~= digit;
        } while(x > 0);

        foreach(b; digits) cereal.grain(b);
    }
}

class MqttMessage {
    void handle(MqttServer server, MqttConnection connection) const {}
}

class MqttConnect: MqttMessage {
public:

    this(MqttFixedHeader header) {
        this.header = header;
    }

    void postBlit(Cereal)(ref Cereal cereal) {
        if(hasWill) cereal.grain(willTopic);
        if(hasWill) cereal.grain(willMessage);
        if(hasUserName) cereal.grain(userName);
        if(hasPassword) cereal.grain(password);
    }

    mixin assertHasPostBlit!MqttConnect;

    @property bool isBadClientId() const { return clientId.length < 1 || clientId.length > 23; }

    override void handle(MqttServer server, MqttConnection connection) const {
        server.newConnection(connection, this);
    }

    MqttFixedHeader header;
    string protoName;
    ubyte protoVersion;
    @Bits!1 bool hasUserName;
    @Bits!1 bool hasPassword;
    @Bits!1 bool hasWillRetain;
    @Bits!2 ubyte willQos;
    @Bits!1 bool hasWill;
    @Bits!1 bool hasClear;
    @Bits!1 bool reserved;
    ushort keepAlive;
    string clientId;
    @NoCereal string willTopic;
    @NoCereal string willMessage;
    @NoCereal string userName;
    @NoCereal string password;
}

class MqttConnack: MqttMessage {

    enum Code: byte {
        ACCEPTED = 0,
        BAD_VERSION = 1,
        BAD_ID = 2,
        SERVER_UNAVAILABLE = 3,
        BAD_USER_OR_PWD = 4,
        NO_AUTH = 5,
    }

    this() {
        header = MqttFixedHeader(MqttType.CONNACK, false, 0, false, 2);
    }

    this(Code code) {
        this.code = code;
        this();
    }

    MqttFixedHeader header;
    ubyte reserved;
    Code code;
}


class MqttPublish: MqttMessage {
public:
    this(MqttFixedHeader header) {
        this.header = header;
    }

    this(in string topic, in ubyte[] payload, ushort msgId = 0) {
        this(false, 0, false, topic, payload, msgId);
    }

    this(in bool dup, in ubyte qos, in bool retain, in string topic, in ubyte[] payload, in ushort msgId = 0) {
        const topicLen = cast(uint)topic.length + 2; //2 for length
        auto remaining = qos ? topicLen + 2 /*msgId*/ : topicLen;
        remaining += payload.length;

        this.header = MqttFixedHeader(MqttType.PUBLISH, dup, qos, retain, remaining);
        this.topic = topic;
        this.payload = payload.dup;
        this.msgId = msgId;
        import cerealed.cerealiser;
        auto enc = Cerealiser();
        this.postBlit(enc);
    }

    void postBlit(Cereal)(ref Cereal cereal) {
        auto payloadLen = header.remaining - (topic.length + MqttFixedHeader.SIZE);
        if(header.qos) {
            static if(Cereal.type == CerealType.ReadBytes) {
                if(header.remaining < 7) {
                    stderr.writeln("Error: PUBLISH message with QOS but no message ID");
                } else {
                    cereal.grain(msgId);
                    payloadLen -= 2;
                }
            } else {
                cereal.grain(msgId);
                payloadLen -= 2;
            }
        }

        static if(Cereal.type == CerealType.ReadBytes) payload.length = payloadLen;
        foreach(ref b; payload) cereal.grain(b);
    }

    mixin assertHasPostBlit!MqttPublish;

    override void handle(MqttServer server, MqttConnection connection) const {
        server.publish(topic, payload);
    }

    MqttFixedHeader header;
    string topic;
    @NoCereal ubyte[] payload;
    @NoCereal ushort msgId;
}


class MqttSubscribe: MqttMessage {
public:
    this(MqttFixedHeader header) {
        this.header = header;
    }

    override void handle(MqttServer server, MqttConnection connection) const {
        server.subscribe(connection, msgId, topics);
    }

    static struct Topic {
        string topic;
        ubyte qos;
    }

    MqttFixedHeader header;
    ushort msgId;
    @RawArray Topic[] topics;
}

class MqttSuback: MqttMessage {
public:

    this(MqttFixedHeader header) {
        this.header = header;
    }

    this(in ushort msgId, in ubyte[] qos) {
        this.header = MqttFixedHeader(MqttType.SUBACK, false, 0, false, cast(uint)qos.length + 2);
        this.msgId = msgId;
        this.qos = qos.dup;
    }

    MqttFixedHeader header;
    ushort msgId;
    @RawArray ubyte[] qos;
}

class MqttUnsubscribe: MqttMessage {
    this(MqttFixedHeader header) {
        this.header = header;
    }

    override void handle(MqttServer server, MqttConnection connection) const {
        server.unsubscribe(connection, msgId, topics);
    }

    MqttFixedHeader header;
    ushort msgId;
    @RawArray string[] topics;
}

class MqttUnsuback: MqttMessage {
    this(in ushort msgId) {
        this.header = MqttFixedHeader(MqttType.UNSUBACK, false, 0, false, 2);
        this.msgId = msgId;
    }

    this(MqttFixedHeader header) {
        this.header = header;
    }

    MqttFixedHeader header;
    ushort msgId;
}

class MqttDisconnect: MqttMessage {
    override void handle(MqttServer server, MqttConnection connection) const {
        server.unsubscribe(connection);
        connection.disconnect();
    }
}

class MqttPingReq: MqttMessage {
    override void handle(MqttServer server, MqttConnection connection) const {
        server.ping(connection);
    }
}

class MqttPingResp: MqttMessage {
    @property const(ubyte[]) encode() const {
        return [0xd0, 0x00];
    }
}
