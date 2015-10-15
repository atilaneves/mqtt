module mqttd.message;


import mqttd.server;
import cerealed;
import std.stdio;
import std.algorithm;
import std.stdio;


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

    bool check(in ubyte[] bytes, in ulong cerealBytesLength) const {
        if(remaining < cerealBytesLength) {
            stderr.writeln("Wrong MQTT remaining size ", cast(int)remaining,
                           ". Real remaining size: ", cerealBytesLength);
        }

        const mqttSize = remaining + SIZE;
        if(mqttSize != bytes.length) {
            stderr.writeln("Malformed packet. Actual size: ", bytes.length,
                    ". Advertised size: ", mqttSize, " (r ", remaining ,")");
            stderr.writeln("Packet:\n", "%(0x%x %)", bytes);

            return false;
        }

        return true;
    }

    void postBlit(Cereal)(ref Cereal cereal) if(isCerealiser!Cereal) {
        setRemainingSize(cereal);
    }

    void postBlit(Cereal)(ref Cereal cereal) if(isDecerealiser!Cereal) {
        remaining = getRemainingSize(cereal);
    }

    mixin assertHasPostBlit!MqttFixedHeader;

private:

    uint getRemainingSize(Cereal)(ref Cereal cereal) if(isDecerealiser!Cereal) {
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

    void setRemainingSize(Cereal)(ref Cereal cereal) const if(isCerealiser!Cereal) {
        remaining <= 127 ? setRemainingSizeOneByte(cereal) : setRemainingSizeMultiByte(cereal);
    }

    void setRemainingSizeOneByte(Cereal)(ref Cereal cereal) const if(isCerealiser!Cereal) {
        cereal ~= cast(ubyte)remaining;
    }

    void setRemainingSizeMultiByte(Cereal)(ref Cereal cereal) const if(isCerealiser!Cereal) {
        //algorithm straight from the MQTT spec, modified for speed optimisation
        enum maxDigits = 4;
        static ubyte[maxDigits] digitStore; //optimisation for speed, no heap allocations
        ubyte[] digits = digitStore[0..0];
        uint x = remaining;
        do {
            ubyte digit = x % 128;
            x /= 128;
            if(x > 0) {
                digit |= 0x80;
            }
            digits ~= digit;
        } while(x > 0);

        foreach(b; digits) cereal.grain(b);
    }

}


struct MqttConnect {
public:

    this(MqttFixedHeader header) {
        this.header = header;
    }

    void postBlit(Cereal)(ref Cereal cereal) {
        if(hasWill)     cereal.grain(willTopic);
        if(hasWill)     cereal.grain(willMessage);
        if(hasUserName) cereal.grain(userName);
        if(hasPassword) cereal.grain(password);
    }

    mixin assertHasPostBlit!MqttConnect;

    @property bool isBadClientId() const { return clientId.length < 1 || clientId.length > 23; }

    void handle(MqttServer server, MqttConnection connection) const {
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

struct MqttConnack {

    enum Code: byte {
        ACCEPTED = 0,
        BAD_VERSION = 1,
        BAD_ID = 2,
        SERVER_UNAVAILABLE = 3,
        BAD_USER_OR_PWD = 4,
        NO_AUTH = 5,
    }

    this(MqttFixedHeader header = MqttFixedHeader(MqttType.CONNACK, false, 0, false, 2)) {
        this.header = header;
    }

    this(Code code) {
        this.code = code;
        this();
    }

    MqttFixedHeader header;
    ubyte reserved;
    Code code;
}


struct MqttPublish {
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
        //only safe if we never change it
        this.payload = cast(ubyte[])payload;
        this.msgId = msgId;
        this.cantDecerealise = true; //because of the cast
    }

    void postBlit(Cereal)(ref Cereal cereal) {
        static if(isDecerealiser!Cereal) {
            assert(!cantDecerealise, "Cannot decerealise if constructed from payload");
        }

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

        static if(isDecerealiser!Cereal) payload.length = payloadLen;
        foreach(ref b; payload) cereal.grain(b);
    }

    mixin assertHasPostBlit!MqttPublish;

    void handle(MqttServer server, MqttConnection connection) const {
        server.publish(topic, payload);
    }

    MqttFixedHeader header;
    string topic;
    @NoCereal ubyte[] payload;
    @NoCereal ushort msgId;
    @NoCereal bool cantDecerealise;
}


struct MqttSubscribe {
public:
    this(MqttFixedHeader header) {
        if(header.qos != 1) {
            stderr.writeln("SUBSCRIBE message with qos ", header.qos, ", should be 1");
        }
        this.header = header;
    }

    void handle(MqttServer server, MqttConnection connection) const {
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

struct MqttSuback {
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

struct MqttUnsubscribe {
    this(MqttFixedHeader header) {
        this.header = header;
    }

    void handle(MqttServer server, MqttConnection connection) const {
        server.unsubscribe(connection, msgId, topics);
    }

    MqttFixedHeader header;
    ushort msgId;
    @RawArray string[] topics;
}

struct MqttUnsuback {
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

struct MqttDisconnect {
    this(MqttFixedHeader) {}
    void handle(MqttServer server, MqttConnection connection) const {
        server.unsubscribe(connection);
        connection.disconnect();
    }
}

struct MqttPingReq {
    this(MqttFixedHeader = MqttFixedHeader()) {}
    void handle(MqttServer server, MqttConnection connection) const {
        server.ping(connection);
    }
}

struct MqttPingResp {
    this(MqttFixedHeader = MqttFixedHeader()) {}
    @property const(ubyte[]) encode() const {
        return [0xd0, 0x00];
    }
}
