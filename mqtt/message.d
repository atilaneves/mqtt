module mqtt.message;

import cerealed.cerealiser;
import cerealed.decerealiser;

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
    MqttType type;
    bool dup;
    ubyte qos;
    bool retain;
    uint remaining;
    const ubyte[] remainingBytes;

    this(MqttType type, bool dup, ubyte qos, bool retain, uint remaining = 0) {
        this.type = type;
        this.dup = dup;
        this.qos = qos;
        this.retain = retain;
        this.remaining = remaining;
    }

    this(ubyte[] bytes) {
        auto cereal = new Decerealiser(bytes);
        _byte1 = cereal.value!ubyte();

        type = cast(MqttType)(_byte1 >> 4);
        dup = cast(bool)(_byte1 & 0x04);
        qos = (_byte1 & 0x06) >> 1;
        retain = cast(bool)(_byte1 & 0x01);

        remaining = getRemainingSize(cereal);

        remainingBytes = cereal.bytes;
    }

    auto encode() const {
        auto cereal = new Cerealiser;
        ubyte byte1 = cast(ubyte)((type << 4) | ((cast(ubyte)dup) << 3) | (qos << 1) | (cast(ubyte)retain));
        cereal ~= byte1;
        setRemainingSize(cereal);
        return cereal.bytes;
    }

    @property const(ubyte[]) bytes() const { return remainingBytes; }

private:
    ubyte _byte1;

    uint getRemainingSize(Decerealiser cereal) {
        int multiplier = 1;
        uint value = 0;
        ubyte digit;
        do {
            digit = cereal.value!ubyte;
            value += (digit & 127) * multiplier;
            multiplier *= 128;
        } while((digit & 128) != 0);

        return value;
    }

    void setRemainingSize(Cerealiser cereal) const {
        ubyte[] digits;
        uint x = remaining;
        do {
            ubyte digit = x % 128;
            x/= 128;
            if(x > 0) {
                digit = digit | 0x80;
            }
            digits ~= digit;
        } while(x > 0);
        foreach(b; digits) {
            cereal ~= b;
        }
    }
}

class MqttMessage {
    this(MqttFixedHeader header) {
        fixedHeader = header;
    }
    MqttFixedHeader fixedHeader;
}

class MqttConnect: MqttMessage {
public:
    this(MqttFixedHeader header) {
        super(header);
        auto cereal = new Decerealiser(fixedHeader.bytes);
        protoName = cereal.value!string;
        protoVersion = cereal.value!ubyte;
        ubyte flags = cereal.value!ubyte;
        hasUserName = cast(bool)(flags & 0x80);
        hasPassword = cast(bool)(flags & 0x40);
        hasWillRetain = cast(bool)(flags & 0x20);
        willQos = (flags & 0x18) >> 3;
        hasWill = cast(bool)(flags & 0x04);
        hasClear = cast(bool)(flags & 0x02);
        keepAlive = cereal.value!ushort;
        clientId = cereal.value!string;
        if(hasWill) willTopic = cereal.value!string;
        if(hasWill) willMessage = cereal.value!string;
        if(hasUserName) userName = cereal.value!string;
        if(hasPassword) password = cereal.value!string;
    }

    @property bool isBadClientId() const { return clientId.length > 23; }

    string protoName;
    ubyte protoVersion;
    ushort keepAlive;
    string clientId;
    bool hasUserName;
    bool hasPassword;
    bool hasWillRetain;
    ubyte willQos;
    bool hasWill;
    bool hasClear;
    string willTopic;
    string willMessage;
    string userName;
    string password;
}

class MqttConnack: MqttMessage {
    this(MqttFixedHeader header) {
        super(header);
    }
}
