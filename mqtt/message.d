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
    }

    auto encode() const {
        auto cereal = new Cerealiser;
        ubyte byte1 = cast(ubyte)((type << 4) | ((cast(ubyte)dup) << 3) | (qos << 1) | (cast(ubyte)retain));
        cereal ~= byte1;
        setRemainingSize(cereal);
        return cereal.bytes;
    }

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
