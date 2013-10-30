module mqtt.message;

import cerealed.cerealiser;
import cerealed.decerealiser;


struct MqttMessage {
public:
    ubyte type;
    bool dup;
    ubyte qos;
    bool retain;
    ubyte remaining;

    this(ubyte type, bool dup, ubyte qos, bool retain, ubyte remaining = 0) {
        this.type = type;
        this.dup = dup;
        this.qos = qos;
        this.retain = retain;
        this.remaining = remaining;
    }

    this(ubyte[] bytes) {
        auto cereal = new Decerealiser(bytes);
        _byte1 = cereal.value!ubyte();
        remaining = cereal.value!ubyte();
        type = _byte1 >> 4;
        dup = cast(bool)(_byte1 & 0x04);
        qos = (_byte1 & 0x06) >> 1;
        retain = cast(bool)(_byte1 & 0x01);
    }

    auto encode() const {
        auto cereal = new Cerealiser;
        ubyte byte1 = cast(ubyte)((type << 4) | ((cast(ubyte)dup) << 3) | (qos << 1) | (cast(ubyte)retain));
        cereal ~= byte1;
        cereal ~= remaining;
        return cereal.bytes;
    }

private:
    ubyte _byte1;
}
