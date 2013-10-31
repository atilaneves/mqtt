module mqtt.utf8;


import cerealed.cerealiser;
import cerealed.decerealiser;
import std.traits;

struct MqttString {
    this(in string str) {
        _string = str;
    }

    this(in ubyte[] bytes) {
        auto cereal = new Decerealiser(bytes);
        _string = cereal.value!string;
    }

    const(ubyte[]) encode() const {
        auto cereal = new Cerealiser();
        cereal ~= _string;
        return cereal.bytes;
    }

    @property string str() const { return _string; }

private:

    string _string;
}
