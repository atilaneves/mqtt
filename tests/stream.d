import unit_threaded.check;
import mqtt.stream;
import mqtt.message;


void testMqttInTwoPackets() {
    ubyte[] bytes1 = [ 0x3c, 0x0f, //fixed header
                       0x00, 0x03, 't', 'o', 'p', //topic name
                       0x00, 0x21, //message ID
                       'b', 'o', 'r' ]; //1st part of payload
    auto stream = MqttStream(bytes1);
    checkFalse(stream.isDone());
    checkNull(stream.createMessage());

    ubyte[] bytes2 = [ 'o', 'r', 'o', 'o', 'n']; //2nd part of payload
    stream ~= bytes2;
    checkTrue(stream.isDone());
    const publish = cast(MqttPublish)stream.createMessage();
    checkNotNull(publish);
}


void testTwoMqttInThreePackets() {
    ubyte[] bytes1 = [ 0x3c, 0x0f, //fixed header
                       0x00, 0x03, 't', 'o', 'p', //topic name
                       0x00, 0x21, //message ID
                       'a', 'b', 'c' ]; //1st part of payload
    import std.stdio;
    writeln("creating stream");
    auto stream = MqttStream(bytes1);
    checkFalse(stream.isDone());
    checkNull(stream.createMessage());
    checkFalse(stream.empty());

    ubyte[] bytes2 = [ 'd', 'e', 'f', 'g', 'h']; //2nd part of payload
    writeln("Assigning to stream");
    stream ~= bytes2;
    checkTrue(stream.isDone());
    const publish = cast(MqttPublish)stream.createMessage();
    checkNotNull(publish);
    checkTrue(stream.empty());

    ubyte[] bytes3 = [0xe0, 0x00];
    writeln("Assinging to stream");
    stream ~= bytes3;
    checkFalse(stream.empty());
    const disconnect = cast(MqttDisconnect)stream.createMessage();
    checkNotNull(disconnect);
    checkTrue(stream.empty());
}
