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
