module tests.stream;

import unit_threaded.check;
import mqttd.stream;
import mqttd.message;


void testMqttInTwoPackets() {
    ubyte[] bytes1 = [ 0x3c, 0x0f, //fixed header
                       0x00, 0x03, 't', 'o', 'p', //topic name
                       0x00, 0x21, //message ID
                       'b', 'o', 'r' ]; //1st part of payload
    auto stream = MqttStream(128);
    stream ~= bytes1;
    checkFalse(stream.hasMessages());
    checkNull(stream.createMessage());

    ubyte[] bytes2 = [ 'o', 'r', 'o', 'o', 'n']; //2nd part of payload
    stream ~= bytes2;
    checkTrue(stream.hasMessages());
    const publish = cast(MqttPublish)stream.createMessage();
    checkNotNull(publish);
}


void testTwoMqttInThreePackets() {
    ubyte[] bytes1 = [ 0x3c, 0x0f, //fixed header
                       0x00, 0x03, 't', 'o', 'p', //topic name
                       0x00, 0x21, //message ID
                       'a', 'b', 'c' ]; //1st part of payload
    auto stream = MqttStream(128);
    stream ~= bytes1;
    checkFalse(stream.hasMessages());
    checkNull(stream.createMessage());
    checkFalse(stream.empty());

    ubyte[] bytes2 = [ 'd', 'e', 'f', 'g', 'h']; //2nd part of payload
    stream ~= bytes2;
    checkTrue(stream.hasMessages());
    const publish = cast(MqttPublish)stream.createMessage();
    checkNotNull(publish);
    checkTrue(stream.empty());

    ubyte[] bytes3 = [0xe0, 0x00];
    stream ~= bytes3;
    checkFalse(stream.empty());
    const disconnect = cast(MqttDisconnect)stream.createMessage();
    checkNotNull(disconnect);
    checkTrue(stream.empty());
}


void testTwoMqttInOnePacket() {
   auto stream = MqttStream(128);
   checkFalse(stream.hasMessages());
   checkTrue(stream.empty());

   ubyte[] bytes1 = [ 0x3c ]; // half of header
   ubyte[] bytes2 = [ 0x0f, //2nd half fixed header
                     0x00, 0x03, 't', 'o', 'p', //topic name
                     0x00, 0x21, //message ID
                     'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', //payload
                     0xe0, 0x00, //header for disconnect
       ];
   stream ~= bytes1;
   checkFalse(stream.empty());
   checkFalse(stream.hasMessages());

   stream ~= bytes2;
   checkFalse(stream.empty());
   checkTrue(stream.hasMessages());

   const publish = cast(MqttPublish)stream.createMessage();
   checkNotNull(publish);
   checkFalse(stream.empty());
   checkTrue(stream.hasMessages());

   const disconnect = cast(MqttDisconnect)stream.createMessage();
   checkNotNull(disconnect);
   checkTrue(stream.empty());
}
