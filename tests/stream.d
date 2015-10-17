module tests.stream;

import unit_threaded;
import mqttd.stream;
import mqttd.message;


void testMqttInTwoPackets() {
    ubyte[] bytes1 = [ 0x3c, 0x0f, //fixed header
                       0x00, 0x03, 't', 'o', 'p', //topic name
                       0x00, 0x21, //message ID
                       'b', 'o', 'r' ]; //1st part of payload
    auto stream = MqttStream2(128);
    stream ~= bytes1;
    stream.hasMessages.shouldBeFalse;

    ubyte[] bytes2 = [ 'o', 'r', 'o', 'o', 'n']; //2nd part of payload
    stream ~= bytes2;
    stream.hasMessages.shouldBeTrue;
    stream.allMessageBytes.shouldEqual([bytes1 ~ bytes2]);
}


void testTwoMqttInThreePackets() {
    ubyte[] bytes1 = [ 0x3c, 0x0f, //fixed header
                       0x00, 0x03, 't', 'o', 'p', //topic name
                       0x00, 0x21, //message ID
                       'a', 'b', 'c' ]; //1st part of payload
    auto stream = MqttStream2(128);
    stream ~= bytes1;
    shouldBeFalse(stream.hasMessages());

    ubyte[] bytes2 = [ 'd', 'e', 'f', 'g', 'h']; //2nd part of payload
    stream ~= bytes2;
    shouldBeTrue(stream.hasMessages());
    stream.allMessageBytes.shouldEqual([bytes1 ~ bytes2]);

    ubyte[] bytes3 = [0xe0, 0x00];
    stream ~= bytes3;
    stream.allMessageBytes.shouldEqual([bytes1 ~ bytes2, bytes3]);
}


// void testTwoMqttInOnePacket() {
//    auto stream = MqttStream(128);
//    shouldBeFalse(stream.hasMessages());
//    shouldBeTrue(stream.empty());

//    ubyte[] bytes1 = [ 0x3c ]; // half of header
//    ubyte[] bytes2 = [ 0x0f, //2nd half fixed header
//                      0x00, 0x03, 't', 'o', 'p', //topic name
//                      0x00, 0x21, //message ID
//                      'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', //payload
//                      0xe0, 0x00, //header for disconnect
//        ];
//    stream ~= bytes1;
//    shouldBeFalse(stream.empty());
//    shouldBeFalse(stream.hasMessages());

//    stream ~= bytes2;
//    shouldBeFalse(stream.empty());
//    shouldBeTrue(stream.hasMessages());
// }
