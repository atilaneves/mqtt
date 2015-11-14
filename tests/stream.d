module tests.stream;

import unit_threaded;
import mqttd.stream;
import mqttd.message;
import mqttd.server;
import mqttd.broker;
import std.stdio;
import std.algorithm;
import std.array;
import cerealed;

struct TestMqttConnection {
    void newMessage(in ubyte[] payload) {
        writeln(&this, " New message: ", payload);
        auto dec = Decerealiser(payload);
        immutable fixedHeader = dec.value!MqttFixedHeader;
        dec.reset;
        switch(fixedHeader.type) with(MqttType) {
            case PUBLISH:
                auto msg = dec.value!MqttPublish;
                payloads ~= msg.payload.dup;
                break;

            default:
                messages ~= payload;
        }
    }

    void disconnect() {
        connected = false;
    }

    alias Payload = ubyte[];
    const(Payload)[] payloads;
    const(Payload)[] messages;
    bool connected = true;

    static assert(isNewMqttSubscriber!TestMqttConnection);
}

void subscribe(S)(ref MqttServer!S server, ref S connection, in ushort msgId, in string[] topics) if(isNewMqttSubscriber!S) {
    MqttSubscribe(msgId, topics.map!(a => MqttSubscribe.Topic(a, 0)).array).cerealise!(b => server.newMessage(connection, b));
}


void testMqttInTwoPackets() {
    auto server = MqttServer!TestMqttConnection();
    auto connection = TestMqttConnection();
    auto stream = MqttStream(128);

    server.subscribe(connection, 33, ["top"]);

    ubyte[] bytes1 = [ 0x3c, 0x0f, //fixed header
                       0x00, 0x03, 't', 'o', 'p', //topic name
                       0x00, 0x21, //message ID
                       1, 2, 3 ]; //1st part of payload

    stream ~= bytes1;
    stream.handleMessages(server, connection);
    connection.payloads.shouldBeEmpty;

    ubyte[] bytes2 = [ 4, 5, 6, 7, 8]; //2nd part of payload
    stream ~= bytes2;
    stream.handleMessages(server, connection);
    connection.payloads.shouldEqual([[1, 2, 3, 4, 5, 6, 7, 8]]);
}


void testTwoMqttInThreePackets() {
    auto server = MqttServer!TestMqttConnection();
    auto connection = TestMqttConnection();
    auto stream = MqttStream(128);

    server.subscribe(connection, 33, ["top"]);

    ubyte[] bytes1 = [ 0x3c, 0x0f, //fixed header
                       0x00, 0x03, 't', 'o', 'p', //topic name
                       0x00, 0x21, //message ID
                       1, 2, 3, ]; //1st part of payload

    stream ~= bytes1;
    stream.handleMessages(server, connection);
    connection.payloads.shouldBeEmpty;

    ubyte[] bytes2 = [ 4, 5, 6, 7, 8]; //2nd part of payload
    stream ~= bytes2;
    stream.handleMessages(server, connection);
    connection.payloads.shouldEqual([[1, 2, 3, 4, 5, 6, 7, 8]]);

    ubyte[] bytes3 = [0xe0, 0x00]; //disconnect
    stream ~= bytes3;
    stream.handleMessages(server, connection);
    connection.payloads.shouldEqual([[1, 2, 3, 4, 5, 6, 7, 8]]);
    connection.connected.shouldBeFalse;
}


void testTwoMqttInOnePacket() {
   auto stream = MqttStream(128);
   auto server = MqttServer!TestMqttConnection();
   auto connection = TestMqttConnection();

   server.subscribe(connection, 33, ["top"]);

   ubyte[] bytes1 = [ 0x3c ]; // half of header
   ubyte[] bytes2 = [ 0x0f, //2nd half fixed header
                     0x00, 0x03, 't', 'o', 'p', //topic name
                     0x00, 0x21, //message ID
                      1, 2, 3, 4, 5, 6, 7, 8, //payload
                     0xe0, 0x00, //header for disconnect
       ];

   stream ~= bytes1;
   stream.handleMessages(server, connection);
   connection.payloads.shouldBeEmpty;

   stream ~= bytes2;
   stream.handleMessages(server, connection);
   connection.payloads.shouldEqual([[1, 2, 3, 4, 5, 6, 7, 8]]);
}

void testBug1() {
    auto stream = MqttStream(128);

    ubyte[] msg = [48, 20, 0, 16, 112, 105, 110, 103, 116, 101, 115, 116, 47, 48, 47, 114, 101, 112, 108, 121, 111, 107];
    ubyte[] bytes1 = msg ~ msg[0..$-4];
    stream ~= bytes1;
    stream.popNextMessageBytes.shouldEqual(msg);
}


void testBug2() {
    auto stream = MqttStream(128);

    ubyte[] bytes1 = [48, 26, 0, 18, 112, 105, 110, 103, 116, 101, 115, 116, 47, 48, 47, 114, 101, 113, 117, 101, 115, 116];
    stream ~= bytes1;
    stream.hasMessages.shouldBeFalse;

    ubyte[] bytes2 = [112, 105, 110, 103, 32, 48];
    stream ~= bytes2;
    stream.hasMessages.shouldBeTrue;
    stream.popNextMessageBytes.shouldEqual(bytes1 ~ bytes2);
}
