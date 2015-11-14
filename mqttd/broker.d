module mqttd.broker;


import mqttd.message;
import std.algorithm;
import std.array;
import std.typecons;
import std.range;
import std.traits;


enum isTopicRange(R) = isForwardRange!R && is(Unqual!(ElementType!R) == string);

enum isInputRangeOf(R, T) = isInputRange!R && is(Unqual!(ElementType!R) == T);

enum isNewMqttSubscriber(T) = is(typeof((){
    const(ubyte)[] bytes;
    auto sub = T.init;
    sub.newMessage(bytes);
}));

struct NewMqttBroker(S) if(isNewMqttSubscriber!S) {

    void subscribe(R)(ref S subscriber, R topics)
        if(isInputRange!R && is(Unqual!(ElementType!R) == string))
    {
        subscribe(subscriber, topics.map!(a => MqttSubscribe.Topic(a.idup, 0)));
    }

    void subscribe(R)(ref S subscriber, R topics)
        if(isInputRange!R && is(ElementType!R == MqttSubscribe.Topic))
    {
        invalidateCache();
        foreach(topic; topics) {
            auto subParts = topic.topic.splitter("/");
            auto node = addOrFindNode(&_tree, subParts);
            node.leaves ~= NewSubscription!(S)(subscriber, topic);
        }
    }

    void unsubscribe(ref S subscriber) {
        static string[] topics;
        unsubscribe(subscriber, topics);
    }

    void unsubscribe(R)(ref S subscriber, R topics)
        if(isInputRange!R && is(ElementType!R == string))
    {
        invalidateCache();
        unsubscribeImpl(&_tree, subscriber, topics.array);
    }

    void publish(in string topic, in ubyte[] payload) {
        if(_useCache && topic in _cache) {
            foreach(subscription; _cache[topic]) subscription.newMessage(payload);
        }

        auto pubParts = topic.splitter("/");
        publishImpl(&_tree, pubParts, topic, payload);
    }

private:

    static struct Node {
        Node*[immutable(string)] children;
        NewSubscription!S[] leaves;
    }

    Flag!"useCache" _useCache;
    Node _tree;
    NewSubscription!S[][string] _cache;

    void invalidateCache() {
        if(_useCache) _cache = _cache.init;
    }

    Node* addOrFindNode(R)(Node* tree, R parts) if(isInputRange!R && is(ElementType!R == string)) {
        if(parts.empty) return tree;

        //create if not already here
        const part = parts.front;
        if(part !in tree.children) tree.children[part.idup] = new Node;

        parts.popFront;
        return addOrFindNode(tree.children[part], parts);
    }

    static void unsubscribeImpl(Node* tree, ref S subscriber, in string[] topics) {
        tree.leaves = tree.leaves.filter!(a => !a.isSubscriber(subscriber, topics)).array;

        if(tree.children.length == 0) return;
        foreach(k, v; tree.children) {
            unsubscribeImpl(v, subscriber, topics);
        }
    }

    void publishImpl(R1, R2)(Node* tree, R1 pubParts, in string topic, R2 bytes)
        if(isTopicRange!R1 && isInputRangeOf!(R2, ubyte))
    {
        if(pubParts.empty) return;

        foreach(part; only(pubParts.front, "#", "+")) {
            if(part in tree.children) {
                auto node = tree.children[part];
                immutable hasOneElement = hasOneElement(pubParts);
                if(hasOneElement && "#" in node.children) {
                    //So that "finance/#" matches "finance"
                    publishNode(node.children["#"], topic, bytes);
                }

                if(hasOneElement || part == "#") publishNode(node, topic, bytes);

                auto r = pubParts.save;
                r.popFront;
                publishImpl(node, r, topic, bytes);
            }
        }
    }

    void publishNode(R)(Node* node, in string topic, R bytes) if(isInputRangeOf!(R, ubyte)) {
        foreach(subscription; node.leaves) {
            subscription.newMessage(bytes);
            if(_useCache) _cache[topic.idup] ~= subscription;
        }
    }

    static bool hasOneElement(R)(R range) if(isTopicRange!R) {
        auto r = range.save;
        r.popFront;
        return r.empty;
    }
}


private struct NewSubscription(S) if(isNewMqttSubscriber!S) {
    this(ref S subscriber, in MqttSubscribe.Topic topic) {
        _subscriber = &subscriber;
        _topic = topic.topic.idup;
        _qos = topic.qos;
    }

    void newMessage(in ubyte[] bytes) {
        _subscriber.newMessage(bytes);
    }

    bool isSubscriber(ref S subscriber, in string[] topics) @trusted const {
        immutable isSameTopic = topics.empty || topics.canFind(_topic);
        return isSameTopic && &subscriber == _subscriber;
    }

    S* _subscriber;
    immutable(string) _topic;
    ubyte _qos;
}
