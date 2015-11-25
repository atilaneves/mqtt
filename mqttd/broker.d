module mqttd.broker;


import mqttd.message;
import std.algorithm;
import std.array;
import std.typecons;
import std.range;
import std.traits;


enum isTopicRange(R) = isInputRange!R && is(Unqual!(ElementType!R) == string);

enum isInputRangeOf(R, T) = isInputRange!R && is(Unqual!(ElementType!R) == T);

enum isMqttSubscriber(T) = is(typeof((){
    const(ubyte)[] bytes;
    auto sub = T.init;
    sub.newMessage(bytes);
}));

struct MqttBroker(S) if(isMqttSubscriber!S) {

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
            node.leaves ~= Subscription!(S)(subscriber, topic);
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
            foreach(subscriber; _cache[topic]) subscriber.newMessage(payload);
            return;
        }
        auto pubParts = topic.splitter("/");
        publishImpl(&_tree, pubParts, topic, payload);
    }

    @property useCache(Flag!"useCache" useIt) {
        _useCache = useIt;
    }

private:

    enum PartType {
        One, //+
        Many, //#
        Other, //anything else
    }

    static struct FastPart {
        PartType type;
        string key;
    }

    static struct PartToNodes {
        static struct Entry {
            string key;
            Node* node;
        }

        Entry oneNode;  //+
        Entry manyNode; //#
        Node*[string] otherNodes;

        Node** opBinaryRight(string op)(in string key) if(op == "in") {
            switch(key) {
            case "+":
                return oneNode.node is null ? null : &oneNode.node;
            case "#":
                return manyNode.node is null ? null : &manyNode.node;
            default:
                return key in otherNodes;
            }
        }

        Node** opBinaryRight(string op)(in FastPart fast) if(op == "in") {
            final switch(fast.type) with(PartType) {
            case One:
                return oneNode.node is null ? null : &oneNode.node;
            case Many:
                return manyNode.node is null ? null : &manyNode.node;
            case Other:
                return fast.key in otherNodes;
            }
        }

        ref Node* opIndex(in string key) pure nothrow @safe {
            switch(key) {
            case "+":
                return oneNode.node;
            case "#":
                return manyNode.node;
            default:
                if(key !in otherNodes) otherNodes[key] = null;
                return otherNodes[key];
            }
        }

        ulong length() const pure nothrow @safe {
            auto length = otherNodes.length;
            if(oneNode.node) ++length;
            if(manyNode.node) ++length;
            return length;
        }

        int opApply(int delegate(ref string, ref Node*) dg) {
            if(oneNode.node) {
                immutable stop = dg(oneNode.key, oneNode.node);
                if(stop) return stop;
            }
            if(manyNode.node) {
                immutable stop = dg(manyNode.key, manyNode.node);
                if(stop) return stop;
            }

            foreach(k, v; otherNodes) {
                immutable stop = dg(k, v);
                if(stop) return stop;
            }

            return 0;
        }

        void insert(in string key, Node* node) {
            if(key in this) return;
            switch(key) {
            case "+":
                oneNode = Entry(key, node);
                break;
            case "#":
                manyNode = Entry(key, node);
                break;
            default:
                otherNodes[key] = node;
                break;
            }
        }
    }

    static struct Node {
        PartToNodes children;
        Subscription!S[] leaves;
    }

    Flag!"useCache" _useCache;
    Node _tree;
    S*[][string] _cache;

    void invalidateCache() {
        if(_useCache) _cache = _cache.init;
    }

    Node* addOrFindNode(R)(Node* tree, R parts) if(isInputRange!R && is(ElementType!R == string)) {
        if(parts.empty) return tree;

        //create if not already here
        const part = parts.front.idup;
        tree.children.insert(part, new Node);

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

    void publishImpl(R1)(Node* tree, R1 pubParts, in string topic, in ubyte[] bytes)
        if(isTopicRange!R1)
    {
        static FastPart[3] partsToCheck = [FastPart(PartType.Other), FastPart(PartType.Many), FastPart(PartType.One)];

        if(pubParts.empty) return;

        partsToCheck[0].key = pubParts.front;
        pubParts.popFront;

        foreach(part; partsToCheck) {
            auto nodePtr = part in tree.children;
            if(nodePtr) {
                auto node = *nodePtr;
                if(pubParts.empty || part.type == PartType.Many) publishNode(node, topic, bytes);

                if(pubParts.empty && FastPart(PartType.Many) in node.children) {
                    //So that "finance/#" matches "finance"
                    publishNode(node.children["#"], topic, bytes);
                }

                publishImpl(node, pubParts, topic, bytes);
            }
        }
    }

    void publishNode(R)(Node* node, in string topic, R bytes) if(isInputRangeOf!(R, ubyte)) {
        foreach(ref subscription; node.leaves) {
            subscription.newMessage(bytes);
            if(_useCache) _cache[topic.idup] ~= subscription._subscriber;
        }
    }
}


private struct Subscription(S) if(isMqttSubscriber!S) {
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
