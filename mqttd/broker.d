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
        foreach(topic; topics) {
            auto subParts = topic.topic.splitter("/");
            auto node = addOrFindNode(&_tree, subParts);
            node.leaves ~= NewSubscription!(S)(subscriber, topic);
        }
    }

    void unsubscribe(ref S subscriber) {
        unsubscribeImpl(&_tree, subscriber, []);
    }

    void unsubscribe(R)(ref S subscriber, R topics)
        if(isInputRange!R && is(ElementType!R == string))
    {
        unsubscribeImpl(&_tree, subscriber, topics.array);
    }

    void publish(in string topic, in ubyte[] payload) {
        auto pubParts = topic.splitter("/");
        publishImpl(&_tree, pubParts, payload);
    }

private:

    static struct Node {
        Node*[string] children;
        NewSubscription!S[] leaves;
    }

    Flag!"useCache" _useCache;
    Node _tree;

    Node* addOrFindNode(R)(Node* tree, R parts) if(isInputRange!R && is(ElementType!R == string)) {
        if(parts.empty) return tree;

        //create if not already here
        const part = parts.front;
        if(part !in tree.children) tree.children[part] = new Node;

        parts.popFront;
        return addOrFindNode(tree.children[part], parts);
    }

    void unsubscribeImpl(Node* tree, ref S subscriber, in string[] topics) {
        tree.leaves = tree.leaves.filter!(a => !a.isSubscriber(subscriber, topics)).array;

        if(tree.children.length == 0) return;
        foreach(k, v; tree.children) {
            unsubscribeImpl(v, subscriber, topics);
        }
    }

    void publishImpl(R1, R2)(Node* tree, R1 pubParts, R2 bytes)
        if(isTopicRange!R1 && isInputRangeOf!(R2, ubyte))
    {
        if(pubParts.empty) return;

        foreach(part; only(pubParts.front, "#", "+")) {
            if(part in tree.children) {
                auto node = tree.children[part];
                immutable hasOneElement = hasOneElement(pubParts);
                if(hasOneElement && "#" in node.children) {
                    //So that "finance/#" matches "finance"
                    publishNode(node.children["#"], bytes);
                }

                if(hasOneElement || part == "#") publishNode(node, bytes);

                auto r = pubParts.save;
                r.popFront;
                publishImpl(node, r, bytes);
            }
        }
    }

    void publishNode(R)(Node* node, R bytes) if(isInputRangeOf!(R, ubyte)) {
        foreach(subscription; node.leaves) {
            subscription.newMessage(bytes);
        }
    }

    bool hasOneElement(R)(R range) if(isTopicRange!R) {
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
    string _topic;
    ubyte _qos;
}


//////////////////////////////////////////////////////////////////
enum isMqttSubscriber(T) = is(typeof((){
    const(ubyte)[] bytes;
    //T.init.newMessage(bytes);
    T.init.newMessage("topic", bytes);
}));



template RefType(T) {
    static if(is(T == struct))
        alias RefType = T*;
    else static if(is(T == class) || is(T == interface))
        alias RefType = T;
    else
        static assert(0);
}

unittest {
    struct Struct {}
    class Class {}
    interface Interface {}
    static assert(is(RefType!Struct == Struct*));
    static assert(is(RefType!Class == Class));
    static assert(is(RefType!Interface == Interface));
}

struct MqttBroker(T) if(isMqttSubscriber!T) {
public:

    alias Subscriber = RefType!T;

    void subscribe(U)(ref U subscriber, in string[] topics) if(is(U == struct) && isMqttSubscriber!U) {
        subscribe(&subscriber, topics);
    }

    void subscribe(Subscriber subscriber, in string[] topics) {
        subscribe(subscriber, topics.map!(a => MqttSubscribe.Topic(a.idup, 0)).array);
    }

    void subscribe(U)(ref U subscriber, in MqttSubscribe.Topic[] topics) if(is(U == struct) && isMqttSubscriber!U) {
        subscribe(&subscriber, topics);
    }

    void subscribe(Subscriber subscriber, in MqttSubscribe.Topic[] topics) {
        foreach(t; topics) {
            const parts = array(splitter(t.topic, "/"));
            _subscriptions.addSubscription(Subscription!Subscriber(subscriber, t, parts), parts);
        }
    }

    void unsubscribe(U)(ref U subscriber) if(is(U == struct) && isMqttSubscriber!U) {
        unsubscribe(&subscriber);
    }

    void unsubscribe(Subscriber subscriber) {
        _subscriptions.removeSubscription(subscriber, _subscriptions._nodes);
    }

    void unsubscribe(U)(ref U subscriber, in string[] topics) if(is(U == struct) && isMqttSubscriber!U) {
        _subscriptions.removeSubscription(&subscriber, topics, _subscriptions._nodes);
    }

    void unsubscribe(Subscriber subscriber, in string[] topics) {
        _subscriptions.removeSubscription(subscriber, topics, _subscriptions._nodes);
    }

    void publish(in string topic, in string payload) {
        publish(topic, cast(ubyte[])payload);
    }

    void publish(in string topic, in ubyte[] payload) {
        publish(topic, splitter(topic, "/"), payload);
    }

    @property void useCache(bool u) {
        _subscriptions._useCache = u;
    }

private:

    SubscriptionTree!Subscriber _subscriptions;

    void publish(R)(in string topic, R topParts, in ubyte[] payload) if(isTopicRange!R) {
        _subscriptions.publish(topic, topParts, payload);
    }
}


private bool revStrEquals(in string str1, in string str2) pure nothrow { //compare strings in reverse
    if(str1.length != str2.length) return false;
    for(auto i = cast(int)str1.length - 1; i >= 0; --i)
        if(str1[i] != str2[i]) return false;
    return true;
}

private bool equalOrPlus(in string pat, in string top) pure nothrow {
    return pat == "+" || pat.revStrEquals(top);
}


private struct SubscriptionTree(T) if(isMqttSubscriber!T) {
    private static struct Node {
        string part;
        Node* parent;
        Node*[string] branches;
        Subscription!T[] leaves;
    }

    void addSubscription(Subscription!T s, in string[] parts) {
        assert(parts.length);
        if(_useCache) _cache = _cache.init; //invalidate cache
        addSubscriptionImpl(s, parts, null, _nodes);
    }

    void addSubscriptionImpl(Subscription!T s, const(string)[] parts,
                             Node* parent, ref Node*[string] nodes) {
        auto part = parts[0];
        parts = parts[1 .. $];
        auto node = addOrFindNode(part, parent, nodes);
        if(parts.empty) {
            node.leaves ~= s;
        } else {
            addSubscriptionImpl(s, parts, node, node.branches);
        }
    }

    Node* addOrFindNode(in string part,
                        Node* parent, ref Node*[string] nodes) {
        if(part in nodes) {
            auto n = nodes[part.idup];
            if(part == n.part) {
                return n;
            }
        }
        auto node = new Node(part.idup, parent);
        nodes[part.idup] = node;
        return node;
    }

    void removeSubscription(T subscriber, ref Node*[string] nodes) {
        if(_useCache) _cache = _cache.init; //invalidate cache
        auto newnodes = nodes.dup;
        foreach(n; newnodes) {
            if(n.leaves) {
                n.leaves = std.algorithm.remove!(l => l.isSubscriber(subscriber))(n.leaves);
                if(n.leaves.empty && !n.branches.length) {
                    removeNode(n.parent, n);
                }
            } else {
                removeSubscription(subscriber, n.branches);
            }
        }
    }

    void removeSubscription(T subscriber, in string[] topic, ref Node*[string] nodes) {
        if(_useCache) _cache = _cache.init; //invalidate cache
        auto newnodes = nodes.dup;
        foreach(n; newnodes) {
            if(n.leaves) {
                n.leaves = std.algorithm.remove!(l => l.isSubscription(subscriber, topic))(n.leaves);
                if(n.leaves.empty && !n.branches.length) {
                    removeNode(n.parent, n);
                }
            } else {
                removeSubscription(subscriber, topic, n.branches);
            }
        }
    }

    void removeNode(Node* parent, Node* child) {
        if(parent) {
            parent.branches.remove(child.part);
        } else {
            _nodes.remove(child.part);
        }
        if(parent && !parent.branches.length && parent.leaves.empty)
            removeNode(parent.parent, parent);
    }

    void publish(R)(in string topic, R topParts, in const(ubyte)[] payload) if(isTopicRange!R) {
        publish(topic, topParts, payload, _nodes);
    }

    void publish(R)(in string topic,
                    R topParts,
                    in const(ubyte)[] payload,
                    Node*[string] nodes)
        if(isTopicRange!R) {

        //check the cache first
        if(_useCache && topic in _cache) {
            foreach(s; _cache[topic]) s.newMessage(topic, payload);
            return;
        }

        if(topParts.empty) return;

        //not in the cache or not using the cache, do it the hard way
        foreach(part; [topParts.front, "#", "+"]) {
            if(part in nodes) {
                immutable hasOneElement = hasOneElement(topParts);
                if(hasOneElement && "#" in nodes[part].branches) {
                    //So that "finance/#" matches finance
                    publishLeaves(topic, payload, topParts.front, hasOneElement, nodes[part].branches["#"].leaves);
                }
                publishLeaves(topic, payload, topParts.front, hasOneElement, nodes[part].leaves);
                if(!topParts.empty) {
                    auto r = topParts.save;
                    r.popFront;
                    publish(topic, r, payload, nodes[part].branches);
                }
            }
        }
    }

    private bool hasOneElement(R)(R range) if(isTopicRange!R) {
        auto r = range.save;
        r.popFront;
        return r.empty;
    }

    void publishLeaves(in string topic,
                       in const(ubyte)[] payload,
                       in string topPart,
                       in bool hasOneElement,
                       Subscription!T[] subscriptions) {
        foreach(sub; subscriptions) {
            if(hasOneElement &&
               equalOrPlus(sub._part, topPart)) {
                publishLeaf(sub, topic, payload);
            }
            else if(sub._part == "#") {
                publishLeaf(sub, topic, payload);
            }
        }
    }

    void publishLeaf(Subscription!T sub, in string topic, in const(ubyte)[] payload) {
        sub.newMessage(topic, payload);
        if(_useCache) _cache[topic.idup] ~= sub;
    }


private:

    bool _useCache;
    Subscription!T[][string] _cache;
    Node*[string] _nodes;
}


private struct Subscription(T) if(isMqttSubscriber!T) {
    this(T subscriber, in MqttSubscribe.Topic topic, in string[] topicParts) {
        _subscriber = subscriber;
        _part = topicParts[$ - 1].idup;
        _topic = topic.topic.idup;
        _qos = topic.qos;
    }

    void newMessage(in string topic, in ubyte[] payload) {
        _subscriber.newMessage(topic, payload);
    }

    bool isSubscriber(T subscriber) const {
        return _subscriber == subscriber;
    }

    bool isSubscription(T subscriber, in string[] topics) const {
        return isSubscriber(subscriber) && isTopic(topics);
    }

    bool isTopic(in string[] topics) const {
        return !topics.find(_topic).empty;
    }

private:
    T _subscriber;
    string _part;
    string _topic;
    ubyte _qos;
}
