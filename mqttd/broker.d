module mqttd.broker;


import mqttd.message;
import std.algorithm;
import std.array;
import std.typecons;
import std.range;


enum isTopicRange(R) = isForwardRange!R;


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

    void subscribe(Subscriber subscriber, in string[] topics) {
        subscribe(subscriber, topics.map!(a => MqttSubscribe.Topic(a, 0)).array);
    }

    void subscribe(Subscriber subscriber, in MqttSubscribe.Topic[] topics) {
        foreach(t; topics) {
            const parts = array(splitter(t.topic, "/"));
            _subscriptions.addSubscription(Subscription!Subscriber(subscriber, t, parts), parts);
        }
    }

    void unsubscribe(Subscriber subscriber) {
        _subscriptions.removeSubscription(subscriber, _subscriptions._nodes);
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
            auto n = nodes[part];
            if(part == n.part) {
                return n;
            }
        }
        auto node = new Node(part, parent);
        nodes[part] = node;
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
        if(_useCache) _cache[topic] ~= sub;
    }


private:

    bool _useCache;
    Subscription!T[][string] _cache;
    Node*[string] _nodes;
}


private struct Subscription(T) if(isMqttSubscriber!T) {
    this(T subscriber, in MqttSubscribe.Topic topic, in string[] topicParts) {
        _subscriber = subscriber;
        _part = topicParts[$ - 1];
        _topic = topic.topic;
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
