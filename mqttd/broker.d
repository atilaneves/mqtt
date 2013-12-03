module mqttd.broker;


import mqttd.message;
import std.algorithm;
import std.array;


interface MqttSubscriber {
    void newMessage(in string topic, in ubyte[] payload);
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


struct MqttBroker {
    alias subscriptions this;

    static bool matches(in string topic, in string pattern) {
        return matches(array(splitter(topic, "/")), pattern);
    }

    static bool matches(in string[] topParts, in string pattern) {
        return matches(topParts, array(splitter(pattern, "/")));
    }

    static bool matches(in string[] topParts, in string[] patParts) {
        immutable hasHash = patParts[$ - 1] == "#";
        if(hasHash) {
            //+1 here allows "finance/#" to match "finance"
            if(patParts.length > topParts.length + 1) return false;
        } else {
            if(patParts.length != topParts.length) return false;
        }

        immutable end = cast(int)(hasHash ? patParts.length - 2 : patParts.length - 1);
        for(int i = end; i >=0 ; --i) { //starts with same thing
            if(!patParts[i].equalOrPlus(topParts[i])) return false;
        }
        return true;
    }

    Subscriptions subscriptions;
}


private struct Subscriptions {
public:

    void subscribe(MqttSubscriber subscriber, in string[] topics) {
        subscribe(subscriber, array(map!(a => MqttSubscribe.Topic(a, 0))(topics)));
    }

    void subscribe(MqttSubscriber subscriber, in MqttSubscribe.Topic[] topics) {
        foreach(t; topics) _subscriptions.addSubscription(Subscription(subscriber, t));
    }

    void unsubscribe(MqttSubscriber subscriber) {
        _subscriptions.removeSubscription(subscriber, _subscriptions._nodes);
    }

    void unsubscribe(MqttSubscriber subscriber, in string[] topics) {
        _subscriptions.removeSubscription(subscriber, topics, _subscriptions._nodes);
    }

    void publish(in string topic, in string payload) {
        publish(topic, cast(ubyte[])payload);
    }

    void publish(in string topic, in ubyte[] payload) {
        auto topParts = array(splitter(topic, "/"));
        publish(topic, topParts, payload);
    }

private:

    SubscriptionTree _subscriptions;

    void publish(in string topic, string[] topParts, in ubyte[] payload) {
        _subscriptions.publish(topic, topParts, payload);
    }
}


private struct SubscriptionTree {
    private static struct Node {
        string part;
        Node* parent;
        Node*[] children;
        Subscription subscription;
    }

    void addSubscription(Subscription s) {
        assert(s.pattern.length);
        addSubscriptionImpl(s, s.pattern, null, _nodes);
    }

    void printNodes(Node*[] nodes, int level = 0) {
        import std.stdio;
        foreach(i, n; nodes) {
            writeln("Node ", i, " at level ", level);
            if(!n.children) {
                writeln("node at level ", level, " with part ", n.part, " topic ", n.subscription._topic,
                        " sub ", n.subscription);
            } else {
                printNodes(n.children, level + 1);
            }
        }
    }

    int countFinalNodes(Node*[] nodes) {
        int acc;
        foreach(n; nodes) {
            if(!n.children) {
                acc++;
            } else {
                acc += countFinalNodes(n.children);
            }
        }

        return acc;
    }

    int countNodes(Node*[] nodes) {
        int acc;
        import std.stdio;
        foreach(n; nodes) {
            acc++;
            if(n.children) {
                acc += countNodes(n.children);
            }
        }

        return acc;
    }


    void addSubscriptionImpl(Subscription s, const(string)[] parts,
                             Node* parent, ref Node*[] nodes) {
        auto part = parts[0];
        parts = parts[1 .. $];
        auto node = addOrFindNode(s, part, parent, nodes);
        if(parts.empty) {
            node.subscription = s; //leaf node
        } else {
            addSubscriptionImpl(s, parts, node, node.children);
        }
    }

    Node* addOrFindNode(Subscription subscription, in string part,
                        Node* parent, ref Node*[] nodes) {
        foreach(n; nodes) {
            if(part == n.part &&
               (n.subscription._subscriber == subscription._subscriber ||
                n.subscription._subscriber is null)) {
                return n;
            }
        }
        auto node = new Node(part, parent);
        nodes ~= node;
        return node;
    }

    void removeSubscription(MqttSubscriber subscriber, ref Node*[] nodes) {
        foreach(n; nodes) {
            if(!n.children && n.subscription.isSubscriber(subscriber)) {
                removeNode(n.parent, n);
            } else {
                removeSubscription(subscriber, n.children);
            }
        }
    }

    void removeSubscription(MqttSubscriber subscriber, in string[] topic, ref Node*[] nodes) {
        foreach(n; nodes) {
            if(!n.children && n.subscription.isSubscription(subscriber, topic)) {
                removeNode(n.parent, n);
            } else {
                removeSubscription(subscriber, topic, n.children);
            }
        }
    }

    void removeNode(Node* parent, Node* child) {
        if(parent)
            parent.children = std.algorithm.remove!(c => c == child)(parent.children);
        else
            _nodes = std.algorithm.remove!(c => c == child)(_nodes);
        if(parent && parent.children.empty) removeNode(parent.parent, parent);
    }

    void publish(in string topic, string[] topParts, in const(ubyte)[] payload) {
        publish(topic, topParts, payload, _nodes);
    }

    void publish(in string topic, string[] topParts, in const(ubyte)[] payload,
                 Node*[] nodes) {
        auto part = topParts[0];
        foreach(n; nodes) {
            if(n.part == "#" && !n.children) {
                n.subscription.newMessage(topic, payload);
                continue;
            }
            if(!equalOrPlus(n.part, part)) {
                continue;
            }
            if(topParts.length == 1) {
                n.subscription.newMessage(topic, payload);
            } else {
                publish(topic, topParts[1 .. $], payload, n.children);
            }
        }
    }


private:

    Node*[] _nodes;
}

private struct Subscription {
    this(MqttSubscriber subscriber, in MqttSubscribe.Topic topic) {
        _subscriber = subscriber;
        _topic = topic.topic;
        _pattern = array(splitter(topic.topic, "/"));
        _qos = topic.qos;
    }

    this(Subscription s) {
        //no need to store anything
    }

    @property const(string)[] pattern() const {
        return _pattern;
    }

    void newMessage(in string topic, in ubyte[] payload) {
        _subscriber.newMessage(topic, payload);
    }

    bool isSubscriber(MqttSubscriber subscriber) const {
        return _subscriber == subscriber;
    }

    bool isSubscription(MqttSubscriber subscriber, in string[] topics) const {
        return isSubscriber(subscriber) && isTopic(topics);
    }

    bool isTopic(in string[] topics) const {
        return !find(topics, _topic).empty;
    }

    Subscription[string] children;

private:
    string _topic;
    string[] _pattern;
    MqttSubscriber _subscriber;
    ubyte _qos;
}
