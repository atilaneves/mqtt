module mqttd.aa;

struct AA(V, int N) {
    ref V opIndex(string key) {
        auto hash = murmur(key.ptr, cast(uint)key.length, 0xc70f6907);
        return _store[hash % N];
    }

    V get(string key, V default_) {
        auto v = this[key];
        return v == V.init ? default_ : v;
    }

    V[] _store = new V[N];
}


private uint murmur(immutable(char)* key, uint len, uint seed) {
    static  uint c1 = 0xcc9e2d51;
    static  uint c2 = 0x1b873593;
    static  uint r1 = 15;
    static  uint r2 = 13;
    static  uint m = 5;
    static  uint n = 0xe6546b64;

    uint hash = seed;

     int nblocks = len / 4;
     uint *blocks = cast(uint*)key;

    for (int i = 0; i < nblocks; i++) {
        uint k = blocks[i];
        k *= c1;
        k = rot32(k, r1);
        k *= c2;

        hash ^= k;
        hash = rot32(hash, r2) * m + n;
    }

    ubyte *tail = cast(ubyte*) (key + nblocks * 4);
    uint k1 = 0;

    switch (len & 3) {
    case 3:
        k1 ^= tail[2] << 16;
        goto case 2;
    case 2:
        k1 ^= tail[1] << 8;
        goto case 1;
    case 1:
        k1 ^= tail[0];

        k1 *= c1;
        k1 = rot32(k1, r1);
        k1 *= c2;
        hash ^= k1;
        break;
    default:
        break;
    }

    hash ^= len;
    hash ^= (hash >> 16);
    hash *= 0x85ebca6b;
    hash ^= (hash >> 13);
    hash *= 0xc2b2ae35;
    hash ^= (hash >> 16);

    return hash;
}

private uint rot32(uint x, uint y) {
    return (x << y) | (x >> (32 - y));
}
