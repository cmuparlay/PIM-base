#pragma once

#include <iostream>
#include <cstdlib>

struct key_value {
    int64_t key;
    int64_t value;
};

inline bool operator<(const key_value& kv1, const key_value& kv2) {
    if (kv1.key > kv2.key) return false;
    if (kv1.key < kv2.key) return true;
    return (kv1.value < kv2.value);
}

inline bool operator==(const key_value& kv1, const key_value& kv2) {
    return (kv1.key == kv2.key) && (kv1.value == kv2.value);
}

inline bool operator!=(const key_value& kv1, const key_value& kv2) {
    return !(kv1 == kv2);
}

ostream& operator<<(ostream& os, const key_value& kv) {
    os << "{Key=" << kv.key << ", Value=" << kv.value << "}";
    return os;
}