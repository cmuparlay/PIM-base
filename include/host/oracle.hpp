#pragma once

#include <parlay/primitives.h>
#include <parlay/range.h>
#include <parlay/sequence.h>
#include <cstring>
#include <vector>
#include "random_generator.hpp"
#include "value.hpp"
using namespace std;

class batch_parallel_oracle {
    template <class TT>
    struct copy_scan {
        using T = TT;
        copy_scan() : identity((key_value){.key = INT64_MIN, .value = INT64_MIN}) {}
        T identity;
        static T f(T a, T b) { return ((b.key == INT64_MIN) && (b.value == INT64_MIN)) ? a : b; }
    };

    const int default_batch_size = 1e6;

    using kv_seq = parlay::slice<key_value*, key_value*>;
    using i64_seq = parlay::slice<int64_t*, int64_t*>;

   public:
    static size_t kv_seq_find_by_key(const kv_seq& s, const int64_t& key) {
        int l = 0, r = s.size();
        while (r - l > 1) {
            int mid = (l + r) >> 1;
            if (s[mid].key <= key) {
                l = mid;
            } else {
                r = mid;
            }
        }
        ASSERT(s[l].key <= key);
        return l;
    }

    static key_value kv_seq_find_result_by_key(const kv_seq& s,
                                               const int64_t& key) {
        return s[kv_seq_find_by_key(s, key)];
    }

   public:
    parlay::sequence<key_value> inserted;
    batch_parallel_oracle() {
        inserted = parlay::tabulate(1, [](size_t i) {
            (void)i;
            return (key_value){.key = INT64_MIN, .value = INT64_MIN};
        });
    }

    template <typename kviterator>
    void init(const parlay::slice<kviterator, kviterator>& kvs) {
        this->insert_batch(kvs);
    }

    size_t predecessor_position(const int64_t& v) {
        return kv_seq_find_by_key(parlay::make_slice(inserted), v);
    }

    key_value predecessor(const int64_t& v) {
        return kv_seq_find_result_by_key(parlay::make_slice(inserted), v);
    }

    template <typename i64iterator>
    parlay::sequence<key_value> predecessor_batch(
        const parlay::slice<i64iterator, i64iterator>& buf) {
        int length = buf.size();
        // using TT = decltype(buf[0]);
        // using X = typename TT::nothing;
        static_assert(
            std::is_same<typename std::int64_t&, decltype(buf[0])>::value);
        return parlay::tabulate(length,
                                [&](size_t i) { return predecessor(buf[i]); });
    }

    template <typename i64iterator>
    auto predecessor_position_batch(
        const parlay::slice<i64iterator, i64iterator>& buf) {
        // static_assert(
        //     std::is_same<typename std::int64_t, decltype(buf[0])>::value);
        int length = buf.size();
        return parlay::tabulate(
            length, [&](size_t i) { return predecessor_position(buf[i]); });
    }

    template <typename kviterator>
    void insert_batch(const parlay::slice<kviterator, kviterator>& buffer) {
        // static_assert(
        //     std::is_same<typename key_value, decltype(buffer[0])>::value);
        auto buffer_sorted = parlay::sort(buffer);

        auto buf =
            parlay::pack(
                buffer_sorted,
                parlay::make_slice(parlay::delayed_seq<bool>(buffer_sorted.size(), [&](size_t i) {
                    return (i == 0) || (buffer_sorted[i].key != buffer_sorted[i - 1].key);
                })));

        int len = buf.size();

        auto keys = parlay::delayed_seq<int64_t>(
            len, [&](size_t i) { return buf[i].key; });

        auto results = predecessor_position_batch(parlay::make_slice(keys));
        parlay::parallel_for(0, len, [&](size_t i) {
            if (buf[i].key == inserted[results[i]].key) {
                inserted[results[i]].value = buf[i].value;
            }
        });
        inserted = parlay::unique(parlay::merge(inserted, buf));
    }

    template <typename i64iterator>
    void remove_batch(const parlay::slice<i64iterator, i64iterator>& buffer) {
        auto keys = parlay::sort(buffer);
        int len = keys.size();
        auto results = predecessor_position_batch(parlay::make_slice(keys));
        parlay::parallel_for(0, len, [&](size_t i) {
            if (keys[i] == inserted[results[i]].key) {
                inserted[results[i]] =
                    (key_value){.key = INT64_MIN, .value = INT64_MIN};
            }
        });
        inserted = parlay::unique(parlay::scan_inclusive(inserted, copy_scan<key_value>()));
    }

    key_value random_element() {
        int j = abs(rn_gen::parallel_rand()) % inserted.size();
        return inserted[j];
    }

    // Range Scan
    template <class SS, class T>
    auto scan_size(const SS& s, T& op) {
        int64_t lkey = op.lkey, rkey = op.rkey;
        int64_t l = 0, r = s.size();
        int64_t mid;
        while (r - l > 1) {
            mid = (l + r) >> 1;
            if (s[mid].key <= lkey) {
                l = mid;
            } else if (s[mid].key > rkey) {
                r = mid;
            } else {
                break;
            }
        }
        int64_t rr = r, mm = mid;
        r = mid;
        while (r - l > 1) {
            mid = (l + r) >> 1;
            if (s[mid].key > lkey) {
                r = mid;
            } else {
                l = mid;
            }
        }
        if (lkey > s[l].key) l++;
        while (rr - mm > 1) {
            mid = (rr + mm) >> 1;
            if (s[mid].key > rkey) {
                rr = mid;
            } else {
                mm = mid;
            }
        }
        if (mm >= s.size() - 1)
            mm = s.size();
        else if (s[mm + 1].key <= rkey)
            mm += 2;
        else
            mm++;
        if(l == mm || l + 1 == mm) {
            if(s[l].key >= lkey && s[l].key <= rkey)
                mm = l + 1;
            else
                mm = l;
        }
        return std::make_pair(l, mm);
    }

    template <class T>
    auto scan_size_batch(slice<T*, T*>& ops) {
        parlay::sort_inplace(
            inserted, [&](key_value kv1, key_value kv2) -> bool {
                return (kv1.key < kv2.key) ||
                       ((kv1.key == kv2.key) && (kv1.value < kv2.value));
            });
        auto inserted_set =
            parlay::unique(inserted, [&](key_value kv1, key_value kv2) -> bool {
                return (kv1.key == kv2.key);
            });
        return parlay::tabulate(ops.size(), [&](int64_t i) {
            return scan_size(inserted_set, ops[i]);
        });
    }
};
