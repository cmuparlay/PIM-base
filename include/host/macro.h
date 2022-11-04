#pragma once
#include "pptr.h"
#include "macro_common.h"
#include "inttypes.h"
#include <parlay/primitives.h>

#define RETURN_X_IF_Y(f, x, y) \
    {                          \
        if ((f) == x) {        \
            return y;          \
        }                      \
    }

#define RETURN_FALSE_IF_FALSE(x) \
    { RETURN_X_IF_Y(x, false, false); }

#define RETURN_TRUE_IF_TRUE(x) \
    { RETURN_X_IF_Y(x, true, true); }

union pptr_i64_converter {
    pptr p;
    int64_t i;
};

static inline int64_t pptr_to_int64(const pptr& p) {
    pptr_i64_converter conv;
    conv.p = p;
    return conv.i;
}

static inline pptr int64_to_pptr(const int64_t& i) {
    pptr_i64_converter conv;
    conv.i = i;
    return conv.p;
}

union opptr_i64_converter {
    offset_pptr op;
    int64_t i;
};

static inline offset_pptr int64_to_opptr(const int64_t& i) {
    opptr_i64_converter conv;
    conv.i = i;
    return conv.op;
}

static bool valid_pptr(pptr x) {
    return (x.id < (uint32_t)nr_of_dpus) && (x.addr != null_pptr.addr);
}

static inline bool equal_pptr(pptr a, pptr b) {
    return a.id == b.id && a.addr == b.addr;
}

static inline bool not_equal_pptr(pptr a, pptr b) { return !equal_pptr(a, b); }

#define PARALLEL_ON true

template <typename F>
inline void parfor_wrap(size_t start, size_t end, F f, bool parallel = true,
                        long granularity = 0L, bool conservative = false) {
    if (PARALLEL_ON && parallel) {
        parlay::parallel_for(start, end, f, granularity, conservative);
    } else {
        for (size_t i = start; i < end; i++) {
            f(i);
        }
    }
}

template <class T>
static void print_parlay_sequence(const T &x) {
    for (size_t i = 0; i < x.size(); i++) {
        printf("[%lu] = %d\n", i, x[i]);
    }
}

static inline void print_array(string name, int64_t *arr, int length,
                               bool x16 = false) {
    for (int i = 0; i < length; i++) {
        if (x16) {
            printf("%s[%d] = %llx\n", name.c_str(), i, (uint64_t)arr[i]);
        } else {
            printf("%s[%d] = %lld\n", name.c_str(), i, (int64_t)arr[i]);
        }
    }
}