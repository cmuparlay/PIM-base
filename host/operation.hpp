#pragma once
#include <parlay/primitives.h>
#include "oracle.hpp"
#include "value.hpp"
#include "barrier.h"
#include <mutex>
using namespace std;
using namespace parlay;

namespace pim_skip_list {

mutex mu;
mutex b_alloc_mutex;

struct wptr {
    uint64_t addr;
};
const wptr null_wptr = (wptr){.addr = (uint64_t)-1};

const int MAXN = 2e6;
const int DB_SIZE = 16;
const int HF_DB_SIZE = DB_SIZE >> 1;
const int64_t BUFFER_SIZE = (32ll << 30);
int threads;

struct bnode {
    wptr up, right;
    int64_t height;
    int64_t size;
    int64_t keys[DB_SIZE];
    wptr addrs[DB_SIZE];
};

using mBptr = bnode*;
using mpwptr = wptr*;

bnode* root, *min_node;
bnode* bbuffer;
// bnode bbuffer[BUFFER_SIZE / sizeof(bnode)];
int bcnt = 1;

pthread_barrier_t b1, b2, b3;

template<typename G>
static void assert_exec(bool x, G g) {
    if (!x) {
        mu.lock();
        g();
        mu.unlock();
        assert(false);
    }
}

static inline void print_array(string name, int64_t* arr, int length, bool x16 = false) {
    for (int i = 0; i < length; i++) {
        if (x16) {
            printf("%s[%d] = %llx\n", name.c_str(), i, arr[i]);
        } else {
            printf("%s[%d] = %lld\n", name.c_str(), i, arr[i]);
        }
    }
}

static inline bool equal_wptr(wptr a, wptr b) { return a.addr == b.addr; }

static inline int64_t wptr_to_int64(wptr x) { return (int64_t)x.addr; }

static inline wptr int64_to_wptr(int64_t x) {
    return (wptr){.addr = (uint64_t)x};
}

static inline wptr mbptr_to_wptr(mBptr x) {
    return (wptr){.addr = (uint64_t)x};
}

static inline mBptr wptr_to_mbptr(wptr x) { return (mBptr)x.addr; }

static inline bool valid_wptr(wptr x) { return x.addr != null_wptr.addr; }

void print_bnode(bnode *bn) {
    printf("addr=%llx\n", bn);
    printf("ht=%lld\tup=%llx\tright=%llx\n", bn->height, bn->up, bn->right);
    printf("size=%lld\n", bn->size);
    for (int i = 0; i < bn->size; i++) {
        if (bn->height > 0) {
            printf("[%d]:key=%lld\tval=%llx\n", i, bn->keys[i], bn->addrs[i]);
        } else {
            printf("[%d]:key=%lld\tval=%lld\n", i, bn->keys[i], bn->addrs[i]);
        }
    }
    printf("\n");
}

void print_tree(bnode *bn) {
    print_bnode(bn);
    if (bn->height > 0) {
        for (int i = 0; i < bn->size; i++) {
            assert(valid_wptr(bn->addrs[i]));
            print_tree(wptr_to_mbptr(bn->addrs[i]));
        }
    }
}

#define RETURN_FALSE(x)   \
    {                     \
        if (!(x)) {       \
            return false; \
        }                 \
    }

#define RETURN_FALSE_PRINT(x, args...) \
    {                                  \
        if (!(x)) {                    \
            printf(args);              \
            return false;              \
        }                              \
    }

bool sancheck(bnode *bn) {
    for (int i = 1; i < bn->size; i++) {
        RETURN_FALSE_PRINT(bn->keys[i] > bn->keys[i - 1],
                           "sc: keyinv %lld %lld\n", bn->keys[i],
                           bn->keys[i - 1]);
    }
    if (bn->height > 0) {
        for (int i = 0; i < bn->size; i++) {
            RETURN_FALSE_PRINT(valid_wptr(bn->addrs[i]),
                               "sc: addrinv %d %llx\n", i,
                               wptr_to_int64(bn->addrs[i]));
            mBptr nn = wptr_to_mbptr(bn->addrs[i]);
            wptr addr = mbptr_to_wptr(bn);
            RETURN_FALSE_PRINT(equal_wptr(nn->up, addr), "sc: up\n");
            RETURN_FALSE(sancheck(nn));
        }
        for (int i = 0; i < bn->size; i++) {
            mBptr ch = wptr_to_mbptr(bn->addrs[i]);
            RETURN_FALSE_PRINT(ch->height == bn->height - 1, "sc: ht\n");
            RETURN_FALSE_PRINT(ch->size > 0, "sc: empty\n");
            RETURN_FALSE_PRINT(bn->keys[i] == ch->keys[0],
                               "sc: wrongkey %lld %lld\n", bn->keys[i],
                               ch->keys[0]);
            if (i < bn->size - 1) {
                RETURN_FALSE_PRINT(ch->keys[ch->size - 1] < bn->keys[i + 1],
                                   "sc: wrongchkey\n");
            }
        }
        for (int i = 1; i < bn->size; i++) {
            mBptr l = wptr_to_mbptr(bn->addrs[i - 1]);
            RETURN_FALSE_PRINT(equal_wptr(l->right, bn->addrs[i]),
                               "sc: wrongintr\n");
        }
        RETURN_FALSE(bn->size > 0);
        if (valid_wptr(bn->right)) {
            mBptr r = wptr_to_mbptr(bn->right);
            mBptr l = wptr_to_mbptr(bn->addrs[bn->size - 1]);
            RETURN_FALSE_PRINT(equal_wptr(l->right, r->addrs[0]),
                               "sc: wrongoutr\n");
        }
    }
    return true;
}

// internal
void mram_write(mBptr a, mBptr b, int size) {
    assert(size == sizeof(bnode));
    memcpy(b, a, size);
    for (int i = 1; i < b->size; i++) {
        assert_exec(b->keys[i] > b->keys[i - 1], [&]() {
            printf("unordered write %d %ld!\n", i, b->size);
            printf("%lld\n<=\n%lld\n", b->keys[i], b->keys[i - 1]);
            print_bnode(b);
        });
    }
}

void mram_read(mBptr a, mBptr b, int size) {
    assert(size == sizeof(bnode));
    memcpy(b, a, size);
    for (int i = 1; i < b->size; i++) {
        assert_exec(b->keys[i] > b->keys[i - 1], [&]() {
            printf("unordered read %d %ld!\n", i, b->size);
            printf("%lld\n<=\n%lld\n", b->keys[i], b->keys[i - 1]);
            print_bnode(b);
        });
    }
}

// void mram_read(void* a, void* b, int size) {
//     memcpy(b, a, size);
// }

inline void b_node_init(bnode *bn, int ht, wptr up, wptr right) {
    bn->height = ht;
    bn->up = up;
    bn->right = right;
    bn->size = 0;
    // #pragma clang loop unroll(full)
    for (int i = 0; i < DB_SIZE; i++) {
        bn->keys[i] = INT64_MIN;
        bn->addrs[i] = null_wptr;
    }
}

inline void b_node_fill(mBptr nn, bnode *bn, int size, int64_t *keys,
                        wptr *addrs) {
    mram_read(nn, bn, sizeof(bnode));
    bn->size = size;
    for (int i = 0; i < size; i++) {
        bn->keys[i] = keys[i];
        bn->addrs[i] = addrs[i];
    }
    for (int i = size; i < DB_SIZE; i++) {
        bn->keys[i] = INT64_MIN;
        bn->addrs[i] = null_wptr;
    }
    mram_write(bn, nn, sizeof(bnode));
    if (bn->height > 0) {
        for (int i = 0; i < size; i ++) {
            mBptr ch = wptr_to_mbptr(bn->addrs[i]);
            ch->up = mbptr_to_wptr(nn);
        }
    }
}

inline mBptr alloc_bn() {
    b_alloc_mutex.lock();
    mBptr nn = bbuffer + bcnt++;
    b_alloc_mutex.unlock();
    assert_exec(bcnt < BUFFER_SIZE, [&]() { printf("bcnt! of\n"); });
    return nn;
}

void init_core() {
    assert(bcnt == 1);
    bbuffer = new bnode[BUFFER_SIZE / sizeof(bnode)];
    mBptr nn = alloc_bn();
    bnode bn;
    b_node_init(&bn, 0, null_wptr, null_wptr);
    bn.size = 1;
    bn.keys[0] = INT64_MIN;
    bn.addrs[0] = null_wptr;
    mram_write(&bn, nn, sizeof(bnode));
    min_node = root = nn;
}

static inline int get_r(mpwptr addrs, int n, int l) {
    int r;
    for (r = l; r < n; r++) {
        if (addrs[r].addr == addrs[l].addr) {
            continue;
        } else {
            break;
        }
    }
    return r;
}

static inline int64_t b_search(int64_t key, mBptr *addr, int64_t *value) {
    mBptr tmp = root;
    bnode bn;
    while (true) {
        mram_read(tmp, &bn, sizeof(bnode));
        int64_t pred = INT64_MIN;
        wptr nxt_addr = null_wptr;
        // #pragma clang loop unroll(full)
        for (int i = 0; i < bn.size; i++) {
            if (bn.keys[i] <= key) {
            // if (valid_wptr(bn.addrs[i]) && bn.keys[i] <= key) {
                pred = bn.keys[i];
                nxt_addr = bn.addrs[i];
            }
        }
        assert(valid_wptr(nxt_addr) || bn.height == 0);
        if (bn.height > 0) {
            tmp = wptr_to_mbptr(nxt_addr);
        } else {
            *addr = tmp;
            *value = wptr_to_int64(nxt_addr);
            return pred;
        }
    }
}

// predecessor
void predecessor_core(int tid, int l, int r, slice<int64_t *, int64_t *> keys,
                      slice<key_value *, key_value *> results) {
    (void)tid;
    for (int i = l; i < r; i++) {
        mBptr nn;
        int64_t value;
        int64_t key = b_search(keys[i], &nn, &value);
        results[i] = (key_value){.key = key, .value = value};
    }
}

// insert
int64_t L3_lfts[MAXN];
int64_t L3_rts[MAXN];
int64_t L3_n;
int64_t mod_keys[MAXN];
wptr mod_values[MAXN];
wptr mod_addrs[MAXN];
int64_t mod_type[MAXN];

int64_t mod_keys2[MAXN];
wptr mod_values2[MAXN];
wptr mod_addrs2[MAXN];
int64_t mod_type2[MAXN];

const int remove_type = 1;
const int change_key_type = 2;
const int underflow_type = 4;

static inline void b_insert_onelevel(int n, int tid, int ht) {
    bnode bn;
    int64_t nnkeys[DB_SIZE];
    wptr nnvalues[DB_SIZE];
    int lft = L3_lfts[tid];
    int rt = L3_rts[tid];
    int nxtlft = lft;
    int nxtrt = nxtlft;
    int siz = 0;

    int l, r;  // catch all inserts to the same node
    for (l = lft; l < rt; l = r) {
        r = get_r(mod_addrs, n, l);
        // if (tid < 2 && (l == lft || r == rt) && (r - l < n)) {
        //     mu.lock();
        //     printf("tid=%d\tlft=%d\trt=%d\n", tid, lft, rt);
        //     printf("tid=%d\tl=%d\tr=%d\n", tid, l, r);
        //     for (int i = l; i < r; i ++) {
        //         printf("key=%lld\taddr=%llx\n", mod_keys[i],
        //         mod_addrs[i].addr);
        //     }
        //     mu.unlock();
        // }
        // printf("tid=%d\tl=%d\tr=%d\n", tid, l, r);
        wptr addr = mod_addrs[l];
        mBptr nn;
        wptr up, right;
        int nnsize;
        mBptr nn0;
        if (valid_wptr(addr)) {
            nn0 = nn = wptr_to_mbptr(addr);
            mram_read(nn, &bn, sizeof(bnode));
            up = bn.up;
            right = bn.right;
            nnsize = bn.size;
            for (int i = 0; i < nnsize; i++) {
                nnkeys[i] = bn.keys[i];
                nnvalues[i] = bn.addrs[i];
            }
        } else {
            nn = alloc_bn();
            up = null_wptr;
            right = null_wptr;
            nnsize = 1;
            nnkeys[0] = INT64_MIN;
            nnvalues[0] = mbptr_to_wptr(root);
            if (ht > 0) {
                root->up = mbptr_to_wptr(nn);
                for (int i = l; i < r; i++) {
                    wptr child_addr = mod_values[i];
                    mBptr ch = wptr_to_mbptr(child_addr);
                    // printf("ch=%llx\n", ch);
                    ch->up = mbptr_to_wptr(nn);
                }
            }
            root = nn;
        }
        b_node_init(&bn, ht, up, right);
        int totsize = 0;

        {
            int nnl = 0;
            int i = l;
            while (i < r || nnl < nnsize) {
                if (i < r && nnl < nnsize) {
                    if (mod_keys[i] == nnkeys[nnl]) {
                        i++;
                        totsize--;  // replace
                    } else if (mod_keys[i] < nnkeys[nnl]) {
                        i++;
                    } else {
                        nnl++;
                    }
                } else if (i == r) {
                    nnl++;
                } else {
                    i++;
                }
                totsize++;
            }
        }

        int nnl = 0;
        bn.size = 0;
        // int blocks = ((totsize - 1) / DB_SIZE) + 1;
        // int block_size = ((totsize - 1) / blocks) + 1;

        int l0 = l;
        for (int i = 0; nnl < nnsize || l < r; i++) {
            if (nnl < nnsize && (l == r || nnkeys[nnl] < mod_keys[l])) {
                bn.keys[bn.size] = nnkeys[nnl];
                bn.addrs[bn.size] = nnvalues[nnl];
                nnl++;
            } else {
                bn.keys[bn.size] = mod_keys[l];
                bn.addrs[bn.size] = mod_values[l];
                if (nnl < nnsize && nnkeys[nnl] == mod_keys[l]) {  // replace
                    nnl++;
                }
                l++;
            }
            bn.size++;
            if (bn.size == 1 && (i > 0)) {  // newnode
                mod_keys2[nxtrt] = bn.keys[0];
                mod_values2[nxtrt] = mbptr_to_wptr(nn);
                mod_addrs2[nxtrt] = bn.up;
                nxtrt++;
                assert_exec(bn.keys[0] != INT64_MIN, [&]() {
                    printf("mod_keys2 = INT64_MIN\n");
                    printf("bbuffer=%x bcnt=%d\n", bbuffer, bcnt);
                    printf("ht=%d addr=%x\n", ht, nn0);
                    printf("l=%d\tr=%d\tnnl=%d\n", l0, r, nnl);
                    printf("i=%d\ttotsize=%d\n", i, totsize);
                    for (int x = 0; x < nnsize; x++) {
                        printf("nn[%d]=%lld\n", x, nnkeys[x]);
                    }
                    mram_read(nn0, &bn, sizeof(bnode));
                    printf("nn0size=%lld nnsize=%d\n", bn.size, nnsize);
                    for (int x = 0; x < bn.size; x++) {
                        printf("nn0[%d]=%lld\n", x, bn.keys[x]);
                    }
                    for (int x = l0; x < r; x++) {
                        printf("mod[%d]=%lld\n", x, mod_keys[x]);
                    }
                    printf("i=%d\ttotsize=%d\n", i, totsize);
                });
            }
            if (bn.size == DB_SIZE ||
                (i + HF_DB_SIZE + 1 == totsize && bn.size > HF_DB_SIZE)) {
                // if (bn.keys[0] == -7125798835171897ll &&
                //     bn.keys[1] == -6795333434612133ll) {
                //     printf("**\n");
                //     printf("bbuffer=%x bcnt=%d\n", bbuffer, bcnt);
                //     printf("ht=%d addr=%x\n", ht, nn0);
                //     printf("lft=%d\trt=%d\n", lft, rt);
                //     printf("l=%d\tr=%d\tnnl=%d\n", l0, r, nnl);
                //     printf("i=%d\ttotsize=%d\n", i, totsize);
                //     for (int x = 0; x < nnsize; x++) {
                //         printf("nn[%d]=%lld\n", x, nnkeys[x]);
                //     }
                //     mram_read(nn0, &bn, sizeof(bnode));
                //     printf("nn0size=%lld nnsize=%d\n", bn.size, nnsize);
                //     for (int x = 0; x < bn.size; x++) {
                //         printf("nn0[%d]=%lld\n", x, bn.keys[x]);
                //     }
                //     for (int x = l0; x < r; x++) {
                //         printf("mod[%d]=%lld\n", x, mod_keys[x]);
                //     }
                //     printf("i=%d\ttotsize=%d\n", i, totsize);
                //     printf("**\n");
                // }
                for (int i = 0; i < bn.size; i++) {
                    if (bn.height > 0) {
                        assert(valid_wptr(bn.addrs[i]));
                        mBptr ch = wptr_to_mbptr(bn.addrs[i]);
                        ch->up = mbptr_to_wptr(nn);
                    }
                }
                if (nnl == nnsize && l == r) {
                    mram_write(&bn, nn, sizeof(bnode));
                } else {
                    wptr up = bn.up, right = bn.right;
                    int64_t ht = bn.height;

                    mBptr nxt_nn = alloc_bn();
                    bn.right = mbptr_to_wptr(nxt_nn);
                    mram_write(&bn, nn, sizeof(bnode));

                    b_node_init(&bn, ht, up, right);
                    nn = nxt_nn;
                }

                // wptr up = bn.up, right = bn.right;
                // int64_t ht = bn.height;
                // b_node_init(&bn, ht, up, right);
                // nn = alloc_bn();
            }
            if (nnl == nnsize && l == r) {
                assert_exec(i + 1 == totsize, [&]() {
                    printf("i+1 != totsize\n");
                    printf("bbuffer=%x bcnt=%d\n", bbuffer, bcnt);
                    printf("ht=%d addr=%x\n", ht, nn0);
                    printf("l=%d\tr=%d\tnnl=%d\n", l0, r, nnl);
                    printf("i=%d\ttotsize=%d\n", i, totsize);
                    for (int x = 0; x < nnsize; x++) {
                        printf("nn[%d]=%lld\n", x, nnkeys[x]);
                    }
                    mram_read(nn0, &bn, sizeof(bnode));
                    printf("nn0size=%lld nnsize=%d\n", bn.size, nnsize);
                    for (int x = 0; x < bn.size; x++) {
                        printf("nn0[%d]=%lld\n", x, bn.keys[x]);
                    }
                    for (int x = l0; x < r; x++) {
                        printf("mod[%d]=%lld\n", x, mod_keys[x]);
                    }
                    printf("i=%d\ttotsize=%d\n", i, totsize);
                });
            }
        }
        if (bn.size != 0) {
            for (int i = 0; i < bn.size; i++) {
                if (bn.height > 0) {
                    assert(valid_wptr(bn.addrs[i]));
                    mBptr ch = wptr_to_mbptr(bn.addrs[i]);
                    ch->up = mbptr_to_wptr(nn);
                }
            }
            mram_write(&bn, nn, sizeof(bnode));
        }
        assert_exec(l == r && nnl == nnsize, [&]() {
            printf("l=%d\tr=%d\tnnl=%d\tnnsize=%d\n", l, r, nnl, nnsize);
        });
    }
    L3_lfts[tid] = nxtlft;
    L3_rts[tid] = nxtrt;
    if (nxtlft != nxtrt) {
        // printf("after ht=%d\ttid=%d\tlft=%d\trt=%d\n", ht, tid, nxtlft,
        // nxtrt);
    }
}

static inline void b_remove_onelevel(int n, int tid, int ht) {
    bnode bn;
    int64_t nnkeys[DB_SIZE];
    wptr nnvalues[DB_SIZE];
    int lft = L3_lfts[tid];
    int rt = L3_rts[tid];
    int nxtlft = lft;
    int nxtrt = nxtlft;
    int siz = 0;

    int l, r;  // catch all inserts to the same node
    for (l = lft; l < rt; l = r) {
        r = get_r(mod_addrs, n, l);

        wptr addr = mod_addrs[l];
        assert(valid_wptr(addr));

        mBptr nn = wptr_to_mbptr(addr);
        mram_read(nn, &bn, sizeof(bnode));
        wptr up = bn.up, right = bn.right;
        int nnsize = bn.size;
        for (int i = 0; i < nnsize; i++) {
            nnkeys[i] = bn.keys[i];
            nnvalues[i] = bn.addrs[i];
        }

        b_node_init(&bn, ht, up, right);
        int totsize = 0;

        {
            int nnl = 0;
            int i = l;
            while (nnl < nnsize) {
                if (i == r || nnkeys[nnl] < mod_keys[i]) {
                    bn.keys[bn.size] = nnkeys[nnl];
                    bn.addrs[bn.size] = nnvalues[nnl];
                    bn.size++;
                    nnl++;
                } else if (nnkeys[nnl] > mod_keys[i]) {
                    i++;
                } else {  // equal
                    if (mod_type[i] == change_key_type) {
                        bn.keys[bn.size] = wptr_to_int64(mod_values[i]);
                        bn.addrs[bn.size] = nnvalues[nnl];
                        bn.size++;
                    }
                    nnl++;
                }
            }
        }
        mram_write(&bn, nn, sizeof(bnode));

        // const int HF_DB_SIZE = DB_SIZE >> 1;
        int future_modif = 0;
        // mu.lock();
        // print_array("mod_keys", mod_keys + l, r - l);
        // print_array("nnkeys", nnkeys, nnsize);
        // print_bnode(&bn);
        // mu.unlock();
        if (bn.size < HF_DB_SIZE) {
            future_modif |= underflow_type;
        }
        if (bn.size == 0 || nnkeys[0] != bn.keys[0]) {
            future_modif |= change_key_type;
        }
        if (nn == root) {
            future_modif = 0;
        }
        if (future_modif > 0) {
            mod_keys2[nxtrt] = nnkeys[0];
            mod_addrs2[nxtrt] = addr;
            mod_values2[nxtrt] = int64_to_wptr(bn.keys[0]);
            mod_type2[nxtrt] = future_modif;
            nxtrt++;
        }
    }
    L3_lfts[tid] = nxtlft;
    L3_rts[tid] = nxtrt;
}

const int SERIAL_HEIGHT = 2;

void insert_core(int n, int tid, int l, int r,
                 slice<key_value *, key_value *> kvs) {
    pthread_barrier_wait(&b2);

    // bottom up
    for (int i = l; i < r; i++) {
        int64_t key = kvs[i].key;
        int64_t value;
        bnode *nn;
        int64_t pred = b_search(key, &nn, &value);
        mod_addrs[i] = mbptr_to_wptr(nn);
        mod_keys[i] = kvs[i].key;
        mod_values[i] = int64_to_wptr(kvs[i].value);
    }
    // printf("%d %d %d\n", tid, l, r);
    // pthread_barrier_wait(&b2);
    // if (tid == 0) {
    //     for (int i = 0; i < n; i++) {
    //         printf("%2d\t%llx\n", i, mod_addrs[i].addr);
    //     }
    // }

    L3_n = n;
    pthread_barrier_wait(&b1);

    for (int ht = 0; ht <= root->height + 1; ht++) {
        if (ht < SERIAL_HEIGHT) {
            // distribute work
            n = L3_n;
            if (tid == 0) {
                printf("PARALLEL:%d\n", n);
            }
            int lft = n * tid / threads;
            int rt = n * (tid + 1) / threads;
            // printf("t0 n=%d\ttid=%d\tlft=%d\trt=%d\n", n, tid, lft, rt);
            if (rt > lft) {
                if (lft != 0) {
                    lft = get_r(mod_addrs, n, lft - 1);
                }
                assert(rt > 0);
                rt = get_r(mod_addrs, n, rt - 1);
            }

            // printf("t1 n=%d\ttid=%d\tlft=%d\trt=%d\n", n, tid, lft, rt);
            L3_lfts[tid] = lft;
            L3_rts[tid] = rt;
            // printf("l=%d\tr=%d\n", L3_lfts[i], L3_rts[i]);
            pthread_barrier_wait(&b2);

            // execute
            b_insert_onelevel(n, tid, ht);
            pthread_barrier_wait(&b3);

            // distribute work
            if (tid == 0) {
                n = 0;
                for (int i = 0; i < threads; i++) {
                    for (int j = L3_lfts[i]; j < L3_rts[i]; j++) {
                        mod_keys[n] = mod_keys2[j];
                        mod_values[n] = mod_values2[j];
                        mod_addrs[n] = mod_addrs2[j];
                        assert(mod_keys[n] != INT64_MIN);
                        if (n > 0) {
                            assert_exec(mod_keys[n] > mod_keys[n - 1], [&]() {
                                printf("j=%d\tlft=%d\trt=%d\n", j, L3_lfts[i],
                                       L3_rts[i]);
                                for (int i = 0; i < threads; i++) {
                                    printf("l=%d\tr=%d\n", L3_lfts[i],
                                           L3_rts[i]);
                                }
                                print_bnode(wptr_to_mbptr(mod_addrs[n]));
                                int l = n - 3, r = n + 1;
                                for (int n = l; n < r; n++) {
                                    printf(
                                        "n=%d\tk=%lld\tv=%llx\ta=%"
                                        "llx\n",
                                        n, mod_keys[n], mod_values[n].addr,
                                        mod_addrs[n].addr);
                                    print_bnode(wptr_to_mbptr(mod_values[n]));
                                }
                            });
                        }
                        n++;
                    }
                }
                L3_n = n;
                // printf("n=%d\n", n);
            }
            pthread_barrier_wait(&b1);
        } else {
            if (tid == 0 && n > 0) {
                printf("SOLO:%d\n", n);
                L3_lfts[0] = 0;
                L3_rts[0] = n;
                b_insert_onelevel(n, tid, ht);
                n = L3_rts[0];
                for (int i = 0; i < n; i++) {
                    mod_keys[i] = mod_keys2[i];
                    mod_values[i] = mod_values2[i];
                    mod_addrs[i] = mod_addrs2[i];
                }
            } else {
                break;
            }
        }
    }
    pthread_barrier_wait(&b3);
}

void b_remove_serial_compact(int n, int ht) {
    bnode bn;
    int64_t nnkeys[DB_SIZE + HF_DB_SIZE];
    wptr nnaddrs[DB_SIZE + HF_DB_SIZE];

    int nxt_n = 0;
    for (int l = 0; l < n;) {
        int mt = mod_type[l];
        if (mt == change_key_type) {
            mod_keys2[nxt_n] = mod_keys[l];
            mod_values2[nxt_n] = mod_values[l];
            mBptr nn = wptr_to_mbptr(mod_addrs[l]);
            mod_addrs2[nxt_n] = nn->up;
            mod_type2[nxt_n] = change_key_type;
            nxt_n++;
            l++;
        } else if (mt & underflow_type) {
            int r = l;
            wptr addr = mod_addrs[l];
            int nnl = 0;
            while (nnl < HF_DB_SIZE) {
                mBptr nn = wptr_to_mbptr(addr);
                mram_read(nn, &bn, sizeof(bnode));
                for (int i = 0; i < bn.size; i++) {
                    nnkeys[nnl] = bn.keys[i];
                    nnaddrs[nnl] = bn.addrs[i];
                    nnl++;
                }
                if (equal_wptr(addr, mod_addrs[r])) {
                    r++;
                } else {
                    assert(bn.size >= HF_DB_SIZE ||
                           equal_wptr(bn.right, null_wptr));
                }
                if (nnl < HF_DB_SIZE && !equal_wptr(bn.right, null_wptr)) {
                    addr = bn.right;
                } else {
                    break;
                }
            }

            mBptr left_nn = wptr_to_mbptr(mod_addrs[l]);
            wptr right_out = bn.right;

            if (nnl > 0) {
                if (nnkeys[0] != mod_keys[l]) {  // change key
                    mod_keys2[nxt_n] = mod_keys[l];
                    mod_values2[nxt_n] = int64_to_wptr(nnkeys[0]);
                    mod_addrs2[nxt_n] = left_nn->up;
                    mod_type2[nxt_n] = change_key_type;
                    nxt_n++;
                }
                left_nn->right = right_out;
            } else {
                mod_keys2[nxt_n] = mod_keys[l];
                mod_values2[nxt_n] = null_wptr;
                mod_addrs2[nxt_n] = left_nn->up;
                mod_type2[nxt_n] = remove_type;
                nxt_n++;
            }

            int mid = nnl >> 1;
            int64_t mid_key = nnkeys[mid];

            if (nnl > 0 && nnl <= DB_SIZE) {
                mBptr nn = wptr_to_mbptr(mod_addrs[l]);
                b_node_fill(nn, &bn, nnl, nnkeys, nnaddrs);
                bool addr_covered = equal_wptr(addr, mod_addrs[l]);
                for (int i = l + 1; i < r; i++) {
                    if (equal_wptr(addr, mod_addrs[i])) {
                        addr_covered = true;
                    }
                    mBptr nn = wptr_to_mbptr(mod_addrs[i]);
                    mod_keys2[nxt_n] = mod_keys[i];
                    mod_values2[nxt_n] = null_wptr;
                    mod_addrs2[nxt_n] = nn->up;
                    mod_type2[nxt_n] = remove_type;
                    nxt_n++;
                }
                if (!addr_covered) {
                    mBptr nn = wptr_to_mbptr(addr);
                    assert(nn->size >= HF_DB_SIZE ||
                           equal_wptr(nn->right, null_wptr));
                    mod_keys2[nxt_n] = nn->keys[0];
                    mod_values2[nxt_n] = null_wptr;
                    mod_addrs2[nxt_n] = nn->up;
                    mod_type2[nxt_n] = remove_type;
                    nxt_n++;
                }
            } else if (nnl > DB_SIZE) {
                bool addr_covered = false;
                mBptr nn = wptr_to_mbptr(mod_addrs[l]);
                b_node_fill(nn, &bn, mid, nnkeys, nnaddrs);
                int filpos = l;
                {
                    for (int i = l; i < r; i++) {
                        if (equal_wptr(addr, mod_addrs[i])) {
                            addr_covered = true;
                        }
                        if (mod_keys[i] <= mid_key) {
                            filpos = i;
                        }
                    }
                    if (!addr_covered) {
                        mBptr nn = wptr_to_mbptr(addr);
                        assert(nn->size >= HF_DB_SIZE ||
                               equal_wptr(nn->right, null_wptr));
                        if (nn->keys[0] <= mid_key) {
                            filpos = -1;
                        }
                    }
                }
                assert_exec(filpos != l, [&]() {
                    printf("l=%d r=%d nnl=%d\n", l, r, nnl);
                    print_array("mod_keys", mod_keys + l, r - l);
                    print_array("nnkeys", nnkeys, nnl);
                    printf("midkey=%lld\n", mid_key);
                });

                for (int i = l + 1; i < r; i++) {
                    mBptr nn = wptr_to_mbptr(mod_addrs[i]);
                    if (i != filpos) {
                        mod_keys2[nxt_n] = mod_keys[i];
                        mod_addrs2[nxt_n] = nn->up;
                        mod_type2[nxt_n] = remove_type;
                        nxt_n++;
                    } else {
                        b_node_fill(nn, &bn, nnl - mid, nnkeys + mid,
                                    nnaddrs + mid);
                        left_nn->right = mod_addrs[i];
                        nn->right = right_out;

                        mod_keys2[nxt_n] = mod_keys[i];
                        mod_values2[nxt_n] = int64_to_wptr(mid_key);
                        mod_addrs2[nxt_n] = nn->up;
                        mod_type2[nxt_n] = change_key_type;
                        nxt_n++;
                    }
                }
                if (!addr_covered) {
                    mBptr nn = wptr_to_mbptr(addr);
                    assert(nn->size >= HF_DB_SIZE ||
                           equal_wptr(nn->right, null_wptr));
                    if (filpos != -1) {
                        mod_keys2[nxt_n] = nn->keys[0];
                        mod_addrs2[nxt_n] = nn->up;
                        mod_type2[nxt_n] = remove_type;
                        nxt_n++;
                    } else {
                        left_nn->right = addr;
                        nn->right = right_out;

                        mod_keys2[nxt_n] = nn->keys[0];
                        mod_values2[nxt_n] = int64_to_wptr(mid_key);
                        mod_addrs2[nxt_n] = nn->up;
                        mod_type2[nxt_n] = change_key_type;
                        nxt_n++;
                        b_node_fill(nn, &bn, nnl - mid, nnkeys + mid,
                                    nnaddrs + mid);
                    }
                }
            } else {
                // nnl == 0, pass
            }
            l = r;
        } else {
            assert_exec(false, [&]() { printf("mt=%d\n", mt); });
        }
    }
    L3_lfts[0] = 0;
    L3_rts[0] = nxt_n;
}

void remove_core(int n, int tid, int l, int r,
                 slice<int64_t *, int64_t *> keys) {
    // bottom up
    for (int i = l; i < r; i++) {
        int64_t key = keys[i];
        int64_t value;
        bnode *nn;
        int64_t pred = b_search(key, &nn, &value);
        mod_addrs[i] = mbptr_to_wptr(nn);
        mod_keys[i] = keys[i];
        mod_type[i] = remove_type;
    }
    // printf("%d %d %d\n", tid, l, r);
    // pthread_barrier_wait(&b2);
    // if (tid == 0) {
    //     for (int i = 0; i < n; i++) {
    //         printf("%2d\t%llx\n", i, mod_addrs[i].addr);
    //     }
    // }

    L3_n = n;
    pthread_barrier_wait(&b1);

    for (int ht = 0; ht <= root->height; ht++) {
        if (ht < SERIAL_HEIGHT) {
            // distribute work
            n = L3_n;
            if (tid == 0) {
                printf("PARALLEL:%d\n", n);
            }
            int lft = n * tid / threads;
            int rt = n * (tid + 1) / threads;
            // printf("t0 n=%d\ttid=%d\tlft=%d\trt=%d\n", n, tid, lft, rt);
            if (rt > lft) {
                if (lft != 0) {
                    lft = get_r(mod_addrs, n, lft - 1);
                }
                assert(rt > 0);
                rt = get_r(mod_addrs, n, rt - 1);
            }

            // printf("t1 n=%d\ttid=%d\tlft=%d\trt=%d\n", n, tid, lft, rt);
            L3_lfts[tid] = lft;
            L3_rts[tid] = rt;
            // printf("l=%d\tr=%d\n", L3_lfts[i], L3_rts[i]);
            pthread_barrier_wait(&b2);

            // execute
            b_remove_onelevel(n, tid, ht);
            // b_insert_onelevel(n, tid, ht);
            pthread_barrier_wait(&b3);

            // distribute work
            if (tid == 0) {
                n = 0;
                for (int i = 0; i < threads; i++) {
                    for (int j = L3_lfts[i]; j < L3_rts[i]; j++) {
                        mod_keys[n] = mod_keys2[j];
                        mod_values[n] = mod_values2[j];
                        mod_addrs[n] = mod_addrs2[j];
                        mod_type[n] = mod_type2[j];
                        n++;
                    }
                }
                printf("n=%d\n", n);
                // print_array("mod_keys", mod_keys, n);
                // print_array("mod_values", (int64_t *)mod_values, n);
                // print_array("mod_addrs", (int64_t *)mod_addrs, n, true);
                // print_array("mod_type", mod_type, n);
                b_remove_serial_compact(n, ht);
                n = L3_rts[0];
                for (int i = 0; i < n; i++) {
                    mod_keys[i] = mod_keys2[i];
                    mod_values[i] = mod_values2[i];
                    mod_addrs[i] = mod_addrs2[i];
                    mod_type[i] = mod_type2[i];
                }
                L3_n = n;
                // print_array("mod_keys", mod_keys, n);
                // print_array("mod_values", (int64_t *)mod_values, n);
                // print_array("mod_addrs", (int64_t *)mod_addrs, n, true);
                // print_array("mod_type", mod_type, n);
                // printf("n=%d\n", n);
            }
            pthread_barrier_wait(&b1);
        } else {
            if (tid == 0 && n > 0) {
                printf("SOLO:%d\n", n);
                L3_lfts[0] = 0;
                L3_rts[0] = n;
                b_remove_onelevel(n, tid, ht);
                n = L3_rts[0];
                for (int i = 0; i < n; i++) {
                    mod_keys[i] = mod_keys2[i];
                    mod_values[i] = mod_values2[i];
                    mod_addrs[i] = mod_addrs2[i];
                    mod_type[i] = mod_type2[i];
                }
                b_remove_serial_compact(n, ht);
                n = L3_rts[0];
                for (int i = 0; i < n; i++) {
                    mod_keys[i] = mod_keys2[i];
                    mod_values[i] = mod_values2[i];
                    mod_addrs[i] = mod_addrs2[i];
                    mod_type[i] = mod_type2[i];
                }
            } else {
                break;
            }
        }
    }
    pthread_barrier_wait(&b3);
}

static batch_parallel_oracle oracle;

static void init() {
    threads = num_workers();
    // threads = 2;
    pthread_barrier_init(&b1, nullptr, threads);
    pthread_barrier_init(&b2, nullptr, threads);
    pthread_barrier_init(&b3, nullptr, threads);
    init_core();
}

static auto get(slice<int64_t *, int64_t *> ops) {
    auto kvs = oracle.predecessor_batch(ops);
    return parlay::map(kvs, [&](const key_value &kv) { return kv.value; });
}

static void update(slice<key_value *, key_value *> ops) {
    oracle.insert_batch(ops);
}

static void scan(slice<scan_operation *, scan_operation *> ops) {
    (void)ops;
    assert(false);
}

static auto predecessor(slice<int64_t *, int64_t *> ops) {
    int n = ops.size();
    int blocks = threads;
    auto keys = ops;
    auto results = parlay::sequence<key_value>(n);
    parallel_for(
        0, blocks,
        [&](size_t tid) {
            int l = n * tid / blocks;
            int r = n * (tid + 1) / blocks;
            predecessor_core(tid, l, r, keys, make_slice(results));
        },
        1);
    return results;
}

static void insert(slice<key_value *, key_value *> kvs) {
    // time_nested("sc", [&]() {
    //     sancheck(root);
    // });
    int n = kvs.size();
    int blocks = threads;
    parlay::sort_inplace(kvs);
    parallel_for(
        0, blocks,
        [&](size_t tid) {
            int l = n * tid / blocks;
            int r = n * (tid + 1) / blocks;
            insert_core(n, tid, l, r, kvs);
        },
        1);
    // print_bnode(root);
}

static void remove(slice<int64_t *, int64_t *> keys) {
    int n = keys.size();
    int blocks = threads;
    parlay::sort_inplace(keys);
    parallel_for(
        0, blocks,
        [&](size_t tid) {
            int l = n * tid / blocks;
            int r = n * (tid + 1) / blocks;
            remove_core(n, tid, l, r, keys);
        },
        1);
}
};  // namespace pim_skip_list