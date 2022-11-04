#pragma once 
#include <cstdlib>

const int MAX_BATCH_SIZE = 5e6;

enum operation_t {
    empty_t,
    get_t,
    update_t,
    predecessor_t,
    scan_t,
    insert_t,
    remove_t
};
const int OPERATION_NR_ITEMS = 7;

int op_count[OPERATION_NR_ITEMS];
struct get_operation {
    int64_t key;
} get_ops[MAX_BATCH_SIZE];

struct update_operation {
    int64_t key;
    int64_t value;
} update_ops[MAX_BATCH_SIZE];

struct predecessor_operation {
    int64_t key;
} predecessor_ops[MAX_BATCH_SIZE];

struct scan_operation {
    int64_t lkey;
    int64_t rkey;
} scan_ops[MAX_BATCH_SIZE];

struct insert_operation {
    int64_t key;
    int64_t value;
} insert_ops[MAX_BATCH_SIZE];

struct remove_operation {
    int64_t key;
} remove_ops[MAX_BATCH_SIZE];

struct operation {
    union {
        get_operation g;
        update_operation u;
        predecessor_operation p;
        scan_operation s;
        insert_operation i;
        remove_operation r;
    } tsk;
    operation_t type;
};