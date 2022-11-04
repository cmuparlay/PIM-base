#pragma once
#include <stdint.h>
#include "pptr.h"

// name
// id
// fixed : true for fixed
// length (expected)
// content
#ifndef TASK
#define TASK(NAME, ID, FIXED, LENGTH, CONTENT)
#endif

TASK(empty_task, 0, true, 0, {})

TASK(fixed_task, 1, true, sizeof(fixed_task), {
    pptr addr;
    int64_t a[1];
})

TASK(fixed_reply, 2, true, sizeof(fixed_reply), { int64_t a[1]; })

// #define FIXED_TSK 1
// typedef struct {
//     pptr addr;
//     int64_t a[3];
// } fixed_task;

// #define FIXED_REP 2
// typedef struct {
//     int64_t a[1];
// } fixed_reply;

// #define VARLEN_TSK 3
// typedef struct {
//     pptr addr;
//     int64_t len;
//     int64_t val[];
// } varlen_task;

// #define VARLEN_REP 4
// typedef struct {
//     int64_t len;
//     int64_t val[];
// } varlen_reply;