#pragma once

#define TASK(NAME, ID, FIXED, LEN, CONTENT) \
    struct NAME {                           \
        static int id;                      \
        static bool fixed;                  \
        static int task_len;                \
        struct CONTENT;                     \
    };                                      \
    int NAME::id = (ID);                    \
    bool NAME::fixed = (FIXED);             \
    int NAME::task_len = (LEN);

// #define is_variable_length(NAME) (NAME::fixed)
// #define task_size(NAME) (NAME::task_len)
// #define task_id(NAME) (NAME::id)

#include "task_base.h"