#pragma once

#define TASK(NAME, ID, FIXED, LEN, CONTENT) \
    typedef struct CONTENT NAME;     \
    const int NAME##_id = (ID);        \
    const bool NAME##_fixed = (FIXED);      \
    const int NAME##_task_len = (LEN);

#define task_fixed(NAME) (NAME##_fixed)
#define task_len(NAME) (NAME##_task_len)
#define task_id(NAME) (NAME##_id)

#include "task_base.h"