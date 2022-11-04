#pragma once
#include "settings.h"
#include <cassert>

#ifdef KHB_CPU_DEBUG
#define ASSERT(x) assert(x)
#define ASSERT_EXEC(x, y) \
    if (!(x)) {           \
        y;                \
        assert(false);    \
    }
#else
#define ASSERT(x) {(void)(x);}
#define ASSERT_EXEC(x, y) {(void)(x);}
#endif