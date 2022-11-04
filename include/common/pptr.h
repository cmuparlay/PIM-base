#pragma once

#include <stdint.h>
#include <assert.h>
#include <stdio.h>
#include <stdbool.h>

#define INVALID_DPU_ID ((uint32_t)-1)
#define INVALID_DPU_ADDR ((uint32_t)-1)

typedef struct pptr {
    uint32_t id;
    uint32_t addr;
} pptr __attribute__((aligned (8)));

typedef struct offset_pptr {
    uint16_t id;
    uint16_t offset;
    uint32_t addr;
} offset_pptr __attribute__((aligned(8)));

const pptr null_pptr = (pptr){.id = INVALID_DPU_ID, .addr = INVALID_DPU_ADDR};