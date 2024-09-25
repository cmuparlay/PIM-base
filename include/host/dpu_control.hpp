#pragma once
#include <iostream>
#include <string>
#include <mutex>
#include "debug.hpp"
#include "pim_interface_header.hpp"

using namespace std;

extern "C" {
    #include <dpu.h>
    #include <dpu_runner.h>
}

dpu_set_t dpu_set, dpu;
int nr_of_dpus;
uint32_t each_dpu;
int epoch_number = 0;

namespace dpu_control {

bool active = false;
bool working_by_id = -1; // idle
mutex dpu_mutex;

// public:
void alloc(int count) {
    ASSERT(active == false);
    DPU_ASSERT(dpu_alloc(count, "regionMode=perf", &dpu_set));
    DPU_ASSERT(dpu_get_nr_dpus(dpu_set, (uint32_t*)&nr_of_dpus));
    printf("Allocated %d DPU(s)\n", nr_of_dpus);
    active = true;
}

void load(string binary) {
    DPU_ASSERT(dpu_load(dpu_set, binary.c_str(), NULL));
    namespace_pim_interface::load_from_dpu_set(dpu_set);
}

template <typename F>
void print_log(F f) {
    DPU_FOREACH(dpu_set, dpu, each_dpu) {
        if (f((int)each_dpu)) {
            cout << "DPU ID = " << each_dpu << endl;
            DPU_ASSERT(dpu_log_read(dpu, stdout));
        }
    }
}

void print_all_log() {
    print_log([](size_t i) {
        (void)i;
        return true;
    });
}

void free() {
    ASSERT(active == true);
    active = false;
    DPU_ASSERT(dpu_free(dpu_set));
}

bool ready() {
    bool a, b;
    bool *done = &a;
    bool *fault = &b;

    *done = true;
    *fault = false;

    for (uint32_t each_rank = 0; each_rank < dpu_set.list.nr_ranks; ++each_rank) {
        dpu_error_t status;
        bool rank_done;
        bool rank_fault;

        if ((status = dpu_status_rank(dpu_set.list.ranks[each_rank], &rank_done, &rank_fault)) != DPU_OK) {
            return status;
        }

        *done = *done && rank_done;
        *fault = *fault || rank_fault;
    }
    return *done;
}
};  // namespace dpu_control