#include <iostream>
#include <cstdio>
#include <cstring>
#include <algorithm>
#include <argparse/argparse.hpp>
#include "random_generator.hpp"
#include "task_framework_host.hpp"
#include "task.hpp"
#include "oracle.hpp"
#include "sort.hpp"
#include "driver.hpp"

#ifndef DPU_BINARY
#define DPU_BINARY "build/pim_base_dpu"
#endif

struct ttmp {
    pptr addr;
};

int taskpos[2000000];
int length = 1e6;

inline void taskgen(bool exec) {
    auto io = alloc_io_manager();
    ASSERT(io == io_managers[0]);
    io->init();
    IO_Task_Batch* batch = nullptr;
    time_nested("alloc", [&]() {
        batch = io->alloc<fixed_task, fixed_reply>(direct);
        // batch =
        //     io->alloc_task_batch(direct, fixed_length, fixed_length, FIXED_TSK,
        //                          sizeof(fixed_task), sizeof(fixed_reply));
    });

    rn_gen rn(137);
    auto addrf = [&](size_t i, int64_t v) {
        (void)i;
        uint64_t rd = abs(v);
        uint32_t id = rd % nr_of_dpus;
        uint32_t addr = (rd / nr_of_dpus) % 8000000;
        return (pptr){.id = id, .addr = addr};
    };

    auto addrs = parlay::sequence<pptr>(length);
    auto keys = parlay::sequence<int64_t>(length);

    time_nested("taskfill", [&]() {
        parfor_wrap(0, length, [&](size_t i) {
            int64_t v1 = rn.random(i * 2);
            int64_t v2 = rn.random(i * 2 + 1);
            addrs[i] = addrf(i, v1);
            keys[i] = v2;
        });
    });

    auto taskf = [&](size_t i) {
        fixed_task t;
        t.addr = addrs[i];
        t.a[0] = keys[i];
        return t;
    };

    parlay::sequence<int> location;
    time_nested("distribute", [&]() {
        location = parlay::sequence<int>(length, 0);
        auto targets = [&](const fixed_task& x) { return x.addr.id; };
        batch->push_task_from_array_by_isort<true>(length, taskf, targets, make_slice(location));
        // parfor_wrap(0, length, [&](size_t i) {
        //     fixed_task&& t = taskf(i);
        //     auto it =
        //         (fixed_task*)batch->push_task_zero_copy(t.addr.id, -1, true,
        //         &location[i]);
        //     *it = t;
        // });
    });
    time_nested("finish", [&]() { io->finish_task_batch(); });

    if (!exec) {
        return;
    }

    ASSERT(io->exec());
    parlay::parallel_for(0, length, [&](size_t i) {
        pptr addr = addrs[i];
        auto reply = (fixed_reply*)batch->ith(addr.id, location[i]);
        assert(reply->a[0] == pptr_to_int64(addr));
    });
}

inline void clean_cache() {
    const int DEF = 5e6;
    int64_t* a = new int64_t[DEF];
    parlay::parallel_for(0, DEF, [&](size_t i) {
        (void)i;
        int pos = abs(rn_gen::parallel_rand()) % DEF;
        a[pos] = rn_gen::parallel_rand();
    });
    delete[] a;
}

inline void taskgen_test() {
    taskgen(false);
    io_managers[0].reset();
    clean_cache();
}

void test_task_framework(int dpus) {
    dpu_control::alloc(dpus);
    dpu_control::load(DPU_BINARY);
    rn_gen::init();

    timer::active = false;
    {
        taskgen(true);
        dpu_control::print_log(
            [&](auto each_dpu) -> bool { return each_dpu < 10; });
        io_managers[0].reset();
    }
    timer::active = true;
    for (int i = 0; i < 1000; i++) {
        time_nested("taskgen", [&]() { taskgen_test(); });
    }
    print_all_timers(print_type::pt_full);
    print_all_timers(print_type::pt_time);
}

void test_oracle() {
    batch_parallel_oracle oracle;
    oracle.insert_batch(
        parlay::make_slice(parlay::delayed_seq<key_value>(50000, [](size_t x) {
            int64_t i = (int64_t)x;
            return (key_value){.key = 2 * i, .value = 2 * i + 1};
        })));
    oracle.insert_batch(
        parlay::make_slice(parlay::delayed_seq<key_value>(30000, [](size_t x) {
            int64_t i = (int64_t)x;
            return (key_value){.key = 4 * i, .value = 4 * i + 10};
        })));
    oracle.remove_batch(
        parlay::make_slice(parlay::delayed_seq<int64_t>(20000, [](size_t x) {
            int64_t i = (int64_t)x;
            return 6 * i;
        })));

    for (int i = -2; i < 30; i++) {
        cout << oracle.predecessor(i) << endl;
    }
}
