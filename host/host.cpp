/*
 * Copyright (c) 2014-2019 - UPMEM
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * @file host.c
 * @brief Template for a Host Application Source File.
 */

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
#include "obsolete_test_cases.hpp"

/**
 * @brief Main of the Host Application.
 */

int pim_skip_list_debug() {
    rn_gen::init();
    pim_skip_list::init();
    {
        const int n = 1e9 / 4096;
        auto kvs = parlay::tabulate(n, [&](int64_t i) {
            return (key_value){.key = rn_gen::parallel_rand(),
                               .value = rn_gen::parallel_rand()};
        });
        pim_skip_list::insert(make_slice(kvs));
        if (!pim_skip_list::sancheck(pim_skip_list::root)) {
            pim_skip_list::print_tree(pim_skip_list::root);
            return -1;
        }

        // for (int i = 0; i < 10; i++) {
        //     auto valid = parlay::tabulate(n, [&](size_t i) -> bool {
        //         return (rn_gen::parallel_rand() & 1) != 0;
        //     });
        //     auto keys =
        //         parlay::tabulate(n, [&](size_t i) { return kvs[i].key; });
        //     auto to_remove = parlay::pack(keys, valid);
        //     pim_skip_list::remove(make_slice(to_remove));
        //     printf("PRINT_TREE_%d\n", i);
        //     if (!pim_skip_list::sancheck(pim_skip_list::root)) {
        //         pim_skip_list::print_tree(pim_skip_list::root);
        //         parlay::sort_inplace(keys);
        //         parlay::sort_inplace(to_remove);
        //         pim_skip_list::print_array("keys", keys.data(), keys.size());
        //         pim_skip_list::print_array("to_remove", to_remove.data(),
        //         to_remove.size()); return -1;
        //     }
        // }
    }
    printf("bcnt=%d\n", pim_skip_list::bcnt);
    return 0;
}
int main(int argc, char* argv[]) {
    // test_task_framework(DPU_ALLOCATE_ALL);
    // return 0;
    // return pim_skip_list_debug();

    driver::init();
    driver::exec(argc, argv);
    printf("bcnt=%d\n", pim_skip_list::bcnt);
    return 0;
}
