/*
 * Copyright (c) 2014-2017 - uPmem
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
 * An example of checksum computation with multiple tasklets.
 *
 * Every tasklet processes specific areas of the MRAM, following the "rake"
 * strategy:
 *  - Tasklet number T is first processing block number TxN, where N is a
 *    constant block size
 *  - It then handles block number (TxN) + (NxM) where M is the number of
 *    scheduled tasklets
 *  - And so on...
 *
 * The host is in charge of computing the final checksum by adding all the
 * individual results.
 */
#include <defs.h>
#include <mram.h>
#include <alloc.h>
#include <perfcounter.h>
#include <barrier.h>
#include <stdint.h>
#include <stdio.h>
#include "driver.h"
#include "task.h"
#include "task_framework_dpu.h"


void exec_fixed_task(int lft, int rt) {
    init_block_with_type(fixed_task, fixed_reply);

    init_task_reader(lft);
    // printf("%d\t%d\t%d\t%d\t%d\t%d\n", task_len(fixed_task), fixed_task_len, sizeof(fixed_task), recv_block_fixlen, tid, lft, get_task_cached(lft));
    for (int i = lft; i < rt; i ++) {
        fixed_task* ft = (fixed_task*)get_task_cached(i);
        IN_DPU_ASSERT(ft->addr.id < 10000, "???");
        if (i == lft) {
            printf("fixed: id=%llx\nl=%d\tr=%d\n", PPTR_TO_I64(ft->addr), lft,
                   rt);
        }
        fixed_reply fr;
        fr.a[0] = PPTR_TO_I64(ft->addr);
        push_fixed_reply(i, &fr);
    }
}

void exec_varlen_task(int lft, int rt) {
    (void)lft;
    (void)rt;
}

void execute(int lft, int rt) {
    uint32_t tid = me();
    switch (recv_block_task_type) {
        case fixed_task_id: {
            exec_fixed_task(lft, rt);
            break;
        }
        // case : {
        //     exec_varlen_task(lft, rt);
        //     break;
        // }
        default: {
            printf("TT = %lld\n", recv_block_task_type);
            IN_DPU_ASSERT(false, "WTT\n");
            break;
        }
    }
    // EXIT();
    finish_reply(recv_block_task_cnt, tid);
}

void init() {

}

int main() {
    run();
    return 0;
}
