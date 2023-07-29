#pragma once
#include <string>
using namespace std;

extern "C" {
    #include <dpu.h>
}

dpu_set_t dpu_set, dpu;
int nr_of_dpus;
uint32_t each_dpu;
int epoch_number = 0;

namespace dpu_control {
bool active = false;

// public:
 void alloc(int count) {
     DPU_ASSERT(dpu_alloc(count, "", &dpu_set));
     DPU_ASSERT(dpu_get_nr_dpus(dpu_set, (uint32_t*)&nr_of_dpus));
     printf("Allocated %d DPU(s)\n", nr_of_dpus);
     active = true;
 }

 void load(string binary) {
     DPU_ASSERT(dpu_load(dpu_set, binary.c_str(), NULL));
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
     active = false;
     DPU_ASSERT(dpu_free(dpu_set));
 }
 };  // namespace dpu_control