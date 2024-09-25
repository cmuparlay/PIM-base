#pragma once

#include <cstdint>
#include <iostream>
#include "operation_def.hpp"
#include "value.hpp"

// abandoned because c++ doesn't support template virtual functions
// used as document now
class IndexInterface {
   public:
   void Init();
   void SetPushPullLimit(size_t limit);
   template<typename GetKeyF>
   std::pair<key_value*, size_t> RunBatchGet(size_t batch_size, GetKeyF get_key);

   template<typename GetKVF>
   void RunBatchUpdate(size_t batch_size, GetKVF get_kv);

   template<typename GetKVF>
   void RunBatchInsert(size_t batch_size, GetKVF get_kv);

   template<typename GetKeyF>
   void RunBatchRemove(size_t batch_size, GetKeyF get_key);

   template<typename GetKeyF>
   void RunBatchPredecessor(size_t batch_size, GetKeyF get_key);

   template<typename GetLRKeyF>
   void RunBatchScan(size_t batch_size, GetLRKeyF get_lrkey);
};