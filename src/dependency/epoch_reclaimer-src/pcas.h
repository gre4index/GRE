// Copyright Xiangpeng Hao. All rights reserved.
// Licensed under the MIT license.
//
// Persistent CAS
#pragma once
#include "utils.h"

#ifdef TEST_BUILD
#include <glog/logging.h>
#include <glog/raw_logging.h>
#include <gtest/gtest_prod.h>
#endif

namespace very_pm {

class DirtyTable {
 public:
  static void Initialize(DirtyTable* table, uint32_t item_cnt) {
    DirtyTable::table_ = table;
    DirtyTable::table_->item_cnt_ = item_cnt;
    DirtyTable::table_->next_free_object_ = 0;
    memset(DirtyTable::table_->items_, 0, sizeof(Item) * item_cnt);
  }

  static void Recovery(DirtyTable* table) {
    for (uint32_t i = 0; i < table->item_cnt_; i += 1) {
      auto& item = table->items_[i];
      if (item.addr_ == nullptr) {
        continue;
      }
      __atomic_compare_exchange_n((uint64_t*)item.addr_, &item.old_, item.new_,
                                  false, __ATOMIC_SEQ_CST, __ATOMIC_SEQ_CST);
      flush(item.addr_);
    }
    table->next_free_object_ = 0;
    memset(table->items_, 0, sizeof(Item) * table->item_cnt_);
  }

  static DirtyTable* GetInstance() { return table_; }

  DirtyTable(DirtyTable const&) = delete;
  void operator=(DirtyTable const&) = delete;
  DirtyTable() = delete;

  /// Item is only padded to half cache line size,
  /// Don't want to waste too much memory
  struct Item {
    void* addr_;
    uint64_t old_;
    uint64_t new_;
    char paddings_[40];
  };

  Item* MyItem() {
    thread_local Item* my_item{nullptr};
    if (my_item != nullptr) {
      return my_item;
    }
    uint32_t next_id = next_free_object_.fetch_add(1);
    my_item = &items_[next_id];
    return my_item;
  }

  /// Why to flush my_item->addr_?
  ///   We employ lazy flush mechanism to hide the high cost of clwb (Oct.
  ///   2019), i.e. there's no flush after a CAS, need to make sure the previous
  ///   CASed value is properly persisted. When clwb is ideally implemented (not
  ///   evicting cache line), we can move the flush after a CAS and potentially
  ///   save a branch here.
  ///
  /// Why flush addr?
  ///   We're trying to assign a new value to addr, need to make sure previous
  ///   store has already persisted, o.w. the newly assigned CAS will fail
  ///   on recovery.
  ///
  /// Why non-temporal store is required?
  ///   We need to atomically and immediately write the value to persistent
  ///   memory, and not relying on the non-deterministic cache eviction policy
  void RegisterItem(void* addr, uint64_t old_v, uint64_t new_v) {
    Item* my_item = MyItem();
    if (my_item->addr_ != nullptr) {
      flush(my_item->addr_);
      __builtin_prefetch(my_item->addr_);
    }
    flush(addr);
    __builtin_prefetch(addr);
    auto value = _mm256_set_epi64x(0, new_v, old_v, (uint64_t)addr);
    _mm256_stream_si256((__m256i*)(my_item), value);
  }

 private:
#ifdef TEST_BUILD
  FRIEND_TEST(DirtyTablePMTest, SimpleCAS);
  FRIEND_TEST(DirtyTablePMTest, SimpleRecovery);
#endif
  static DirtyTable* table_;

  std::atomic<uint32_t> next_free_object_{0};

  uint32_t item_cnt_{0};

  char paddings_[56];

  Item items_[0];
};

DirtyTable* DirtyTable::table_{nullptr};

/// Why a single CAS is not enough?
///   Checkout this post: https://blog.haoxp.xyz/posts/crash-consistency/
///
/// The RegisterItem will record the CAS operation, and on recovery try to
/// redo the CAS.
///   1. If a crash happens right after the RegisterItem, the new
///   value is not seen by any other thread and we are good to redo the CAS
///   during recovery.
///   2. If a crash happens right after the CAS, before the new value is
///   persisted, on recovery we can help finish the CAS, make it in a
///   consistent state
///   3. The only worry for me right now, is when CAS is finished and
///   persisted, on recovery we will still try to redo the CAS. This should be
///   fine in most cases, but the chances to hit ABA problem is much higher.
///
/// Why there's no flush for the newly installed value?
///   We typically don't need to, because on recovery we'll be able to redo
///   the CAS. This requires later writers to flush the old value before
///   install new values.
static uint64_t PersistentCAS(void* addr, uint64_t old_v, uint64_t new_v) {
  DirtyTable::GetInstance()->RegisterItem(addr, old_v, new_v);
  __atomic_compare_exchange_n((uint64_t*)addr, &old_v, new_v, false,
                              __ATOMIC_SEQ_CST, __ATOMIC_SEQ_CST);
  return old_v;
}

}  // namespace pm_tool
