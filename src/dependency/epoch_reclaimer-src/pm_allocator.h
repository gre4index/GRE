#pragma once
#include <libpmemobj.h>
#include "utils.h"

namespace very_pm {

static const char* layout_name = "very_pm_alloc_layout";

POBJ_LAYOUT_BEGIN(allocator);
POBJ_LAYOUT_TOID(allocator, char)
POBJ_LAYOUT_END(allocator)

/// The problem is how do you allocate memory for the allocator:
///  1. Use allocator as root object.
///  2. Have a separate pool management.
class Allocator {
 public:
  static void Initialize(const char* pool_name, size_t pool_size) {
    PMEMobjpool* pm_pool{nullptr};
    if (!very_pm::FileExists(pool_name)) {
      LOG(INFO) << "creating a new pool" << std::endl;
      pm_pool =
          pmemobj_create(pool_name, layout_name, pool_size, CREATE_MODE_RW);
      if (pm_pool == nullptr) {
        LOG(FATAL) << "failed to create a pool" << std::endl;
      }
    } else {
      LOG(INFO) << "opening an existing pool, and trying to map to same address"
                << std::endl;
      pm_pool = pmemobj_open(pool_name, layout_name);
      if (pm_pool == nullptr) {
        LOG(FATAL) << "failed to open the pool" << std::endl;
      }
    }
    auto allocator_oid = pmemobj_root(pm_pool, sizeof(Allocator));
    allocator_ = (Allocator*)pmemobj_direct(allocator_oid);
    allocator_->pm_pool_ = pm_pool;
    LOG(INFO) << "pool opened at: " << std::hex << allocator_->pm_pool_
              << std::dec << std::endl;
    very_pm::flush(allocator_->pm_pool_);
  }

  /// PMDK allocator will add 16-byte meta to each allocated memory, which
  /// breaks the padding, we fix it by adding 48-byte more.
  static void Allocate(void** addr, size_t size) {
    PMEMoid ptr;
    int ret = pmemobj_zalloc(allocator_->pm_pool_, &ptr,
                             sizeof(char) * (size + kPMDK_PADDING),
                             TOID_TYPE_NUM(char));
    if (ret) {
      LOG(FATAL) << "POBJ_ALLOC error" << std::endl;
    }
    *addr = (char*)pmemobj_direct(ptr) + kPMDK_PADDING;
  }

  static void Free(void* addr) {
    auto addr_oid = pmemobj_oid(addr);
    pmemobj_free(&addr_oid);
  }

 private:
  static Allocator* allocator_;
  PMEMobjpool* pm_pool_{nullptr};
};

Allocator* Allocator::allocator_{nullptr};

}  // namespace very_pm
