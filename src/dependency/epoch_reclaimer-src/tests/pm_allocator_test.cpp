#include "../pm_allocator.h"
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <libpmemobj.h>

static const constexpr char* allocator_pool = "allocator_test";
static const constexpr uint64_t pool_size = 1024 * 1024 * 1024;
static const constexpr uint64_t kCacheLineMask = 0x3F;

GTEST_TEST(AllocatorTest, Allocation) {
  very_pm::Allocator::Initialize(allocator_pool, pool_size);

  const uint32_t kAllocateCount = 1024;
  std::vector<void*> allocated(kAllocateCount);

  for (uint32_t i = 1; i < kAllocateCount; i += 1) {
    very_pm::Allocator::Allocate(&allocated[i], i * very_pm::kCacheLineSize);
    ASSERT_NE(allocated[i], nullptr);
    ASSERT_EQ((uint64_t)allocated[i] & kCacheLineMask, 0);
  }

  for (uint32_t i = 1; i < kAllocateCount; i += 1) {
    very_pm::Allocator::Free(allocated[i]);
  }
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
