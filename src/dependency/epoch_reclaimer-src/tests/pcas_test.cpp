#include "../pcas.h"
#include <glog/logging.h>
#include <gtest/gtest.h>

GTEST_TEST(PCASTest, SmokeTest) {
  LOG(INFO) << "running tests";
  ASSERT_TRUE(true);
}

namespace very_pm {

class DirtyTablePMTest : public ::testing::Test {
 public:
  DirtyTablePMTest() {}

 protected:
  static const constexpr uint32_t item_cnt_ = 48 * 2;
  very_pm::DirtyTable* table;
  virtual void SetUp() {
    posix_memalign((void**)&table, very_pm::kCacheLineSize,
                   sizeof(very_pm::DirtyTable) +
                       sizeof(very_pm::DirtyTable::Item) * item_cnt_);
    very_pm::DirtyTable::Initialize(table, item_cnt_);
  }

  virtual void TearDown() { delete table; }
};

TEST_F(DirtyTablePMTest, SimpleCAS) {
  uint64_t target{0};
  for (uint32_t i = 1; i < 100; i += 1) {
    table->next_free_object_ = 1;
    table->item_cnt_ = item_cnt_;
    table->items_[0].addr_ = &target;
    table->items_[0].old_ = i - 1;
    table->items_[0].new_ = i;
    auto rv = very_pm::PersistentCAS(&target, i - 1, i);
    EXPECT_EQ(rv, i - 1);
    EXPECT_EQ(target, i);
  }
}

TEST_F(DirtyTablePMTest, SimpleRecovery) {
  uint64_t target{0};
  for (uint32_t i = 1; i < 100; i += 1) {
    very_pm::PersistentCAS(&target, i - 1, i);
  }
  EXPECT_EQ(target, 99);
  table->Recovery(DirtyTable::GetInstance());
  EXPECT_EQ(target, 99);
}

}  // namespace very_pm

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
