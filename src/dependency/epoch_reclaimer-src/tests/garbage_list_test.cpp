#include "../garbage_list.h"
#include <glog/logging.h>
#include <gtest/gtest.h>
#include "../garbage_list_unsafe.h"

struct MockItem {
 public:
  MockItem() : deallocations{0} {}

  static void Destroy(void* destroyContext, void* p) {
    ++(reinterpret_cast<MockItem*>(p))->deallocations;
  }

  std::atomic<uint64_t> deallocations;
};

class GarbageListTest : public ::testing::Test {
 public:
  GarbageListTest() {}

 protected:
  EpochManager epoch_manager_;
  GarbageList garbage_list_;
  std::stringstream out_;

  virtual void SetUp() {
    out_.str("");
    ASSERT_TRUE(epoch_manager_.Initialize());
    ASSERT_TRUE(garbage_list_.Initialize(&epoch_manager_, 1024));
  }

  virtual void TearDown() {
    EXPECT_TRUE(garbage_list_.Uninitialize());
    EXPECT_TRUE(epoch_manager_.Uninitialize());
    Thread::ClearRegistry(true);
  }
};

TEST_F(GarbageListTest, Uninitialize) {
  MockItem items[2];

  EXPECT_TRUE(garbage_list_.Push(&items[0], MockItem::Destroy, nullptr));
  EXPECT_TRUE(garbage_list_.Push(&items[1], MockItem::Destroy, nullptr));
  EXPECT_TRUE(garbage_list_.Uninitialize());
  EXPECT_EQ(1, items[0].deallocations);
  EXPECT_EQ(1, items[1].deallocations);
}

class GarbageListUnsafeTest : public ::testing::Test {
 public:
  GarbageListUnsafeTest() {}

 protected:
  EpochManager epoch_manager_;
  GarbageListUnsafe garbage_list_;
  std::stringstream out_;

  virtual void SetUp() {
    out_.str("");
    ASSERT_TRUE(epoch_manager_.Initialize());
    ASSERT_TRUE(garbage_list_.Initialize(&epoch_manager_, 1024));
  }

  virtual void TearDown() {
    EXPECT_TRUE(garbage_list_.Uninitialize());
    EXPECT_TRUE(epoch_manager_.Uninitialize());
    Thread::ClearRegistry(true);
  }
};

TEST_F(GarbageListUnsafeTest, Uninitialize) {
  MockItem items[2];

  EXPECT_TRUE(garbage_list_.Push(&items[0], MockItem::Destroy, nullptr));
  EXPECT_TRUE(garbage_list_.Push(&items[1], MockItem::Destroy, nullptr));
  EXPECT_TRUE(garbage_list_.Uninitialize());
  EXPECT_EQ(1, items[0].deallocations);
  EXPECT_EQ(1, items[1].deallocations);
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
