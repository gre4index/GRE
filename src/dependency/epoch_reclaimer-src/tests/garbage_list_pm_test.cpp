#include <glog/logging.h>
#include <gtest/gtest.h>
#include <libpmemobj.h>
#include "../garbage_list.h"
#include "../garbage_list_unsafe.h"

static const char* pool_name = "garbage_list_pool.data";
static const char* layout_name = "garbagelist";
static const uint64_t pool_size = 1024 * 1024 * 1024;

struct MockItem {
 public:
  MockItem() : deallocations{0} {}

  static void Destroy(void* destroyContext, void* p) {
    ++(reinterpret_cast<MockItem*>(p))->deallocations;
  }

  std::atomic<uint64_t> deallocations;
};

class GarbageListPMTest : public ::testing::Test {
 public:
  GarbageListPMTest() {}

 protected:
  EpochManager epoch_manager_;
  GarbageList garbage_list_;
  std::stringstream out_;
  PMEMobjpool* pool_{nullptr};

  virtual void SetUp() {
    PMEMobjpool* tmp_pool;
    if (!very_pm::FileExists(pool_name)) {
      tmp_pool = pmemobj_create(pool_name, layout_name, pool_size,
                                very_pm::CREATE_MODE_RW);
      LOG_ASSERT(tmp_pool != nullptr);
    } else {
      tmp_pool = pmemobj_open(pool_name, layout_name);
      LOG_ASSERT(tmp_pool != nullptr);
    }
    pool_ = tmp_pool;

    out_.str("");
    ASSERT_TRUE(epoch_manager_.Initialize());
    ASSERT_TRUE(garbage_list_.Initialize(&epoch_manager_, pool_, 1024));
  }

  virtual void TearDown() {
    EXPECT_TRUE(garbage_list_.Uninitialize());
    EXPECT_TRUE(epoch_manager_.Uninitialize());
    pmemobj_close(pool_);
    Thread::ClearRegistry(true);
  }
};

TEST_F(GarbageListPMTest, Uninitialize) {
  MockItem items[2];

  EXPECT_TRUE(garbage_list_.Push(&items[0], MockItem::Destroy, nullptr));
  EXPECT_TRUE(garbage_list_.Push(&items[1], MockItem::Destroy, nullptr));
  EXPECT_TRUE(garbage_list_.Uninitialize());
  EXPECT_EQ(1, items[0].deallocations);
  EXPECT_EQ(1, items[1].deallocations);
}

TEST_F(GarbageListPMTest, Recovery) {
  MockItem items[2];

  EXPECT_TRUE(garbage_list_.Push(&items[0], MockItem::Destroy, nullptr));
  EXPECT_TRUE(garbage_list_.Push(&items[1], MockItem::Destroy, nullptr));
  EXPECT_TRUE(garbage_list_.Recovery(&epoch_manager_, pool_));
  EXPECT_EQ(1, items[0].deallocations);
  EXPECT_EQ(1, items[1].deallocations);
}

TEST_F(GarbageListPMTest, ReserveMemory) {
  const uint64_t test_items = 20;
  std::vector<GarbageList::Item*> reserved_vec(test_items);
  for (int i = 0; i < test_items; i += 1) {
    GarbageList::Item* reserved = garbage_list_.ReserveItem();
    reserved_vec[i] = reserved;
  }
  for (int i = 0; i < garbage_list_.tail_ - 1; i += 1) {
    auto& item = garbage_list_.items_[i];
    EXPECT_EQ(item.removal_epoch, garbage_list_.invalid_epoch);
  }

  for (auto res : reserved_vec) {
    garbage_list_.ResetItem(res);
  }
  for (int i = 0; i < garbage_list_.tail_ - 1; i += 1) {
    auto& item = garbage_list_.items_[i];
    EXPECT_EQ(item.removal_epoch, 0);
  }
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
