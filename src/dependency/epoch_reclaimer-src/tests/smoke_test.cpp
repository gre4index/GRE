#include <glog/logging.h>
#include <gtest/gtest.h>
#include <map>
#include <random>

GTEST_TEST(SmokeTest, SmokeTest) {
  LOG(INFO) << "running tests";
  ASSERT_TRUE(true);
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
