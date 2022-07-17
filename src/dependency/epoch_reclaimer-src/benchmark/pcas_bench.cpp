#include <glog/logging.h>
#include <gtest/gtest.h>
#include <libpmemobj.h>
#include <memory>
#include <random>
#include "../pcas.h"
#include "bench_common.h"

POBJ_LAYOUT_BEGIN(benchmark);
POBJ_LAYOUT_TOID(benchmark, char);
POBJ_LAYOUT_END(benchmark);

static const constexpr uint32_t kItemCnt = 48 * 2;
static const constexpr uint32_t kArrayLen = 10000;
static const constexpr uint32_t kOpCnt = 100000;

struct BaseBench : public PerformanceTest {
  PMEMobjpool* pool{nullptr};
  BaseBench() {
    static const char* pool_name = "/mnt/pmem0/pcas_pool.data";
    static const char* layout_name = "benchmark";
    static const uint64_t pool_size = 1024 * 1024 * 1024;
    if (!very_pm::FileExists(pool_name)) {
      pool = pmemobj_create(pool_name, layout_name, pool_size,
                            very_pm::CREATE_MODE_RW);
    } else {
      pool = pmemobj_open(pool_name, layout_name);
    }
    EXPECT_NE(pool, nullptr);
  }
  ~BaseBench() { pmemobj_close(pool); }

  std::mt19937 rng{42};

  size_t read_count_{0};

  uint64_t* array{nullptr};

  void WorkLoadInit() {
    auto table = (very_pm::DirtyTable*)ZAlloc(
        sizeof(very_pm::DirtyTable) +
        sizeof(very_pm::DirtyTable::Item) * kItemCnt);
    very_pm::DirtyTable::Initialize(table, kItemCnt);
    array = (uint64_t*)ZAlloc(very_pm::kCacheLineSize * kArrayLen);
  }

  void* ZAlloc(size_t size) {
    PMEMoid ptr;
    pmemobj_zalloc(pool, &ptr, very_pm::kPMDK_PADDING + size,
                   TOID_TYPE_NUM(char));
    auto abs_ptr = (char*)pmemobj_direct(ptr) + very_pm::kPMDK_PADDING;
    return abs_ptr;
  }

  void Teardown() override {
    uint64_t sum{0};
    for (uint32_t i = 0; i < kArrayLen; i += 1) {
      uint64_t* target = array + i * very_pm::kCacheLineSize / sizeof(uint64_t);
      sum += *target;
    }
    std::cout << "Succeeded CAS: " << sum << std::endl;

    auto oid = pmemobj_oid((char*)very_pm::DirtyTable::GetInstance() -
                           very_pm::kPMDK_PADDING);
    pmemobj_free(&oid);
    oid = pmemobj_oid((char*)array - very_pm::kPMDK_PADDING);
    pmemobj_free(&oid);
  }
};

struct PCASBench : public BaseBench {
  const char* GetBenchName() override { return "PCASBench"; }

  PCASBench() : BaseBench() {}

  void Entry(size_t thread_idx, size_t thread_count) override {
    if (thread_idx == 0) {
      WorkLoadInit();
    }

    std::uniform_int_distribution<std::mt19937::result_type> dist(
        0, kArrayLen - 1);

    WaitForStart();

    for (uint32_t i = 0; i < kOpCnt; i += 1) {
      uint32_t pos = dist(rng);
      uint64_t* target =
          array + pos * very_pm::kCacheLineSize / sizeof(uint64_t);
      uint64_t value = *target;
      very_pm::PersistentCAS(target, value, value + 1);
    }
  }
};

struct NaiveCASBench : public BaseBench {
  const char* GetBenchName() override { return "NaiveCASBench"; }
  NaiveCASBench() : BaseBench() {}

  void Entry(size_t thread_idx, size_t thread_count) override {
    if (thread_idx == 0) {
      WorkLoadInit();
    }

    std::uniform_int_distribution<std::mt19937::result_type> dist(
        0, kArrayLen - 1);

    WaitForStart();

    for (uint32_t i = 0; i < kOpCnt; i += 1) {
      uint32_t pos = dist(rng);
      uint64_t* target =
          array + pos * very_pm::kCacheLineSize / sizeof(uint64_t);
      uint64_t value = *target;

      very_pm::CompareExchange64(target, value + 1, value);
    }
  }
};

struct CASBench : public BaseBench {
  const char* GetBenchName() override { return "CASBench"; }
  CASBench() : BaseBench() {}

  void Entry(size_t thread_idx, size_t thread_count) override {
    if (thread_idx == 0) {
      WorkLoadInit();
    }

    std::uniform_int_distribution<std::mt19937::result_type> dist(
        0, kArrayLen - 1);

    WaitForStart();

    for (uint32_t i = 0; i < kOpCnt; i += 1) {
      uint32_t pos = dist(rng);
      uint64_t* target =
          array + pos * very_pm::kCacheLineSize / sizeof(uint64_t);
      uint64_t value = *target;

      very_pm::CompareExchange64(target, value + 1, value);
      very_pm::flush(target);
      very_pm::fence();
    }
  }
};

struct DirtyCASBench : public BaseBench {
  const char* GetBenchName() override { return "DirtyCASBench"; }
  DirtyCASBench() : BaseBench() {}

  static const uint64_t kDirtyBitMask = 0x8000000000000000ull;

  void Entry(size_t thread_idx, size_t thread_count) override {
    if (thread_idx == 0) {
      WorkLoadInit();
    }

    std::uniform_int_distribution<std::mt19937::result_type> dist(
        0, kArrayLen - 1);

    WaitForStart();

    for (uint32_t i = 0; i < kOpCnt; i += 1) {
      uint32_t pos = dist(rng);
      uint64_t* target =
          array + pos * very_pm::kCacheLineSize / sizeof(uint64_t);

      uint64_t old_v = *target;
      while ((old_v & kDirtyBitMask) != 0) {
        uint64_t cleared = old_v & (~kDirtyBitMask);
        very_pm::CompareExchange64(target, cleared, old_v);
        old_v = *target;
      }

      uint64_t new_v = old_v + 1;
      uint64_t dirty_value = new_v | kDirtyBitMask;

      very_pm::CompareExchange64(target, dirty_value, old_v);
      very_pm::flush(target);
      very_pm::fence();
      very_pm::CompareExchange64(target, new_v, dirty_value);
    }
  }
};

int main(int argc, char** argv) {
  if (argc == 2) {
    int32_t bench_to_run = atoi(argv[1]);
    switch (bench_to_run) {
      case 1: {
        auto pcas_bench = std::make_unique<PCASBench>();
        pcas_bench->Run(16);
        break;
      }

      case 2: {
        auto cas_bench = std::make_unique<CASBench>();
        cas_bench->Run(16);
        break;
      }
      case 3: {
        auto dirty_cas_bench = std::make_unique<DirtyCASBench>();
        dirty_cas_bench->Run(16);
        break;
      }
      default:
        break;
    }
    return 0;
  }

  {
    auto pcas_bench = std::make_unique<PCASBench>();
    pcas_bench->Run(1)->Run(2)->Run(4)->Run(8)->Run(16)->Run(24);
  }
  {
    auto cas_bench = std::make_unique<CASBench>();
    cas_bench->Run(1)->Run(2)->Run(4)->Run(8)->Run(16)->Run(24);
  }
  {
    auto dirty_cas_bench = std::make_unique<DirtyCASBench>();
    dirty_cas_bench->Run(1)->Run(2)->Run(4)->Run(8)->Run(16)->Run(24);
  }
  {
    auto naive_cas_bench = std::make_unique<NaiveCASBench>();
    naive_cas_bench->Run(1)->Run(2)->Run(4)->Run(8)->Run(16)->Run(24);
  }
}
