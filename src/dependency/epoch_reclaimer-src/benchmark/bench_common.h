#pragma once
#include <atomic>
#include <chrono>
#include <cstdint>
#include <deque>
#include <iostream>
#include <mutex>
#include <thread>
#include <vector>

static const char* BLUE = "\033[34m";
static const char* RESET = "\033[0m";
static const char* YELLOW = "\033[33m";
static const char* MAGENTA = "\033[35m";

class PerformanceTest {
 protected:
 public:
  PerformanceTest()
      : threads_ready_{0},
        threads_finished_{0},
        start_running_{false},
        run_seconds_{} {}

  /// Provided by derived classes; contains that each thread should run during
  /// the test. Somewhere in this method WaitForStart() must be called,
  /// which acts as a barrier to synchronize the start of all the threads
  /// in the test and the start of timing in the main thread.
  virtual void Entry(size_t thread_index, size_t threadCount) {}

  /// Can be overridden by derived classes to reset member variables into
  /// the appropriate state before a set of threads enters Entry().
  virtual void Setup() {}

  /// Can be overridden by derived classes to reset member variables into
  /// the appropriate state after a set of threads finishes running
  /// Entry(). This may be a good place to dump out statistics that are
  /// being tracked in member variables as well.
  virtual void Teardown() {}

  virtual const char* GetBenchName() { return "PerformanceTest"; }

  /// Display result, need to be called after the Run()
  void Report(double avg_thread) {
    std::cout << GetBenchName() << "/thread:" << threads_finished_ << std::endl;
    std::cout << "thread avg: \t" << YELLOW << avg_thread << RESET << std::endl;
    std::cout << BLUE << "================================" << RESET
              << std::endl;
  }

  double RunOnce(size_t threadCount) {
    threads_finished_ = 0;
    threads_ready_ = 0;
    start_running_ = false;
    run_seconds_.clear();

    Setup();

    // Start threads
    std::deque<std::thread> threads;
    for (size_t i = 0; i < threadCount; ++i) {
      threads.emplace_back(&PerformanceTest::entry, this, i, threadCount);
    }

    // Wait for threads to be ready
    while (threads_ready_.load() < threadCount)
      ;

    // Start the experiment
    start_ = std::chrono::high_resolution_clock::now();
    start_running_.store(true, std::memory_order_release);

    // Wait for all threads to finish their workload
    while (threads_finished_.load() < threadCount) {
      std::this_thread::sleep_for(std::chrono::microseconds(10));
    }

    for (auto& thread : threads) {
      thread.join();
    }

    auto end = std::chrono::high_resolution_clock::now();

    // Record the run time in seconds
    run_seconds_.push_back(
        std::chrono::duration_cast<std::chrono::duration<double>>(end - start_)
            .count());

    Teardown();
    double tmp_sum{0};
    for (uint8_t j = 0; j < threads_finished_; j += 1) {
      tmp_sum += run_seconds_[j];
    }
    return tmp_sum / threads_finished_;
  }

  /// Run \a threadCount threads running Entry() and measure how long the
  /// run takes.Afterward, GetLastRunSeconds() can be used to retrieve how
  /// long the execution took.This method will call Setup() and Teardown()
  /// respectively before and after the executions of Entry().
  PerformanceTest* Run(size_t threadCount, uint32_t repeat = 3) {
    double average_time{0};
    for (uint8_t i = 0; i < repeat; i += 1) {
      average_time += RunOnce(threadCount);
    }
    Report(average_time / repeat);
    return this;
  }

  /// Repeatedly run Entry() first starting with \a startingThreadCount
  /// and increasing by one until Entry() has been run with \a endThreadCount
  /// threads.Afterward, GetAllRunSeconds() can be used to retrieve how
  /// long each of the executions took.This method will call Setup()
  /// just before running each new round with a different number of threads
  /// through Entry().Similarly, Teardown() will be called after each
  /// set of threads finishes Entry(), before the next number of threads
  /// is run through Entry().
  void Run(size_t startThreadCount, size_t endThreadCount) {
    for (size_t threadCount = startThreadCount; threadCount <= endThreadCount;
         ++threadCount) {
      Run(threadCount);
    }
  }

  /// Must be called in Entry() after initial setup and before the real
  /// work of the experiment starts.This acts as a barrier; the main thread
  /// waits for all threads running as part of the measurement to reach this
  /// point and then releases them to start to body of the experiment as
  /// closely synchronized as possible.Forgetting to call this in Entry()
  /// will result in an infinite loop.
  void WaitForStart() {
    threads_ready_.fetch_add(1, std::memory_order_acq_rel);
    while (!start_running_.load(std::memory_order_acquire))
      ;
  }

  size_t GetThreadsFinished() { return threads_finished_; }

  /// Return the number of seconds Entry() ran on any of the threads starting
  /// just after WaitForStart() up through the end of Entry().
  double GetLastRunSeconds() { return run_seconds_.back(); }

  /// Return the number of seconds Entry() ran on any of the threads starting
  /// just after WaitForStart() up through the end of Entry() for all of the
  /// run of varying sets of threads.
  std::vector<double> GetAllRunSeconds() { return run_seconds_; }

 private:
  /// Internal springboard used to automatically notify the main thread of
  /// Entry() completion.The last thread to finish the calls stuffs a snapshot
  /// of the ending time so the main thread can accurately determine how long
  /// the run took without polling aggressively and wasting a core.
  void entry(size_t threadIndex, size_t threadCount) {
    Entry(threadIndex, threadCount);
    auto end = std::chrono::high_resolution_clock::now();
    threads_finished_.fetch_add(1, std::memory_order_acq_rel);

    bench_mtx_.lock();
    run_seconds_.push_back(
        std::chrono::duration_cast<std::chrono::duration<double>>(end - start_)
            .count());
    bench_mtx_.unlock();
  }

 private:
  /// Number of threads that have reached WaitForStart(). Used by the main
  /// thread to synchronize the start of the experiment.
  std::atomic<size_t> threads_ready_;

  /// Number of threads that have exited Entry(). Used by the main thread
  /// to determine when the experiment has completed.
  std::atomic<size_t> threads_finished_;

  /// The main thread sets this to true once #m_threadReady equals the
  /// number of threads that are part of the experiment. This releases all
  /// the threads to run the remainder of Entry() that follows the call
  /// to WaitForStart().
  std::atomic<bool> start_running_;

  /// Tracks how long in seconds each collective run of Entry() took for
  /// a set of threads.
  std::vector<double> run_seconds_;

  /// Time in QueryPerformanceCounter ticks that the beginning of its work
  std::chrono::time_point<std::chrono::system_clock> start_{};

  std::mutex bench_mtx_;
};
