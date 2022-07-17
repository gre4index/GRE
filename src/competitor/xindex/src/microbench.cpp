/*
 * The code is part of the XIndex project.
 *
 *    Copyright (C) 2020 Institute of Parallel and Distributed Systems (IPADS),
 * Shanghai Jiao Tong University. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * For more about XIndex, visit:
 *     https://ppopp20.sigplan.org/details/PPoPP-2020-papers/13/XIndex-A-Scalable-Learned-Index-for-Multicore-Data-Storage
 */

#include <getopt.h>
#include <stdlib.h>
#include <sys/types.h>
#include <unistd.h>
#include <algorithm>
#include <atomic>
#include <memory>
#include <random>
#include <string>
#include <vector>

#include "helper.h"
#include "xindex.h"
#include "xindex_impl.h"

struct alignas(CACHELINE_SIZE) FGParam;
class Key;

typedef FGParam fg_param_t;
typedef Key index_key_t;
typedef xindex::XIndex<index_key_t, uint64_t> xindex_t;

inline void prepare_xindex(xindex_t *&table);

void run_benchmark(xindex_t *table, size_t sec);

void *run_fg(void *param);

inline void parse_args(int, char **);

// parameters
double read_ratio = 1;
double insert_ratio = 0;
double update_ratio = 0;
double delete_ratio = 0;
double scan_ratio = 0;
size_t table_size = 1000000;
size_t runtime = 10;
size_t fg_n = 1;
size_t bg_n = 1;

volatile bool running = false;
std::atomic<size_t> ready_threads(0);
std::vector<index_key_t> exist_keys;
std::vector<index_key_t> non_exist_keys;

struct alignas(CACHELINE_SIZE) FGParam {
  xindex_t *table;
  uint64_t throughput;
  uint32_t thread_id;
};

class Key {
  typedef std::array<double, 1> model_key_t;

 public:
  static constexpr size_t model_key_size() { return 1; }
  static Key max() {
    static Key max_key(std::numeric_limits<uint64_t>::max());
    return max_key;
  }
  static Key min() {
    static Key min_key(std::numeric_limits<uint64_t>::min());
    return min_key;
  }

  Key() : key(0) {}
  Key(uint64_t key) : key(key) {}
  Key(const Key &other) { key = other.key; }
  Key &operator=(const Key &other) {
    key = other.key;
    return *this;
  }

  model_key_t to_model_key() const {
    model_key_t model_key;
    model_key[0] = key;
    return model_key;
  }

  friend bool operator<(const Key &l, const Key &r) { return l.key < r.key; }
  friend bool operator>(const Key &l, const Key &r) { return l.key > r.key; }
  friend bool operator>=(const Key &l, const Key &r) { return l.key >= r.key; }
  friend bool operator<=(const Key &l, const Key &r) { return l.key <= r.key; }
  friend bool operator==(const Key &l, const Key &r) { return l.key == r.key; }
  friend bool operator!=(const Key &l, const Key &r) { return l.key != r.key; }

  uint64_t key;
} PACKED;

int main(int argc, char **argv) {
  parse_args(argc, argv);
  xindex_t *tab_xi;
  prepare_xindex(tab_xi);
  run_benchmark(tab_xi, runtime);
  if (tab_xi != nullptr) delete tab_xi;
}

inline void prepare_xindex(xindex_t *&table) {
  // prepare data
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<int64_t> rand_int64(
      0, std::numeric_limits<int64_t>::max());

  exist_keys.reserve(table_size);
  for (size_t i = 0; i < table_size; ++i) {
    exist_keys.push_back(index_key_t(rand_int64(gen)));
  }

  if (insert_ratio > 0) {
    non_exist_keys.reserve(table_size);
    for (size_t i = 0; i < table_size; ++i) {
      non_exist_keys.push_back(index_key_t(rand_int64(gen)));
    }
  }

  COUT_VAR(exist_keys.size());
  COUT_VAR(non_exist_keys.size());

  // initilize XIndex (sort keys first)
  std::sort(exist_keys.begin(), exist_keys.end());
  std::vector<uint64_t> vals(exist_keys.size(), 1);
  table = new xindex_t(exist_keys, vals, fg_n, bg_n);
}

void *run_fg(void *param) {
  fg_param_t &thread_param = *(fg_param_t *)param;
  uint32_t thread_id = thread_param.thread_id;
  xindex_t *table = thread_param.table;

  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_real_distribution<> ratio_dis(0, 1);

  size_t exist_key_n_per_thread = exist_keys.size() / fg_n;
  size_t exist_key_start = thread_id * exist_key_n_per_thread;
  size_t exist_key_end = (thread_id + 1) * exist_key_n_per_thread;
  std::vector<index_key_t> op_keys(exist_keys.begin() + exist_key_start,
                                   exist_keys.begin() + exist_key_end);

  if (non_exist_keys.size() > 0) {
    size_t non_exist_key_n_per_thread = non_exist_keys.size() / fg_n;
    size_t non_exist_key_start = thread_id * non_exist_key_n_per_thread,
           non_exist_key_end = (thread_id + 1) * non_exist_key_n_per_thread;
    op_keys.insert(op_keys.end(), non_exist_keys.begin() + non_exist_key_start,
                   non_exist_keys.begin() + non_exist_key_end);
  }

  COUT_THIS("[micro] Worker" << thread_id << " Ready.");
  size_t query_i = 0, insert_i = op_keys.size() / 2, delete_i = 0, update_i = 0;
  // exsiting keys fall within range [delete_i, insert_i)
  ready_threads++;
  volatile bool res = false;
  uint64_t dummy_value = 1234;
  UNUSED(res);

  while (!running)
    ;

  while (running) {
    double d = ratio_dis(gen);
    if (d <= read_ratio) {  // get
      res = table->get(op_keys[(query_i + delete_i) % op_keys.size()],
                       dummy_value, thread_id);
      query_i++;
      if (unlikely(query_i == op_keys.size() / 2)) {
        query_i = 0;
      }
    } else if (d <= read_ratio + update_ratio) {  // update
      res = table->put(op_keys[(update_i + delete_i) % op_keys.size()],
                       dummy_value, thread_id);
      update_i++;
      if (unlikely(update_i == op_keys.size() / 2)) {
        update_i = 0;
      }
    } else if (d <= read_ratio + update_ratio + insert_ratio) {  // insert
      res = table->put(op_keys[insert_i], dummy_value, thread_id);
      insert_i++;
      if (unlikely(insert_i == op_keys.size())) {
        insert_i = 0;
      }
    } else if (d <= read_ratio + update_ratio + insert_ratio +
                        delete_ratio) {  // remove
      res = table->remove(op_keys[delete_i], thread_id);
      delete_i++;
      if (unlikely(delete_i == op_keys.size())) {
        delete_i = 0;
      }
    } else {  // scan
      std::vector<std::pair<index_key_t, uint64_t>> results;
      table->scan(op_keys[(query_i + delete_i) % op_keys.size()], 10, results,
                  thread_id);
      query_i++;
      if (unlikely(query_i == op_keys.size() / 2)) {
        query_i = 0;
      }
    }
    thread_param.throughput++;
  }

  pthread_exit(nullptr);
}

void run_benchmark(xindex_t *table, size_t sec) {
  pthread_t threads[fg_n];
  fg_param_t fg_params[fg_n];
  // check if parameters are cacheline aligned
  for (size_t i = 0; i < fg_n; i++) {
    if ((uint64_t)(&(fg_params[i])) % CACHELINE_SIZE != 0) {
      COUT_N_EXIT("wrong parameter address: " << &(fg_params[i]));
    }
  }

  running = false;
  for (size_t worker_i = 0; worker_i < fg_n; worker_i++) {
    fg_params[worker_i].table = table;
    fg_params[worker_i].thread_id = worker_i;
    fg_params[worker_i].throughput = 0;
    int ret = pthread_create(&threads[worker_i], nullptr, run_fg,
                             (void *)&fg_params[worker_i]);
    if (ret) {
      COUT_N_EXIT("Error:" << ret);
    }
  }

  COUT_THIS("[micro] prepare data ...");
  while (ready_threads < fg_n) sleep(1);

  running = true;
  std::vector<size_t> tput_history(fg_n, 0);
  size_t current_sec = 0;
  while (current_sec < sec) {
    sleep(1);
    uint64_t tput = 0;
    for (size_t i = 0; i < fg_n; i++) {
      tput += fg_params[i].throughput - tput_history[i];
      tput_history[i] = fg_params[i].throughput;
    }
    COUT_THIS("[micro] >>> sec " << current_sec << " throughput: " << tput);
    ++current_sec;
  }

  running = false;
  void *status;
  for (size_t i = 0; i < fg_n; i++) {
    int rc = pthread_join(threads[i], &status);
    if (rc) {
      COUT_N_EXIT("Error:unable to join," << rc);
    }
  }

  size_t throughput = 0;
  for (auto &p : fg_params) {
    throughput += p.throughput;
  }
  COUT_THIS("[micro] Throughput(op/s): " << throughput / sec);
}

inline void parse_args(int argc, char **argv) {
  struct option long_options[] = {
      {"read", required_argument, 0, 'a'},
      {"insert", required_argument, 0, 'b'},
      {"remove", required_argument, 0, 'c'},
      {"update", required_argument, 0, 'd'},
      {"scan", required_argument, 0, 'e'},
      {"table-size", required_argument, 0, 'f'},
      {"runtime", required_argument, 0, 'g'},
      {"fg", required_argument, 0, 'h'},
      {"bg", required_argument, 0, 'i'},
      {"xindex-root-err-bound", required_argument, 0, 'j'},
      {"xindex-root-memory", required_argument, 0, 'k'},
      {"xindex-group-err-bound", required_argument, 0, 'l'},
      {"xindex-group-err-tolerance", required_argument, 0, 'm'},
      {"xindex-buf-size-bound", required_argument, 0, 'n'},
      {"xindex-buf-compact-threshold", required_argument, 0, 'o'},
      {0, 0, 0, 0}};
  std::string ops = "a:b:c:d:e:f:g:h:i:j:k:l:m:n:o:";
  int option_index = 0;

  while (1) {
    int c = getopt_long(argc, argv, ops.c_str(), long_options, &option_index);
    if (c == -1) break;

    switch (c) {
      case 0:
        if (long_options[option_index].flag != 0) break;
        abort();
        break;
      case 'a':
        read_ratio = strtod(optarg, NULL);
        INVARIANT(read_ratio >= 0 && read_ratio <= 1);
        break;
      case 'b':
        insert_ratio = strtod(optarg, NULL);
        INVARIANT(insert_ratio >= 0 && insert_ratio <= 1);
        break;
      case 'c':
        delete_ratio = strtod(optarg, NULL);
        INVARIANT(delete_ratio >= 0 && delete_ratio <= 1);
        break;
      case 'd':
        update_ratio = strtod(optarg, NULL);
        INVARIANT(update_ratio >= 0 && update_ratio <= 1);
        break;
      case 'e':
        scan_ratio = strtod(optarg, NULL);
        INVARIANT(scan_ratio >= 0 && scan_ratio <= 1);
        break;
      case 'f':
        table_size = strtoul(optarg, NULL, 10);
        INVARIANT(table_size > 0);
        break;
      case 'g':
        runtime = strtoul(optarg, NULL, 10);
        INVARIANT(runtime > 0);
        break;
      case 'h':
        fg_n = strtoul(optarg, NULL, 10);
        INVARIANT(fg_n > 0);
        break;
      case 'i':
        bg_n = strtoul(optarg, NULL, 10);
        break;
      case 'j':
        xindex::config.root_error_bound = strtol(optarg, NULL, 10);
        INVARIANT(xindex::config.root_error_bound > 0);
        break;
      case 'k':
        xindex::config.root_memory_constraint =
            strtol(optarg, NULL, 10) * 1024 * 1024;
        INVARIANT(xindex::config.root_memory_constraint > 0);
        break;
      case 'l':
        xindex::config.group_error_bound = strtol(optarg, NULL, 10);
        INVARIANT(xindex::config.group_error_bound > 0);
        break;
      case 'm':
        xindex::config.group_error_tolerance = strtol(optarg, NULL, 10);
        INVARIANT(xindex::config.group_error_tolerance > 0);
        break;
      case 'n':
        xindex::config.buffer_size_bound = strtol(optarg, NULL, 10);
        INVARIANT(xindex::config.buffer_size_bound > 0);
        break;
      case 'o':
        xindex::config.buffer_compact_threshold = strtol(optarg, NULL, 10);
        INVARIANT(xindex::config.buffer_compact_threshold > 0);
        break;
      default:
        abort();
    }
  }

  COUT_THIS("[micro] Read:Insert:Update:Delete:Scan = "
            << read_ratio << ":" << insert_ratio << ":" << update_ratio << ":"
            << delete_ratio << ":" << scan_ratio)
  double ratio_sum =
      read_ratio + insert_ratio + delete_ratio + scan_ratio + update_ratio;
  INVARIANT(ratio_sum > 0.9999 && ratio_sum < 1.0001);  // avoid precision lost
  COUT_VAR(runtime);
  COUT_VAR(fg_n);
  COUT_VAR(bg_n);
  COUT_VAR(xindex::config.root_error_bound);
  COUT_VAR(xindex::config.root_memory_constraint);
  COUT_VAR(xindex::config.group_error_bound);
  COUT_VAR(xindex::config.group_error_tolerance);
  COUT_VAR(xindex::config.buffer_size_bound);
  COUT_VAR(xindex::config.buffer_size_tolerance);
  COUT_VAR(xindex::config.buffer_compact_threshold);
}
