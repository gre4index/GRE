// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.
#pragma once

#include <sstream>
#include <iostream>
#include <functional>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <sys/types.h>
#include <unistd.h>
#include <signal.h>
#include "zipf.h"
#include "omp.h"
#include <cassert>
#include <chrono>
#include <cstring>
#include <fstream>
#include <algorithm>
#include <vector>
#include "tbb/parallel_sort.h"

#define PROFILE 1

#if !defined(HELPER_H)
#define HELPER_H

#define COUT_THIS(this) std::cout << this << std::endl;
#define COUT_VAR(this) std::cout << #this << ": " << this << std::endl;
#define COUT_POS() COUT_THIS("at " << __FILE__ << ":" << __LINE__)
#define COUT_N_EXIT(msg) \
  COUT_THIS(msg);        \
  COUT_POS();            \
  abort();
#define INVARIANT(cond)            \
  if (!(cond)) {                   \
    COUT_THIS(#cond << " failed"); \
    COUT_POS();                    \
    abort();                       \
  }

#if defined(NDEBUGGING)
#define DEBUG_THIS(this)
#else
#define DEBUG_THIS(this) std::cerr << this << std::endl
#endif

#define UNUSED(var) ((void)var)

#define CACHELINE_SIZE (1 << 6)

#define PACKED __attribute__((packed))

#define likely(x) __builtin_expect(!!(x), 1)
#define unlikely(x) __builtin_expect(!!(x), 0)
#ifndef FENCE
#define FENCE

inline void memory_fence() { asm volatile("mfence" : : : "memory"); }

#endif
#ifndef MEMORY_FENCE
#define MEMORY_FENCE

/** @brief Compiler fence.
 * Prevents reordering of loads and stores by the compiler. Not intended to
 * synchronize the processor's caches. */
inline void fence() { asm volatile("" : : : "memory"); }

#endif

inline uint64_t cmpxchg(uint64_t *object, uint64_t expected,
                        uint64_t desired) {
    asm volatile("lock; cmpxchgq %2,%1"
    : "+a"(expected), "+m"(*object)
    : "r"(desired)
    : "cc");
    fence();
    return expected;
}

inline uint8_t cmpxchgb(uint8_t *object, uint8_t expected,
                        uint8_t desired) {
    asm volatile("lock; cmpxchgb %2,%1"
    : "+a"(expected), "+m"(*object)
    : "r"(desired)
    : "cc");
    fence();
    return expected;
}

#endif  // HELPER_H

struct System {
    static void profile(const std::string &name, std::function<void()> body) {
        std::string filename = name.find(".data") == std::string::npos ? (name + ".data") : name;

        // Launch profiler
        pid_t pid;
#ifdef PROFILE
        std::stringstream s;
        s << getpid();
#endif
        int ppid = getpid();
        pid = fork();
        if (pid == 0) {
            // perf to generate the record file
#ifdef PROFILE
            auto fd = open("/dev/null", O_RDWR);
            dup2(fd, 1);
            dup2(fd, 2);
            exit(execl("/usr/bin/perf", "perf", "record", "-o", filename.c_str(), "-p", s.str().c_str(), nullptr));
#else
            // perf the cache misses of the file
            char buf[200];
            //sprintf(buf, "perf stat -e cache-misses,cache-references,L1-dcache-load-misses,LLC-loads,LLC-load-misses,LLC-stores,LLC-store-misses,r412e -p %d > %s 2>&1",ppid,filename.c_str());
            sprintf(buf, "perf stat -p %d > %s 2>&1",ppid,filename.c_str());
            execl("/bin/sh", "sh", "-c", buf, NULL);
#endif
        }
#ifndef PROFILE
        setpgid(pid, 0);
#endif
        sleep(3);
        // Run body
        body();
        // Kill profiler
#ifdef PROFILE
        kill(pid, SIGINT);
#else
        kill(-pid, SIGINT);
#endif
        sleep(1);
//waitpid(pid,nullptr,0);
    }

    static void profile(std::function<void()> body) {
        profile("perf.data", body);
    }
};

template<class T>
long long load_binary_data(T *&data, long long length, const std::string &file_path) {
    // open key file
    std::ifstream is(file_path.c_str(), std::ios::binary | std::ios::in);
    if (!is.is_open()) {
        return 0;
    }

    std::cout << file_path << std::endl;

    // read the number of keys
    T max_size;
    is.read(reinterpret_cast<char*>(&max_size), sizeof(T));

    std::cout << max_size << std::endl;

    // create array
    if(length < 0 || length > max_size) length = max_size;
    data = new T[length];

    // read keys
    is.read(reinterpret_cast<char *>(data), std::streamsize(length * sizeof(T)));
    is.close();
    return length;
}

template<class T>
long long load_text_data(T *&array, long long length, const std::string &file_path) {
    std::ifstream is(file_path.c_str());
    if (!is.is_open()) {
        return 0;
    }
    long long i = 0;
    std::string str;

    std::vector<T> temp_keys;
    temp_keys.reserve(200000000);
    while (std::getline(is, str) && (i < length || length < 0)) {
        std::istringstream ss(str);
        T key;
        ss >> key;
        temp_keys.push_back(key);
        i++;
    }

    array = new T[temp_keys.size()];
    for(int j = 0; j < temp_keys.size(); j++) {
        array[j] = temp_keys[j];
    }
    is.close();
    return temp_keys.size();
}

template<class T>
T *get_search_keys(T array[], int num_keys, int num_searches, size_t *seed = nullptr) {
    auto *keys = new T[num_searches];

#pragma omp parallel
    {
        std::mt19937_64 gen(std::random_device{}());
        if (seed) {
            gen.seed(*seed + omp_get_thread_num());
        }
        std::uniform_int_distribution<int> dis(0, num_keys - 1);
#pragma omp for
        for (int i = 0; i < num_searches; i++) {
            int pos = dis(gen);
            keys[i] = array[pos];
        }
    }

    return keys;
}


bool file_exists(const std::string &str) {
    std::ifstream fs(str);
    return fs.is_open();
}

template<class T>
T *get_search_keys_zipf(T array[], int num_keys, int num_searches, size_t *seed = nullptr) {
    auto *keys = new T[num_searches];
    ScrambledZipfianGenerator zipf_gen(num_keys, seed);
    for (int i = 0; i < num_searches; i++) {
        int pos = zipf_gen.nextValue();
        keys[i] = array[pos];
    }
    return keys;
}


template<typename T>
T *unique_data(T *key1, size_t &size1, T *key2, size_t &size2) {
    size_t ptr1 = 0;
    size_t ptr2 = 0;

    std::sort(key1, key1 + size1);
    size1 = std::unique(key1, key1 + size1) - key1;
    std::sort(key2, key2 + size2);
    size2 = std::unique(key2, key2 + size2) - key2;

    size_t result = 0;

    while (ptr1 < size1 && ptr2 < size2) {
        while (key1[ptr1] < key2[ptr2] && ptr1 < size1) {
            ptr1++;
        }
        if (key1[ptr1] == key2[ptr2]) {
            ptr2++;
            continue;
        }
        key2[result++] = key2[ptr2++];
    }

    while (ptr2 < size2) {
        key2[result++] = key2[ptr2++];
    }

    size2 = result;
    std::random_shuffle(key2, key2 + size2);

    return &key2[result];
}


