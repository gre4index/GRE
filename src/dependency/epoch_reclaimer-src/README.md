# VeryPM - persistent memory tool box 

[![Build Status](https://dev.azure.com/haoxiangpeng/VeryPM/_apis/build/status/XiangpengHao.VeryPM?branchName=master)](https://dev.azure.com/haoxiangpeng/VeryPM/_build/latest?definitionId=2&branchName=master)

A set of progressive, non-intrusive, easy-to-use, and high performance persistent memory tools.

## What's included:

1. Epoch Manager<sup>1</sup>
2. Garbage List
3. Persistent CAS <sup><a href="https://blog.haoxp.xyz/posts/persistent-cas/">2</a></sup>
4. PM allocator (in progress)
5. Pool Management (in progress)

## Features

- Progressive: components are self-sufficient, you don't need to include the whole forest just for a leaf. 

- Non-intrusive: pointers are 8-byte, typings are C++ standard. 

- Header-only

- Persistent memory support, tested on real device.

- High performance


## Build

```bash
mkdir build && cd build
cmake -DCMAKE_BUILD_TYPE={Debug|Release} -DPMEM={0|1} ..
make tests
```

1: Code adapted from [PMwCAS](https://github.com/microsoft/pmwcas) with a few new features, all bugs are mine.



## Usage - Epoch Manager and Garbage List 

```c++
struct MockItem {
 public:
  MockItem() {}

  static void Destroy(void* destroyContext, void* p) {
    // some complex memory cleanup
  }

  /*
   * Usage one: manual epoch management
   * */ 
  void SolveNP(){
    epoch_manager_->Protect();

    // some algorithms to solve SAT

    epoch_manager_->Unprotect(); 
  }


  /*
   * Usage two: use epoch guard to automatically protect and unprotect
   * Makes it easy to ensure epoch protection boundaries tightly adhere to stack life
   * time even with complex control flow.
   * */
  void SolveP(){
    EpochGuard guard();

    // Some algorithms
  }
};


EpochManager epoch_manager_;
epoch_manager_.Initialize()

GarbageList garbage_list_;
// 1024 is the number of addresses that can be held aside for pointer stability.
garbage_list_.Initialize(&epoch_manager_, 1024);

// MockItem::Destory is the deallocation callback
garbage_list_.Push(new MockItem(), MockItem::Destroy, nullptr);
garbage_list_.Push(new MockItem(), MockItem::Destroy, nullptr);
```


## Persistent Memory support

We require [PMDK](https://pmem.io/pmdk/) to support safe and efficient persistent memory operations.

### Recovery

```c++
pool_ = pmemobj_open(pool_name, layout_name, pool_size, CREATE_MODE_RW);

// to create a new garbage list
garbage_list_.Initialize(&epoch_manager_, pool_, 1024);

// to recover an existing garbage list
garbage_list_.Recovery(&epoch_manager_, pool_);
```

### Reserve Memory

Some persistent memory allocator, e.g. PMDK's, requires applications to pass a pre-existing memory location to store the pointer to the allocated memory.

For example:
```c++
void* mem = nullptr;
posix_memalign(&mem, CACHELINE_SIZE, size);
```
Storing `mem` on the stack is not safe, thus requires the application to mantain an `allocation list`, 
or even change the implementation significantly, so there is this function:

```c++
Garbagelist::Item* mem = garbage_list_.ReserveItem();
posix_memalign(&mem->removed_item, CACHELINE_SIZE, size);
```

The `mem->removed_item` is used to temporarily take the ownership of the allocated memory.

After the `mem->removed_item` is handed back to the data structure, the reserved memory slot should be cleared, otherwise it will be reclaimed on recovery:

```c++
garbage_list_.ResetItem(mem);
```

#### Caveat
Although both `ReserveItem` and `ResetItem` is crash/thread safe, when being used, typically they are protected by a (PMDK) transaction,
 because these functions implicitly implied ownership transfer which requires multi-cache line operations.

