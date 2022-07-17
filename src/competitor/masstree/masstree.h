#include"./src/mtIndexAPI.hh"
#include "./src/config.h"
#include"../indexInterface.h"

template<class KEY_TYPE, class PAYLOAD_TYPE>
class MasstreeInterface : public indexInterface<KEY_TYPE, PAYLOAD_TYPE> {
public:
  typedef mt_index<Masstree::default_table> MapType;

  MasstreeInterface() {
    idx = new MapType{};

    ti[0].instance = threadinfo::make(threadinfo::TI_MAIN, -1);
    idx->setup(ti[0].instance);
  }

  void init(Param *param = nullptr) {
    thread_num = param->worker_num;
    for (int i = 1; i < thread_num; i++) {
      ti[i].instance = threadinfo::make(threadinfo::TI_MAIN, -1);
    }
  }

  // inline void swap_endian(uint64_t &i) {
  //   // Note that masstree internally treat input as big-endian
  //   // integer values, so we need to swap here
  //   // This should be just one instruction
  //   i = __bswap_64(i);
  // }

  inline void swap_endian(uint32_t &i) {
    i = __builtin_bswap32(i);
  }
  inline void swap_endian(uint64_t &i) {
    i = __builtin_bswap64(i);
  }

  void bulk_load(std::pair<KEY_TYPE, PAYLOAD_TYPE> *key_value, size_t num, Param *param = nullptr);

  bool get(KEY_TYPE key, PAYLOAD_TYPE &val, Param *param = nullptr);

  bool put(KEY_TYPE key, PAYLOAD_TYPE value, Param *param = nullptr);

  bool update(KEY_TYPE key, PAYLOAD_TYPE value, Param *param = nullptr);

  bool remove(KEY_TYPE key, Param *param = nullptr);

  size_t scan(KEY_TYPE key_low_bound, size_t key_num, std::pair<KEY_TYPE, PAYLOAD_TYPE> *result,
              Param *param = nullptr) { return 0; }

  ~MasstreeInterface() {
    if (idx) {
      delete idx;
    }
  }

  long long memory_consumption() { return 0; }

private:
  MapType *idx;
  struct alignas(CACHELINE_SIZE) threadinfo_align {
        struct threadinfo *instance;
  };
  struct threadinfo_align ti[256];
  size_t thread_num;
};

template<class KEY_TYPE, class PAYLOAD_TYPE>
void MasstreeInterface<KEY_TYPE, PAYLOAD_TYPE>::bulk_load(std::pair<KEY_TYPE, PAYLOAD_TYPE> *key_value, size_t num,
                                                          Param *param) {
  for (auto i = 0; i < num; i++) {
    auto key = key_value[i].first;
    auto value = key_value[i].second;
    swap_endian(key);
    idx->put_uv((const char *) &key, sizeof(KEY_TYPE), (const char *) &value, sizeof(PAYLOAD_TYPE), ti[param->thread_id].instance);
  }
}

template<class KEY_TYPE, class PAYLOAD_TYPE>
bool MasstreeInterface<KEY_TYPE, PAYLOAD_TYPE>::get(KEY_TYPE key, PAYLOAD_TYPE &val, Param *param) {
  Str v;
  swap_endian(key);
  idx->get((const char *) &key, sizeof(KEY_TYPE), v, ti[param->thread_id].instance);
  if (v.s) {
    val = static_cast<PAYLOAD_TYPE>(*(uint64_t *) v.s);
    return true;
  } else {
    return false;
  }
}

template<class KEY_TYPE, class PAYLOAD_TYPE>
bool MasstreeInterface<KEY_TYPE, PAYLOAD_TYPE>::put(KEY_TYPE key, PAYLOAD_TYPE value, Param *param) {
  swap_endian(key);
  idx->put_uv((const char *) &key, sizeof(KEY_TYPE), (const char *) &value, 8, ti[param->thread_id].instance);
  return true;
}

template<class KEY_TYPE, class PAYLOAD_TYPE>
bool MasstreeInterface<KEY_TYPE, PAYLOAD_TYPE>::update(KEY_TYPE key, PAYLOAD_TYPE value, Param *param) {
  swap_endian(key);
  idx->put((const char *) &key, sizeof(KEY_TYPE), (const char *) &value, 8, ti[param->thread_id].instance);
  return true;
}

template<class KEY_TYPE, class PAYLOAD_TYPE>
bool MasstreeInterface<KEY_TYPE, PAYLOAD_TYPE>::remove(KEY_TYPE key, Param *param) {
//  index.erase(key);
  return true;
}

