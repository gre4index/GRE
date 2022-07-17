#include "./src/lib.h"
#include "./src/kv.h"
#include "./src/wh.h"
#include "../indexInterface.h"
#include "omp.h"

template<class KEY_TYPE, class PAYLOAD_TYPE>
class WormholeInterface : public indexInterface<KEY_TYPE, PAYLOAD_TYPE> {
public:

  WormholeInterface() {
    wh = wh_create();
  }

  void init(Param *param = nullptr) {
    for (int i = 0; i < param->worker_num; i++) {
      ref[i].instance = wh_ref(wh);
    }
    this->thread_num = param->worker_num;
  }

  void bulk_load(std::pair <KEY_TYPE, PAYLOAD_TYPE> *key_value, size_t num, Param *param = nullptr);

  bool get(KEY_TYPE key, PAYLOAD_TYPE &val, Param *param = nullptr);

  bool put(KEY_TYPE key, PAYLOAD_TYPE value, Param *param = nullptr);

  bool remove(KEY_TYPE key, Param *param = nullptr);

  size_t scan(KEY_TYPE key_low_bound, size_t key_num,  std::pair<KEY_TYPE, PAYLOAD_TYPE> *result,
              Param *param = nullptr) {
    auto iter = wh_iter_create(ref[param->thread_id].instance);
    auto key_rev = __builtin_bswap64(key_low_bound);
    wh_iter_seek(iter, &key_rev, sizeof(KEY_TYPE));
    size_t scan_num = key_num;
    for (int i = 0; i < key_num; i++) {
      if(!wh_iter_valid(iter)) {
        scan_num = i + 1;
        break;
      }
      uint64_t val;
      uint64_t key;
      u32 key_len, val_len;
      wh_iter_peek(iter, (char *) &key, sizeof(KEY_TYPE), &key_len, (char *) &val, sizeof(PAYLOAD_TYPE), &val_len);
      result[i] = {__builtin_bswap64(key), __builtin_bswap64(val)};
      wh_iter_skip1(iter);
    }
    wh_iter_destroy(iter);
    return scan_num;
  }

  ~WormholeInterface() {
    for (int i = 0; i < this->thread_num; i++) {
      wh_unref(ref[i].instance);
    }
    wh_destroy(wh);
  }

  long long memory_consumption() { return 0; }

private:
  struct wormhole *wh;
  struct alignas(CACHELINE_SIZE) wormref_align {
    struct wormref *instance;
  };
  struct wormref_align ref[256];
  size_t thread_num;
};

template<class KEY_TYPE, class PAYLOAD_TYPE>
void WormholeInterface<KEY_TYPE, PAYLOAD_TYPE>::bulk_load(std::pair <KEY_TYPE, PAYLOAD_TYPE> *key_value, size_t num, Param *param) {
// #pragma omp parallel for
  for (auto i = 0; i < num; i++) {
    auto key = __builtin_bswap64(key_value[i].first);
    auto value = __builtin_bswap64(key_value[i].second);
    wh_put(this->ref[param->thread_id].instance, &key, sizeof(KEY_TYPE), &value, sizeof(PAYLOAD_TYPE));
  }
}

template<class KEY_TYPE, class PAYLOAD_TYPE>
bool WormholeInterface<KEY_TYPE, PAYLOAD_TYPE>::get(KEY_TYPE key, PAYLOAD_TYPE &val, Param *param) {
  u32 len_val;
  uint64_t rev_key = __builtin_bswap64(key);
  uint64_t rev_val;
  auto ret = wh_get(ref[param->thread_id].instance, &rev_key, sizeof(KEY_TYPE), &rev_val, sizeof(PAYLOAD_TYPE), &len_val);
  if (ret) {
    val = __builtin_bswap64(rev_val);
    return true;
  } else {
    return false;
  }
}

template<class KEY_TYPE, class PAYLOAD_TYPE>
bool WormholeInterface<KEY_TYPE, PAYLOAD_TYPE>::put(KEY_TYPE key, PAYLOAD_TYPE value, Param *param) {
  uint64_t rev_key = __builtin_bswap64(key);
  uint64_t rev_val = __builtin_bswap64(value);
  return wh_put(ref[param->thread_id].instance, &rev_key, sizeof(KEY_TYPE), &rev_val, sizeof(PAYLOAD_TYPE));
}

template<class KEY_TYPE, class PAYLOAD_TYPE>
bool WormholeInterface<KEY_TYPE, PAYLOAD_TYPE>::remove(KEY_TYPE key, Param *param) {
//  index.erase(key);
  return false;
}
