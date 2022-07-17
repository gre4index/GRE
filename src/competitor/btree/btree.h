#include"./src/include/stx/btree.h"
#include"../indexInterface.h"

template<class KEY_TYPE, class PAYLOAD_TYPE>
class BTreeInterface : public indexInterface<KEY_TYPE, PAYLOAD_TYPE> {
public:
  void init(Param *param = nullptr) {}

  void bulk_load(std::pair <KEY_TYPE, PAYLOAD_TYPE> *key_value, size_t num, Param *param = nullptr);

  bool get(KEY_TYPE key, PAYLOAD_TYPE &val, Param *param = nullptr);

  bool put(KEY_TYPE key, PAYLOAD_TYPE value, Param *param = nullptr);

  bool update(KEY_TYPE key, PAYLOAD_TYPE value, Param *param = nullptr);

  bool remove(KEY_TYPE key, Param *param = nullptr);

  size_t scan(KEY_TYPE key_low_bound, size_t key_num, std::pair<KEY_TYPE, PAYLOAD_TYPE> *result,
              Param *param = nullptr) {
    auto iter = idx.lower_bound(key_low_bound);

    if (iter == idx.end()) {
      return 0;
    }

    uint64_t sum = 0;
    sum++;
    PAYLOAD_TYPE accumulator = 0;
    for (size_t i = 0; i < key_num; i++) {
      ++iter;
      sum++;
      if (iter == idx.end()) {
        break;
      }
      accumulator += iter->second;
    }
    return sum;

  }

  long long memory_consumption() { return 0; }

private:
  stx::btree<KEY_TYPE, PAYLOAD_TYPE> idx;
};

template<class KEY_TYPE, class PAYLOAD_TYPE>
void BTreeInterface<KEY_TYPE, PAYLOAD_TYPE>::bulk_load(std::pair <KEY_TYPE, PAYLOAD_TYPE> *key_value, size_t num,
                                                          Param *param) {
  idx.bulk_load(key_value, key_value + num);
}

template<class KEY_TYPE, class PAYLOAD_TYPE>
bool BTreeInterface<KEY_TYPE, PAYLOAD_TYPE>::get(KEY_TYPE key, PAYLOAD_TYPE &val, Param *param) {
  auto iter = idx.find(key);
  if(iter != idx.end()) {
    val = iter.data();
    return true;
  } else {
    return false;
  }
}

template<class KEY_TYPE, class PAYLOAD_TYPE>
bool BTreeInterface<KEY_TYPE, PAYLOAD_TYPE>::put(KEY_TYPE key, PAYLOAD_TYPE value, Param *param) {
  auto res_pair = idx.insert(key, value);
  return res_pair.second;
}

template<class KEY_TYPE, class PAYLOAD_TYPE>
bool BTreeInterface<KEY_TYPE, PAYLOAD_TYPE>::update(KEY_TYPE key, PAYLOAD_TYPE value, Param *param) {
    return false;
}

template<class KEY_TYPE, class PAYLOAD_TYPE>
bool BTreeInterface<KEY_TYPE, PAYLOAD_TYPE>::remove(KEY_TYPE key, Param *param) {
  auto num_erase = idx.erase(key);
  return num_erase > 0;
}

