#include"./src/src/core/alex.h"
#include"../indexInterface.h"

template<class KEY_TYPE, class PAYLOAD_TYPE>
class alexInterface : public indexInterface<KEY_TYPE, PAYLOAD_TYPE> {
public:
  void init(Param *param = nullptr) {}

  void bulk_load(std::pair <KEY_TYPE, PAYLOAD_TYPE> *key_value, size_t num, Param *param = nullptr);

  bool get(KEY_TYPE key, PAYLOAD_TYPE &val, Param *param = nullptr);

  bool put(KEY_TYPE key, PAYLOAD_TYPE value, Param *param = nullptr);

  bool update(KEY_TYPE key, PAYLOAD_TYPE value, Param *param = nullptr);

  bool remove(KEY_TYPE key, Param *param = nullptr);

  size_t scan(KEY_TYPE key_low_bound, size_t key_num, std::pair<KEY_TYPE, PAYLOAD_TYPE> *result,
              Param *param = nullptr);

  long long memory_consumption() { return index.model_size() + index.data_size(); }

private:
  alex::Alex<KEY_TYPE, PAYLOAD_TYPE, alex::AlexCompare, std::allocator < std::pair < KEY_TYPE, PAYLOAD_TYPE>>, false>
  index;
};

template<class KEY_TYPE, class PAYLOAD_TYPE>
void alexInterface<KEY_TYPE, PAYLOAD_TYPE>::bulk_load(std::pair <KEY_TYPE, PAYLOAD_TYPE> *key_value, size_t num,
                                                      Param *param) {
  index.bulk_load(key_value, (int) num);
}

template<class KEY_TYPE, class PAYLOAD_TYPE>
bool alexInterface<KEY_TYPE, PAYLOAD_TYPE>::get(KEY_TYPE key, PAYLOAD_TYPE &val, Param *param) {
  PAYLOAD_TYPE *res = index.get_payload(key);
  if (res != nullptr) {
    val = *res;
    return true;
  }
  return false;
}

template<class KEY_TYPE, class PAYLOAD_TYPE>
bool alexInterface<KEY_TYPE, PAYLOAD_TYPE>::put(KEY_TYPE key, PAYLOAD_TYPE value, Param *param) {
  return index.insert(key, value).second;
}

template<class KEY_TYPE, class PAYLOAD_TYPE>
bool alexInterface<KEY_TYPE, PAYLOAD_TYPE>::update(KEY_TYPE key, PAYLOAD_TYPE value, Param *param) {
    // return index.update(key, value);
    return false;
}

template<class KEY_TYPE, class PAYLOAD_TYPE>
bool alexInterface<KEY_TYPE, PAYLOAD_TYPE>::remove(KEY_TYPE key, Param *param) {
  auto num_erase = index.erase(key);
  return num_erase > 0;
}

template<class KEY_TYPE, class PAYLOAD_TYPE>
size_t alexInterface<KEY_TYPE, PAYLOAD_TYPE>::scan(KEY_TYPE key_low_bound, size_t key_num,
                                                   std::pair<KEY_TYPE, PAYLOAD_TYPE> *result,
                                                   Param *param) {
  auto iter = index.lower_bound(key_low_bound);
  int scan_size = 0;
  for (scan_size = 0; scan_size < key_num && !iter.is_end(); scan_size++) {
    result[scan_size] = {(*iter).first, (*iter).second};
    iter++;
  }
  return scan_size;
}