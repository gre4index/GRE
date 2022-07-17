#include <hot/rowex/HOTRowex.hpp>
#include <idx/contenthelpers/PairPointerKeyExtractor.hpp>
#include <algorithm>
#include <random>
#include"../indexInterface.h"
#include "omp.h"
#include <hot/rowex/HOTRowexIterator.hpp>
#include<cstdio>

template<class KEY_TYPE, class PAYLOAD_TYPE>
class HotRowexInterface : public indexInterface<KEY_TYPE, PAYLOAD_TYPE> {
public:
  HotRowexInterface() {
    idx = new hot::rowex::HOTRowex < std::pair < KEY_TYPE, PAYLOAD_TYPE > *,
      idx::contenthelpers::PairPointerKeyExtractor > ();
    return;
  }


  void init(Param *param = nullptr) {
  }

  void bulk_load(std::pair <KEY_TYPE, PAYLOAD_TYPE> *key_value, size_t num, Param *param = nullptr);

  bool get(KEY_TYPE key, PAYLOAD_TYPE &val, Param *param = nullptr);

  bool put(KEY_TYPE key, PAYLOAD_TYPE value, Param *param = nullptr);

  bool update(KEY_TYPE key, PAYLOAD_TYPE value, Param *param = nullptr);

  bool remove(KEY_TYPE key, Param *param = nullptr);

  size_t scan(KEY_TYPE key_low_bound, size_t key_num, std::pair<KEY_TYPE, PAYLOAD_TYPE> *result,
              Param *param = nullptr);

  long long memory_consumption();

  ~HotRowexInterface() {
    delete idx;
  }

private:
  std::vector <std::pair<KEY_TYPE, PAYLOAD_TYPE>> data;
  hot::rowex::HOTRowex<std::pair < KEY_TYPE, PAYLOAD_TYPE>*, idx::contenthelpers::PairPointerKeyExtractor> *
  idx;
};

template<class KEY_TYPE, class PAYLOAD_TYPE>
void HotRowexInterface<KEY_TYPE, PAYLOAD_TYPE>::bulk_load(std::pair <KEY_TYPE, PAYLOAD_TYPE> *key_value, size_t num,
                                                     Param *param) {
  std::random_device rd;
  std::mt19937 gen(rd());
  for (auto i = 0; i < num; i++) {
    idx->upsert(&(key_value[i]));
  }
  data.reserve(num);
}

template<class KEY_TYPE, class PAYLOAD_TYPE>
bool HotRowexInterface<KEY_TYPE, PAYLOAD_TYPE>::get(KEY_TYPE key, PAYLOAD_TYPE &val, Param *param) {
  auto result = idx->lookup(key);
  if (result.mIsValid) {
    val = result.mValue->second;
    return true;
  } else {
    return false;
  }
}

template<class KEY_TYPE, class PAYLOAD_TYPE>
bool HotRowexInterface<KEY_TYPE, PAYLOAD_TYPE>::put(KEY_TYPE key, PAYLOAD_TYPE value, Param *param) {
  auto val = new std::pair<KEY_TYPE, PAYLOAD_TYPE>(key, value);
  return idx->insert(val);;
}

template<class KEY_TYPE, class PAYLOAD_TYPE>
bool HotRowexInterface<KEY_TYPE, PAYLOAD_TYPE>::update(KEY_TYPE key, PAYLOAD_TYPE value, Param *param) {
    return false;
}

template<class KEY_TYPE, class PAYLOAD_TYPE>
bool HotRowexInterface<KEY_TYPE, PAYLOAD_TYPE>::remove(KEY_TYPE key, Param *param) {
  printf("HOT doesn't support remove!");
  exit(0);
  return true;
}

template<class KEY_TYPE, class PAYLOAD_TYPE>
size_t HotRowexInterface<KEY_TYPE, PAYLOAD_TYPE>::scan(KEY_TYPE key_low_bound, size_t key_num,
                                                  std::pair<KEY_TYPE, PAYLOAD_TYPE> *result,
                                                  Param *param) {
  auto iterator = idx->lower_bound(key_low_bound);
  PAYLOAD_TYPE accumulator = 0;
  size_t num;
  //for (num = 0u; num < key_num && iterator != hot::rowex::HOTRowexSynchronizedIterator < std::pair < KEY_TYPE, PAYLOAD_TYPE > *, idx::contenthelpers::PairPointerKeyExtractor > ::end(); ++num) {
  for (num = 0u; num < key_num && iterator != iterator.end(); ++num) {
    result[num] = {(*(iterator))->first,(*(iterator))->second};
    ++iterator;
  }
  return num;
}

template<class KEY_TYPE, class PAYLOAD_TYPE>
long long HotRowexInterface<KEY_TYPE, PAYLOAD_TYPE>::memory_consumption() {
  return (idx->getStatistics()).first;
}