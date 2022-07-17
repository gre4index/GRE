#include <hot/singlethreaded/HOTSingleThreaded.hpp>
#include <idx/contenthelpers/PairPointerKeyExtractor.hpp>
#include <algorithm>
#include <random>
#include"../indexInterface.h"
#include "omp.h"
//#include <hot/singlethreaded/HOTSingleThreadedIterator.hpp>
#include<cstdio>

template<class KEY_TYPE, class PAYLOAD_TYPE>
class HotInterface : public indexInterface<KEY_TYPE, PAYLOAD_TYPE> {
public:
  HotInterface() {
    idx = new hot::singlethreaded::HOTSingleThreaded < std::pair < KEY_TYPE, PAYLOAD_TYPE > *,
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

  ~HotInterface() {
    delete idx;
  }

private:
  std::vector <std::pair<KEY_TYPE, PAYLOAD_TYPE>> data;
  hot::singlethreaded::HOTSingleThreaded<std::pair < KEY_TYPE, PAYLOAD_TYPE>*, idx::contenthelpers::PairPointerKeyExtractor> *
  idx;
};

template<class KEY_TYPE, class PAYLOAD_TYPE>
void HotInterface<KEY_TYPE, PAYLOAD_TYPE>::bulk_load(std::pair <KEY_TYPE, PAYLOAD_TYPE> *key_value, size_t num,
                                                     Param *param) {
  std::random_device rd;
  std::mt19937 gen(rd());
  for (auto i = 0; i < num; i++) {
    idx->upsert(&(key_value[i]));
  }
  data.reserve(num);
  //hot::singlethreaded::hot_stat.clear();
}

template<class KEY_TYPE, class PAYLOAD_TYPE>
bool HotInterface<KEY_TYPE, PAYLOAD_TYPE>::get(KEY_TYPE key, PAYLOAD_TYPE &val, Param *param) {
  auto result = idx->lookup(key);
  if (result.mIsValid) {
    val = result.mValue->second;
    return true;
  } else {
    return false;
  }
}

template<class KEY_TYPE, class PAYLOAD_TYPE>
bool HotInterface<KEY_TYPE, PAYLOAD_TYPE>::put(KEY_TYPE key, PAYLOAD_TYPE value, Param *param) {
  auto val = new std::pair<KEY_TYPE, PAYLOAD_TYPE>(key, value);
  return idx->insert(val);;
}

template<class KEY_TYPE, class PAYLOAD_TYPE>
bool HotInterface<KEY_TYPE, PAYLOAD_TYPE>::update(KEY_TYPE key, PAYLOAD_TYPE value, Param *param) {
    return false;
}

template<class KEY_TYPE, class PAYLOAD_TYPE>
bool HotInterface<KEY_TYPE, PAYLOAD_TYPE>::remove(KEY_TYPE key, Param *param) {
  return idx->remove(key);
}

template<class KEY_TYPE, class PAYLOAD_TYPE>
size_t HotInterface<KEY_TYPE, PAYLOAD_TYPE>::scan(KEY_TYPE key_low_bound, size_t key_num,
                                                  std::pair<KEY_TYPE, PAYLOAD_TYPE> *result,
                                                  Param *param) {
  auto iterator = idx->lower_bound(key_low_bound);
  PAYLOAD_TYPE accumulator = 0;
  size_t num;
  for (num = 0u;
       num < key_num && iterator != idx->END_ITERATOR; ++num) {
    result[num] = {(*(iterator))->first,(*(iterator))->second};
    ++iterator;
  }
  return num;
}

template<class KEY_TYPE, class PAYLOAD_TYPE>
long long HotInterface<KEY_TYPE, PAYLOAD_TYPE>::memory_consumption() {
  return (idx->getStatistics()).first;
}