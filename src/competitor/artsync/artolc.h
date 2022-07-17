#include"./src/OptimisticLockCoupling/Tree.h"
#include"./src/OptimisticLockCoupling/Tree.cpp"
#include"../indexInterface.h"
#include "tbb/tbb.h"
#include <utility>

template<class KEY_TYPE, class PAYLOAD_TYPE>
class ARTOLCInterface : public indexInterface<KEY_TYPE, PAYLOAD_TYPE> {
public:

    ARTOLCInterface() {
        idx = new ART_OLC::Tree(loadKey);
        auto temp= new std::pair<KEY_TYPE,PAYLOAD_TYPE>(~0ull,0);
        loadKey(reinterpret_cast<uint64_t>(&temp), maxKey);
    }

    void init(Param *param = nullptr) {}

    void bulk_load(std::pair <KEY_TYPE, PAYLOAD_TYPE> *key_value, size_t num, Param *param = nullptr);

    bool get(KEY_TYPE key, PAYLOAD_TYPE &val, Param *param = nullptr);

    bool put(KEY_TYPE key, PAYLOAD_TYPE value, Param *param = nullptr);

    bool update(KEY_TYPE key, PAYLOAD_TYPE value, Param *param = nullptr);

    bool remove(KEY_TYPE key, Param *param = nullptr);

    size_t scan(KEY_TYPE key_low_bound, size_t key_num, std::pair <KEY_TYPE, PAYLOAD_TYPE> *result,
                Param *param = nullptr) {
        thread_local static auto t = idx->getThreadInfo();
        Key k;
        k.setKeyLen(sizeof(key_low_bound));
        reinterpret_cast<KEY_TYPE *>(&k[0])[0] = swap_endian(key_low_bound);

        TID results[key_num];
        size_t resultCount;
        Key continueKey;
        idx->lookupRange(k, maxKey, continueKey, results, key_num, resultCount, t);
        printf("%u, ", resultCount);

        return resultCount;
    }

    long long memory_consumption() { return 0; }

    static void loadKey(TID tid, Key &key) {
        // Store the key of the tuple into the key vector
        // Implementation is database specific

        std::pair<KEY_TYPE, PAYLOAD_TYPE> * valPtr = reinterpret_cast<std::pair<KEY_TYPE, PAYLOAD_TYPE> *>(tid);
        KEY_TYPE reversed = swap_endian(valPtr->first);
        key.set(reinterpret_cast<char *>(&reversed), sizeof(valPtr->first));
        // key.setKeyLen(sizeof(valPtr->first));
        // reinterpret_cast<KEY_TYPE *>(&key[0])[0] = swap_endian(valPtr->first);
    }

    ~ARTOLCInterface() {
        delete idx;
    }

private:
    Key maxKey;
    ART_OLC::Tree *idx;
    inline static uint32_t swap_endian(uint32_t i) {
        return __builtin_bswap32(i);
    }
    inline static uint64_t swap_endian(uint64_t i) {
        return __builtin_bswap64(i);
    }
};

template<class KEY_TYPE, class PAYLOAD_TYPE>
void ARTOLCInterface<KEY_TYPE, PAYLOAD_TYPE>::bulk_load(std::pair <KEY_TYPE, PAYLOAD_TYPE> *key_value, size_t num,
                                                        Param *param) {
    for (auto i = 0; i < num; i++) {
        put(key_value[i].first, key_value[i].second, param);
    }
}

template<class KEY_TYPE, class PAYLOAD_TYPE>
bool ARTOLCInterface<KEY_TYPE, PAYLOAD_TYPE>::get(KEY_TYPE key, PAYLOAD_TYPE &val, Param *param) {
    thread_local static auto t = idx->getThreadInfo();
    Key k;
    KEY_TYPE reversed = swap_endian(key);
    // k.setKeyLen(sizeof(key));
    // reinterpret_cast<KEY_TYPE *>(&k[0])[0] = swap_endian(key);
    k.set(reinterpret_cast<char *>(&reversed), sizeof(key));
    auto valPtr = reinterpret_cast<std::pair<KEY_TYPE, PAYLOAD_TYPE>*>(idx->lookup(k,t));
    if(valPtr) {
        val = valPtr->second;
        return true;
    } else {
        return false;
    }
}

template<class KEY_TYPE, class PAYLOAD_TYPE>
bool ARTOLCInterface<KEY_TYPE, PAYLOAD_TYPE>::put(KEY_TYPE key, PAYLOAD_TYPE value, Param *param) {
    thread_local static auto t = idx->getThreadInfo();

    auto temp = new std::pair<KEY_TYPE, PAYLOAD_TYPE>(key,value);
    Key k;
    KEY_TYPE reversed = swap_endian(key);
    // k.setKeyLen(sizeof(key));
    // reinterpret_cast<KEY_TYPE *>(&k[0])[0] = swap_endian(key);
    k.set(reinterpret_cast<char *>(&reversed), sizeof(key));
    idx->insert(k, reinterpret_cast<TID>(temp),t);
    return true;
}

template<class KEY_TYPE, class PAYLOAD_TYPE>
bool ARTOLCInterface<KEY_TYPE, PAYLOAD_TYPE>::update(KEY_TYPE key, PAYLOAD_TYPE value, Param *param) {
    thread_local static auto t = idx->getThreadInfo();
    auto temp = new std::pair<KEY_TYPE, PAYLOAD_TYPE>(key,value);
    Key k;
    KEY_TYPE reversed = swap_endian(key);
    // k.setKeyLen(sizeof(key));
    // reinterpret_cast<KEY_TYPE *>(&k[0])[0] = swap_endian(key);
    k.set(reinterpret_cast<char *>(&reversed), sizeof(key));
    return idx->update(k, reinterpret_cast<TID>(temp), t);
}


template<class KEY_TYPE, class PAYLOAD_TYPE>
bool ARTOLCInterface<KEY_TYPE, PAYLOAD_TYPE>::remove(KEY_TYPE key, Param *param) {
    thread_local static auto t = idx->getThreadInfo();
    Key k;
    KEY_TYPE reversed = swap_endian(key);
    // k.setKeyLen(sizeof(key));
    // reinterpret_cast<KEY_TYPE *>(&k[0])[0] = swap_endian(key);
    k.set(reinterpret_cast<char *>(&reversed), sizeof(key));
    idx->remove(k, t);
    return true;
}
