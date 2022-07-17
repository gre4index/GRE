#include"./src/src/core/lipp.h"
#include"../indexInterface.h"

template<class KEY_TYPE, class PAYLOAD_TYPE>
class LIPPInterface : public indexInterface<KEY_TYPE, PAYLOAD_TYPE> {
public:
    void init(Param *param = nullptr) {}

    void bulk_load(std::pair <KEY_TYPE, PAYLOAD_TYPE> *key_value, size_t num, Param *param = nullptr);

    bool get(KEY_TYPE key, PAYLOAD_TYPE &val, Param *param = nullptr);

    bool put(KEY_TYPE key, PAYLOAD_TYPE value, Param *param = nullptr);

    bool update(KEY_TYPE key, PAYLOAD_TYPE value, Param *param = nullptr);

    bool remove(KEY_TYPE key, Param *param = nullptr);

    size_t scan(KEY_TYPE key_low_bound, size_t key_num, std::pair <KEY_TYPE, PAYLOAD_TYPE> *result,
                Param *param = nullptr);

    long long memory_consumption() { return lipp.total_size(); }

private:
    LIPP <KEY_TYPE, PAYLOAD_TYPE> lipp;
};

template<class KEY_TYPE, class PAYLOAD_TYPE>
void LIPPInterface<KEY_TYPE, PAYLOAD_TYPE>::bulk_load(std::pair <KEY_TYPE, PAYLOAD_TYPE> *key_value, size_t num,
                                                      Param *param) {
    lipp.bulk_load(key_value, static_cast<int>(num));
}

template<class KEY_TYPE, class PAYLOAD_TYPE>
bool LIPPInterface<KEY_TYPE, PAYLOAD_TYPE>::get(KEY_TYPE key, PAYLOAD_TYPE &val, Param *param) {
    bool exist;
    val = lipp.at(key, false, exist);
    return exist;
}

template<class KEY_TYPE, class PAYLOAD_TYPE>
bool LIPPInterface<KEY_TYPE, PAYLOAD_TYPE>::put(KEY_TYPE key, PAYLOAD_TYPE value, Param *param) {
    return lipp.insert(key, value);

}

template<class KEY_TYPE, class PAYLOAD_TYPE>
bool LIPPInterface<KEY_TYPE, PAYLOAD_TYPE>::update(KEY_TYPE key, PAYLOAD_TYPE value, Param *param) {
    return lipp.update(key, value);
}

template<class KEY_TYPE, class PAYLOAD_TYPE>
bool LIPPInterface<KEY_TYPE, PAYLOAD_TYPE>::remove(KEY_TYPE key, Param *param) {
    return lipp.remove(key);
}

template<class KEY_TYPE, class PAYLOAD_TYPE>
size_t LIPPInterface<KEY_TYPE, PAYLOAD_TYPE>::scan(KEY_TYPE key_low_bound, size_t key_num,
                                                   std::pair <KEY_TYPE, PAYLOAD_TYPE> *result,
                                                   Param *param) {
    if(!result) {
        result = new std::pair <KEY_TYPE, PAYLOAD_TYPE>[key_num];
    }
    return lipp.range_query_len(result, key_low_bound, key_num);
}