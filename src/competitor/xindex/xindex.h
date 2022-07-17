#include <vector>

#include "./src/xindex.h"
#include "./src/xindex_impl.h"
#include "../indexInterface.h"

namespace xindex {
    template<class KEY_TYPE>
    class Key {
        typedef std::array<double, 1> model_key_t;

    public:
        static constexpr size_t

        model_key_size() { return 1; }

        static Key max() {
            static Key max_key(std::numeric_limits<KEY_TYPE>::max());
            return max_key;
        }

        static Key min() {
            static Key min_key(std::numeric_limits<KEY_TYPE>::min());
            return min_key;
        }

        Key() : key(0) {}

        Key(KEY_TYPE key) : key(key) {}

        Key(const Key<KEY_TYPE> &other) { key = other.key; }

        Key &operator=(const Key<KEY_TYPE> &other) {
            key = other.key;
            return *this;
        }

        model_key_t to_model_key() const {
            model_key_t model_key;
            model_key[0] = key;
            return model_key;
        }

        friend bool operator<(const Key<KEY_TYPE> &l, const Key<KEY_TYPE> &r) { return l.key < r.key; }

        friend bool operator>(const Key<KEY_TYPE> &l, const Key<KEY_TYPE> &r) { return l.key > r.key; }

        friend bool operator>=(const Key<KEY_TYPE> &l, const Key<KEY_TYPE> &r) { return l.key >= r.key; }

        friend bool operator<=(const Key<KEY_TYPE> &l, const Key<KEY_TYPE> &r) { return l.key <= r.key; }

        friend bool operator==(const Key<KEY_TYPE> &l, const Key<KEY_TYPE> &r) { return l.key == r.key; }

        friend bool operator!=(const Key<KEY_TYPE> &l, const Key<KEY_TYPE> &r) { return l.key != r.key; }

        KEY_TYPE key;
    } PACKED;
}

template<class KEY_TYPE, class PAYLOAD_TYPE>
class xindexInterface : public indexInterface<KEY_TYPE, PAYLOAD_TYPE> {
public:
    void bulk_load(std::pair <KEY_TYPE, PAYLOAD_TYPE> *key_value, size_t num, Param *param);

    bool get(KEY_TYPE key, PAYLOAD_TYPE &val, Param *param);

    bool put(KEY_TYPE key, PAYLOAD_TYPE value, Param *param);

    bool update(KEY_TYPE key, PAYLOAD_TYPE value, Param *param = nullptr);

    bool remove(KEY_TYPE key, Param *param);

    size_t
    scan(KEY_TYPE key_low_bound, size_t key_num, std::pair <KEY_TYPE, PAYLOAD_TYPE> *result, Param *param);

    void init(Param *param);

    long long memory_consumption() {
        return 0;
    }

    ~xindexInterface() {
        delete this->index;
    }

    xindexInterface() {}

    size_t worker_num;
    size_t bg_n;
private :
    xindex::XIndex <xindex::Key<KEY_TYPE>, PAYLOAD_TYPE> *index;
    size_t core_num;
};

template<class KEY_TYPE, class PAYLOAD_TYPE>
void xindexInterface<KEY_TYPE, PAYLOAD_TYPE>::init(Param *param) {
    worker_num = param->worker_num;
    bg_n = param->worker_num / 12 + 1;
//    core_num = param->worker_num;
//    if(core_num == 1) {
//        worker_num = 1;
//        bg_n = 1;
//        return;
//    }
//    worker_num = core_num - (core_num / 12 + 1 - (core_num % 12 == 0));
//    bg_n = core_num - worker_num;
}

template<class KEY_TYPE, class PAYLOAD_TYPE>
void xindexInterface<KEY_TYPE, PAYLOAD_TYPE>::bulk_load(std::pair <KEY_TYPE, PAYLOAD_TYPE> *key_value, size_t num,
                                                        Param *param) {
    std::vector <xindex::Key<KEY_TYPE>> key;
    std::vector <PAYLOAD_TYPE> value;
    bool random_insert = false;

    KEY_TYPE last_key;
    for (int i = 0; i < num; i++) {
        // if (i != 0 && key_value[i].first == last_key) { continue; }
        key.push_back(xindex::Key<KEY_TYPE>(key_value[i].first));
        value.push_back(key_value[i].second);
        // last_key = key_value[i].first;
    }

    printf("worker_num: %llu, bg_n: %llu\n", worker_num, bg_n);
    index = new xindex::XIndex<xindex::Key<KEY_TYPE>, PAYLOAD_TYPE>(key, value, worker_num, bg_n);

    // for (int i = num / 2; i < num; i++) {
    //     this->put(key_value[i].first, key_value[i].second, param);
    // }

}

template<class KEY_TYPE, class PAYLOAD_TYPE>
bool xindexInterface<KEY_TYPE, PAYLOAD_TYPE>::get(KEY_TYPE key, PAYLOAD_TYPE &val, Param *param) {
    auto ret = index->get(xindex::Key<KEY_TYPE>(key), val, param->thread_id);
    return ret;
}

template<class KEY_TYPE, class PAYLOAD_TYPE>
bool xindexInterface<KEY_TYPE, PAYLOAD_TYPE>::put(KEY_TYPE key, PAYLOAD_TYPE value, Param *param) {
    return index->put(xindex::Key<KEY_TYPE>(key), value, param->thread_id);
}

template<class KEY_TYPE, class PAYLOAD_TYPE>
bool xindexInterface<KEY_TYPE, PAYLOAD_TYPE>::update(KEY_TYPE key, PAYLOAD_TYPE value, Param *param) {
    return index->put(xindex::Key<KEY_TYPE>(key), value, param->thread_id);
}

template<class KEY_TYPE, class PAYLOAD_TYPE>
bool xindexInterface<KEY_TYPE, PAYLOAD_TYPE>::remove(KEY_TYPE key, Param *param) {
    return index->remove(xindex::Key<KEY_TYPE>(key), param->thread_id);
}

template<class KEY_TYPE, class PAYLOAD_TYPE>
size_t xindexInterface<KEY_TYPE, PAYLOAD_TYPE>::scan(KEY_TYPE key_low_bound, size_t key_num,
                                                     std::pair <KEY_TYPE, PAYLOAD_TYPE> *result,
                                                     Param *param) {
    std::vector <std::pair<xindex::Key<KEY_TYPE>, PAYLOAD_TYPE>> result_temp;
    auto scan_num = index->scan(xindex::Key<KEY_TYPE>(key_low_bound), key_num, result_temp, param->thread_id);
    PAYLOAD_TYPE accumulator = 0;
    for (auto i : result_temp) {
        accumulator += i.second;
    }
    return scan_num;
}