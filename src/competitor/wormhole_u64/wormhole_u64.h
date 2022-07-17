#include "./src/lib.h"
#include "./src/kv.h"
#include "./src/wh.h"
#include "../indexInterface.h"
#include "omp.h"

template<class KEY_TYPE, class PAYLOAD_TYPE>
class WormholeU64Interface : public indexInterface<KEY_TYPE, PAYLOAD_TYPE> {
public:

    WormholeU64Interface() {
        wh = wormhole_u64_create(&mm_nnn); 
    }

    void init(Param *param = nullptr) {
        // this->thread_num = param->worker_num;
        // for (int i = 0; i < thread_num; i++) {
            // ref[i].instance = wormhole_ref(wh);
        // }
    }

    void bulk_load(std::pair <KEY_TYPE, PAYLOAD_TYPE> *key_value, size_t num, Param *param = nullptr);

    bool get(KEY_TYPE key, PAYLOAD_TYPE &val, Param *param = nullptr);

    bool put(KEY_TYPE key, PAYLOAD_TYPE value, Param *param = nullptr);

    bool update(KEY_TYPE key, PAYLOAD_TYPE value, Param *param = nullptr);

    bool remove(KEY_TYPE key, Param *param = nullptr);

    size_t scan(KEY_TYPE key_low_bound, size_t key_num, std::pair <KEY_TYPE, PAYLOAD_TYPE> *result,
                Param *param = nullptr) {
        // auto iter = wh_iter_create(ref[param->thread_id].instance);
        // auto key_rev = __builtin_bswap64(key_low_bound);
        // wh_iter_seek(iter, &key_rev, sizeof(KEY_TYPE));
        // size_t scan_num = key_num;
        // for (int i = 0; i < key_num; i++) {
        //   if(!wh_iter_valid(iter)) {
        //     scan_num = i + 1;
        //     break;
        //   }
        //   uint64_t val;
        //   uint64_t key;
        //   u32 key_len, val_len;
        //   wh_iter_peek(iter, (char *) &key, sizeof(KEY_TYPE), &key_len, (char *) &val, sizeof(PAYLOAD_TYPE), &val_len);
        //   result[i] = {__builtin_bswap64(key), __builtin_bswap64(val)};
        //   wh_iter_skip1(iter);
        // }
        // wh_iter_destroy(iter);
        // return scan_num;
        return 0;
    }

    ~WormholeU64Interface() {
        // for (int i = 0; i < thread_num; i++) {
        //     wormhole_unref(ref[i].instance);
        // }
        wormhole_u64_destroy(wh);
    }

    long long memory_consumption() { return 0; }

private:
    const struct kvmap_mm mm_nnn = {kvmap_mm_in_noop, kvmap_mm_out_noop, kvmap_mm_free_noop, NULL};
    struct wormhole *wh;
    // struct alignas(CACHELINE_SIZE)
    // wormref_align {
    //     struct wormref *instance;
    // };
    // struct wormref_align ref[256];
    // size_t thread_num;
};

template<class KEY_TYPE, class PAYLOAD_TYPE>
void WormholeU64Interface<KEY_TYPE, PAYLOAD_TYPE>::bulk_load(std::pair <KEY_TYPE, PAYLOAD_TYPE> *key_value, size_t num,Param *param) {
// #pragma omp parallel for
    struct wormref * const ref = wormhole_ref(wh);
    for (auto i = 0; i < num; i++) {
        wormhole_u64_set(ref, key_value[i].first, (void *)key_value[i].second);
    }
    wormhole_unref(ref);
}

template<class KEY_TYPE, class PAYLOAD_TYPE>
bool WormholeU64Interface<KEY_TYPE, PAYLOAD_TYPE>::get(KEY_TYPE key, PAYLOAD_TYPE &val, Param *param) {
    struct wormref * const ref = wormhole_ref(wh);
    if(!wormhole_u64_probe(ref, key)) {
        wormhole_unref(ref);
        return false;
    }
    val = (PAYLOAD_TYPE)wormhole_u64_get(ref, key, NULL);
    wormhole_unref(ref);
    return true;
}

template<class KEY_TYPE, class PAYLOAD_TYPE>
bool WormholeU64Interface<KEY_TYPE, PAYLOAD_TYPE>::put(KEY_TYPE key, PAYLOAD_TYPE value, Param *param) {
    struct wormref * const ref = wormhole_ref(wh);
    auto ret = wormhole_u64_set(ref, key, (void *)value);
    wormhole_unref(ref);
    return ret;
}

template<class KEY_TYPE, class PAYLOAD_TYPE>
bool WormholeU64Interface<KEY_TYPE, PAYLOAD_TYPE>::update(KEY_TYPE key, PAYLOAD_TYPE value, Param *param) {
    struct wormref * const ref = wormhole_ref(wh);
    auto ret = wormhole_u64_set(ref, key, (void *)value);
    wormhole_unref(ref);
    return ret;
}

template<class KEY_TYPE, class PAYLOAD_TYPE>
bool WormholeU64Interface<KEY_TYPE, PAYLOAD_TYPE>::remove(KEY_TYPE key, Param *param) {
    struct wormref * const ref = wormhole_ref(wh);
    auto ret = wormhole_u64_del(ref, key);
    wormhole_unref(ref);
    return ret;
}
