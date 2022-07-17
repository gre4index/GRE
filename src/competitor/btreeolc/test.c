#include"./src/BTreeOLC_child_layout.h"
#include <cstdio>
#include <random>

int main(){
    btreeolc::BTree<uint64_t, uint64_t> idx;

    printf("Bulkloading\n");

#pragma omp parallel for
    for(int i = 0; i < 1000000; i++) {
        idx.insert(i, i+1);
    }
    printf("Finish bulkloading\n");

    const int scan_len = 1000;
    uint64_t *val = new uint64_t[scan_len];

    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> rand_int(0, 1000000);
    auto begin_entry =rand_int(gen) ;

    idx.scan(begin_entry, scan_len, val);

    uint64_t sum = 0;

    for(int i = 0; i < scan_len; i++) {
        sum += val[i];
    }

    printf("The scan result is %llu, expected result is %llu\n", sum, (scan_len+begin_entry * 2 + 1)*(scan_len)/2);

    return 0;
}