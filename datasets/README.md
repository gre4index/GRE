# Download Datasets
You can download all the dataset by the script
```
bash download.sh [extra]
```
This will download all 10 datasets used in the paper. If you wish to download extra dataset, pass argument `extra`.

`wiki`, `books`, `fb`, and `osm` are taken from [SOSD](https://github.com/learnedsystems/SOSD).
> Kipf, Andreas, et al. ‘SOSD: A Benchmark for Learned Indexes’. NeurIPS Workshop on Machine Learning for Systems, 2019.

> Marcus, Ryan, et al. ‘Benchmarking Learned Indexes’. Proceedings of the VLDB Endowment, vol. 14, no. 1, Sept. 2020, pp. 1–13., https://doi.org/10.14778/3421424.3421425.

# Using the Synthetic Data Generator
Usage:
```
# compile
g++ --std=c++17 generator.cpp -o generator

./generator {le} {ge} {lv} {gv} {num} {path}
```
- `le`: Epsilon value of local non-linearity PLA
- `ge`: Epsilon value of global non-linearity PLA
- `lv`: Local non-linearity (number of model)
- `gv`: Global non-linearity (number of model)
- `num`: Number of keys to generate
- `path`: Path for the output key file

Source code is adapted from [PGM-Index](https://pgm.di.unipi.it)
> Ferragina, Paolo, and Giorgio Vinciguerra. ‘The PGM-Index: A Fully-Dynamic Compressed Learned Index with Provable Worst-Case Bounds’. Proceedings of the VLDB Endowment, vol. 13, no. 8, Apr. 2020, pp. 1162–75. https://doi.org/10.14778/3389133.3389135.
