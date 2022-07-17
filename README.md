# GRE
GRE is a benchmark suite for learned indexes and traditional indexes to measure throughput and latency with custom workload (read / write ratio) and any dataset. GRE allows users to add indexes as competitors with ease via the interface.

## Requirements
- gcc 8.3.0+
- cmake 3.14.0+

## Dependencies
- intel-mkl 2018.4.274
- intel-tbb 2020.3
- jemalloc

## Build
```
git submodule update --init # only for the first time
mkdir -p build
cd build
cmake -DCMAKE_BUILD_TYPE=Release .. && make
```

## Basic usage
To calculate throughput:
```
./build/microbench \
--keys_file=./data/dataset \
--keys_file_type={binary,text} \
--read=0.5 --insert=0.5 \
--operations_num=800000000 \
--table_size=-1 \
--init_table_ratio=0.5 \
--thread_num=24 \
--index=index_name \
```
table_size=-1 is to infer from the first line of the file.
init_table_ratio is to specify the proportion of the dataset to bulkload.

For additional features, add additional flags:
- Latency
```
--latency_sample --latency_sample_ratio=0.01
```
- Range query (eg. range = 100)
```
--scan_ratio=1 --scan_num=100
```
- To use Zipfian distribution for lookup
```
--sample_distribution=zipf
```
- To perform data-shift experiment. Note that the key file needs to be generated like so (changing from one dataset to another). This flag just simply prevent the keys be shuffled and preserving the order in the key file
```
--data_shift
```
- Calculate data hardness (PLA-metric) with specified model error bound of the input dataset
```
--dataset_statistic --error_bound=32
```
- If the index implement memory consumption interface
```
--memory
```
All the result will be output to the csv file specified in --output_path flag.
