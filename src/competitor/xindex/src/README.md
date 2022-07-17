# XIndex-R

XIndex is a concurrent learned range index, which was introduced in paper "[XIndex: A Scalable Learned Index for Multicore Data Storage](https://ppopp20.sigplan.org/details/PPoPP-2020-papers/13/XIndex-A-Scalable-Learned-Index-for-Multicore-Data-Storage)".

## Prerequisites

This project uses [CMake](https://cmake.org/) (3.5+) for building and testing.
It also requires dependencies of [Intel MKL](https://software.intel.com/en-us/mkl) and (optionally) [jemalloc](https://github.com/jemalloc/jemalloc).

### Installing Intel MKL
Detailed steps can be found [here](https://software.intel.com/en-us/articles/installing-intel-free-libs-and-python-apt-repo).

```shell
$ cd /tmp
$ wget https://apt.repos.intel.com/intel-gpg-keys/GPG-PUB-KEY-INTEL-SW-PRODUCTS-2019.PUB
$ apt-key add GPG-PUB-KEY-INTEL-SW-PRODUCTS-2019.PUB
$ rm GPG-PUB-KEY-INTEL-SW-PRODUCTS-2019.PUB

$ sh -c 'echo deb https://apt.repos.intel.com/mkl all main > /etc/apt/sources.list.d/intel-mkl.list'
$ apt-get update
$ apt-get install -y intel-mkl-2019.0-045
```

After the installation, please modify the following two lines in `CMakeLists.txt` accordingly.

```cmake
set(MKL_LINK_DIRECTORY "/opt/intel/mkl/lib/intel64")
set(MKL_INCLUDE_DIRECTORY "/opt/intel/mkl/include")
```

### (optional) Installing jemalloc

```shell
$ apt-get -y install libjemalloc1
$ cd /usr/lib/x86_64-linux-gnu/
$ ln -s libjemalloc.so.1 libjemalloc.so
```

After the installation, please modify the following line in `CMakeLists.txt` accordingly.

```cmake
set(JEMALLOC_DIR "/usr/lib/x86_64-linux-gnu")
```

## Build and Run

We use CMake to build XIndex-R.

```shell
$ cmake . -DCMAKE_BUILD_TYPE=Release  # or -DCMAKE_BUILD_TYPE=Debug for more debug information
$ make microbench
```

To run the microbenchmark:

```shell
$ ./microbench
```

The [microbench](microbench.cpp) has several parameters you can pass, such as `read/write ratio`, `table size` and configurations of XIndex-R.

```shell
$ ./microbench --read 0.9 --insert 0.05 --remove 0.05 --table-size 100000
```

