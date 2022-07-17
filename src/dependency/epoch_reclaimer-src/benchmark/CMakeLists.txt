add_executable(pcas_bench pcas_bench.cpp)
target_link_libraries(pcas_bench gtest_main glog::glog pthread pmemobj)
gtest_add_tests(TARGET pcas_bench)
