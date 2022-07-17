if(${PMEM})
    add_executable(garbage_pm_test garbage_list_pm_test.cpp)
    target_link_libraries(garbage_pm_test gtest_main glog::glog pthread pmemobj)
    gtest_add_tests(TARGET garbage_pm_test)
else()
    add_executable(garbage_test garbage_list_test.cpp)
    target_link_libraries(garbage_test gtest_main glog::glog pthread)
    gtest_add_tests(TARGET garbage_test)
endif()

add_executable(pm_allocator pm_allocator_test.cpp)
target_link_libraries(pm_allocator gtest_main glog::glog pthread pmemobj)
gtest_add_tests(TARGET pm_allocator)

add_executable(pcas_test pcas_test.cpp)
target_link_libraries(pcas_test gtest_main glog::glog pthread)
gtest_add_tests(TARGET pcas_test)

add_executable(smoke_test smoke_test.cpp)
target_link_libraries(smoke_test gtest_main glog::glog)
gtest_add_tests(TARGET smoke_test)

add_executable(epoch_test epoch_test.cpp)
target_link_libraries(epoch_test gtest_main glog::glog pthread)
gtest_add_tests(TARGET epoch_test)

