if (POLICY CMP0048)
  cmake_policy(SET CMP0048 NEW)
endif ()

project(template)
cmake_minimum_required(VERSION 3.11)
include(FetchContent)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-long-long -pedantic -fPIC -march=native")
set(CMAKE_ENABLE_COMPILE_COMMANDS "ON")

if (${CMAKE_BUILD_TYPE} STREQUAL "Debug")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS}")
elseif (${CMAKE_BUILD_TYPE} STREQUAL "Release")
  message("-- Release mode, all optimizations enabled")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O3 -Ofast")
endif ()

# Google Test
FetchContent_Declare(
        googletest
        GIT_REPOSITORY https://github.com/google/googletest.git
        GIT_TAG release-1.8.1
)
FetchContent_GetProperties(googletest)
if (NOT googletest_POPULATED)
  FetchContent_Populate(googletest)
  add_subdirectory(${googletest_SOURCE_DIR} ${googletest_BINARY_DIR})
endif ()
include(GoogleTest)

# Google Log
FetchContent_Declare(
        glog
        GIT_REPOSITORY https://github.com/google/glog.git
        GIT_TAG v0.4.0
)
FetchContent_GetProperties(glog)
if (NOT glog_POPULATED)
  FetchContent_Populate(glog)
  set(WITH_GFLAGS OFF CACHE BOOL "we don't want gflags")
  add_subdirectory(${glog_SOURCE_DIR} ${glog_BINARY_DIR})
endif()

option(PMEM "support persistent memory" OFF)
if(${PMEM})
  set(PMDK_LIB_PATH "/usr/local/lib" CACHE STRING "PMDK lib install path")
  set(PMDK_HEADER_PATH "/usr/local/include" CACHE STRING "PMDK header install path")
  message("-- Build with persistent memory support, PMDK lib path:" ${PMDK_LIB_PATH} ", PMDK header path: " ${PMDK_HEADER_PATH})
  include_directories(${PMDK_HEADER_path})
  add_library(pmemobj SHARED IMPORTED)
  set_property(TARGET pmemobj PROPERTY IMPORTED_LOCATION ${PMDK_LIB_PATH}/libpmemobj.so)
  add_definitions(-DPMEM)
endif()

enable_testing()

add_definitions(-DTEST_BUILD)

if (NOT DEFINED CASCADE_LAKE)
  set(CASCADE_LAKE 1)
endif()
add_definitions(-DCASCADE_LAKE=${CASCADE_LAKE})
if(${CASCADE_LAKE})
  message("-- Cascade lake defined")
endif()

add_subdirectory(benchmark)
add_subdirectory(tests)

