cmake_minimum_required(VERSION 3.14)
project(spark_push_auth_subset CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

include_directories(${CMAKE_CURRENT_SOURCE_DIR}/third_party)
include_directories(${CMAKE_CURRENT_SOURCE_DIR})

# 优先使用仓库内的 muduo（base/net）
add_subdirectory(muduo/base)
add_subdirectory(muduo/net)
add_subdirectory(common)
add_subdirectory(proto)
add_subdirectory(logic)
add_subdirectory(comet)
