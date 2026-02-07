cmake_minimum_required(VERSION 3.14)
project(spark_push LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# 让 #include <muduo/...> 优先命中本仓库下的 muduo 目录
include_directories(${CMAKE_CURRENT_SOURCE_DIR})

# 优先使用当前仓库内的 muduo 源码，而不是系统安装版本
# 这样 HttpRequest/HttpContext 等扩展（如 body() 支持、无 boost 依赖）才能生效
add_subdirectory(muduo/base)
add_subdirectory(muduo/net)

option(BUILD_WEB_DEMO "Build web demo (static files only)" ON)

# 默认使用 Debug
if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Debug)
endif()

add_subdirectory(proto)
add_subdirectory(common)
add_subdirectory(logic)
add_subdirectory(comet)
add_subdirectory(job)

if(BUILD_WEB_DEMO)
  add_subdirectory(web_demo)
endif()

# 压测工具
add_subdirectory(load_test)


