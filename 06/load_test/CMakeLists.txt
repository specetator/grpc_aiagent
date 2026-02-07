cmake_minimum_required(VERSION 3.14)

project(spark_push_load_test LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(Threads REQUIRED)

# 公共 WebSocket/HTTP 客户端库，供功能 CLI 与压测程序复用
add_library(ws_client_lib STATIC
    ws_client_lib.cpp
)
target_link_libraries(ws_client_lib
    PRIVATE Threads::Threads
)

# 交互式功能验证 CLI
add_executable(ws_client_cli
    ws_client_cli.cpp
)
target_link_libraries(ws_client_cli
    PRIVATE Threads::Threads
            ws_client_lib
)

# 压测客户端，包含 ws/http/biz 场景
add_executable(load_tester
    load_tester.cpp
)
target_link_libraries(load_tester
    PRIVATE Threads::Threads
            ws_client_lib
)

# 基于epoll的异步WebSocket压测工具
add_executable(async_ws_bench
    async_ws_bench.cpp
)
target_link_libraries(async_ws_bench
    PRIVATE Threads::Threads
)

# 基于epoll的HTTP消息推送压测工具
add_executable(http_bench
    http_bench.cpp
)
target_link_libraries(http_bench
    PRIVATE Threads::Threads
)


