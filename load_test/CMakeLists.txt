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

# 双账号端到端验证：发送方 ACK + Kafka/Job/Comet 接收方送达 + 延迟分位数
add_executable(e2e_bench e2e_bench.cpp)
target_link_libraries(e2e_bench PRIVATE Threads::Threads ws_client_lib)

add_executable(connection_bench connection_bench.cpp)
target_link_libraries(connection_bench PRIVATE Threads::Threads ws_client_lib)

add_executable(room_fanout_bench room_fanout_bench.cpp)
target_link_libraries(room_fanout_bench PRIVATE Threads::Threads ws_client_lib)

add_executable(storage_bench
  storage_bench.cpp
  ../logic/redis_store.cpp
  ../logic/message_dao.cpp
)
target_include_directories(storage_bench PRIVATE ../logic)
target_link_libraries(storage_bench PRIVATE Threads::Threads spark_push_common)

# 绕过 Logic，隔离测量 Kafka -> Job -> Comet -> WebSocket 链路。
add_executable(kafka_delivery_bench kafka_delivery_bench.cpp)
target_link_libraries(kafka_delivery_bench
  PRIVATE Threads::Threads ws_client_lib spark_push_common spark_push_proto
)

# 带 Bearer 鉴权的历史查询延迟/吞吐压测。
add_executable(history_bench history_bench.cpp)
target_link_libraries(history_bench PRIVATE Threads::Threads ws_client_lib)
