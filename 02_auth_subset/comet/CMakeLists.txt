cmake_minimum_required(VERSION 3.14)

# comet_server 可执行文件，包含 WebSocket + gRPC 服务端实现。
# 仅列出与 comet 进程直接相关的源文件，公共依赖通过 target_link_libraries 引入。
add_executable(comet_server
    main.cpp
    app.cpp
    comet_server.cpp
    websocket_utils.cpp
)

target_link_libraries(comet_server
    PRIVATE
        # 业务公共组件（配置、日志等）
        spark_push_common
        # gRPC/Protobuf 生成的协议依赖
        spark_push_proto
        # 网络库 muduo
        muduo_net
        muduo_base
        # gRPC 运行时依赖
        grpc++
        grpc
        # pthread 线程库
        pthread
)
