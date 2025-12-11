# ============================================================================
# comet 服务构建脚本
# 
# 编译 comet_server 可执行文件，包含：
# - main.cpp: 程序入口，初始化并启动服务
# - app.cpp: comet 服务主逻辑封装
# - comet_server.cpp: WebSocket 服务器实现（握手、帧解析、鉴权）
# - websocket_utils.cpp: WebSocket 帧构造工具
# ============================================================================
cmake_minimum_required(VERSION 3.14)

# 定义可执行文件及其源文件
# 仅列出 comet 特有的源文件，公共模块通过链接库引入
add_executable(comet_server
    main.cpp
    app.cpp
    comet_server.cpp
    comet_grpc_service.cpp
    websocket_utils.cpp
)

# 链接依赖库
target_link_libraries(comet_server
    PRIVATE
        spark_push_common  # 公共组件（配置、日志等）
        spark_push_proto   # protobuf/gRPC 生成代码
        muduo_net          # muduo 网络库（TCP 服务器）
        muduo_base         # muduo 基础库（日志、线程等）
        grpc++             # gRPC C++ 运行时
        grpc               # gRPC 核心库
        pthread            # POSIX 线程库
)
