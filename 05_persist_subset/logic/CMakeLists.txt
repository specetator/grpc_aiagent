# ============================================================================
# logic 服务构建脚本
# 
# 编译 logic_server 可执行文件，包含：
# - main.cpp: 程序入口，初始化各模块并启动服务
# - grpc_service.cpp: gRPC 服务实现（token 验证）
# - http_server.cpp: HTTP 服务实现（注册/登录）
# - user_dao.cpp: 用户数据访问层（MySQL 操作）
# - redis_store.cpp: Redis 存储封装（token 和路由）
# ============================================================================
cmake_minimum_required(VERSION 3.14)

# 定义可执行文件及其源文件
add_executable(logic_server
  main.cpp
  grpc_service.cpp
  http_server.cpp
  user_dao.cpp
  room_dao.cpp
  room_store.cpp
  redis_store.cpp
  message_dao.cpp
 )

# 链接依赖库
target_link_libraries(logic_server
  PRIVATE
    spark_push_common      # 公共组件（配置、日志、连接池）
    spark_push_proto       # protobuf/gRPC 生成代码
    muduo_net              # muduo 网络库（TCP 服务器）
    muduo_http             # muduo HTTP 服务器
    muduo_base             # muduo 基础库（日志、线程等）
    pthread                # POSIX 线程库
)
