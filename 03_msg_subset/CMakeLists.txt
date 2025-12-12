# ============================================================================
# spark_push 认证子集项目构建脚本
# 
# 本项目是 spark_push 即时通讯系统的最小可运行版本，包含：
# - logic: 业务逻辑服务（用户注册/登录、token 验证）
# - comet: 长连接服务（WebSocket 接入、握手鉴权）
# - proto: gRPC 协议定义
# - common: 公共基础组件（配置、日志、MySQL/Redis 连接池）
# ============================================================================
cmake_minimum_required(VERSION 3.14)
project(spark_push_auth_subset CXX)

# 使用 C++17 标准
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# 设置为 Debug 模式
set(CMAKE_BUILD_TYPE "Debug")

# C++ 调试编译选项
set(CMAKE_CXX_FLAGS_DEBUG "$ENV{CXXFLAGS} -O0 -Wall -g -ggdb")

# C 调试编译选项（如有需要）
set(CMAKE_C_FLAGS_DEBUG "$ENV{CFLAGS} -O0 -Wall -g -ggdb")

# 添加第三方库和当前目录到头文件搜索路径
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/third_party)
include_directories(${CMAKE_CURRENT_SOURCE_DIR})

# 按依赖顺序添加子目录
# 1. 基础库：muduo 网络库
add_subdirectory(muduo/base)
add_subdirectory(muduo/net)

# 2. 公共组件：配置、日志、数据库连接池等
add_subdirectory(common)

# 3. 协议定义：编译 .proto 生成 C++ 代码
add_subdirectory(proto)

# 4. 业务模块：logic / comet / job
add_subdirectory(logic)
add_subdirectory(comet)
add_subdirectory(job)
