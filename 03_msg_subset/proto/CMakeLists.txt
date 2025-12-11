# ============================================================================
# proto 目录构建脚本：负责将 .proto 文件编译为 C++ 代码
# 
# 主要功能：
# 1. 查找 protobuf 和 gRPC 工具链
# 2. 为每个 .proto 文件生成 .pb.cc/.pb.h 和 .grpc.pb.cc/.grpc.pb.h
# 3. 将生成的代码打包为静态库 spark_push_proto，供其他模块链接
# ============================================================================
cmake_minimum_required(VERSION 3.14)

# 查找 Protobuf 编译器和库
find_package(Protobuf REQUIRED)

# 查找 gRPC C++ 插件，用于生成 gRPC 服务代码
find_program(GRPC_CPP_PLUGIN_EXECUTABLE NAMES grpc_cpp_plugin)
if(NOT GRPC_CPP_PLUGIN_EXECUTABLE)
  message(FATAL_ERROR "grpc_cpp_plugin not found. Please install gRPC C++ plugin.")
endif()

# 优先用 pkg-config 查找 gRPC（常见于系统包安装），找不到再用 CMake config
find_package(PkgConfig QUIET)
if(PkgConfig_FOUND)
  pkg_check_modules(GRPC_PKG QUIET grpc++)
endif()
if(NOT GRPC_PKG_FOUND)
  find_package(gRPC CONFIG REQUIRED)
endif()

# 定义需要编译的 proto 文件列表
set(PROTO_SRC
  spark_push.proto
)

# 设置生成代码的输出目录
set(GENERATED_DIR ${CMAKE_CURRENT_BINARY_DIR}/generated)
file(MAKE_DIRECTORY ${GENERATED_DIR})

# 初始化生成文件列表
set(GENERATED_SRCS "")
set(GENERATED_HDRS "")

# 遍历每个 proto 文件，为其创建代码生成规则
foreach(proto ${PROTO_SRC})
  # 提取文件名（不含扩展名）
  get_filename_component(fname ${proto} NAME_WE)
  
  # 定义四个生成文件的路径：protobuf 消息定义 + gRPC 服务定义
  set(cc_file ${GENERATED_DIR}/${fname}.pb.cc)
  set(h_file  ${GENERATED_DIR}/${fname}.pb.h)
  set(grpc_cc_file ${GENERATED_DIR}/${fname}.grpc.pb.cc)
  set(grpc_h_file  ${GENERATED_DIR}/${fname}.grpc.pb.h)

  # 添加自定义命令：使用 protoc 编译器生成 C++ 代码
  # --grpc_out: 生成 gRPC 服务代码
  # --cpp_out: 生成 protobuf 消息代码
  # --plugin: 指定 gRPC C++ 插件路径
  add_custom_command(
    OUTPUT ${cc_file} ${h_file} ${grpc_cc_file} ${grpc_h_file}
    COMMAND ${Protobuf_PROTOC_EXECUTABLE}
    ARGS --grpc_out=${GENERATED_DIR}
         --cpp_out=${GENERATED_DIR}
         --plugin=protoc-gen-grpc=${GRPC_CPP_PLUGIN_EXECUTABLE}
         -I ${CMAKE_CURRENT_SOURCE_DIR}
         ${CMAKE_CURRENT_SOURCE_DIR}/${proto}
    DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/${proto}
  )

  # 将生成的源文件和头文件加入列表
  list(APPEND GENERATED_SRCS ${cc_file} ${grpc_cc_file})
  list(APPEND GENERATED_HDRS ${h_file} ${grpc_h_file})
endforeach()

# 创建静态库，包含所有生成的 protobuf 和 gRPC 代码
add_library(spark_push_proto ${GENERATED_SRCS})

# 将生成代码目录添加到公共头文件搜索路径，使其他模块可以 #include 生成的头文件
target_include_directories(spark_push_proto PUBLIC ${GENERATED_DIR})

# 链接 protobuf 和 gRPC 库
# 优先使用 pkg-config 查找结果（适用于系统安装的 gRPC）
# 否则使用 CMake config 方式（适用于从源码编译的 gRPC）
if(GRPC_PKG_FOUND)
  target_include_directories(spark_push_proto PUBLIC ${GRPC_PKG_INCLUDE_DIRS})
  target_link_libraries(spark_push_proto PUBLIC protobuf::libprotobuf ${GRPC_PKG_LIBRARIES})
else()
  target_link_libraries(spark_push_proto PUBLIC protobuf::libprotobuf gRPC::grpc++ gRPC::grpc)
endif()




