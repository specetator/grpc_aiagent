cmake_minimum_required(VERSION 3.14)

find_package(Protobuf REQUIRED)
find_package(gRPC REQUIRED)
find_program(GRPC_CPP_PLUGIN_EXECUTABLE NAMES grpc_cpp_plugin)
if(NOT GRPC_CPP_PLUGIN_EXECUTABLE)
  message(FATAL_ERROR "grpc_cpp_plugin not found. Please install gRPC C++ plugin.")
endif()

set(PROTO_SRC
  spark_push.proto
)

set(GENERATED_DIR ${CMAKE_CURRENT_BINARY_DIR}/generated)
file(MAKE_DIRECTORY ${GENERATED_DIR})

set(GENERATED_SRCS "")
set(GENERATED_HDRS "")

foreach(proto ${PROTO_SRC})
  get_filename_component(fname ${proto} NAME_WE)
  set(cc_file ${GENERATED_DIR}/${fname}.pb.cc)
  set(h_file  ${GENERATED_DIR}/${fname}.pb.h)
  set(grpc_cc_file ${GENERATED_DIR}/${fname}.grpc.pb.cc)
  set(grpc_h_file  ${GENERATED_DIR}/${fname}.grpc.pb.h)

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

  list(APPEND GENERATED_SRCS ${cc_file} ${grpc_cc_file})
  list(APPEND GENERATED_HDRS ${h_file} ${grpc_h_file})
endforeach()

add_library(spark_push_proto ${GENERATED_SRCS})
target_include_directories(spark_push_proto PUBLIC ${GENERATED_DIR})
target_link_libraries(spark_push_proto PUBLIC protobuf::libprotobuf gRPC::grpc++ gRPC::grpc++)




