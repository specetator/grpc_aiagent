find_package(Protobuf REQUIRED)
find_package(gRPC REQUIRED)
find_package(PkgConfig REQUIRED)
pkg_check_modules(GRPCXX REQUIRED IMPORTED_TARGET grpc++)
find_program(GRPC_CPP_PLUGIN_EXECUTABLE NAMES grpc_cpp_plugin REQUIRED)
set(GENERATED_DIR ${CMAKE_CURRENT_BINARY_DIR}/generated)
file(MAKE_DIRECTORY ${GENERATED_DIR})
set(PROTO_FILE ${CMAKE_CURRENT_SOURCE_DIR}/inference.proto)
set(GENERATED_FILES
  ${GENERATED_DIR}/inference.pb.cc ${GENERATED_DIR}/inference.pb.h
  ${GENERATED_DIR}/inference.grpc.pb.cc ${GENERATED_DIR}/inference.grpc.pb.h)
add_custom_command(OUTPUT ${GENERATED_FILES}
  COMMAND ${Protobuf_PROTOC_EXECUTABLE}
  ARGS --cpp_out=${GENERATED_DIR} --grpc_out=${GENERATED_DIR}
       --plugin=protoc-gen-grpc=${GRPC_CPP_PLUGIN_EXECUTABLE}
       -I ${CMAKE_CURRENT_SOURCE_DIR} ${PROTO_FILE}
  DEPENDS ${PROTO_FILE})
add_library(inference_proto ${GENERATED_DIR}/inference.pb.cc
  ${GENERATED_DIR}/inference.grpc.pb.cc)
target_include_directories(inference_proto PUBLIC ${GENERATED_DIR})
target_link_libraries(inference_proto PUBLIC protobuf::libprotobuf PkgConfig::GRPCXX)
