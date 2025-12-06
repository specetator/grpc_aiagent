cmake_minimum_required(VERSION 3.14)

# Fallback FindgRPC module for distributions未提供 gRPC CMake config 的情况

include(FindPackageHandleStandardArgs)

find_path(gRPC_INCLUDE_DIR
  NAMES grpc/grpc.h
  PATH_SUFFIXES include)

find_library(gRPC_GRPC_LIBRARY NAMES grpc)
find_library(gRPC_GRPCPP_LIBRARY NAMES grpc++)
find_library(gRPC_GPR_LIBRARY NAMES gpr)

find_package_handle_standard_args(gRPC
  REQUIRED_VARS gRPC_INCLUDE_DIR gRPC_GRPC_LIBRARY gRPC_GRPCPP_LIBRARY gRPC_GPR_LIBRARY
)

if(NOT TARGET gRPC::gpr)
  add_library(gRPC::gpr UNKNOWN IMPORTED)
  set_target_properties(gRPC::gpr PROPERTIES
    IMPORTED_LOCATION "${gRPC_GPR_LIBRARY}"
    INTERFACE_INCLUDE_DIRECTORIES "${gRPC_INCLUDE_DIR}"
  )
endif()

if(NOT TARGET gRPC::grpc)
  add_library(gRPC::grpc UNKNOWN IMPORTED)
  set_target_properties(gRPC::grpc PROPERTIES
    IMPORTED_LOCATION "${gRPC_GRPC_LIBRARY}"
    INTERFACE_INCLUDE_DIRECTORIES "${gRPC_INCLUDE_DIR}"
    INTERFACE_LINK_LIBRARIES gRPC::gpr
  )
endif()

if(NOT TARGET gRPC::grpc++)
  add_library(gRPC::grpc++ UNKNOWN IMPORTED)
  set_target_properties(gRPC::grpc++ PROPERTIES
    IMPORTED_LOCATION "${gRPC_GRPCPP_LIBRARY}"
    INTERFACE_INCLUDE_DIRECTORIES "${gRPC_INCLUDE_DIR}"
    INTERFACE_LINK_LIBRARIES gRPC::grpc
  )
endif()

set(gRPC_FOUND TRUE)
set(gRPC_INCLUDE_DIRS "${gRPC_INCLUDE_DIR}")
set(gRPC_LIBRARIES "${gRPC_GRPCPP_LIBRARY}" "${gRPC_GRPC_LIBRARY}" "${gRPC_GPR_LIBRARY}")

