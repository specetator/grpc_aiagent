cmake_minimum_required(VERSION 3.14)

# Fallback FindgRPC module for distributions未提供 gRPC CMake config 的情况
# 使用 pkg-config 获取完整的依赖信息

include(FindPackageHandleStandardArgs)

# 尝试使用 pkg-config 获取 gRPC 配置
find_package(PkgConfig QUIET)
if(PkgConfig_FOUND)
  pkg_check_modules(GRPCPP grpc++)
  pkg_check_modules(GRPC grpc)
endif()

find_path(gRPC_INCLUDE_DIR
  NAMES grpc/grpc.h
  PATH_SUFFIXES include)

find_library(gRPC_GRPC_LIBRARY NAMES grpc)
find_library(gRPC_GRPCPP_LIBRARY NAMES grpc++)
find_library(gRPC_GPR_LIBRARY NAMES gpr)

find_package_handle_standard_args(gRPC
  REQUIRED_VARS gRPC_INCLUDE_DIR gRPC_GRPC_LIBRARY gRPC_GRPCPP_LIBRARY gRPC_GPR_LIBRARY
)

# 如果 pkg-config 找到了 grpc++，使用它的链接标志
if(GRPCPP_FOUND)
  set(GRPC_ALL_LIBS ${GRPCPP_LIBRARIES})
  set(GRPC_LIBRARY_DIRS ${GRPCPP_LIBRARY_DIRS})
else()
  # 手动列出 gRPC 静态库所需的所有依赖
  set(GRPC_ALL_LIBS
    grpc++ grpc address_sorting re2 upb cares z gpr ssl crypto
    absl_raw_hash_set absl_hashtablez_sampler absl_hash absl_city absl_low_level_hash
    absl_random_distributions absl_random_seed_sequences absl_random_internal_pool_urbg
    absl_random_internal_randen absl_random_internal_randen_hwaes absl_random_internal_randen_hwaes_impl
    absl_random_internal_randen_slow absl_random_internal_platform absl_random_internal_seed_material
    absl_random_seed_gen_exception absl_statusor absl_status absl_cord absl_cordz_info
    absl_cord_internal absl_cordz_functions absl_exponential_biased absl_cordz_handle
    absl_bad_optional_access absl_str_format_internal absl_synchronization absl_graphcycles_internal
    absl_stacktrace absl_symbolize absl_debugging_internal absl_demangle_internal absl_malloc_internal
    absl_time absl_civil_time absl_strings absl_strings_internal rt absl_base absl_spinlock_wait
    absl_int128 absl_throw_delegate absl_time_zone absl_bad_variant_access absl_raw_logging_internal
    absl_log_severity
  )
  set(GRPC_LIBRARY_DIRS /usr/local/lib)
endif()

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
  add_library(gRPC::grpc++ INTERFACE IMPORTED)
  set_target_properties(gRPC::grpc++ PROPERTIES
    INTERFACE_INCLUDE_DIRECTORIES "${gRPC_INCLUDE_DIR}"
    INTERFACE_LINK_DIRECTORIES "${GRPC_LIBRARY_DIRS}"
    INTERFACE_LINK_LIBRARIES "${GRPC_ALL_LIBS}"
  )
endif()

set(gRPC_FOUND TRUE)
set(gRPC_INCLUDE_DIRS "${gRPC_INCLUDE_DIR}")
set(gRPC_LIBRARIES "${gRPC_GRPCPP_LIBRARY}" "${gRPC_GRPC_LIBRARY}" "${gRPC_GPR_LIBRARY}")
