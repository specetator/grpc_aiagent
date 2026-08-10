add_executable(security_test
  security_test.cpp
  ${PROJECT_SOURCE_DIR}/common/security.cpp
)
target_include_directories(security_test PRIVATE ${PROJECT_SOURCE_DIR}/common)
target_link_libraries(security_test PRIVATE OpenSSL::Crypto)
add_test(NAME security_test COMMAND security_test)

add_executable(config_test config_test.cpp)
target_link_libraries(config_test PRIVATE spark_push_common)
add_test(NAME config_test COMMAND config_test)

add_executable(rate_limiter_test rate_limiter_test.cpp)
target_link_libraries(rate_limiter_test PRIVATE spark_push_common)
add_test(NAME rate_limiter_test COMMAND rate_limiter_test)

add_executable(websocket_utils_test
  websocket_utils_test.cpp
  ${PROJECT_SOURCE_DIR}/comet/websocket_utils.cpp
)
target_include_directories(websocket_utils_test PRIVATE ${PROJECT_SOURCE_DIR}/comet)
add_test(NAME websocket_utils_test COMMAND websocket_utils_test)

add_executable(redis_sequence_integration_test
  redis_sequence_integration_test.cpp
  ${PROJECT_SOURCE_DIR}/logic/redis_store.cpp
)
target_include_directories(redis_sequence_integration_test PRIVATE
  ${PROJECT_SOURCE_DIR}/logic
)
target_link_libraries(redis_sequence_integration_test PRIVATE spark_push_common)
add_test(NAME redis_sequence_integration_test
         COMMAND redis_sequence_integration_test)
set_tests_properties(redis_sequence_integration_test PROPERTIES
  SKIP_RETURN_CODE 77
)
