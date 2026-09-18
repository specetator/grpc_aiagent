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

add_executable(hermes_command_test
  hermes_command_test.cpp
  ${PROJECT_SOURCE_DIR}/logic/hermes_command.cpp
)
target_include_directories(hermes_command_test PRIVATE
  ${PROJECT_SOURCE_DIR}/logic
)
add_test(NAME hermes_command_test COMMAND hermes_command_test)

add_executable(hermes_reply_test
  hermes_reply_test.cpp
  ${PROJECT_SOURCE_DIR}/logic/hermes_reply.cpp
  ${PROJECT_SOURCE_DIR}/logic/citation_validation.cpp
)
target_include_directories(hermes_reply_test PRIVATE ${PROJECT_SOURCE_DIR}/logic ${PROJECT_SOURCE_DIR}/common)
add_test(NAME hermes_reply_test COMMAND hermes_reply_test)

add_executable(kafka_failure_integration_test kafka_failure_integration_test.cpp)
target_link_libraries(kafka_failure_integration_test PRIVATE spark_push_common)
add_test(NAME kafka_failure_integration_test COMMAND kafka_failure_integration_test)
set_tests_properties(kafka_failure_integration_test PROPERTIES TIMEOUT 75)

add_executable(hermes_sse_parser_test
  hermes_sse_parser_test.cpp
  ${PROJECT_SOURCE_DIR}/hermes_bridge/sse_parser.cpp
)
target_include_directories(hermes_sse_parser_test PRIVATE
  ${PROJECT_SOURCE_DIR}/hermes_bridge
  ${PROJECT_SOURCE_DIR}/common
)
add_test(NAME hermes_sse_parser_test COMMAND hermes_sse_parser_test)

find_package(Python3 3.10 COMPONENTS Interpreter REQUIRED)
add_executable(agent_control_client_probe
  agent_control_client_probe.cpp
  ${PROJECT_SOURCE_DIR}/hermes_bridge/hermes_client.cpp
  ${PROJECT_SOURCE_DIR}/hermes_bridge/sse_parser.cpp
  ${PROJECT_SOURCE_DIR}/logic/hermes_reply.cpp
  ${PROJECT_SOURCE_DIR}/logic/citation_validation.cpp
)
target_include_directories(agent_control_client_probe PRIVATE
  ${PROJECT_SOURCE_DIR}/hermes_bridge ${PROJECT_SOURCE_DIR}/logic)
target_link_libraries(agent_control_client_probe PRIVATE spark_push_common)
add_test(NAME agent_control_integration_test
  COMMAND ${Python3_EXECUTABLE} ${PROJECT_SOURCE_DIR}/tests/agent_control_integration_test.py
          $<TARGET_FILE:agent_control_client_probe>)
set_tests_properties(agent_control_integration_test PROPERTIES
  ENVIRONMENT "PYTHONDONTWRITEBYTECODE=1" TIMEOUT 40)
add_test(NAME pi_gateway_test
  COMMAND ${Python3_EXECUTABLE} ${PROJECT_SOURCE_DIR}/cannbot/scripts/test_pi_gateway.py
)
add_test(NAME agent_router_test
  COMMAND ${Python3_EXECUTABLE} ${PROJECT_SOURCE_DIR}/cannbot/scripts/test_agent_router.py)
set_tests_properties(agent_router_test PROPERTIES ENVIRONMENT "PYTHONDONTWRITEBYTECODE=1" TIMEOUT 30)
set_tests_properties(pi_gateway_test PROPERTIES
  ENVIRONMENT "PYTHONDONTWRITEBYTECODE=1"
  TIMEOUT 45
)
add_test(NAME client_reliability_contract_test
  COMMAND ${Python3_EXECUTABLE} ${PROJECT_SOURCE_DIR}/tests/client_reliability_contract_test.py)
set_tests_properties(client_reliability_contract_test PROPERTIES
  ENVIRONMENT "PYTHONDONTWRITEBYTECODE=1"
  TIMEOUT 20
)

add_executable(citation_validation_test
  citation_validation_test.cpp
  ${PROJECT_SOURCE_DIR}/logic/citation_validation.cpp
)
target_include_directories(citation_validation_test PRIVATE
  ${PROJECT_SOURCE_DIR}/logic
)
add_test(NAME citation_validation_test COMMAND citation_validation_test)

add_executable(knowledge_catalog_test
  knowledge_catalog_test.cpp
  ${PROJECT_SOURCE_DIR}/logic/knowledge_catalog.cpp
)
target_include_directories(knowledge_catalog_test PRIVATE
  ${PROJECT_SOURCE_DIR}/logic
)
add_test(NAME knowledge_catalog_test COMMAND knowledge_catalog_test)

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
