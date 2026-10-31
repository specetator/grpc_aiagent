add_executable(security_test
  security_test.cpp
  ${PROJECT_SOURCE_DIR}/common/security.cpp
)
target_include_directories(security_test PRIVATE ${PROJECT_SOURCE_DIR}/common)
target_link_libraries(security_test PRIVATE OpenSSL::Crypto)
add_test(NAME security_test COMMAND security_test)
add_executable(broadcast_fanout_test broadcast_fanout_test.cpp)
target_link_libraries(broadcast_fanout_test PRIVATE spark_push_proto spark_push_common)
add_test(NAME broadcast_fanout_test COMMAND broadcast_fanout_test)
add_executable(queue_capacity_test queue_capacity_test.cpp)
target_link_libraries(queue_capacity_test PRIVATE spark_push_common)
add_test(NAME queue_capacity_test COMMAND queue_capacity_test)
add_executable(message_acceptance_test message_acceptance_test.cpp)
target_link_libraries(message_acceptance_test PRIVATE spark_push_logic)
add_test(NAME message_acceptance_test COMMAND message_acceptance_test)
add_executable(logging_rotation_test logging_rotation_test.cpp)
target_link_libraries(logging_rotation_test PRIVATE spark_push_common)
add_test(NAME logging_rotation_test COMMAND logging_rotation_test)
add_executable(route_generation_integration_test route_generation_integration_test.cpp
  ${PROJECT_SOURCE_DIR}/logic/redis_store.cpp)
target_include_directories(route_generation_integration_test PRIVATE ${PROJECT_SOURCE_DIR}/logic)
target_link_libraries(route_generation_integration_test PRIVATE spark_push_common)
add_test(NAME route_generation_integration_test COMMAND route_generation_integration_test)
set_tests_properties(route_generation_integration_test PROPERTIES SKIP_RETURN_CODE 77 TIMEOUT 15)
set_tests_properties(queue_capacity_test PROPERTIES TIMEOUT 5)

add_executable(http_parser_test http_parser_test.cpp)
target_link_libraries(http_parser_test PRIVATE muduo_http muduo_net muduo_base)
add_test(NAME http_parser_test COMMAND http_parser_test)
set_tests_properties(http_parser_test PROPERTIES TIMEOUT 5)
add_executable(internal_rpc_test internal_rpc_test.cpp ${PROJECT_SOURCE_DIR}/logic/grpc_service.cpp)
target_link_libraries(internal_rpc_test PRIVATE spark_push_logic)
add_test(NAME internal_rpc_test COMMAND internal_rpc_test)
set_tests_properties(internal_rpc_test PROPERTIES TIMEOUT 10)
add_executable(mysql_pool_recovery_test mysql_pool_recovery_test.cpp)
target_link_libraries(mysql_pool_recovery_test PRIVATE spark_push_common)
target_link_options(mysql_pool_recovery_test PRIVATE -Wl,--wrap=mysql_real_connect -Wl,--wrap=mysql_ping)
add_test(NAME mysql_pool_recovery_test COMMAND mysql_pool_recovery_test)
set_tests_properties(mysql_pool_recovery_test PROPERTIES TIMEOUT 5)
add_executable(message_reservation_integration_test message_reservation_integration_test.cpp)
target_link_libraries(message_reservation_integration_test PRIVATE spark_push_logic)
add_test(NAME message_reservation_integration_test COMMAND message_reservation_integration_test)
set_tests_properties(message_reservation_integration_test PROPERTIES SKIP_RETURN_CODE 77 TIMEOUT 45)

add_executable(logic_business_service_test logic_business_service_test.cpp)
target_link_libraries(logic_business_service_test PRIVATE spark_push_logic)
add_test(NAME logic_business_service_test COMMAND logic_business_service_test)
add_executable(room_agent_service_test room_agent_service_test.cpp)
target_link_libraries(room_agent_service_test PRIVATE spark_push_logic)
add_test(NAME room_agent_service_test COMMAND room_agent_service_test)

add_executable(config_test config_test.cpp)
target_link_libraries(config_test PRIVATE spark_push_common)
add_test(NAME config_test COMMAND config_test)

add_executable(image_attachment_test image_attachment_test.cpp)
target_link_libraries(image_attachment_test PRIVATE spark_push_common)
add_test(NAME image_attachment_test COMMAND image_attachment_test)

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
add_executable(kafka_parallel_integration_test kafka_parallel_integration_test.cpp)
target_link_libraries(kafka_parallel_integration_test PRIVATE spark_push_common)
add_test(NAME kafka_parallel_integration_test COMMAND kafka_parallel_integration_test)
set_tests_properties(kafka_parallel_integration_test PROPERTIES TIMEOUT 60)
add_executable(kafka_native_control_integration_test kafka_native_control_integration_test.cpp)
target_link_libraries(kafka_native_control_integration_test PRIVATE spark_push_common)
add_test(NAME kafka_native_control_integration_test COMMAND kafka_native_control_integration_test)
set_tests_properties(kafka_native_control_integration_test PROPERTIES TIMEOUT 60)
add_executable(hermes_native_config_test hermes_native_config_test.cpp
  ${PROJECT_SOURCE_DIR}/hermes_bridge/config.cpp)
add_test(NAME hermes_native_config_test COMMAND hermes_native_config_test)

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
add_test(NAME dlq_replay_test COMMAND ${Python3_EXECUTABLE} ${PROJECT_SOURCE_DIR}/tests/dlq_replay_test.py $<TARGET_FILE:dlq_replay>)
add_executable(comet_network_probe comet_network_probe.cpp
  ${PROJECT_SOURCE_DIR}/comet/app.cpp ${PROJECT_SOURCE_DIR}/comet/comet_server.cpp
  ${PROJECT_SOURCE_DIR}/comet/comet_grpc_service.cpp ${PROJECT_SOURCE_DIR}/comet/websocket_utils.cpp)
target_include_directories(comet_network_probe PRIVATE ${PROJECT_SOURCE_DIR}/comet)
target_link_libraries(comet_network_probe PRIVATE spark_push_proto spark_push_common muduo_net muduo_base)
add_test(NAME comet_network_integration_test COMMAND ${Python3_EXECUTABLE}
  ${PROJECT_SOURCE_DIR}/tests/comet_network_integration_test.py $<TARGET_FILE:comet_network_probe>)
set_tests_properties(comet_network_integration_test PROPERTIES TIMEOUT 25)
add_test(NAME comet_stream_timeout_test COMMAND ${Python3_EXECUTABLE}
  ${PROJECT_SOURCE_DIR}/tests/comet_network_integration_test.py $<TARGET_FILE:comet_network_probe> --stream)
set_tests_properties(comet_stream_timeout_test PROPERTIES TIMEOUT 25)
add_executable(http_api_probe http_api_probe.cpp ${PROJECT_SOURCE_DIR}/logic/http_server.cpp)
target_link_libraries(http_api_probe PRIVATE spark_push_logic muduo_http muduo_net muduo_base)
add_test(NAME http_api_integration_test COMMAND ${Python3_EXECUTABLE}
  ${PROJECT_SOURCE_DIR}/tests/http_api_integration_test.py $<TARGET_FILE:http_api_probe>)
set_tests_properties(http_api_integration_test PROPERTIES TIMEOUT 20)
add_test(NAME user_agent_http_integration_test COMMAND ${Python3_EXECUTABLE}
  ${PROJECT_SOURCE_DIR}/tests/user_agent_http_integration_test.py $<TARGET_FILE:http_api_probe>)
set_tests_properties(user_agent_http_integration_test PROPERTIES
  ENVIRONMENT "PYTHONDONTWRITEBYTECODE=1" TIMEOUT 30)
add_test(NAME room_agent_test
  COMMAND ${Python3_EXECUTABLE} ${PROJECT_SOURCE_DIR}/cannbot/scripts/test_room_agent.py)
set_tests_properties(room_agent_test PROPERTIES ENVIRONMENT "PYTHONDONTWRITEBYTECODE=1" TIMEOUT 45)
find_program(SPARK_NODE_EXECUTABLE node)
if(SPARK_NODE_EXECUTABLE)
  add_test(NAME deployment_endpoints_test
    COMMAND ${SPARK_NODE_EXECUTABLE} ${PROJECT_SOURCE_DIR}/tests/deployment_endpoints_test.cjs)
  add_test(NAME kernel_subagents_test
    COMMAND ${SPARK_NODE_EXECUTABLE} ${PROJECT_SOURCE_DIR}/tests/kernel_subagents_test.cjs)
  add_test(NAME kernel_subagents_integration_test
    COMMAND ${SPARK_NODE_EXECUTABLE} ${PROJECT_SOURCE_DIR}/tests/kernel_subagents_integration_test.cjs)
  set_tests_properties(kernel_subagents_test kernel_subagents_integration_test PROPERTIES
    TIMEOUT 60 SKIP_RETURN_CODE 77)
  add_test(NAME cursor_recovery_test COMMAND ${SPARK_NODE_EXECUTABLE}
    ${PROJECT_SOURCE_DIR}/tests/cursor_recovery_test.cjs)
  add_test(NAME room_agent_channel_test
    COMMAND ${SPARK_NODE_EXECUTABLE} ${PROJECT_SOURCE_DIR}/tests/room_agent_channel_test.cjs)
endif()
add_test(NAME logic_layer_contract_test
  COMMAND ${Python3_EXECUTABLE} ${PROJECT_SOURCE_DIR}/tests/logic_layer_contract_test.py)
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
add_test(NAME user_agent_connections_test
  COMMAND ${Python3_EXECUTABLE} ${PROJECT_SOURCE_DIR}/cannbot/scripts/test_user_agent_connections.py)
set_tests_properties(user_agent_connections_test PROPERTIES ENVIRONMENT "PYTHONDONTWRITEBYTECODE=1" TIMEOUT 30)
add_executable(agent_connection_service_test agent_connection_service_test.cpp)
target_link_libraries(agent_connection_service_test PRIVATE spark_push_logic)
add_test(NAME agent_connection_service_test COMMAND agent_connection_service_test)
set_tests_properties(agent_router_test PROPERTIES ENVIRONMENT "PYTHONDONTWRITEBYTECODE=1" TIMEOUT 30)
add_test(NAME hermes_native_test
  COMMAND ${Python3_EXECUTABLE} ${PROJECT_SOURCE_DIR}/cannbot/scripts/test_hermes_native.py)
set_tests_properties(hermes_native_test PROPERTIES ENVIRONMENT "PYTHONDONTWRITEBYTECODE=1" TIMEOUT 30)
add_test(NAME hermes_native_gateway_test
  COMMAND ${Python3_EXECUTABLE} ${PROJECT_SOURCE_DIR}/cannbot/scripts/test_hermes_native_gateway.py)
set_tests_properties(hermes_native_gateway_test PROPERTIES ENVIRONMENT "PYTHONDONTWRITEBYTECODE=1" TIMEOUT 30)
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

add_executable(space_service_test space_service_test.cpp)
target_link_libraries(space_service_test PRIVATE spark_push_logic)
add_test(NAME space_service_test COMMAND space_service_test)
add_executable(space_mysql_integration_test space_mysql_integration_test.cpp)
target_link_libraries(space_mysql_integration_test PRIVATE spark_push_logic)
add_test(NAME space_mysql_integration_test COMMAND space_mysql_integration_test)
set_tests_properties(space_mysql_integration_test PROPERTIES SKIP_RETURN_CODE 77 TIMEOUT 45)

add_test(NAME generic_room_agent_test COMMAND ${Python3_EXECUTABLE} ${PROJECT_SOURCE_DIR}/cannbot/scripts/test_generic_room_agent.py)
set_tests_properties(generic_room_agent_test PROPERTIES ENVIRONMENT "PYTHONDONTWRITEBYTECODE=1" TIMEOUT 45)

add_test(NAME room_project_snapshot_test COMMAND ${Python3_EXECUTABLE} ${PROJECT_SOURCE_DIR}/cannbot/scripts/test_room_workspace.py)
add_test(NAME hermes_public_workspace_contract_test COMMAND ${Python3_EXECUTABLE} ${PROJECT_SOURCE_DIR}/cannbot/scripts/test_hermes_public_workspace.py)
set_tests_properties(room_project_snapshot_test PROPERTIES ENVIRONMENT "PYTHONDONTWRITEBYTECODE=1" TIMEOUT 30)
