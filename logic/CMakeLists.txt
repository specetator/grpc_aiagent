cmake_minimum_required(VERSION 3.14)

add_library(spark_push_logic STATIC
  hermes_command.cpp
  hermes_reply.cpp
  citation_validation.cpp
  knowledge_catalog.cpp
  audit_log_dao.cpp
  user_dao.cpp
  redis_store.cpp
  danmaku_dao.cpp
  group_dao.cpp
  session_dao.cpp
  message_dao.cpp
  message_reservation.cpp
  user_session_state_dao.cpp
  conversation_store.cpp
  model.cpp
  attachment_dao.cpp
  repositories/dao_repositories.cpp
  repositories/space_repository.cpp
  repositories/room_agent_repository.cpp
  services/room_agent_service.cpp
  services/room_agent_catalog.cpp
  services/room_workspace_service.cpp
  services/room_task_service.cpp
  services/user_service.cpp
  services/conversation_service.cpp
  services/attachment_service.cpp
  services/message_service.cpp
  services/admin_service.cpp
  services/danmaku_service.cpp
  services/knowledge_service.cpp
  services/room_service.cpp
  services/agent_connection_service.cpp
)

find_package(PkgConfig REQUIRED)
pkg_check_modules(AGENT_CURL REQUIRED IMPORTED_TARGET libcurl)

target_include_directories(spark_push_logic PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(spark_push_logic PUBLIC spark_push_common spark_push_proto PkgConfig::AGENT_CURL)

add_executable(logic_server main.cpp grpc_service.cpp http_server.cpp)

target_link_libraries(logic_server
  PRIVATE
    spark_push_logic
    muduo_net
    muduo_http
    muduo_base
    pthread
)
