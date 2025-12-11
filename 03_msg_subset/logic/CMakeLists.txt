cmake_minimum_required(VERSION 3.14)

add_executable(logic_server
  main.cpp
  grpc_service.cpp
  http_server.cpp
  user_dao.cpp
  conversation_store.cpp
  session_dao.cpp
  message_dao.cpp
  redis_store.cpp
  user_session_state_dao.cpp
  model.cpp
)

target_link_libraries(logic_server
  PRIVATE
    spark_push_common
    spark_push_proto
    muduo_net
    muduo_http
    muduo_base
    pthread
)
