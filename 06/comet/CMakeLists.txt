cmake_minimum_required(VERSION 3.14)

add_executable(comet_server
  main.cpp
  app.cpp
  comet_server.cpp
  comet_grpc_service.cpp
  websocket_utils.cpp
)

find_package(nlohmann_json 3 REQUIRED)

target_link_libraries(comet_server
  PRIVATE
    spark_push_common
    spark_push_proto
    muduo_net
    muduo_base
    grpc++
    grpc
    pthread
    nlohmann_json::nlohmann_json
)
