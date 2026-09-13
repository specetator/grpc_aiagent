cmake_minimum_required(VERSION 3.14)

add_executable(hermes_bridge
  main.cpp
  config.cpp
  hermes_client.cpp
  sse_parser.cpp
)

target_link_libraries(hermes_bridge
  PRIVATE
    spark_push_common
    pthread
)
