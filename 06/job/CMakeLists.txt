cmake_minimum_required(VERSION 3.14)

add_executable(job_server
  main.cpp
  service.cpp
  push_handler.cpp
  persist_handler.cpp
)

target_link_libraries(job_server
  PRIVATE
    spark_push_common
    spark_push_proto
    grpc++
    grpc
)


