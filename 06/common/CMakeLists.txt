cmake_minimum_required(VERSION 3.14)

add_library(spark_push_common
  config.cpp
  logging.cpp
  mysql_pool.cpp
  redis_pool.cpp
  kafka_producer.cpp
  kafka_consumer.cpp
  thread_pool.cpp
)

target_include_directories(spark_push_common
  PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}
)

find_package(Threads REQUIRED)

target_link_libraries(spark_push_common
  PUBLIC
    Threads::Threads
    mysqlclient
    hiredis
    muduo_base
    rdkafka++
    rdkafka
)


