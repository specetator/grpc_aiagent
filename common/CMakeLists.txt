cmake_minimum_required(VERSION 3.14)

# 公共组件库，封装配置、日志、连接池等通用能力
add_library(spark_push_common
    config.cpp
    logging.cpp
    mysql_pool.cpp
    redis_pool.cpp
    kafka_producer.cpp
    kafka_consumer.cpp
    thread_pool.cpp
    security.cpp
    metrics.cpp
    metrics_http_server.cpp
    rate_limiter.cpp
)

target_include_directories(spark_push_common
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}
)

find_package(Threads REQUIRED)
find_package(OpenSSL REQUIRED)

target_link_libraries(spark_push_common
    PUBLIC
        Threads::Threads
        mysqlclient
        hiredis
        muduo_base
        rdkafka++
        rdkafka
        OpenSSL::Crypto
)
