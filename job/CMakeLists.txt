cmake_minimum_required(VERSION 3.14)

# 生成 job_server 可执行程序，包含主入口与服务逻辑。
add_executable(job_server
    main.cpp
    service.cpp
    ../logic/message_dao.cpp
    ../logic/session_dao.cpp
)

target_include_directories(job_server PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../logic)

target_link_libraries(job_server
    PRIVATE
        spark_push_common
        spark_push_proto
        grpc++
        grpc
)
