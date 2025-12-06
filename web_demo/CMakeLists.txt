cmake_minimum_required(VERSION 3.14)

add_custom_target(web_demo ALL
  COMMAND ${CMAKE_COMMAND} -E copy_directory
          ${CMAKE_CURRENT_SOURCE_DIR}/static
          ${CMAKE_CURRENT_BINARY_DIR}/static
)

add_executable(web_demo_server
  web_server.cpp
)

target_link_libraries(web_demo_server
  PRIVATE
    muduo_net
    muduo_http
    muduo_base
    pthread
)

