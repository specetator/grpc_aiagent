cmake_minimum_required(VERSION 3.14)

# 将 static 目录复制到构建目录，方便直接运行 web_server 访问页面资源
add_custom_target(web_demo_04_room_subset ALL
  COMMAND ${CMAKE_COMMAND} -E copy_directory
          ${CMAKE_CURRENT_SOURCE_DIR}/static
          ${CMAKE_CURRENT_BINARY_DIR}/static
)

add_executable(web_demo_server_04_room_subset
  web_server.cpp
)

target_link_libraries(web_demo_server_04_room_subset
  PRIVATE
    muduo_net
    muduo_http
    muduo_base
    pthread
)


