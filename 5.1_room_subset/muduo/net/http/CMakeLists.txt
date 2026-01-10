set(http_SRCS
  HttpServer.cc
  HttpContext.cc
  )

add_library(muduo_http ${http_SRCS})
target_link_libraries(muduo_http muduo_net)

install(TARGETS muduo_http DESTINATION lib)
set(HEADERS
  HttpContext.h
  HttpRequest.h
  HttpResponse.h
  HttpServer.h
  )
install(FILES ${HEADERS} DESTINATION include/muduo/net/http)
 