#pragma once

#include <muduo/net/EventLoop.h>
#include <muduo/net/http/HttpRequest.h>
#include <muduo/net/http/HttpResponse.h>
#include <muduo/net/http/HttpServer.h>

#include "redis_store.h"
#include "user_dao.h"

namespace sparkpush {

class HttpApiServer {
   public:
    // 构造：仅依赖用户 DAO 与 Redis，用于认证相关接口
    HttpApiServer(muduo::net::EventLoop* loop,
                  const muduo::net::InetAddress& listenAddr, UserDao* user_dao,
                  RedisStore* redis_store);

    void start();

   private:
    void onRequest(const muduo::net::HttpRequest& req,
                   muduo::net::HttpResponse* resp);

    void handleLogin(const muduo::net::HttpRequest& req,
                     muduo::net::HttpResponse* resp);
    void handleRegister(const muduo::net::HttpRequest& req,
                        muduo::net::HttpResponse* resp);

    UserDao* user_dao_;
    RedisStore* redis_store_;
    muduo::net::HttpServer server_;
};

}  // namespace sparkpush
