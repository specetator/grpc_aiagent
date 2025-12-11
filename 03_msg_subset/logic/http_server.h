#pragma once

#include <muduo/net/EventLoop.h>
#include <muduo/net/http/HttpRequest.h>
#include <muduo/net/http/HttpResponse.h>
#include <muduo/net/http/HttpServer.h>

#include "conversation_store.h"
#include "redis_store.h"
#include "user_dao.h"

namespace sparkpush {

class HttpApiServer {
   public:
    HttpApiServer(muduo::net::EventLoop* loop,
                  const muduo::net::InetAddress& listenAddr,
                  UserDao* user_dao, RedisStore* redis_store,
                  ConversationStore* store);

    void start();

   private:
    void onRequest(const muduo::net::HttpRequest& req,
                   muduo::net::HttpResponse* resp);

    void handleLogin(const muduo::net::HttpRequest& req,
                     muduo::net::HttpResponse* resp);
    void handleRegister(const muduo::net::HttpRequest& req,
                        muduo::net::HttpResponse* resp);
    void handleSendMessage(const muduo::net::HttpRequest& req,
                           muduo::net::HttpResponse* resp);
    void handleSessionList(const muduo::net::HttpRequest& req,
                           muduo::net::HttpResponse* resp);
    void handleUnread(const muduo::net::HttpRequest& req,
                      muduo::net::HttpResponse* resp);

    UserDao* user_dao_;
    RedisStore* redis_store_;
    ConversationStore* store_;
    muduo::net::HttpServer server_;
};

}  // namespace sparkpush
