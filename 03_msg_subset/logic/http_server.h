#pragma once

#include <muduo/net/EventLoop.h>
#include <muduo/net/http/HttpRequest.h>
#include <muduo/net/http/HttpResponse.h>
#include <muduo/net/http/HttpServer.h>

#include "kafka_producer.h"
#include "redis_store.h"
#include "spark_push.pb.h"
#include "user_dao.h"

namespace sparkpush {

class HttpApiServer {
   public:
    HttpApiServer(muduo::net::EventLoop* loop,
                  const muduo::net::InetAddress& listenAddr, UserDao* user_dao,
                  RedisStore* redis_store, KafkaProducer* push_producer);

    void start();

   private:
    void onRequest(const muduo::net::HttpRequest& req,
                   muduo::net::HttpResponse* resp);

    bool GetUserIdFromRequest(const muduo::net::HttpRequest& req, int64_t* uid);

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
    void handleMarkRead(const muduo::net::HttpRequest& req,
                        muduo::net::HttpResponse* resp);

    UserDao* user_dao_;
    RedisStore* redis_store_;
    KafkaProducer* push_producer_;
    muduo::net::HttpServer server_;
};

}  // namespace sparkpush
