#pragma once

#include <muduo/net/EventLoop.h>
#include <muduo/net/http/HttpRequest.h>
#include <muduo/net/http/HttpResponse.h>
#include <muduo/net/http/HttpServer.h>

#include "kafka_producer.h"
#include "redis_store.h"
#include "room_store.h"
#include "spark_push.pb.h"
#include "user_dao.h"

namespace sparkpush {

class HttpApiServer {
   public:
    HttpApiServer(muduo::net::EventLoop* loop,
                  const muduo::net::InetAddress& listenAddr, UserDao* user_dao,
                  RedisStore* redis_store, RoomStore* room_store,
                  KafkaProducer* push_producer);

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

    // ========== 话题(房间)/聊天室相关 ==========
    // 管理员创建话题
    void handleRoomCreate(const muduo::net::HttpRequest& req,
                          muduo::net::HttpResponse* resp);
    // 拉取话题列表
    void handleRoomList(const muduo::net::HttpRequest& req,
                        muduo::net::HttpResponse* resp);
    // 加入/退出话题
    void handleRoomJoin(const muduo::net::HttpRequest& req,
                        muduo::net::HttpResponse* resp);
    void handleRoomLeave(const muduo::net::HttpRequest& req,
                         muduo::net::HttpResponse* resp);
    // 拉取话题用户
    void handleRoomUsers(const muduo::net::HttpRequest& req,
                         muduo::net::HttpResponse* resp);
    // 房间消息发送（HTTP 入口，便于命令行压测/调试，不依赖 WebSocket）
    void handleRoomSendMessage(const muduo::net::HttpRequest& req,
                               muduo::net::HttpResponse* resp);

    // 登录/注册成功后做“用户入库（users:all）+ 自动加入所有已存在话题”
    void PostAuthInitUser(int64_t user_id);

    UserDao* user_dao_;
    RedisStore* redis_store_;
    RoomStore* room_store_;
    KafkaProducer* push_producer_;
    muduo::net::HttpServer server_;
};

}  // namespace sparkpush
