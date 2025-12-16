#pragma once

#include <muduo/net/EventLoop.h>
#include <muduo/net/http/HttpRequest.h>
#include <muduo/net/http/HttpResponse.h>
#include <muduo/net/http/HttpServer.h>

#include "kafka_producer.h"
#include "message_dao.h"
#include "redis_store.h"
#include "room_store.h"
#include "spark_push.pb.h"
#include "user_dao.h"

namespace sparkpush {

// HttpApiServer：logic 侧对外 HTTP API 服务
//
// 职责：
// - 提供登录/注册/单聊/会话/未读等 HTTP 接口
// - 提供话题(房间)/聊天室相关的 HTTP 管理与消息入口（便于 Web/命令行联调）
//
// 依赖：
// - UserDao：访问用户数据（MySQL）
// - RedisStore：token/路由/会话/未读等缓存与状态
// - RoomStore：房间元信息与成员管理（MySQL+Redis）
// - KafkaProducer：写入推送/持久化等消息队列
class HttpApiServer {
   public:
    // 构造函数：创建 HTTP Server 并注入所需依赖
    HttpApiServer(muduo::net::EventLoop* loop,
                  const muduo::net::InetAddress& listenAddr, UserDao* user_dao,
                  RedisStore* redis_store, RoomStore* room_store,
                  MessageDao* message_dao, KafkaProducer* push_producer);

    // 启动 HTTP 服务（开始监听并接收请求）
    void start();

   private:
    // muduo HttpServer 回调入口：按 path/method 路由到具体 handler
    void onRequest(const muduo::net::HttpRequest& req,
                   muduo::net::HttpResponse* resp);

    // 从 HTTP 请求中解析并校验用户身份（例如从 token/headers 中取 uid）
    // @param req: HTTP 请求
    // @param uid: 输出参数，解析得到的用户 ID
    // @return: 成功返回 true；失败返回 false（并由调用方返回相应错误）
    bool GetUserIdFromRequest(const muduo::net::HttpRequest& req, int64_t* uid);

    // 登录接口：校验账号密码，签发 token，并返回用户信息
    void handleLogin(const muduo::net::HttpRequest& req,
                     muduo::net::HttpResponse* resp);
    // 注册接口：创建用户，签发 token，并返回用户信息
    void handleRegister(const muduo::net::HttpRequest& req,
                        muduo::net::HttpResponse* resp);
    // 发送单聊消息（HTTP 入口）：写入 Kafka（推送/持久化）
    void handleSendMessage(const muduo::net::HttpRequest& req,
                           muduo::net::HttpResponse* resp);
    // 按会话分页拉取历史消息（持久化回溯）
    void handleMessageHistory(const muduo::net::HttpRequest& req,
                              muduo::net::HttpResponse* resp);
    // 获取会话列表（按用户维度）
    void handleSessionList(const muduo::net::HttpRequest& req,
                           muduo::net::HttpResponse* resp);
    // 查询单个会话未读数
    void handleUnread(const muduo::net::HttpRequest& req,
                      muduo::net::HttpResponse* resp);
    // 批量查询多个会话未读数
    void handleUnreadBatch(const muduo::net::HttpRequest& req,
                           muduo::net::HttpResponse* resp);
    // 标记会话已读（更新 read_seq/清理未读等）
    void handleMarkRead(const muduo::net::HttpRequest& req,
                        muduo::net::HttpResponse* resp);

    // 批量查询用户昵称（用于前端显示真实 name）
    // 入参：{"user_ids":[1,2,3]}
    // 出参：{"users":[{"user_id":1,"name":"Alice"},...]}
    void handleUserBatch(const muduo::net::HttpRequest& req,
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

    // 用户 DAO（MySQL）：账号/昵称/密码 hash 等
    UserDao* user_dao_;
    // Redis 存储：token/路由/会话/未读/房间成员缓存等
    RedisStore* redis_store_;
    // 房间/话题存储：MySQL(RoomDao) + Redis 缓存的组合封装
    RoomStore* room_store_;
    // 消息持久化 DAO（MySQL）
    MessageDao* message_dao_;
    // Kafka 推送生产者：把消息投递给 comet/persist 等下游
    KafkaProducer* push_producer_;
    // muduo 的 HTTP Server 实例
    muduo::net::HttpServer server_;
};

}  // namespace sparkpush
