#pragma once

#include <muduo/net/EventLoop.h>
#include <muduo/net/http/HttpRequest.h>
#include <muduo/net/http/HttpResponse.h>
#include <muduo/net/http/HttpServer.h>

#include "conversation_store.h"
#include "danmaku_dao.h"
#include "group_dao.h"
#include "kafka_producer.h"
#include "redis_store.h"
#include "user_dao.h"

namespace sparkpush {

class HttpApiServer {
   public:
    // 功能：构造 HTTP API 服务，注入业务依赖
    // 参数：loop 事件循环；listenAddr 监听地址；store 会话存储；user_dao 用户
    // DAO；
    //       group_dao 群 DAO；group_member_dao 成员 DAO；redis_store 路由存储；
    //       kafka_producer 推送 Kafka；broadcast_producer 广播
    //       Kafka；danmaku_dao 弹幕 DAO
    // 返回：无
    HttpApiServer(muduo::net::EventLoop* loop,
                  const muduo::net::InetAddress& listenAddr,
                  ConversationStore* store, UserDao* user_dao,
                  GroupDao* group_dao, GroupMemberDao* group_member_dao,
                  RedisStore* redis_store, KafkaProducer* kafka_producer,
                  KafkaProducer* broadcast_producer, DanmakuDao* danmaku_dao);

    // 功能：启动 HTTP 服务监听
    // 参数：无
    // 返回：无
    void start();

   private:
    // 功能：统一请求入口，路由到具体处理函数
    // 参数：req 请求；resp 响应
    // 返回：无
    void onRequest(const muduo::net::HttpRequest& req,
                   muduo::net::HttpResponse* resp);

    // 账号体系：登录 / 注册
    // 功能：处理登录
    // 参数：req/resp HTTP 请求响应
    void handleLogin(const muduo::net::HttpRequest& req,
                     muduo::net::HttpResponse* resp);
    // 功能：处理注册
    // 参数：req/resp HTTP 请求响应
    void handleRegister(const muduo::net::HttpRequest& req,
                        muduo::net::HttpResponse* resp);

    // 功能：单聊发送消息
    // 参数：req/resp HTTP 请求响应
    void handleSendMessage(const muduo::net::HttpRequest& req,
                           muduo::net::HttpResponse* resp);

    // 功能：拉取会话历史
    void handleHistory(const muduo::net::HttpRequest& req,
                       muduo::net::HttpResponse* resp);

    // 功能：标记已读
    void handleMarkRead(const muduo::net::HttpRequest& req,
                        muduo::net::HttpResponse* resp);

    // 功能：查询未读
    void handleUnread(const muduo::net::HttpRequest& req,
                      muduo::net::HttpResponse* resp);

    // 会话 / 房间列表
    // 功能：列出单聊会话列表
    void handleSingleSessionList(const muduo::net::HttpRequest& req,
                                 muduo::net::HttpResponse* resp);
    // 功能：列出聊天室会话列表
    void handleChatroomList(const muduo::net::HttpRequest& req,
                            muduo::net::HttpResponse* resp);

    // 功能：加入聊天室
    void handleChatroomJoin(const muduo::net::HttpRequest& req,
                            muduo::net::HttpResponse* resp);

    // 功能：离开聊天室（在线路由层）
    void handleChatroomLeave(const muduo::net::HttpRequest& req,
                             muduo::net::HttpResponse* resp);

    // 取消订阅聊天室：从“我的房间列表”中移除，但不影响历史消息
    // 功能：取消订阅聊天室
    void handleChatroomUnsubscribe(const muduo::net::HttpRequest& req,
                                   muduo::net::HttpResponse* resp);

    // 功能：查询聊天室在线人数
    void handleChatroomOnlineCount(const muduo::net::HttpRequest& req,
                                   muduo::net::HttpResponse* resp);

    // 弹幕相关：接受前端 HTTP 请求，写入 DB + 内存模型，并通过 Kafka 推送给
    // comet 功能：发送弹幕
    void handleDanmakuSend(const muduo::net::HttpRequest& req,
                           muduo::net::HttpResponse* resp);
    // 功能：拉取弹幕
    void handleDanmakuList(const muduo::net::HttpRequest& req,
                           muduo::net::HttpResponse* resp);

    // 管理后台：创建 / 列出聊天室（基于 im_group）
    // 功能：管理员创建聊天室
    void handleAdminCreateChatroom(const muduo::net::HttpRequest& req,
                                   muduo::net::HttpResponse* resp);
    // 功能：管理员分页列出聊天室
    void handleAdminListChatroom(const muduo::net::HttpRequest& req,
                                 muduo::net::HttpResponse* resp);

    // 管理后台：发送广播（封装到 Kafka broadcast topic）
    // 功能：管理员广播消息
    void handleAdminBroadcast(const muduo::net::HttpRequest& req,
                              muduo::net::HttpResponse* resp);

    ConversationStore* store_;
    UserDao* user_dao_;
    GroupDao* group_dao_;
    GroupMemberDao* group_member_dao_;
    RedisStore* redis_store_;
    KafkaProducer* kafka_producer_;
    KafkaProducer* broadcast_producer_;
    DanmakuDao* danmaku_dao_;
    muduo::net::HttpServer server_;
};

}  // namespace sparkpush
