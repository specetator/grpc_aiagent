#pragma once

#include "conversation_store.h"
#include "user_dao.h"
#include "redis_store.h"
#include "kafka_producer.h"
#include "danmaku_dao.h"
#include "group_dao.h"

#include <muduo/net/http/HttpServer.h>
#include <muduo/net/http/HttpRequest.h>
#include <muduo/net/http/HttpResponse.h>
#include <muduo/net/EventLoop.h>

namespace sparkpush {

class HttpApiServer {
 public:
  HttpApiServer(muduo::net::EventLoop* loop,
                const muduo::net::InetAddress& listenAddr,
                ConversationStore* store,
                UserDao* user_dao,
                GroupDao* group_dao,
                GroupMemberDao* group_member_dao,
                RedisStore* redis_store,
                KafkaProducer* kafka_producer,
                KafkaProducer* broadcast_producer,
                DanmakuDao* danmaku_dao);

  void setThreadNum(int numThreads) { server_.setThreadNum(numThreads); }
  void start();

 private:
  void onRequest(const muduo::net::HttpRequest& req,
                 muduo::net::HttpResponse* resp);

  // 账号体系：登录 / 注册
  void handleLogin(const muduo::net::HttpRequest& req,
                   muduo::net::HttpResponse* resp);
  void handleRegister(const muduo::net::HttpRequest& req,
                      muduo::net::HttpResponse* resp);

  void handleSendMessage(const muduo::net::HttpRequest& req,
                         muduo::net::HttpResponse* resp);

  void handleHistory(const muduo::net::HttpRequest& req,
                     muduo::net::HttpResponse* resp);

  void handleMarkRead(const muduo::net::HttpRequest& req,
                      muduo::net::HttpResponse* resp);

  void handleUnread(const muduo::net::HttpRequest& req,
                    muduo::net::HttpResponse* resp);

  // 会话 / 房间列表
  void handleSingleSessionList(const muduo::net::HttpRequest& req,
                               muduo::net::HttpResponse* resp);
  void handleChatroomList(const muduo::net::HttpRequest& req,
                          muduo::net::HttpResponse* resp);

  void handleChatroomJoin(const muduo::net::HttpRequest& req,
                          muduo::net::HttpResponse* resp);

  void handleChatroomLeave(const muduo::net::HttpRequest& req,
                           muduo::net::HttpResponse* resp);

  // 取消订阅聊天室：从“我的房间列表”中移除，但不影响历史消息
  void handleChatroomUnsubscribe(const muduo::net::HttpRequest& req,
                                 muduo::net::HttpResponse* resp);

  void handleChatroomOnlineCount(const muduo::net::HttpRequest& req,
                                 muduo::net::HttpResponse* resp);

  // 弹幕相关：接受前端 HTTP 请求，写入 DB + 内存模型，并通过 Kafka 推送给 comet
  void handleDanmakuSend(const muduo::net::HttpRequest& req,
                         muduo::net::HttpResponse* resp);
  void handleDanmakuList(const muduo::net::HttpRequest& req,
                         muduo::net::HttpResponse* resp);

  // 管理后台：创建 / 列出聊天室（基于 im_group）
  void handleAdminCreateChatroom(const muduo::net::HttpRequest& req,
                                 muduo::net::HttpResponse* resp);
  void handleAdminListChatroom(const muduo::net::HttpRequest& req,
                               muduo::net::HttpResponse* resp);

  // 管理后台：发送广播（封装到 Kafka broadcast topic）
  void handleAdminBroadcast(const muduo::net::HttpRequest& req,
                            muduo::net::HttpResponse* resp);

  // ---- 统一鉴权 ----
  // 从请求中提取 token（优先 Authorization: Bearer <token>，降级到 JSON body 的 "token" 字段），
  // 通过 Redis 校验后返回 user_id。失败时自动写入 401 响应并返回 0。
  int64_t AuthenticateRequest(const muduo::net::HttpRequest& req,
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


