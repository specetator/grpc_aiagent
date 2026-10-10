#pragma once

#include <muduo/net/EventLoop.h>
#include <muduo/net/http/HttpRequest.h>
#include <muduo/net/http/HttpResponse.h>
#include <muduo/net/http/HttpServer.h>

#include "services/admin_service.h"
#include "services/attachment_service.h"
#include "services/conversation_service.h"
#include "services/danmaku_service.h"
#include "services/knowledge_service.h"
#include "services/message_service.h"
#include "services/room_service.h"
#include "services/user_service.h"

namespace sparkpush {

// Composition root supplies business services, never DAOs or network clients.
struct LogicHttpServices {
    UserService& users;
    ConversationService& conversations;
    AttachmentService& attachments;
    MessageService& messages;
    RoomService& rooms;
    AdminService& admin;
    DanmakuService& danmaku;
    KnowledgeService& knowledge;
};

// HTTP adapter: request parsing, bearer extraction and response serialization.
class HttpApiServer {
   public:
    HttpApiServer(muduo::net::EventLoop* loop, const muduo::net::InetAddress& address,
                  LogicHttpServices services);
    void start();

   private:
    // 功能：统一请求入口，路由到具体处理函数
    // 参数：req 请求；resp 响应
    // 返回：无
    void onRequest(const muduo::net::HttpRequest& req, muduo::net::HttpResponse* resp);

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
    void handleAttachmentUpload(const muduo::net::HttpRequest& req,
                                muduo::net::HttpResponse* resp);
    void handleAttachmentGet(const muduo::net::HttpRequest& req,
                             muduo::net::HttpResponse* resp);

    // 功能：拉取会话历史
    void handleHistory(const muduo::net::HttpRequest& req,
                       muduo::net::HttpResponse* resp);

    // 已登录用户共享的只读 CANN 原文接口，只接受 catalog 稳定 ID。
    void handleKnowledgeDocument(const muduo::net::HttpRequest& req,
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

    // 弹幕协议适配；持久化和推送由 DanmakuService 编排。
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

    // 管理后台：用户列表、状态变更、Token 撤销、审计日志。
    void handleAdminListUsers(const muduo::net::HttpRequest& req,
                              muduo::net::HttpResponse* resp);
    void handleAdminSetUserStatus(const muduo::net::HttpRequest& req,
                                  muduo::net::HttpResponse* resp);
    void handleAdminUpdateUser(const muduo::net::HttpRequest& req,
                               muduo::net::HttpResponse* resp);
    void handleAdminRevokeUserTokens(const muduo::net::HttpRequest& req,
                                     muduo::net::HttpResponse* resp);
    void handleAdminListAuditLogs(const muduo::net::HttpRequest& req,
                                  muduo::net::HttpResponse* resp);

    bool requireUser(const muduo::net::HttpRequest& req, muduo::net::HttpResponse* resp,
                     int64_t* user_id);
    bool requireAdmin(const muduo::net::HttpRequest& req,
                      muduo::net::HttpResponse* resp);

    UserService* user_service_;
    ConversationService* conversation_service_;
    AttachmentService* attachment_service_;
    MessageService* message_service_;
    RoomService* room_service_;
    AdminService* admin_service_;
    DanmakuService* danmaku_service_;
    KnowledgeService* knowledge_service_;
    muduo::net::HttpServer server_;
};
}  // namespace sparkpush
