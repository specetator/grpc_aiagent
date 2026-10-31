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
#include "services/room_agent_service.h"
#include "services/agent_connection_service.h"

namespace sparkpush {

// Composition root supplies business services, never DAOs or network clients.
//
// 译：组装根（main.cpp）只供给业务 service，绝不供给 DAO 或网络客户端。
//
// 这条约束把 HTTP 适配层钉在了边界上：本文件里的代码只能调 service，
// 因此任何鉴权/幂等/序号规则都不可能在这里被“绕过去”。
// 对比 gRPC 适配层（grpc_service.h）：它同样只持 service 指针。
//
// 用引用而不是指针的八个成员是**必需**依赖（main.cpp 里都是栈上对象）；
// 最后两个用指针 + 默认 nullptr，因为房间 Agent 是**可选特性**
//（未启用时 main.cpp 不传），handler 里靠空指针判定回 503。
struct LogicHttpServices {
    UserService& users;
    ConversationService& conversations;
    AttachmentService& attachments;
    MessageService& messages;
    RoomService& rooms;
    AdminService& admin;
    DanmakuService& danmaku;
    KnowledgeService& knowledge;
    RoomAgentService* room_agents{nullptr};
    OperatorTaskService* operator_tasks{nullptr};
    AgentConnectionService* agent_connections{nullptr};
};

// HTTP adapter: request parsing, bearer extraction and response serialization.
//
// 译：HTTP 适配层：请求解析、bearer 提取、响应序列化。
//
// 阅读本类前先记住四件事（都在 .cpp 顶部的大段注释里展开了）：
//   1. 响应体统一是 {"code":..,"message":..,"data":{..}}，而 **HTTP 状态码
//      与 body.code 并不一一对应**：500/503/409/413/429 等业务码会以
//      HTTP 200 返回。客户端必须读 body.code，不能只看状态码。
//   2. 鉴权分两道门：requireUser（普通用户，kHttpUser audience）与
//      requireAdmin（管理员，验 token 解出的 id 必须等于 kAdminUserId）。
//      requireAdmin **不输出 user_id**，各 admin handler 直接用常量
//      kAdminUserId 当 actor——绝不从请求 JSON 里取。
//   3. 路由是一张 path -> 成员函数指针的静态表，**多对一是允许的**：
//      四个 /api/admin/user/{status,disable,restore,delete} 共用一个 handler，
//      三个 /api/chatroom/agent/* 共用 handleRoomAgent，内部再按 path 分派。
//   4. /ready 与 /metrics 在路由表**之前**处理且不需鉴权。
//
// 线程模型：muduo 的 HttpServer 配 io_threads 个 IO 线程，handler 可能
// 并发执行。因此本类**没有任何可变成员状态**（十个指针构造后不再变），
// 并发安全完全依靠 service 层自己的锁。
class HttpApiServer {
   public:
    HttpApiServer(muduo::net::EventLoop* loop, const muduo::net::InetAddress& address,
                  LogicHttpServices services, int io_threads = 1);
    void start();

   private:
    void handleWorkspaceAuthorization(const muduo::net::HttpRequest&, muduo::net::HttpResponse*);
    void handleSpaces(const muduo::net::HttpRequest&, muduo::net::HttpResponse*);
    void handleContactLookup(const muduo::net::HttpRequest&, muduo::net::HttpResponse*);
    void handleAgentConnections(const muduo::net::HttpRequest& req,
                                muduo::net::HttpResponse* resp);
    AgentConnectionService* agent_connection_service_;
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

    // 两道鉴权门。两者都已经把错误写进 resp 了，因此调用方的写法
    // 统一是 `if (!requireUser(req, resp, &uid)) return;`，不需要再回一次错。
    //
    // requireUser：解出 Bearer token -> Authenticate(kHttpUser) -> 写 *user_id。
    //   注意 audience 与 gRPC 侧的 VerifyToken 不同（那边是 kRealtimeUser），
    //   差异是：kRealtimeUser 下内置管理员账号与 bot 账号会被 403，
    //   而 kHttpUser 下**不会**。也就是说管理员 token 可以调普通用户接口。
    // requireAdmin：AdminEnabled() 门禁 + AuthenticateAdmin（要求 id == kAdminUserId）。
    //   它的鉴权失败一律回 HTTP 400（而不是 401/403），与 requireUser 不一致，
    //   这是历史遗留（见 .cpp 里 WriteAdminServiceError 的说明）。
    bool requireUser(const muduo::net::HttpRequest& req, muduo::net::HttpResponse* resp,
                     int64_t* user_id);
    // 房间共享 Agent 的三个端点共用本 handler（configure / task / checkpoint），
    // 内部按 req.path() 分派。task 路径是兜底分支（else）。
    void handleRoomAgent(const muduo::net::HttpRequest& req,
                         muduo::net::HttpResponse* resp);
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
    RoomAgentService* room_agent_service_;
    OperatorTaskService* operator_task_service_;
    muduo::net::HttpServer server_;
};
}  // namespace sparkpush
