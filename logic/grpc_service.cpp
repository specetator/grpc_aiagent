#include "grpc_service.h"

#include "logging.h"
#include "internal_rpc.h"
#include "message_codec.h"

namespace sparkpush {
namespace {
// 把业务错误写进 protobuf。两个重载：一个收 (code, message)，
// 一个收 ServiceStatus（直接转发，避开每个调用点写 .code/.message）。
//
// code != 0 时额外记一条 LOG_WARN：因为 handler 一律返回 grpc::Status::OK
//（见 grpc_service.h 顶部说明），服务端日志是**唯一**能看到业务失败的地方。
// 若不记，一个持续 403 的客户端在日志里会完全隐形。
void SetError(ErrorInfo* error, int code, const std::string& message) {
    if (code != 0) LOG_WARN << "Logic error code=" << code << ", message=" << message;
    error->set_code(code);
    error->set_message(message);
}
void SetError(ErrorInfo* error, const ServiceStatus& status) {
    SetError(error, status.code, status.message);
}
// protobuf 请求 -> 业务层结构体。
//
// 注意这里用的是**位置初始化**（没有写字段名），因此顺序必须与
// message_service.h 里 UpstreamCommand 的声明顺序逐字对应：
//   scene, from_user_id, to_user_id, group_id,
//   content_json, client_msg_id, source_comet_id, auth_token
// 这是个隐性耦合：在 UpstreamCommand 中间插一个新字段，本函数会
// 静默地把值赋给错的字段（只要类型兼容就不报错）。
// 改动 UpstreamCommand 时必须同步改这里，最好改成具名初始化。
UpstreamCommand DecodeUpstream(const UpstreamMessageRequest& request) {
    return {request.scene(),          request.from_user_id(), request.to_user_id(),
            request.group_id(),       request.content_json(), request.client_msg_id(),
            request.source_comet_id(), request.auth_token()};
}
}  // namespace

// Comet 握手入口：验 token -> 注册用户路由 -> 下发路由凭证。
//
// token 的 audience 固定为 kRealtimeUser。与 kHttpUser 的关键差异
//（见 user_service.cpp 的 Authenticate）：实时 audience 下内置管理员账号
//（kAdminUserId）与 bot 账号会被直接 403，因此管理面 token 和机器人账号
// 都**不能**用来建立 WebSocket 连接。这里写错一个 audience 就会静默地
// 把那道门禁关掉。
::grpc::Status LogicServiceImpl::VerifyToken(::grpc::ServerContext* context,
                                             const VerifyTokenRequest* request,
                                             VerifyTokenReply* response) {
    const auto authorized = AuthorizeInternalRpc(context, "comet");
    if (!authorized.ok()) return authorized;
    LOG_INFO << "VerifyToken called for comet_id=" << request->comet_id();
    int64_t user = 0;
    const auto status = users_->Authenticate(
        request->token(), UserService::TokenAudience::kRealtimeUser, &user);
    SetError(response->mutable_error(), status);
    if (status.ok()) {
        response->set_user_id(user);
        std::string token;
        // AddAuthenticatedRoute 一个 Lua 脚本里原子完成三件事：
        // INCR 路由代数 + SET 路由令牌 + SADD 路由集（见 redis_store.h）。
        // 返回的 generation <= 0 就意味着注册没成（Redis 不可用）。
        const int64_t generation = messages_->AddAuthenticatedRoute(user, request->comet_id(), &token);
        response->set_route_token(token);
        // 注意字段写入顺序带来的一个坑：user_id 与 route_token 已经写进去了，
        // 这里才把 error 覆写成 503。因此回复会是一个“既有 user_id
        // 又有错误码”的混合体。Comet 必须**先判 error.code**再用其余字段，
        // 否则会拿着一个未注册成功的 route_token 去摘路由。
        //（不改成“失败就清空 user_id”是因为那会丢掉排查线索：
        // “token 验对了但 Redis 挂了”与“token 就是错的”必须能区分开。）
        if (generation <= 0) SetError(response->mutable_error(), 503, "route registration failed");
        else response->set_route_generation(generation);
    }
    return ::grpc::Status::OK;
}

void LogicServiceImpl::HandleUpstreamMessage(const UpstreamMessageRequest& request,
                                             UpstreamMessageReply* response) {
    const auto result = messages_->Submit(DecodeUpstream(request));
    SetError(response->mutable_error(), result.status);
    // has_message 与 accepted 是两个独立标志，不要合并：
    //   has_message  序号已分配、消息体已构造（幂等命中时也为真）
    //   accepted     persist 事件已拿到 Kafka delivery report
    // 因此“有消息但未 accepted”是合法组合（Kafka 不可用），
    // 此时仍然要把消息体回给调用方供排查，但不得设 ACK_STAGE_ACCEPTED。
    if (result.has_message)
        FillChatMessage(result.message, response->mutable_message());
    if (result.accepted) {
        // accepted_ack 的语义边界（全项目最容易误读的一个词）：
        // 它只意味着“事件已交给 Kafka”，**不**意味着已落库，
        // 也**不**意味着对端已收到。后两者分别由 Job 的落库与
        // delivered_ack 表示。
        response->set_ack_stage(ACK_STAGE_ACCEPTED);
        response->set_accepted_at_ms(result.accepted_at_ms);
    }
}
::grpc::Status LogicServiceImpl::SendUpstreamMessage(
    ::grpc::ServerContext* context, const UpstreamMessageRequest* request,
    UpstreamMessageReply* response) {
    const auto authorized = AuthorizeInternalRpc(context, "comet");
    if (!authorized.ok()) return authorized;
    HandleUpstreamMessage(*request, response);
    return ::grpc::Status::OK;
}

// 双向流上行。并发与串行语义见 grpc_service.h 里对 MessageStream 的说明。
::grpc::Status LogicServiceImpl::MessageStream(
    ::grpc::ServerContext* context,
    ::grpc::ServerReaderWriter<::sparkpush::StreamResponse, ::sparkpush::StreamMessage>*
        stream) {
    // 鉴权只在**建流时**做一次，而不是每条消息一次：
    // 初始 metadata 每个 RPC 只发一次，后续消息里本来就没有密钥可验。
    // 这也是为什么流一旦建立就是一个长生命期的信任域：
    // 密钥轮换必须配合重建流（Comet 侧的重连逻辑）才能生效。
    const auto authorized = AuthorizeInternalRpc(context, "comet");
    if (!authorized.ok()) return authorized;
    StreamMessage msg;
    // Read 返回 false 有两种情况：对端正常关流，或链路断了。
    // 两者在本 handler 里处理相同（退出循环并返回 OK），因为需要做的
    // 清理（摘路由）是由 Comet 单独发 UserOffline 完成的，不在这里做。
    while (stream->Read(&msg)) {
        StreamResponse resp;
        // request_id 无条件回显（包括下面 default 分支）：Comet 的读线程
        // 靠它在 pending 表里找回调。漏了回显会让那个请求永远挂着，
        // 直到 deadline 看门狗超时。
        resp.set_request_id(msg.request_id());
        // payload_case() 是 protobuf 对 oneof 生成的枚举，因此四种请求
        // 在同一时刻只可能有一种，switch 不需要考虑组合情况。
        switch (msg.payload_case()) {
            case StreamMessage::kUpstream: {
                UpstreamMessageReply reply;
                HandleUpstreamMessage(msg.upstream(), &reply);
                // error 写了**两份**（信封层 + reply 内部），看起来冗余但是故意的：
                // Comet 的读线程可以先看信封层 error 快速判断“这条回复是不是
                // 整体失败”，而不需要先 oneof 分派再拆子消息。
                *resp.mutable_error() = reply.error();
                *resp.mutable_upstream_reply() = reply;
                break;
            }
            case StreamMessage::kOffline: {
                SimpleReply reply;
                // 下线上报带着 route_generation + route_token：Logic 侧的
                // RemoveRouteGeneration 只在两者都对得上时才真摘路由，
                // 防止“迟到的下线上报误删新连接的路由”。
                // 注意 UserOffline 返回 void（Redis 失败也不能报错），
                // 因此这里无条件回 0/"ok"。
                messages_->UserOffline(msg.offline().user_id(),
                                       msg.offline().comet_id(), msg.offline().route_generation(), msg.offline().route_token());
                SetError(reply.mutable_error(), 0, "ok");
                *resp.mutable_error() = reply.error();
                *resp.mutable_simple_reply() = reply;
                break;
            }
            case StreamMessage::kRoomJoin: {
                SimpleReply reply;
                // 复用现有 RPC 逻辑：直接调用成员函数实现
                // 与下面独立的 ReportRoomJoin handler 调的是同一个
                // RoomService 方法，因此两条路径的计数语义完全一致。
                SetError(reply.mutable_error(), rooms_->ReportRoomJoin(msg.room_join().room_id(), msg.room_join().comet_id(), msg.room_join().user_id()));
                *resp.mutable_error() = reply.error();
                *resp.mutable_simple_reply() = reply;
                break;
            }
            case StreamMessage::kRoomLeave: {
                SimpleReply reply;
                SetError(reply.mutable_error(), rooms_->ReportRoomLeave(msg.room_leave().room_id(), msg.room_leave().comet_id(), msg.room_leave().user_id()));
                *resp.mutable_error() = reply.error();
                *resp.mutable_simple_reply() = reply;
                break;
            }
            default:
                // PAYLOAD_NOT_SET 也落到这里。回 400 而不是断流：
                // 一个版本不一致的 Comet 发来了新类型时，只废掉那一条请求，
                // 不要把整条流（以及其他正常请求）一起弄死。
                SetError(resp.mutable_error(), 400, "unknown message type");
                break;
        }
        // Write 失败 = 对端已不再读，继续处理只会白耗，直接退循环。
        if (!stream->Write(resp)) {
            break;
        }
    }
    return ::grpc::Status::OK;
}

// 下面几个 handler 结构完全相同：验角色密钥 -> 调 service -> SetError -> OK。
// 不再逐个重复注释，只标注与“默认形式”不同的地方。

// SetError(..., {}) 传的是默认构造的 ServiceStatus（code=0），
// 即“无条件成功”：UserOffline 是尽力而为语义，Redis 挂了也得让 Comet
// 把连接清掉，否则本地路由表会泄漏。
::grpc::Status LogicServiceImpl::UserOffline(::grpc::ServerContext* context,
                                             const UserOfflineRequest* request,
                                             SimpleReply* response) {
    const auto authorized = AuthorizeInternalRpc(context, "comet");
    if (!authorized.ok()) return authorized;
    messages_->UserOffline(request->user_id(), request->comet_id(), request->route_generation(), request->route_token());
    SetError(response->mutable_error(), {});
    return ::grpc::Status::OK;
}
::grpc::Status LogicServiceImpl::ReportRoomJoin(::grpc::ServerContext* context,
                                                const RoomReportRequest* request,
                                                SimpleReply* response) {
    const auto authorized = AuthorizeInternalRpc(context, "comet");
    if (!authorized.ok()) return authorized;
    SetError(response->mutable_error(),
             rooms_->ReportRoomJoin(request->room_id(), request->comet_id(),
                                    request->user_id()));
    return ::grpc::Status::OK;
}
::grpc::Status LogicServiceImpl::ReportRoomLeave(::grpc::ServerContext* context,
                                                 const RoomReportRequest* request,
                                                 SimpleReply* response) {
    const auto authorized = AuthorizeInternalRpc(context, "comet");
    if (!authorized.ok()) return authorized;
    SetError(response->mutable_error(),
             rooms_->ReportRoomLeave(request->room_id(), request->comet_id(),
                                     request->user_id()));
    return ::grpc::Status::OK;
}
// 唯一一个验 "admin" 而不是 "comet" 密钥的 handler：广播是管理动作，
// 不应该让一个拿到 comet 密钥的节点就能向全体用户发消息。
// task_id 无条件回写（即使失败），便于调用方拿着它去日志里追查。
::grpc::Status LogicServiceImpl::Broadcast(::grpc::ServerContext* context,
                                           const BroadcastRequest* request,
                                           BroadcastReply* response) {
    const auto authorized = AuthorizeInternalRpc(context, "admin");
    if (!authorized.ok()) return authorized;
    const auto result = messages_->Broadcast(request->scope(), request->group_id(),
                                             request->content_json());
    SetError(response->mutable_error(), result.status);
    response->set_task_id(result.task_id);
    return ::grpc::Status::OK;
}
::grpc::Status LogicServiceImpl::SyncMessages(::grpc::ServerContext* context,
                                              const SyncMessagesRequest* request,
                                              SyncMessagesReply* response) {
    const auto authorized = AuthorizeInternalRpc(context, "comet");
    if (!authorized.ok()) return authorized;
    MessagePage page;
    const auto status =
        conversations_->SyncAfter(request->user_id(), request->session_id(),
                                  request->after_seq(), request->limit(), &page);
    SetError(response->mutable_error(), status);
    // 失败时不写任何分页字段：写了会让调用方拿到一个“看起来成功但
    // 空页”的回复，从而把本地游标推到一个错误的位置。
    if (status.ok()) {
        for (const auto& message : page.messages)
            FillChatMessage(message, response->add_messages());
        response->set_has_more(page.has_more);
        // next_seq 与 safe_seq 必须一起回，且**不能互相代替**：
        //   next_seq  本页实际读到的最后一条序号
        //   safe_seq  持久化安全水位 = min(SafeSyncPrefix, next_seq)
        // 把 next_seq 当 safe_seq 用会让客户端丢掉尚未落库的消息，
        // 这是集成测试专门盯的一个回归（见 conversation_service.cpp 的 Sync）。
        response->set_next_seq(page.next_seq);
        response->set_safe_seq(page.safe_seq);
    }
    return ::grpc::Status::OK;
}
::grpc::Status LogicServiceImpl::SyncOffline(::grpc::ServerContext* context,
                                             const SyncOfflineRequest* request,
                                             SyncOfflineReply* response) {
    const auto authorized = AuthorizeInternalRpc(context, "comet");
    if (!authorized.ok()) return authorized;
    MessagePage page;
    // 参数顺序注意：after_session_id / after_seq 两个游标在最后，
    // 因为它们在 ServiceStatus SyncOffline(...) 的签名里带默认值。
    const auto status =
        conversations_->SyncOffline(request->user_id(), request->limit(), &page,
                                     request->after_session_id(), request->after_seq());
    SetError(response->mutable_error(), status);
    if (status.ok()) {
        for (const auto& message : page.messages)
            FillChatMessage(message, response->add_messages());
        response->set_has_more(page.has_more);
        // 跨会话补推的游标是**二维**的（session_id, seq），因此比
        // SyncMessages 多回一个 next_session_id。会话集在服务层按 id
        // 排序后遍历，保证同一个游标两次续传的结果确定可重现。
        response->set_next_session_id(page.next_session_id);
        response->set_next_seq(page.next_seq);
    }
    return ::grpc::Status::OK;
}
::grpc::Status LogicServiceImpl::MarkDelivered(::grpc::ServerContext* context,
                                               const MarkDeliveredRequest* request,
                                               SimpleReply* response) {
    const auto authorized = AuthorizeInternalRpc(context, "comet");
    if (!authorized.ok()) return authorized;
    // 两个字段对应**新旧两套契约**，在这里被压平成一个 cursors 向量：
    //   cursors（旧）      逐会话游标，user_id 统一取顶层的 request->user_id()
    //   user_cursors（新） 每个游标自带 user_id（一次回报多个用户的送达）
    // 传给服务层的 bool 就是 user_cursors_size() > 0，而它在
    // ConversationService::MarkDelivered 里**只**用于放宽入口校验
    //（user <= 0 && !has_user_cursors 才报 400），循环里一律用
    // cursor.user_id。因此两套契约到了服务层已经等价。
    // 注意：若一个请求同时填了两个字段，两者会被**拼接**而不是互斥，
    // 当前 Comet 不会这么发，但协议上并没有拦住。
    std::vector<DeliveryCursor> cursors;
    for (const auto& cursor : request->cursors())
        cursors.push_back({request->user_id(), cursor.session_id(), cursor.msg_seq()});
    for (const auto& cursor : request->user_cursors())
        cursors.push_back({cursor.user_id(), cursor.session_id(), cursor.msg_seq()});
    SetError(response->mutable_error(),
             conversations_->MarkDelivered(request->user_id(),
                                           request->user_cursors_size() > 0, cursors));
    return ::grpc::Status::OK;
}
}  // namespace sparkpush
