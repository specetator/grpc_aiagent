// ============================================================================
// logic 服务 gRPC 接口实现
// ============================================================================
#include "grpc_service.h"

#include <algorithm>
#include <chrono>
#include <nlohmann/json.hpp>

#include "logging.h"

namespace sparkpush {

// 构造函数：保存 Redis 存储的引用
LogicServiceImpl::LogicServiceImpl(UserDao* user_dao,
                                   KafkaProducer* push_producer,
                                   RedisStore* redis_store,
                                   int connection_ttl_ms)
    : user_dao_(user_dao),
      push_producer_(push_producer),
      redis_store_(redis_store),
      connection_ttl_ms_(connection_ttl_ms) {}

// 设置错误码和错误消息到 protobuf 的 ErrorInfo 字段
// 统一的错误处理逻辑，避免重复代码
void LogicServiceImpl::SetError(ErrorInfo* e, int code,
                                const std::string& msg) {
    LOG_INFO << "SetError called with code: " << std::to_string(code)
             << ", message: " << msg;
    e->set_code(code);
    e->set_message(msg);
}

// VerifyToken RPC 实现：验证客户端 token 并记录路由
//
// 调用流程：
// 1. comet 收到 WebSocket 握手请求，从 URL 参数提取 token
// 2. comet 通过此 RPC 向 logic 验证 token 的有效性
// 3. logic 查询 Redis 中的 token -> user_id 映射
// 4. 验证成功后返回 user_id，并在 Redis 记录 user_id -> comet_id 路由
// 5. comet 根据返回结果决定是否完成 WebSocket 握手
::grpc::Status LogicServiceImpl::VerifyToken(
    ::grpc::ServerContext*, const ::sparkpush::VerifyTokenRequest* request,
    ::sparkpush::VerifyTokenReply* response) {
    LOG_INFO << "VerifyToken called with token: " << request->token()
             << ", comet_id: " << request->comet_id();
    // 检查 Redis 存储是否已初始化
    if (!redis_store_) {
        SetError(response->mutable_error(), 500, "redis store not initialized");
        return ::grpc::Status::OK;
    }
    int64_t uid = 0;
    if (!redis_store_->GetUserIdByToken(request->token(), &uid)) {
        SetError(response->mutable_error(), 401, "invalid or expired token");
        return ::grpc::Status::OK;
    }
    response->set_user_id(uid);
    SetError(response->mutable_error(), 0, "ok");
    if (redis_store_) {
        redis_store_->UpsertUserConnection(
            uid, request->comet_id(), request->conn_id(), connection_ttl_ms_);
    }
    return ::grpc::Status::OK;
}

::grpc::Status LogicServiceImpl::SendUpstreamMessage(
    ::grpc::ServerContext*, const ::sparkpush::UpstreamMessageRequest* request,
    ::sparkpush::UpstreamMessageReply* response) {
    int64_t from_user = request->from_user_id();  //来自谁
    if (from_user <= 0) {
        SetError(response->mutable_error(), 400,
                 "from_user_id must be positive");
        return ::grpc::Status::OK;
    }

    int64_t to_user = request->target_id(); //发给谁
    if (request->target_type() != "single_chat" || to_user <= 0) {
        SetError(response->mutable_error(), 400, "only single_chat supported");
        return ::grpc::Status::OK;
    }

    int64_t user1 = std::min(from_user, to_user);
    int64_t user2 = std::max(from_user, to_user);
    std::string session_id =
        "s_" + std::to_string(user1) + ":" + std::to_string(user2);

    int64_t msg_seq = 0;
    std::string msg_id;
    if (redis_store_ && redis_store_->NextSingleMsgId(user1, user2, &msg_seq)) {
        msg_id = "msgid:" + std::to_string(user1) + ":" +
                 std::to_string(user2) + "-" + std::to_string(msg_seq);
    } else {
        SetError(response->mutable_error(), 500, "alloc msg_seq failed");
        return ::grpc::Status::OK;
    }
    LOG_INFO << "Allocated msg_id " << msg_id << ", session_id " << session_id;
               

    int64_t now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                         std::chrono::system_clock::now().time_since_epoch())
                         .count();

    // 构造 content_json，补充服务端字段并去重同义字段
    std::string final_json = request->content_json(); // web客户端发送过来的json串
    std::string preview = "[unsupported]";
    std::string msg_type =
        request->msg_type().empty() ? "single_chat" : request->msg_type();
    try {
        auto j = nlohmann::json::parse(final_json);
        // 清理客户端同义字段，避免重复
        // j.erase("type");
        // j.erase("to_user_id");  // 第一节课的协议里的
        if (j.contains("timestamp")) {
            try {
                j["client_timestamp_ms"] = j["timestamp"];
            } catch (...) {
            }
            j.erase("timestamp");
        }

        // 规范化字段
        j["msg_id"] = msg_id;   //msgid是服务器分配
        j["msg_seq"] = msg_seq; //msg 序号也是服务器分配的
        j["create_time"] = now_ms; //消息创建时间，服务器时间戳
        j["from_user_id"] = from_user; //发消息的人
        j["target_type"] = "single_chat";
        j["target_id"] = to_user;
        j["msg_type"] = msg_type;
        if (!j.contains("client_msg_id")) {
            j["client_msg_id"] = request->client_msg_id();
        }
        if (j.contains("content") && j["content"].is_string()) {
            preview = j["content"].get<std::string>();
            if (preview.size() > 50) preview = preview.substr(0, 50);
        } else if (j.contains("text") && j["text"].is_string()) {
            preview = j["text"].get<std::string>();
            if (preview.size() > 50) preview = preview.substr(0, 50);
        } else {
            preview = msg_type;
        }
        final_json = j.dump();
    } catch (...) {
    }

    ChatMessage* cm = response->mutable_message();
    cm->set_msg_id(msg_id); //服务器
    cm->set_session_id(session_id); //服务器 会话id
    cm->set_msg_seq(msg_seq); //服务器
    cm->set_sender_id(from_user);
    cm->set_timestamp_ms(now_ms); //服务器时间
    cm->set_msg_type(msg_type);
    cm->set_content_json(final_json);
    cm->set_client_msg_id(request->client_msg_id());
    SetError(response->mutable_error(), 0, "ok");

    if (!push_producer_) {
        LOG_ERROR << "Kafka producer not ready";
        return ::grpc::Status::OK;
    }

    // 维护 Redis 会话与未读计数
    if (redis_store_) {
        redis_store_->AddUserSession(from_user, session_id);
        redis_store_->AddUserSession(to_user, session_id);
        redis_store_->SetSessionLastSeq(session_id, msg_seq);
        redis_store_->IncrUnreadCount(to_user, session_id, 1);
        redis_store_->SetUserSessionMeta(from_user, session_id, msg_id, msg_seq,
                                         msg_type, now_ms, preview);
        redis_store_->SetUserSessionMeta(to_user, session_id, msg_id, msg_seq,
                                         msg_type, now_ms, preview);
    }

    std::unordered_map<std::string, std::vector<int64_t>> comet_to_users;
    std::vector<std::string> comets;
    if (redis_store_ &&
        redis_store_->GetUserConnectionComets(to_user, &comets)) {
        for (const auto& cid : comets) {
            comet_to_users[cid].push_back(to_user);
        }
    }

    // 若无在线目标，仍写入 Kafka 供下游离线处理
    if (comet_to_users.empty()) {
        LOG_WARN << "No online target for message " << msg_id;
        PushToCometRequest req;
        req.set_comet_id("");
        *req.mutable_message() = *cm;
        // 仍然填充 targets，便于下游（job）在 comet_id 缺失时进行广播兜底，
        // 由各 comet 根据本机连接集合决定是否真正下发。
        auto* t = req.add_targets();
        t->set_user_id(to_user);
        std::string payload;
        if (req.SerializeToString(&payload)) {
            push_producer_->Send("", payload);
        }
        return ::grpc::Status::OK;
    }

    for (const auto& kv : comet_to_users) {
        const std::string& comet_id = kv.first;
        const auto& users = kv.second;

        PushToCometRequest req;
        req.set_comet_id(comet_id);
        *req.mutable_message() = *cm;
        for (int64_t uid : users) {
            auto* target = req.add_targets();
            target->set_user_id(uid);
        }

        std::string payload;
        if (!req.SerializeToString(&payload)) {
            LOG_ERROR << "Serialize PushToCometRequest failed";
            continue;
        }
        LOG_INFO << "Send payload to comet " << comet_id
                 << " payload: " << payload;
        if (!push_producer_->Send(comet_id, payload)) {
            LOG_ERROR << "Kafka send failed for comet " << comet_id;
        }
    }

    return ::grpc::Status::OK;
}

::grpc::Status LogicServiceImpl::UserOffline(
    ::grpc::ServerContext*, const ::sparkpush::UserOfflineRequest* request,
    ::sparkpush::SimpleReply* response) {
    if (redis_store_) {
        if (!request->conn_id().empty()) {
            redis_store_->RemoveUserConnection(
                request->user_id(), request->comet_id(), request->conn_id());
        } else {
            // conn_id 为空时无法精确删除 hash field（<comet_id>:<conn_id>）。
            // 依赖 user_connections:* 的 TTL 自动过期兜底，避免回退到旧版
            // route:user:* 结构。
            LOG_WARN
                << "UserOffline without conn_id, skip redis cleanup. user_id="
                << request->user_id() << " comet_id=" << request->comet_id();
        }
    }
    SetError(response->mutable_error(), 0, "ok");
    return ::grpc::Status::OK;
}

}  // namespace sparkpush
