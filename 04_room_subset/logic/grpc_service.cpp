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
                                   RoomStore* room_store, int connection_ttl_ms)
    : user_dao_(user_dao),
      push_producer_(push_producer),
      redis_store_(redis_store),
      room_store_(room_store),
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

// SendUpstreamMessage RPC 实现：处理客户端上行消息（单聊/群聊/房间等）
// - 负责分配 msg_seq/msg_id、维护会话与未读、并投递 Kafka 给 comet/persist
::grpc::Status LogicServiceImpl::SendUpstreamMessage(
    ::grpc::ServerContext*, const ::sparkpush::UpstreamMessageRequest* request,
    ::sparkpush::UpstreamMessageReply* response) {
    int64_t from_user = request->from_user_id();
    if (from_user <= 0) {
        SetError(response->mutable_error(), 400,
                 "from_user_id must be positive");
        return ::grpc::Status::OK;
    }

    if (!redis_store_) {
        SetError(response->mutable_error(), 500, "redis store not initialized");
        return ::grpc::Status::OK;
    }

    const std::string& target_type = request->target_type();
    int64_t target_id = request->target_id();
    if (target_id <= 0) {
        SetError(response->mutable_error(), 400, "target_id must be positive");
        return ::grpc::Status::OK;
    }

    // 统一准备时间戳/消息类型等公共字段
    int64_t now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                         std::chrono::system_clock::now().time_since_epoch())
                         .count();
    std::string msg_type =
        request->msg_type().empty() ? target_type : request->msg_type();

    // ========== 分支1：单聊 ==========
    if (target_type == "single_chat") {
        int64_t to_user = target_id;
        int64_t user1 = std::min(from_user, to_user);
        int64_t user2 = std::max(from_user, to_user);
        std::string session_id =
            "s_" + std::to_string(user1) + ":" + std::to_string(user2);

        int64_t msg_seq = 0;
        std::string msg_id;
        if (redis_store_->NextSingleMsgId(user1, user2, &msg_seq)) {
            msg_id = "msgid:" + std::to_string(user1) + ":" +
                     std::to_string(user2) + "-" + std::to_string(msg_seq);
        } else {
            SetError(response->mutable_error(), 500, "alloc msg_seq failed");
            return ::grpc::Status::OK;
        }

        std::string final_json = request->content_json();
        std::string preview = "[unsupported]";
        try {
            auto j = nlohmann::json::parse(final_json);
            j.erase("type");
            j.erase("to_user_id");
            if (j.contains("timestamp")) {
                try {
                    j["client_timestamp_ms"] = j["timestamp"];
                } catch (...) {
                }
                j.erase("timestamp");
            }
            j["msg_id"] = msg_id;
            j["msg_seq"] = msg_seq;
            j["create_time"] = now_ms;
            j["from_user_id"] = from_user;
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
        cm->set_msg_id(msg_id);
        cm->set_session_id(session_id);
        cm->set_msg_seq(msg_seq);
        cm->set_sender_id(from_user);
        cm->set_timestamp_ms(now_ms);
        cm->set_msg_type(msg_type);
        cm->set_content_json(final_json);
        cm->set_client_msg_id(request->client_msg_id());
        SetError(response->mutable_error(), 0, "ok");

        if (!push_producer_) {
            LOG_ERROR << "Kafka producer not ready";
            return ::grpc::Status::OK;
        }

        // Redis 会话/未读/快照
        redis_store_->AddUserSession(from_user, session_id);
        redis_store_->AddUserSession(to_user, session_id);
        redis_store_->SetSessionLastSeq(session_id, msg_seq);
        int64_t new_unread = 0;
        redis_store_->IncrUnreadCount(to_user, session_id, 1, &new_unread);
        redis_store_->SetUserSessionMeta(from_user, session_id, msg_id, msg_seq,
                                         msg_type, now_ms, preview);
        redis_store_->SetUserSessionMeta(to_user, session_id, msg_id, msg_seq,
                                         msg_type, now_ms, preview);

        // 额外推送会话更新（session_update）：用于前端减少 /unread 轮询
        // 仅推给接收方：包含 unread_count、last_preview、last_time_ms 等。
        ChatMessage session_update;
        session_update.set_msg_id("sessupd:" + session_id + ":" +
                                  std::to_string(msg_seq));
        session_update.set_session_id(session_id);
        session_update.set_msg_seq(msg_seq);
        session_update.set_sender_id(0);
        session_update.set_timestamp_ms(now_ms);
        session_update.set_msg_type("session_update");
        try {
            nlohmann::json j;
            j["type"] = "session_update";
            j["session_id"] = session_id;
            j["peer_user_id"] = to_user == from_user ? 0 : from_user;
            j["unread_count"] = new_unread;
            j["last_preview"] = preview;
            j["last_time_ms"] = now_ms;
            j["last_msg_seq"] = msg_seq;
            session_update.set_content_json(j.dump());
        } catch (...) {
            session_update.set_content_json(
                "{\"type\":\"session_update\",\"session_id\":\"" + session_id +
                "\",\"unread_count\":" + std::to_string(new_unread) + "}");
        }

        std::unordered_map<std::string, std::vector<int64_t>> comet_to_users;
        std::vector<std::string> comets;
        if (redis_store_->GetUserConnectionComets(to_user, &comets)) {
            for (const auto& cid : comets) {
                comet_to_users[cid].push_back(to_user);
            }
        }

        if (comet_to_users.empty()) {
            LOG_WARN << "No online target for message " << msg_id;
            PushToCometRequest req;
            req.set_comet_id("");
            *req.mutable_message() = *cm;
            auto* t = req.add_targets();
            t->set_user_id(to_user);
            std::string payload;
            if (req.SerializeToString(&payload)) {
                push_producer_->Send("", payload);
            }
            // 同样写入离线占位，让 job 广播兜底（targets 带 to_user）
            PushToCometRequest req_u;
            req_u.set_comet_id("");
            *req_u.mutable_message() = session_update;
            auto* tu = req_u.add_targets();
            tu->set_user_id(to_user);
            std::string payload_u;
            if (req_u.SerializeToString(&payload_u)) {
                push_producer_->Send("", payload_u);
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
            if (!push_producer_->Send(comet_id, payload)) {
                LOG_ERROR << "Kafka send failed for comet " << comet_id;
            }

            // 发送 session_update（同一 comet_id，同一 targets）
            PushToCometRequest req2;
            req2.set_comet_id(comet_id);
            *req2.mutable_message() = session_update;
            for (int64_t uid : users) {
                auto* target = req2.add_targets();
                target->set_user_id(uid);
            }
            std::string payload2;
            if (!req2.SerializeToString(&payload2)) {
                LOG_ERROR << "Serialize session_update PushToCometRequest failed";
                continue;
            }
            if (!push_producer_->Send(comet_id, payload2)) {
                LOG_ERROR << "Kafka send failed for comet " << comet_id
                          << " (session_update)";
            }
        }
        return ::grpc::Status::OK;
    }

    // ========== 分支2：聊天室（room） ==========
    if (target_type == "room") {
        int64_t room_id = target_id;

        // 发送前校验：必须是房间成员（避免“随便填 room_id”刷屏）。
        bool is_in = false;
        if (!room_store_ ||
            !room_store_->IsUserInRoom(from_user, room_id, &is_in, nullptr) ||
            !is_in) {
            SetError(response->mutable_error(), 403,
                     "user not in room, join first");
            return ::grpc::Status::OK;
        }

        std::string session_id = "r_" + std::to_string(room_id);
        int64_t msg_seq = 0;
        std::string msg_id;
        // 复用 NextGroupMsgId 做 room 级别的严格递增序号分配（key 为
        // msgid_group:<room_id>）。
        if (redis_store_->NextGroupMsgId(room_id, &msg_seq)) {
            msg_id = "msgid_room:" + std::to_string(room_id) + "-" +
                     std::to_string(msg_seq);
        } else {
            SetError(response->mutable_error(), 500, "alloc msg_seq failed");
            return ::grpc::Status::OK;
        }

        std::string final_json = request->content_json();
        std::string preview = "[room]";
        try {
            auto j = nlohmann::json::parse(final_json);
            j.erase("type");
            if (j.contains("timestamp")) {
                try {
                    j["client_timestamp_ms"] = j["timestamp"];
                } catch (...) {
                }
                j.erase("timestamp");
            }
            // 规范化字段
            j["msg_id"] = msg_id;
            j["msg_seq"] = msg_seq;
            j["create_time"] = now_ms;
            j["from_user_id"] = from_user;
            j["target_type"] = "room";
            j["target_id"] = room_id;
            // 兼容字段：room_id
            j["room_id"] = room_id;
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
        cm->set_msg_id(msg_id);
        cm->set_session_id(session_id);
        cm->set_msg_seq(msg_seq);
        cm->set_sender_id(from_user);
        cm->set_timestamp_ms(now_ms);
        cm->set_msg_type(msg_type);
        cm->set_content_json(final_json);
        cm->set_client_msg_id(request->client_msg_id());
        SetError(response->mutable_error(), 0, "ok");

        if (!push_producer_) {
            LOG_ERROR << "Kafka producer not ready";
            return ::grpc::Status::OK;
        }

        // fanout：拉取房间成员 -> 按用户在线路由聚合到 comet -> 写 Kafka
        std::vector<int64_t> members;
        if (room_store_) {
            room_store_->ListRoomMembers(room_id, &members, nullptr);
        } else {
            members.clear();
        }
        if (members.empty()) {
            // 房间没有任何成员时，直接返回成功（消息不投递）。
            return ::grpc::Status::OK;
        }

        // Redis 会话/未读/快照（可选，但能复用现有 session/unread 逻辑）
        redis_store_->SetSessionLastSeq(session_id, msg_seq);
        for (int64_t uid : members) {
            redis_store_->AddUserSession(uid, session_id);
            if (uid != from_user) {
                redis_store_->IncrUnreadCount(uid, session_id, 1);
            }
            redis_store_->SetUserSessionMeta(uid, session_id, msg_id, msg_seq,
                                             msg_type, now_ms, preview);
        }

        std::unordered_map<std::string, std::vector<int64_t>> comet_to_users;
        for (int64_t uid : members) {
            std::vector<std::string> comets;
            if (!redis_store_->GetUserConnectionComets(uid, &comets)) continue;
            for (const auto& cid : comets) {
                comet_to_users[cid].push_back(uid);
            }
        }

        if (comet_to_users.empty()) {
            // 没有任何在线成员：仍写 Kafka“离线占位”，让 job 按配置广播兜底。
            PushToCometRequest req;
            req.set_comet_id("");
            *req.mutable_message() = *cm;
            for (int64_t uid : members) {
                auto* t = req.add_targets();
                t->set_user_id(uid);
            }
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
            if (!push_producer_->Send(comet_id, payload)) {
                LOG_ERROR << "Kafka send failed for comet " << comet_id;
            }
        }
        return ::grpc::Status::OK;
    }

    SetError(response->mutable_error(), 400, "unsupported target_type");
    return ::grpc::Status::OK;
}

// UserOffline RPC 实现：用户连接断开时的清理/通知入口
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
