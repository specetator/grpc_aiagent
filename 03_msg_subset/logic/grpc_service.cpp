// ============================================================================
// logic 服务 gRPC 接口实现
// ============================================================================
#include "grpc_service.h"

#include <algorithm>
#include <chrono>
#include <nlohmann/json.hpp>

#include "conversation_store.h"
#include "logging.h"

namespace sparkpush {

LogicServiceImpl::LogicServiceImpl(UserDao* user_dao, KafkaProducer* producer,
                                   RedisStore* redis_store,
                                   ConversationStore* conversation_store)
    : user_dao_(user_dao),
      producer_(producer),
      redis_store_(redis_store),
      conversation_store_(conversation_store) {}

void LogicServiceImpl::SetError(ErrorInfo* e, int code,
                                const std::string& msg) {
    LOG_INFO << "SetError called with code: " << std::to_string(code)
             << ", message: " << msg;
    e->set_code(code);
    e->set_message(msg);
}

::grpc::Status LogicServiceImpl::VerifyToken(
    ::grpc::ServerContext*, const ::sparkpush::VerifyTokenRequest* request,
    ::sparkpush::VerifyTokenReply* response) {
    LOG_INFO << "VerifyToken called with token: " << request->token()
             << ", comet_id: " << request->comet_id();
    const std::string& token = request->token();
    if (!redis_store_) {
        SetError(response->mutable_error(), 500, "redis store not initialized");
        return ::grpc::Status::OK;
    }
    int64_t uid = 0;
    if (!redis_store_->GetUserIdByToken(token, &uid)) {
        SetError(response->mutable_error(), 401, "invalid or expired token");
        return ::grpc::Status::OK;
    }
    response->set_user_id(uid);
    SetError(response->mutable_error(), 0, "ok");
    redis_store_->AddRoute(uid, request->comet_id());
    return ::grpc::Status::OK;
}

::grpc::Status LogicServiceImpl::SendUpstreamMessage(
    ::grpc::ServerContext*, const ::sparkpush::UpstreamMessageRequest* request,
    ::sparkpush::UpstreamMessageReply* response) {
    const std::string& scene = request->scene();
    int64_t from_user = request->from_user_id();
    if (from_user <= 0) {
        SetError(response->mutable_error(), 400,
                 "from_user_id must be positive");
        return ::grpc::Status::OK;
    }

    Session session;
    std::unordered_map<std::string, std::vector<int64_t>> comet_to_users;
    std::string err;
    int64_t msg_seq = 0;  // 从 msg_id 提取的严格递增序号（用于排序和未读计数）
    std::string msg_id;
    bool has_custom_msg_id = false;

    // 仅支持单聊，提前构造会话信息
    if (scene != "single") {
        SetError(response->mutable_error(), 400, "only single scene supported");
        return ::grpc::Status::OK;
    }

    // 单聊逻辑
    int64_t to_user = request->to_user_id();
    if (to_user <= 0) {
        SetError(response->mutable_error(), 400, "to_user_id must be positive");
        return ::grpc::Status::OK;
    }
    // 组装单聊会话信息
    session.type = SessionType::kSingle;
    session.user1_id = std::min(from_user, to_user);
    session.user2_id = std::max(from_user, to_user);
    session.id = "s_" + std::to_string(session.user1_id) + "_" +
                 std::to_string(session.user2_id);

    std::vector<std::string> comets;
    if (!redis_store_ || !redis_store_->GetUserRoutes(to_user, &comets)) {
        LOG_ERROR << "GetUserRoutes from redis failed for user "
                  << std::to_string(to_user);
    } else {
        for (const auto& cid : comets) {
            comet_to_users[cid].push_back(to_user);
        }
    }

    // 生成服务端消息ID（包含严格递增序号）
    if (redis_store_) {
        if (redis_store_->NextSingleMsgId(session.user1_id, session.user2_id,
                                          &msg_seq)) {
            msg_id = "msgid:" + std::to_string(session.user1_id) + ":" +
                     std::to_string(session.user2_id) + "-" +
                     std::to_string(msg_seq);
            has_custom_msg_id = true;
        } else {
            LOG_ERROR << "NextSingleMsgId failed for users "
                      << session.user1_id << "," << session.user2_id;
        }
    }

    int64_t now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                         std::chrono::system_clock::now().time_since_epoch())
                         .count();

    // 构造消息内容JSON（补充服务端生成的字段）
    std::string final_json = request->content_json();
    try {
        auto j = nlohmann::json::parse(final_json);
        // 补充服务端生成的 msg_id
        if (has_custom_msg_id) {
            j["msg_id"] = msg_id;
            j["msg_seq"] = msg_seq;  // 从 msg_id 提取的序号
        } else {
            j["msg_id"] = session.id + "-" +
                          std::to_string(session.last_msg_seq + 1);
        }
        j["create_time"] = now_ms;
        j["from_user_id"] = from_user;
        if (!j.contains("client_msg_id")) {
            j["client_msg_id"] = request->client_msg_id();
        }
        if (user_dao_) {
            User u;
            std::string ue;
            if (user_dao_->GetUserById(from_user, &u, &ue)) {
                j["sender"] = {
                    {"uid", from_user},
                    {"name", u.name.empty() ? u.account : u.name}};
            }
        }
        final_json = j.dump();
    } catch (...) {
    }

    // 调用存储层写入消息到 MySQL
    // msg_id 由服务端生成，msg_seq 从 msg_id 提取
    Message msg;
    bool append_ok = false;
    if (!conversation_store_) {
        SetError(response->mutable_error(), 500,
                 "conversation store not initialized");
        return ::grpc::Status::OK;
    }
    append_ok = conversation_store_->AppendMessage(
        session, msg_id, msg_seq, from_user,
        "text", final_json, now_ms, request->client_msg_id(), &msg, &err);

    if (!append_ok) {
        SetError(response->mutable_error(), 500,
                 "append message failed: " + err);
        return ::grpc::Status::OK;
    }

    // 更新消息内容JSON，补充实际的 msg_seq（从 msg_id 提取）
    try {
        auto j = nlohmann::json::parse(final_json);
        j["msg_id"] = msg.msg_id;
        j["msg_seq"] = msg.msg_seq;  // 使用从 msg_id 提取的序号
        msg.content_json = j.dump();
    } catch (...) {
    }

    // 填充响应消息
    ChatMessage* cm = response->mutable_message();
    cm->set_msg_id(msg.msg_id);
    cm->set_session_id(msg.session_id);
    cm->set_msg_seq(msg.msg_seq);  // 从 msg_id 提取的序号
    cm->set_sender_id(msg.sender_id);
    cm->set_timestamp_ms(msg.timestamp_ms);
    cm->set_msg_type(msg.msg_type);

    // 构造推送内容JSON
    std::string push_json = msg.content_json;
    try {
        auto j = nlohmann::json::parse(push_json);
        j["msg_id"] = msg.msg_id;
        j["msg_seq"] = msg.msg_seq;  // 从 msg_id 提取的序号
        j["create_time"] = msg.timestamp_ms;
        j["from_user_id"] = msg.sender_id;
        if (!j.contains("client_msg_id"))
            j["client_msg_id"] = msg.client_msg_id;
        push_json = j.dump();
    } catch (...) {
    }

    cm->set_content_json(push_json);
    cm->set_client_msg_id(msg.client_msg_id);
    SetError(response->mutable_error(), 0, "ok");

    if (!producer_) {
        LOG_ERROR << "Kafka producer not ready";
        return ::grpc::Status::OK;
    }

    if (comet_to_users.empty()) {
        LOG_INFO << "no online targets, skip push";
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
        LOG_INFO << "Sending push to comet " << comet_id
                 << " for " << users.size() << " users, payload: " << payload;
        if (!producer_->Send(comet_id, payload)) {
            LOG_ERROR << "Kafka send failed for comet " << comet_id;
        }
    }

    return ::grpc::Status::OK;
}

::grpc::Status LogicServiceImpl::AckMessage(
    ::grpc::ServerContext*, const ::sparkpush::AckMessageRequest* request,
    ::sparkpush::AckMessageReply* response) {
    LOG_INFO << "AckMessage called, user_id: " << request->user_id()
             << ", msg_seq: " << request->msg_seq()
             << ", session_id: " << request->session_id();

    int64_t user_id = request->user_id();
    int64_t msg_seq = request->msg_seq();
    const std::string& session_id = request->session_id();

    if (user_id <= 0 || msg_seq < 0 || session_id.empty()) {
        SetError(response->mutable_error(), 400,
                 "invalid user_id/msg_seq/session_id");
        return ::grpc::Status::OK;
    }

    // TODO: 实现 ACK 逻辑
    // 1. 可以记录到 Redis 中该用户对该会话的已确认 msg_seq
    // 2. 可以通过 Kafka 推送 ACK 通知给发送方
    // 3. 可以用于重传机制的确认

    // 当前简化实现：仅记录日志
    LOG_INFO << "Message ACK confirmed: user=" << user_id
             << ", session=" << session_id << ", msg_seq=" << msg_seq;

    SetError(response->mutable_error(), 0, "ok");
    return ::grpc::Status::OK;
}

::grpc::Status LogicServiceImpl::UserOffline(
    ::grpc::ServerContext*, const ::sparkpush::UserOfflineRequest* request,
    ::sparkpush::SimpleReply* response) {
    if (redis_store_) {
        redis_store_->RemoveRoute(request->user_id(), request->comet_id());
    }
    SetError(response->mutable_error(), 0, "ok");
    return ::grpc::Status::OK;
}

}  // namespace sparkpush
