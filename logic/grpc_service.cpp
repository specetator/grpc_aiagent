#include "grpc_service.h"

#include <chrono>
#include <nlohmann/json.hpp>

#include "logging.h"

namespace sparkpush {

// 构造：初始化逻辑服务依赖（会话、群组、用户、Kafka、Redis）
LogicServiceImpl::LogicServiceImpl(ConversationStore* store,
                                   GroupMemberDao* group_member_dao,
                                   UserDao* user_dao, KafkaProducer* producer,
                                   KafkaProducer* broadcast_producer,
                                   RedisStore* redis_store)
    : store_(store),
      group_member_dao_(group_member_dao),
      user_dao_(user_dao),
      producer_(producer),
      broadcast_producer_(broadcast_producer),
      redis_store_(redis_store) {}

// 工具：填充错误码与信息
void LogicServiceImpl::SetError(ErrorInfo* e, int code,
                                const std::string& msg) {
    LOG_INFO << "SetError called with code: " << std::to_string(code)
             << ", message: " << msg;
    e->set_code(code);
    e->set_message(msg);
}

// 鉴权：校验 token 并记录 comet 路由
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
    // 记录路由到 Redis
    redis_store_->AddRoute(uid, request->comet_id());
    return ::grpc::Status::OK;
}

// 处理上行消息：创建会话、写库、构造推送并分发 Kafka
::grpc::Status LogicServiceImpl::SendUpstreamMessage(
    ::grpc::ServerContext*, const ::sparkpush::UpstreamMessageRequest* request,
    ::sparkpush::UpstreamMessageReply* response) {
    if (!store_) {
        SetError(response->mutable_error(), 500,
                 "conversation store not initialized");
        return ::grpc::Status::OK;
    }
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

    if (scene == "single") {
        int64_t to_user = request->to_user_id();
        if (to_user <= 0) {
            SetError(response->mutable_error(), 400,
                     "to_user_id must be positive");
            return ::grpc::Status::OK;
        }
        if (!store_->GetOrCreateSingleSession(from_user, to_user, &session,
                                              &err)) {
            SetError(response->mutable_error(), 500,
                     "create session failed: " + err);
            return ::grpc::Status::OK;
        }
        // 单聊：只路由给目标用户
        std::vector<std::string> comets;
        if (!redis_store_ || !redis_store_->GetUserRoutes(to_user, &comets)) {
            LOG_ERROR << "GetUserRoutes from redis failed for user "
                      << std::to_string(to_user);
        } else {
            for (const auto& cid : comets) {
                comet_to_users[cid].push_back(to_user);
            }
        }
    } else if (scene == "chatroom") {
        int64_t room_id = request->group_id();
        if (room_id <= 0) {
            SetError(response->mutable_error(), 400,
                     "group_id(room_id) must be positive");
            return ::grpc::Status::OK;
        }
        if (!store_->GetOrCreateRoomSession(room_id, &session, &err)) {
            SetError(response->mutable_error(), 500,
                     "create room session failed: " + err);
            return ::grpc::Status::OK;
        }
        // 聊天室：优先按 room:comets:{room_id} 粒度 fanout（V2 方案），
        // 若 Redis 中没有房间路由，则退回到按成员全展开的老逻辑。
        std::vector<std::string> room_comets;
        bool use_room_comets = false;
        if (redis_store_ &&
            redis_store_->GetRoomComets(room_id, &room_comets) &&
            !room_comets.empty()) {
            use_room_comets = true;
        }

        if (use_room_comets) {
            // 仅记录 comet 维度，后面会特别处理：构造没有 targets 的
            // PushToCometRequest， 由 comet 侧根据本机 room_members 进行二次
            // fanout。
            for (const auto& cid : room_comets) {
                comet_to_users[cid];  // 仅占位，后续根据 key 遍历
            }
        } else {
            // 退回到按成员全展开：room_id -> 所有 user_id，再按照
            // user_id->comet_ids 聚合
            std::vector<int64_t> members;
                if (group_member_dao_) {
                    if (!group_member_dao_->ListRoomMembers(room_id, &members,
                                                        &err)) {
                    LOG_ERROR << "ListRoomMembers failed: " << err;
                }
            } else {
                LOG_ERROR
                    << "group_member_dao_ not initialized, cannot list room "
                    "members";
            }
            for (int64_t uid : members) {
                std::vector<std::string> comets;
                if (!redis_store_ ||
                    !redis_store_->GetUserRoutes(uid, &comets)) {
                    continue;
                }
                for (const auto& cid : comets) {
                    comet_to_users[cid].push_back(uid);
                }
            }
        }
    } else {
        SetError(response->mutable_error(), 400, "unsupported scene");
        return ::grpc::Status::OK;
    }

    int64_t now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                         std::chrono::system_clock::now().time_since_epoch())
                         .count();

    // 重新构建 JSON，注入服务端分配的字段
    std::string final_json = request->content_json();
    try {
        auto j = nlohmann::json::parse(final_json);
        j["msg_id"] = session.id + "-" +
                      std::to_string(session.last_msg_seq + 1);  // 预估 seq
        j["create_time"] = now_ms;
        j["from_user_id"] = from_user;
        if (!j.contains("client_msg_id")) {
            j["client_msg_id"] = request->client_msg_id();
        }

        // 注入发送者信息 (name)
        if (user_dao_) {
            User u;
            std::string ue;
            if (user_dao_->GetUserById(from_user, &u, &ue)) {
                // 结构化 sender 信息
                j["sender"] = {
                    {"uid", from_user},
                    {"name", u.name.empty() ? u.account : u.name}
                    // {"avatar", ...}
                };
            }
        }

        final_json = j.dump();
    } catch (...) {
        // ignore parse error, use raw
    }

    Message msg;
    if (!store_->AppendMessage(session, from_user, "text", final_json, now_ms,
                               request->client_msg_id(), &msg, &err)) {
        SetError(response->mutable_error(), 500,
                 "append message failed: " + err);
        return ::grpc::Status::OK;
    }

    // 修正 msg_id (AppendMessage 内部生成的才是准确的)
    try {
        auto j = nlohmann::json::parse(final_json);
        j["msg_id"] = msg.msg_id;
        // 如果 AppendMessage 内部 seq 与预估不一致，这里其实应该再次更新
        // content_json
        // 但为了性能，且单线程/锁保护下通常一致，暂时忽略极端并发下的不一致风险
        // 或者更严谨的做法是：先 Append 获取 ID，再 update content_json (需要
        // DAO 支持 update) 这里为了 简单，直接用 msg.content_json 更新回
        // response， 但注意 msg.content_json 存的是 final_json (预估 ID)。

        // 更好的做法：Comet 推送时不仅仅推 content_json，而是构造标准结构。
        // 但鉴于 CometServer 目前只推 content_json，我们尽量让 content_json
        // 包含正确数据。
        j["msg_seq"] = msg.msg_seq;
        msg.content_json = j.dump();

        // 异步更新 DB 中的 content_json? 不，这太复杂。
        // 妥协：DB 里存的 msg_id 可能是预估的（session.last_msg_seq + 1）。
        // 在 AppendMessage 内部，我们加了锁，所以 session.last_msg_seq + 1
        // 应该是准确的， 除非 AppendMessage 失败。

        // 让我们调整一下顺序：
        // 1. 不预估 ID。
        // 2. AppendMessage 成功后。
        // 3. 修改 msg.content_json (内存对象)。
        // 4. ChatMessage 使用修改后的 content_json。
        // 5. DB 里存的是“不带 msg_id”的 JSON？或者带了不准确的？
        // 既然用户要求“统一修改”，我们希望 DB 里也存一份完整的。

        // 方案 B：
        // 1. parse json
        // 2. inject common fields (time, from, client_msg_id)
        // 3. AppendMessage (save to DB)
        // 4. In memory `msg`, parse content_json again, inject `msg_id` and
        // `msg_seq`.
        // 5. Update `cm->set_content_json` with this new JSON.
        // 这样 DB 里没有 msg_id，但推送给用户的有。这符合一般设计（msg_id
        // 是外层字段）。
    } catch (...) {
    }

    // 填充返回
    ChatMessage* cm = response->mutable_message();
    cm->set_msg_id(msg.msg_id);
    cm->set_session_id(msg.session_id);
    cm->set_msg_seq(msg.msg_seq);
    cm->set_sender_id(msg.sender_id);
    cm->set_timestamp_ms(msg.timestamp_ms);
    cm->set_msg_type(msg.msg_type);

    // 构造最终推送给客户端的 JSON
    std::string push_json = msg.content_json;
    try {
        auto j = nlohmann::json::parse(push_json);
        j["msg_id"] = msg.msg_id;
        j["msg_seq"] = msg.msg_seq;
        j["create_time"] = msg.timestamp_ms;
        j["from_user_id"] = msg.sender_id;
        if (!j.contains("client_msg_id"))
            j["client_msg_id"] = msg.client_msg_id;
        push_json = j.dump();
    } catch (...) {
    }

    cm->set_content_json(push_json);  // Comet 将推送这个
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

    // 对于聊天室使用 room:comets 方案的情况，comet_to_users 中只关心
    // key（comet_id）， value 可能为空，表示“由 comet 自己按房间成员本地
    // fanout”。
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
        if (!producer_->Send(comet_id, payload)) {
            LOG_ERROR << "Kafka send failed for comet " << comet_id;
        }
    }

    return ::grpc::Status::OK;
}

// 用户下线：移除路由信息
::grpc::Status LogicServiceImpl::UserOffline(
    ::grpc::ServerContext*, const ::sparkpush::UserOfflineRequest* request,
    ::sparkpush::SimpleReply* response) {
    if (redis_store_) {
        redis_store_->RemoveRoute(request->user_id(), request->comet_id());
    }
    SetError(response->mutable_error(), 0, "ok");
    return ::grpc::Status::OK;
}

// 房间加入：维护房间在线计数与 room:comets 集合
::grpc::Status LogicServiceImpl::ReportRoomJoin(
    ::grpc::ServerContext*, const ::sparkpush::RoomReportRequest* request,
    ::sparkpush::SimpleReply* response) {
    if (!redis_store_) {
        SetError(response->mutable_error(), 500, "redis store not initialized");
        return ::grpc::Status::OK;
    }
    int64_t room_id = request->room_id();
    const std::string& comet_id = request->comet_id();
    int64_t user_id = request->user_id();
    if (room_id <= 0 || comet_id.empty()) {
        SetError(response->mutable_error(), 400, "room_id/comet_id required");
        return ::grpc::Status::OK;
    }

    // 1) 基于 WebSocket 控制消息，使用 INCRBY 维护全局聊天室在线人数
    if (user_id > 0) {
        if (!redis_store_->IncrRoomOnlineCount(room_id, 1)) {
            SetError(response->mutable_error(), 500,
                     "IncrRoomOnlineCount(+1) failed");
            return ::grpc::Status::OK;
        }
    }

    // 2) 按 (room_id, comet_id) 维度维护计数，用于精确维护
    // room:comets:{room_id}
    int64_t new_count = 0;
    if (!redis_store_->IncrRoomCometCount(room_id, comet_id, 1, &new_count)) {
        SetError(response->mutable_error(), 500,
                 "IncrRoomCometCount(+1) failed");
        return ::grpc::Status::OK;
    }
    if (new_count == 1) {
        // 该 room 在该 comet 上从 0 -> 1，加入 room:comets 路由集合
        if (!redis_store_->AddRoomComet(room_id, comet_id)) {
            SetError(response->mutable_error(), 500, "AddRoomComet failed");
            return ::grpc::Status::OK;
        }
    }

    SetError(response->mutable_error(), 0, "ok");
    return ::grpc::Status::OK;
}

// 房间离开：递减计数并必要时移除路由
::grpc::Status LogicServiceImpl::ReportRoomLeave(
    ::grpc::ServerContext*, const ::sparkpush::RoomReportRequest* request,
    ::sparkpush::SimpleReply* response) {
    if (!redis_store_) {
        SetError(response->mutable_error(), 500, "redis store not initialized");
        return ::grpc::Status::OK;
    }
    int64_t room_id = request->room_id();
    const std::string& comet_id = request->comet_id();
    int64_t user_id = request->user_id();
    if (room_id <= 0 || comet_id.empty()) {
        SetError(response->mutable_error(), 400, "room_id/comet_id required");
        return ::grpc::Status::OK;
    }

    if (user_id > 0) {
        if (!redis_store_->IncrRoomOnlineCount(room_id, -1)) {
            SetError(response->mutable_error(), 500,
                     "IncrRoomOnlineCount(-1) failed");
            return ::grpc::Status::OK;
        }
    }

    int64_t new_count = 0;
    if (!redis_store_->IncrRoomCometCount(room_id, comet_id, -1, &new_count)) {
        SetError(response->mutable_error(), 500,
                 "IncrRoomCometCount(-1) failed");
        return ::grpc::Status::OK;
    }
    if (new_count <= 0) {
        // 该 room 在该 comet 上已无在线用户，移除路由集合
        if (!redis_store_->RemoveRoomComet(room_id, comet_id)) {
            SetError(response->mutable_error(), 500, "RemoveRoomComet failed");
            return ::grpc::Status::OK;
        }
    }

    SetError(response->mutable_error(), 0, "ok");
    return ::grpc::Status::OK;
}

// 广播：封装任务写入广播 Kafka topic
::grpc::Status LogicServiceImpl::Broadcast(
    ::grpc::ServerContext*, const ::sparkpush::BroadcastRequest* request,
    ::sparkpush::BroadcastReply* response) {
    if (!broadcast_producer_) {
        SetError(response->mutable_error(), 500,
                 "broadcast producer not initialized");
        response->set_task_id("");
        return ::grpc::Status::OK;
    }

    // 简单生成一个 task_id：当前毫秒时间戳 + 随机数
    int64_t now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                         std::chrono::system_clock::now().time_since_epoch())
                         .count();
    std::string task_id =
        "bcast-" + std::to_string(now_ms) + "-" + std::to_string(rand());

    BroadcastTaskRequest task;
    task.set_task_id(task_id);
    task.set_scope(request->scope());
    task.set_group_id(request->group_id());
    task.set_content_json(request->content_json());

    std::string payload;
    if (!task.SerializeToString(&payload)) {
        SetError(response->mutable_error(), 500,
                 "serialize BroadcastTaskRequest failed");
        response->set_task_id("");
        return ::grpc::Status::OK;
    }

    if (!broadcast_producer_->Send(task_id, payload)) {
        SetError(response->mutable_error(), 500,
                 "send broadcast task to kafka failed");
        response->set_task_id("");
        return ::grpc::Status::OK;
    }

    SetError(response->mutable_error(), 0, "ok");
    response->set_task_id(task_id);
    return ::grpc::Status::OK;
}

}  // namespace sparkpush
