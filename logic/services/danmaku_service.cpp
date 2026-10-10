#include "services/danmaku_service.h"

#include <algorithm>
#include <chrono>
#include <sstream>
#include <unordered_map>

#include "conversation_store.h"
#include "danmaku_dao.h"
#include "group_dao.h"
#include "kafka_producer.h"
#include "logging.h"
#include "redis_store.h"
#include "spark_push.pb.h"

namespace sparkpush {
ServiceStatus DanmakuService::SendAvailable() const {
    if (!store_) return {500, "conversation store not initialized"};
    if (!producer_) return {500, "group kafka producer not initialized"};
    return {};
}
DanmakuResult DanmakuService::Send(const std::string& token,
                                   const std::string& video_id, int64_t timeline_ms,
                                   const std::string& text) {
    DanmakuResult result;
    result.status = SendAvailable();
    if (!result.status.ok()) return result;
    int64_t user_id = 0;
    // 发送弹幕必须是已登录用户：通过 token 在 Redis 中反查 user_id
    if (token.empty()) {
        result.status = {401, "token required for danmaku send"};
        return result;
    }
    if (!redis_) {
        result.status = {500, "redis store not initialized"};
        return result;
    }
    int64_t token_user_id = 0;
    if (!redis_->GetUserIdByToken(token, &token_user_id) || token_user_id <= 0) {
        result.status = {401, "invalid token"};
        return result;
    }
    user_id = token_user_id;

    if (user_id <= 0 || video_id.empty() || text.empty()) {
        result.status = {400, "user_id/video_id/text required"};
        return result;
    }

    // 对 demo 而言，用固定的 room_id 承载该视频的弹幕聊天室。
    // 如果未来支持多视频，可约定 video_id 与 room_id 的映射策略。
    const int64_t kDanmakuRoomId = 1001;

    // 构造下行给前端的 JSON 内容
    std::ostringstream content_oss;
    content_oss << "{\"type\":\"danmaku\"" << ",\"video_id\":\"" << video_id << "\""
                << ",\"timeline_ms\":" << timeline_ms << ",\"content\":{\"text\":\""
                << text << "\"}}";
    std::string content_json = content_oss.str();

    // 写入 MySQL 的 video_danmaku 表（如果已配置）
    if (dao_) {
        int64_t row_id = 0;
        std::string err;
        if (!dao_->InsertDanmaku(video_id, timeline_ms, user_id, content_json, &row_id,
                                 &err)) {
            // 中：写库失败只打日志，不中断实时推送
            LOG_ERROR << "InsertDanmaku failed: " << err;
        }
    }

    // 写入内存模型（会话 & 消息），并更新 Redis 中的 last_seq
    Session session;
    std::string err;
    if (!store_->GetOrCreateRoomSession(kDanmakuRoomId, &session, &err)) {
        result.status = {500, "get room session failed: " + err};
        return result;
    }
    int64_t now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                         std::chrono::system_clock::now().time_since_epoch())
                         .count();
    Message msg;
    if (!store_->AppendMessage(session, user_id, "danmaku", content_json, now_ms,
                               "",  // client_msg_id (HTTP 暂不透传)
                               &msg, &err)) {
        result.status = {500, "append danmaku failed: " + err};
        return result;
    }

    result.message = msg;

    // 构造 ChatMessage
    ChatMessage cm;
    cm.set_msg_id(msg.msg_id);
    cm.set_session_id(msg.session_id);
    cm.set_msg_seq(msg.msg_seq);
    cm.set_sender_id(msg.sender_id);
    cm.set_timestamp_ms(msg.timestamp_ms);
    cm.set_msg_type(msg.msg_type);
    cm.set_content_json(msg.content_json);

    // 计算需要推送到哪些 comet
    std::unordered_map<std::string, std::vector<int64_t>> comet_to_users;
    std::vector<std::string> room_comets;
    bool use_room_comets = false;
    if (redis_ && redis_->GetRoomComets(kDanmakuRoomId, &room_comets) &&
        !room_comets.empty()) {
        use_room_comets = true;
    }

    if (use_room_comets) {
        // 仅按 comet 维度 fanout，targets 留空，由 comet 自己按房间成员二次分发
        for (const auto& cid : room_comets) {
            comet_to_users[cid];  // 占位
        }
    } else {
        // 回退：根据房间成员+用户路由做精确 fanout
        std::vector<int64_t> members;
        if (members_) {
            if (!members_->ListRoomMembers(kDanmakuRoomId, &members, &err)) {
                LOG_ERROR << "ListRoomMembers(danmaku) failed: " << err;
            }
        }
        for (int64_t uid : members) {
            std::vector<std::string> comets;
            if (!redis_ || !redis_->GetUserRoutes(uid, &comets)) {
                continue;
            }
            for (const auto& cid : comets) {
                comet_to_users[cid].push_back(uid);
            }
        }
    }

    if (comet_to_users.empty()) {
        // 没有在线用户，返回成功但不推送
        return result;
    }

    // 通过 Kafka 将 PushToCometRequest 写入 push topic
    for (const auto& kv : comet_to_users) {
        const std::string& comet_id = kv.first;
        const auto& users = kv.second;

        PushToCometRequest req_pb;
        req_pb.set_comet_id(comet_id);
        req_pb.set_scene("group");
        *req_pb.mutable_message() = cm;
        for (int64_t uid : users) {
            auto* t = req_pb.add_targets();
            t->set_user_id(uid);
        }

        std::string payload;
        if (!req_pb.SerializeToString(&payload)) {
            LOG_ERROR << "Serialize PushToCometRequest(danmaku) failed";
            continue;
        }
        LOG_INFO << "Sending danmaku to comet " << comet_id << " for " << users.size()
                 << " users, payload_bytes=" << payload.size();
        if (!producer_->Send(comet_id, payload)) {
            LOG_ERROR << "Kafka send danmaku failed for comet " << comet_id;
        }
    }

    result.pushed = true;
    return result;
}
ServiceStatus DanmakuService::List(const std::string& video, int64_t from, int64_t to,
                                   int limit, std::vector<DanmakuItem>* result) {
    if (video.empty()) return {400, "video_id cannot be empty"};
    if (!dao_) return {500, "danmaku dao not initialized"};
    std::string error;
    if (!result || !dao_->ListDanmaku(video, result, &error, std::max<int64_t>(0, from),
                                      std::max<int64_t>(0, to), limit))
        return {500, "list danmaku failed: " + error};
    return {};
}
}  // namespace sparkpush
