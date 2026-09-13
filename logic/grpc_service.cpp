#include "grpc_service.h"

#include <chrono>
#include <algorithm>
#include <nlohmann/json.hpp>

#include "citation_validation.h"
#include "hermes_reply.h"
#include "logging.h"
#include "metrics.h"

namespace sparkpush {

namespace {

std::string ExtractTextFromContentJson(const std::string& content_json) {
    if (content_json.empty()) return {};
    try {
        const auto value = nlohmann::json::parse(content_json);
        if (value.is_string()) return value.get<std::string>();
        if (!value.is_object() || !value.contains("content")) return {};
        const auto& content = value.at("content");
        if (content.is_string()) return content.get<std::string>();
        if (content.is_object() && content.contains("text") &&
            content.at("text").is_string()) {
            return content.at("text").get<std::string>();
        }
    } catch (const nlohmann::json::exception&) {
    }
    return {};
}

void ExtractHermesEntryMetadata(const std::string& content_json,
                                HermesConversationEntry* entry) {
    if (!entry || !entry->assistant || content_json.empty()) return;
    try {
        const auto value = nlohmann::json::parse(content_json);
        if (!value.is_object() || !value.contains("content") ||
            !value.at("content").is_object()) {
            return;
        }
        const auto& content = value.at("content");
        if (content.contains("hermes_context_start_seq") && content["hermes_context_start_seq"].is_number_integer()) {
            entry->context_start_seq = content["hermes_context_start_seq"].get<int64_t>();
            entry->has_context_state = entry->context_start_seq >= 0;
        }
        entry->command_reply =
            content.value("source", "") == "hermes_command";
        // Compatibility only for the exact successful legacy /new reply.
        // New cards and failed/preview commands never infer a boundary from text.
        entry->legacy_new_confirmation = entry->command_reply && !entry->has_context_state &&
            !content.contains("agent_event") && content.value("hermes_command", "") == "new" &&
            entry->text.rfind("已开启新的 Pi Agent 上下文。", 0) == 0;
        if (content.value("hermes_model_state_confirmed", true) &&
            content.contains("hermes_model_override") &&
            content.at("hermes_model_override").is_boolean()) {
            entry->has_model_state = true;
            entry->model_override =
                content.at("hermes_model_override").get<bool>();
            entry->model = content.value("hermes_model", "");
            entry->provider = content.value("hermes_provider", "");
        }
    } catch (const nlohmann::json::exception&) {
    }
}

}  // namespace

// 构造：初始化逻辑服务依赖；持久化由独立 Kafka topic 承担。
LogicServiceImpl::LogicServiceImpl(ConversationStore* store,
                                   GroupMemberDao* group_member_dao,
                                   UserDao* user_dao, KafkaProducer* producer,
                                   KafkaProducer* group_producer,
                                   KafkaProducer* broadcast_producer,
                                   KafkaProducer* persist_producer,
                                   RedisStore* redis_store,
                                   const RateLimitConfig& rate_limit,
                                   int persist_kafka_timeout_ms,
                                   KafkaProducer* hermes_request_producer,
                                   bool hermes_enabled,
                                   int64_t hermes_bot_user_id,
                                   std::map<int64_t, std::string> agent_bot_users)
    : store_(store),
      group_member_dao_(group_member_dao),
      user_dao_(user_dao),
      producer_(producer),
      group_producer_(group_producer),
      broadcast_producer_(broadcast_producer),
      persist_producer_(persist_producer),
      hermes_request_producer_(hermes_request_producer),
      redis_store_(redis_store),
      rate_limiter_(rate_limit),
      persist_kafka_timeout_ms_(persist_kafka_timeout_ms),
      hermes_enabled_(hermes_enabled),
      hermes_bot_user_id_(hermes_bot_user_id), agent_bot_users_(std::move(agent_bot_users)) {
    agent_bot_users_[hermes_bot_user_id] = kHermesBotName;
}

// 工具：填充错误码与信息
void LogicServiceImpl::SetError(ErrorInfo* e, int code,
                                const std::string& msg) {
    if (code != 0) {
        LOG_WARN << "Logic error code=" << std::to_string(code)
                 << ", message=" << msg;
    }
    e->set_code(code);
    e->set_message(msg);
}

bool LogicServiceImpl::GetActiveUser(int64_t user_id, User* user,
                                     std::string* err) {
    if (!user || !user_dao_ || user_id <= 0) {
        if (err) *err = "invalid active user lookup arguments";
        return false;
    }
    const auto now = std::chrono::steady_clock::now();
    {
        std::lock_guard<std::mutex> lock(user_cache_mu_);
        const auto it = active_user_cache_.find(user_id);
        if (it != active_user_cache_.end() && it->second.expires_at > now) {
            *user = it->second.user;
            MetricsRegistry::Instance().Increment(
                "spark_push_user_active_cache_hit_total");
            return user->status == kUserStatusActive && user->deleted_at.empty();
        }
        if (it != active_user_cache_.end()) active_user_cache_.erase(it);
    }

    User loaded;
    if (!user_dao_->GetUserById(user_id, &loaded, err)) {
        MetricsRegistry::Instance().Increment(
            "spark_push_user_active_cache_miss_total");
        return false;
    }
    MetricsRegistry::Instance().Increment(
        "spark_push_user_active_cache_miss_total");
    if (loaded.status != kUserStatusActive || !loaded.deleted_at.empty()) {
        return false;
    }
    {
        std::lock_guard<std::mutex> lock(user_cache_mu_);
        active_user_cache_[user_id] = UserCacheEntry{
            loaded, now + std::chrono::seconds(2)};
    }
    *user = std::move(loaded);
    return true;
}

// 鉴权：校验 token 并记录 comet 路由
::grpc::Status LogicServiceImpl::VerifyToken(
    ::grpc::ServerContext*, const ::sparkpush::VerifyTokenRequest* request,
    ::sparkpush::VerifyTokenReply* response) {
    LOG_INFO << "VerifyToken called for comet_id=" << request->comet_id();
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
    // Token 只是 Redis 中的会话凭证，最终用户状态仍以 MySQL 为准。
    // 这样管理员禁用/软删除后，旧 token 即使因缓存或连接尚未断开，也不能再次
    // 完成新的 WebSocket 鉴权。
    if (!user_dao_ || uid == kAdminUserId || agent_bot_users_.count(uid)) {
        SetError(response->mutable_error(), 403, "user token is not allowed");
        return ::grpc::Status::OK;
    }
    User user;
    std::string user_error;
    if (!user_dao_->GetUserById(uid, &user, &user_error) ||
        user.status != kUserStatusActive || !user.deleted_at.empty()) {
        SetError(response->mutable_error(), 403,
                 "user account is disabled or deleted");
        return ::grpc::Status::OK;
    }
    response->set_user_id(uid);
    SetError(response->mutable_error(), 0, "ok");
    // 记录路由到 Redis
    redis_store_->AddRoute(uid, request->comet_id());
    return ::grpc::Status::OK;
}

::grpc::Status LogicServiceImpl::SendUpstreamMessage(
    ::grpc::ServerContext*, const ::sparkpush::UpstreamMessageRequest* request,
    ::sparkpush::UpstreamMessageReply* response) {
    HandleUpstreamMessage(*request, response);
    return ::grpc::Status::OK;
}

void LogicServiceImpl::FillChatMessage(const Message& message,
                                       ChatMessage* output) {
    if (!output) return;
    output->set_msg_id(message.msg_id);
    output->set_session_id(message.session_id);
    output->set_msg_seq(message.msg_seq);
    output->set_sender_id(message.sender_id);
    output->set_timestamp_ms(message.timestamp_ms);
    output->set_msg_type(message.msg_type);
    output->set_content_json(message.content_json);
    output->set_client_msg_id(message.client_msg_id);
}

bool LogicServiceImpl::PersistToTopic(const Message& message,
                                      const std::string& scene, int64_t user1,
                                      int64_t user2, int64_t room_id,
                                      std::string* err) {
    if (!persist_producer_) {
        if (err) *err = "persist Kafka producer not initialized";
        return false;
    }
    PersistMessageRequest request;
    FillChatMessage(message, request.mutable_message());
    request.set_scene(scene);
    request.set_user1_id(user1);
    request.set_user2_id(user2);
    request.set_room_id(room_id);
    std::string payload;
    if (!request.SerializeToString(&payload)) {
        if (err) *err = "serialize PersistMessageRequest failed";
        return false;
    }
    if (!persist_producer_->SendAndWait(message.session_id, payload,
                                        persist_kafka_timeout_ms_)) {
        if (err) *err = "persist topic delivery failed";
        return false;
    }
    MetricsRegistry::Instance().Increment(
        "spark_push_persist_events_accepted_total");
    return true;
}

void LogicServiceImpl::RememberHermesMessage(const Message& message) {
    if (message.session_id.empty() || message.msg_id.empty()) return;
    std::lock_guard<std::mutex> lock(hermes_cache_mu_);
    auto it = hermes_history_cache_.find(message.session_id);
    if (it == hermes_history_cache_.end()) {
        if (hermes_history_cache_.size() >= 1024) {
            hermes_history_cache_.erase(hermes_history_cache_.begin());
        }
        it = hermes_history_cache_
                 .emplace(message.session_id, std::deque<Message>{})
                 .first;
    }
    for (auto& cached : it->second) {
        if (cached.msg_id == message.msg_id) {
            cached = message;
            return;
        }
    }
    it->second.push_back(message);
    while (it->second.size() > 100) it->second.pop_front();
}

bool LogicServiceImpl::IsHermesStreamCompleted(const std::string& request_id) {
    if (request_id.empty()) return false;
    std::lock_guard<std::mutex> lock(hermes_stream_mu_);
    return hermes_stream_completed_.find(request_id) !=
           hermes_stream_completed_.end();
}

void LogicServiceImpl::MarkHermesStreamCompleted(const std::string& request_id) {
    if (request_id.empty()) return;
    std::lock_guard<std::mutex> lock(hermes_stream_mu_);
    if (!hermes_stream_completed_.insert(request_id).second) return;
    hermes_stream_completed_order_.push_back(request_id);
    constexpr size_t kCompletedLimit = 100000;
    while (hermes_stream_completed_order_.size() > kCompletedLimit) {
        hermes_stream_completed_.erase(hermes_stream_completed_order_.front());
        hermes_stream_completed_order_.pop_front();
    }
}

// 返回 true 表示该增量已经见过或其最终消息已经处理；返回 false 表示本次
// 首次登记。增量是体验数据，允许在进程重启后丢失，最终 ai_reply 仍可补齐。
bool LogicServiceImpl::MarkHermesDeltaSeen(const std::string& delta_id) {
    if (delta_id.empty()) return true;
    std::lock_guard<std::mutex> lock(hermes_stream_mu_);
    if (hermes_stream_completed_.find(delta_id) !=
        hermes_stream_completed_.end()) {
        return true;
    }
    if (!hermes_delta_seen_.insert(delta_id).second) return true;
    hermes_delta_seen_order_.push_back(delta_id);
    constexpr size_t kDeltaLimit = 200000;
    while (hermes_delta_seen_order_.size() > kDeltaLimit) {
        hermes_delta_seen_.erase(hermes_delta_seen_order_.front());
        hermes_delta_seen_order_.pop_front();
    }
    return false;
}

bool LogicServiceImpl::BuildHermesRequest(const Message& current,
                                          nlohmann::json* request,
                                          std::string* err) {
    if (!request) {
        if (err) *err = "Hermes request output is null";
        return false;
    }
    int64_t bot_user_id = 0;
    for (const auto& bot : agent_bot_users_) {
        const auto expected = "s_" + std::to_string(std::min(current.sender_id, bot.first)) + "_" +
                              std::to_string(std::max(current.sender_id, bot.first));
        if (current.session_id == expected) { bot_user_id = bot.first; break; }
    }
    if (!bot_user_id) { if (err) *err = "invalid Agent conversation"; return false; }

    // ConversationStore::GetHistory 已将 MessageDao 的倒序查询翻转为
    // msg_seq 升序。按这个顺序交给 Hermes，保证多轮上下文与会话顺序一致。
    // 当前消息可能尚未被 Job 落盘，因此最后显式补入 current。
    std::vector<Message> history;
    std::string history_error;
    if (store_->GetHistory(current.session_id, 0, 50, &history,
                           &history_error)) {
    } else {
        // 历史查询失败不应悄悄伪装成“无历史”。当前请求仍可发送，
        // 但把原因记录到日志，方便区分首次对话与数据库异常。
        LOG_WARN << "Build Hermes history failed for session "
                 << current.session_id << ": " << history_error;
    }

    // 当前消息可能已经被 Job 竞争性地写入 MySQL；它会在函数末尾显式追加，
    // 所以先按 msg_id/client_msg_id 排除，避免同一轮用户输入被放进 Prompt 两次。
    const auto same_message = [](const Message& lhs, const Message& rhs) {
        return (!lhs.msg_id.empty() && lhs.msg_id == rhs.msg_id) ||
               (!lhs.client_msg_id.empty() &&
                lhs.client_msg_id == rhs.client_msg_id);
    };
    history.erase(
        std::remove_if(history.begin(), history.end(),
                       [&](const Message& item) {
                           return same_message(item, current);
                       }),
        history.end());

    // Job 的 persist consumer 可能还没把上一轮消息写入 MySQL。将本进程
    // 最近的 Hermes 消息与 MySQL 历史按 msg_id/client_msg_id 合并，覆盖这个
    // 短暂窗口；进程重启后仍以 MySQL 为事实来源。
    {
        std::lock_guard<std::mutex> lock(hermes_cache_mu_);
        const auto cache_it = hermes_history_cache_.find(current.session_id);
        if (cache_it != hermes_history_cache_.end()) {
            for (const auto& cached : cache_it->second) {
                if (same_message(cached, current)) continue;
                const bool already_present = std::any_of(
                    history.begin(), history.end(), [&](const Message& item) {
                        return same_message(item, cached);
                    });
                if (!already_present) history.push_back(cached);
            }
        }
    }
    std::sort(history.begin(), history.end(),
              [](const Message& lhs, const Message& rhs) {
                  if (lhs.msg_seq != rhs.msg_seq) {
                      return lhs.msg_seq < rhs.msg_seq;
                  }
                  return lhs.msg_id < rhs.msg_id;
              });
    if (history.size() > 50) {
        history.erase(history.begin(), history.end() - 50);
    }

    const std::string current_text = ExtractTextFromContentJson(current.content_json);
    if (current_text.empty()) {
        if (err) *err = "Hermes only supports text messages in first stage";
        return false;
    }

    std::vector<HermesConversationEntry> entries;
    entries.reserve(history.size());
    for (const auto& item : history) {
        HermesConversationEntry entry;
        entry.msg_seq = item.msg_seq;
        entry.assistant = item.sender_id == bot_user_id;
        entry.text = ExtractTextFromContentJson(item.content_json);
        ExtractHermesEntryMetadata(item.content_json, &entry);
        entries.push_back(std::move(entry));
    }

    // 命令规划器负责过滤本地命令、建立 /new 边界、重写 /retry 上下文，
    // 并在约 12 KiB 的预算内保留最近对话。
    const HermesPromptPlan plan =
        BuildHermesPromptPlan(entries, current_text, current.msg_seq);
    MetricsRegistry::Instance().Observe("spark_push_hermes_prompt_chars",
                                        static_cast<int64_t>(plan.prompt_chars));
    MetricsRegistry::Instance().Observe("spark_push_hermes_prompt_messages",
                                        static_cast<int64_t>(plan.messages.size()));

    *request = {
        {"request_id", current.msg_id},
        {"message_seq", current.msg_seq},
        {"session_id", current.session_id},
        {"user_id", current.sender_id},
        {"bot_user_id", bot_user_id},
        {"client_msg_id", current.client_msg_id},
        {"messages", nlohmann::json::array()},
        {"call_model", plan.call_model},
        {"force_knowledge_retrieval", plan.force_knowledge_retrieval},
        {"model_override", plan.model_override},
        {"context_start_seq", plan.context_start_seq},
        {"context_message_count", plan.context_message_count},
        {"prompt_chars", plan.prompt_chars},
    };
    for (const auto& item : plan.messages) {
        (*request)["messages"].push_back(
            {{"role", item.first}, {"content", item.second}});
    }
    if (plan.model_override) {
        (*request)["model"] = plan.model;
        if (!plan.provider.empty()) (*request)["provider"] = plan.provider;
    }
    if (plan.command.kind != HermesCommandKind::kNone) {
        (*request)["command"] = {
            {"name", HermesCommandName(plan.command.kind)},
            {"input_name", plan.command.name},
            {"arguments", plan.command.arguments},
            {"local_error", plan.local_error},
        };
    }
    return true;
}

bool LogicServiceImpl::EnqueueHermesRequest(const Message& current,
                                            std::string* err) {
    if (!hermes_request_producer_) {
        if (err) *err = "Hermes request Kafka producer not initialized";
        return false;
    }
    nlohmann::json request;
    if (!BuildHermesRequest(current, &request, err)) return false;
    const std::string payload = request.dump();
    if (!hermes_request_producer_->SendAndWait(
            current.session_id, payload, persist_kafka_timeout_ms_)) {
        if (err) *err = "Hermes request topic delivery failed";
        return false;
    }
    MetricsRegistry::Instance().Increment("spark_push_hermes_requests_total");
    return true;
}

// 处理上行消息（高性能热路径）：
// 1) 本地构造 session_id  2) Redis INCR  3) 路由缓存+Kafka  4) 异步落盘
void LogicServiceImpl::HandleUpstreamMessage(
    const UpstreamMessageRequest& request, UpstreamMessageReply* response) {
    if (!store_ || !redis_store_) {
        SetError(response->mutable_error(), 500,
                 "store/redis not initialized");
        return;
    }
    const std::string& scene = request.scene();
    int64_t from_user = request.from_user_id();
    if (from_user <= 0) {
        SetError(response->mutable_error(), 400,
                 "from_user_id must be positive");
        return;
    }
    if (!user_dao_) {
        SetError(response->mutable_error(), 500, "user dao not initialized");
        return;
    }
    User sender;
    std::string sender_error;
    if (!GetActiveUser(from_user, &sender, &sender_error)) {
        SetError(response->mutable_error(), 403,
                 "sender account is disabled or deleted");
        return;
    }
    if (request.client_msg_id().size() > 128) {
        SetError(response->mutable_error(), 400,
                 "client_msg_id exceeds 128 bytes");
        return;
    }

    const std::string rate_key =
        scene == "chatroom" ? std::to_string(request.group_id())
                            : std::to_string(from_user);
    if (!rate_limiter_.Allow(scene, rate_key)) {
        MetricsRegistry::Instance().Increment(
            "spark_push_rate_limited_total_" + scene);
        SetError(response->mutable_error(), 429,
                 "scene rate limit exceeded; retry later");
        return;
    }

    std::unordered_map<std::string, std::vector<int64_t>> comet_to_users;
    std::string err;
    std::string session_id;
    int64_t single_u1 = 0, single_u2 = 0, room_id = 0;
    bool hermes_target = false;

    if (scene == "single") {
        int64_t to_user = request.to_user_id();
        if (to_user <= 0) {
            SetError(response->mutable_error(), 400,
                     "to_user_id must be positive");
            return;
        }
        single_u1 = from_user;
        single_u2 = to_user;
        if (single_u1 > single_u2) std::swap(single_u1, single_u2);
        session_id = "s_" + std::to_string(single_u1) + "_" +
                     std::to_string(single_u2);
        hermes_target = agent_bot_users_.count(to_user) != 0;
        if (hermes_target) {
            if (!hermes_enabled_ || !hermes_request_producer_) {
                SetError(response->mutable_error(), 503,
                         "Hermes Bot is disabled");
                return;
            }
            User bot;
            std::string bot_error;
            if (!GetActiveUser(to_user, &bot, &bot_error)) {
                SetError(response->mutable_error(), 503,
                         "Hermes Bot is disabled or unavailable");
                return;
            }
        } else {
            std::vector<std::string> comets;
            if (!redis_store_->GetUserRoutes(to_user, &comets)) {
                LOG_ERROR << "GetUserRoutes failed for user "
                          << std::to_string(to_user);
            } else {
                for (const auto& cid : comets) {
                    comet_to_users[cid].push_back(to_user);
                }
            }
        }
    } else if (scene == "chatroom") {
        room_id = request.group_id();
        if (room_id <= 0) {
            SetError(response->mutable_error(), 400,
                     "group_id(room_id) must be positive");
            return;
        }
        session_id = "r_" + std::to_string(room_id);
        std::vector<std::string> room_comets;
        bool use_room_comets = false;
        if (redis_store_->GetRoomComets(room_id, &room_comets) &&
            !room_comets.empty()) {
            use_room_comets = true;
        }
        if (use_room_comets) {
            for (const auto& cid : room_comets) {
                comet_to_users[cid];
            }
        } else {
            std::vector<int64_t> members;
            if (group_member_dao_) {
                if (!group_member_dao_->ListRoomMembers(room_id, &members,
                                                        &err)) {
                    LOG_ERROR << "ListRoomMembers failed: " << err;
                }
            }
            for (int64_t uid : members) {
                std::vector<std::string> comets;
                if (!redis_store_->GetUserRoutes(uid, &comets)) {
                    continue;
                }
                for (const auto& cid : comets) {
                    comet_to_users[cid].push_back(uid);
                }
            }
        }
    } else {
        SetError(response->mutable_error(), 400, "unsupported scene");
        return;
    }

    int64_t now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                         std::chrono::system_clock::now().time_since_epoch())
                         .count();

    Message msg;
    bool is_new = false;
    if (!store_->AppendMessageHotPath(session_id, from_user, "text",
                                      request.content_json(), now_ms,
                                      request.client_msg_id(), &msg, &is_new,
                                      &err)) {
        SetError(response->mutable_error(), 500,
                 "hot path append failed: " + err);
        return;
    }

    std::string push_json = msg.content_json;
    try {
        auto j = nlohmann::json::parse(push_json);
        j["msg_id"] = msg.msg_id;
        j["msg_seq"] = msg.msg_seq;
        j["create_time"] = msg.timestamp_ms;
        j["from_user_id"] = from_user;
        if (!j.contains("client_msg_id")) {
            j["client_msg_id"] = request.client_msg_id();
        }
        j["sender"] = {{"uid", from_user}};
        push_json = j.dump();
        msg.content_json = push_json;
    } catch (...) {
    }
    if (hermes_target) RememberHermesMessage(msg);

    ChatMessage* cm = response->mutable_message();
    FillChatMessage(msg, cm);
    cm->set_content_json(push_json);

    // accepted_ack 的边界是持久化事件已经收到 Kafka delivery report；
    // 这一步替代 Logic 本地线程池，Logic 崩溃后仍可由 Job 从 topic 重放。
    if (!PersistToTopic(msg, scene, single_u1, single_u2, room_id, &err)) {
        MetricsRegistry::Instance().Increment(
            "spark_push_persist_events_failed_total");
        SetError(response->mutable_error(), 503, err);
        return;
    }
    response->set_ack_stage(ACK_STAGE_ACCEPTED);
    response->set_accepted_at_ms(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch())
            .count());
    MetricsRegistry::Instance().Increment(
        "spark_push_messages_accepted_total_" + scene);

    // 相同 sender/session/client_msg_id 的重试直接返回原 msg_seq，不重复推送和落库。
    if (!is_new) {
        MetricsRegistry::Instance().Increment(
            "spark_push_duplicate_total_" + scene);
        if (hermes_target) {
            // 客户端可能在“用户消息已 durable、ai_request 尚未 delivery”
            // 的窗口重试；允许再次入队，Bridge 以 request_id 做幂等去重。
            if (!EnqueueHermesRequest(msg, &err)) {
                MetricsRegistry::Instance().Increment(
                    "spark_push_hermes_errors_total");
                SetError(response->mutable_error(), 503, err);
                return;
            }
            SetError(response->mutable_error(), 0,
                     "duplicate accepted; Hermes request retried");
            return;
        }
        SetError(response->mutable_error(), 0, "duplicate accepted");
        return;
    }

    if (hermes_target) {
        // 输入消息已经越过 accepted_ack 的持久化边界；AI 请求单独进入
        // ai_request，Bridge 崩溃或 Hermes 暂时不可用时由 Kafka 重试。
        if (!EnqueueHermesRequest(msg, &err)) {
            MetricsRegistry::Instance().Increment(
                "spark_push_hermes_errors_total");
            SetError(response->mutable_error(), 503, err);
            return;
        }
        SetError(response->mutable_error(), 0, "Hermes request accepted");
        return;
    }

    if (comet_to_users.empty()) {
        SetError(response->mutable_error(), 0, "accepted for offline storage");
        return;
    }
    KafkaProducer* realtime_producer =
        scene == "single" ? producer_ : group_producer_;
    if (!realtime_producer) {
        SetError(response->mutable_error(), 503,
                 "scene Kafka producer not ready");
        return;
    }

    size_t accepted_routes = 0;
    for (const auto& kv : comet_to_users) {
        const std::string& comet_id = kv.first;
        const auto& users = kv.second;
        PushToCometRequest req;
        req.set_comet_id(comet_id);
        req.set_request_id(msg.msg_id + "@" + comet_id);
        req.set_ack_comet_id(request.source_comet_id());
        req.set_ack_user_id(from_user);
        req.set_scene(scene == "single" ? "single" : "group");
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
        if (!realtime_producer->Send(comet_id, payload)) {
            LOG_ERROR << "Kafka send failed for comet " << comet_id;
        } else {
            ++accepted_routes;
        }
    }
    if (accepted_routes == 0) {
        // 消息已经在 persist topic 中 durable；实时 topic 暂时不可用时，
        // 客户端仍拿到 accepted_ack，重连后由 cursor/offline sync 补齐。
        MetricsRegistry::Instance().Increment(
            "spark_push_realtime_enqueue_failed_total_" + scene);
        SetError(response->mutable_error(), 0,
                 "accepted; realtime delivery pending");
        return;
    }
    if (accepted_routes != comet_to_users.size()) {
        LOG_ERROR << "Kafka accepted only " << accepted_routes << "/"
                  << comet_to_users.size() << " routes for msg_id="
                  << msg.msg_id;
    }
    MetricsRegistry::Instance().Increment("spark_push_realtime_enqueued_total_" +
                                         scene,
                                         static_cast<int64_t>(accepted_routes));
    SetError(response->mutable_error(), 0, "ok");
}

bool LogicServiceImpl::HandleHermesReply(const std::string& payload,
                                         const std::string& kafka_key) {
    (void)kafka_key;
    PreparedHermesReply reply;
    std::string err;
    const auto envelope = nlohmann::json::parse(payload, nullptr, false);
    if (!envelope.is_object() || !envelope.contains("bot_user_id") ||
        !envelope["bot_user_id"].is_number_integer()) return false;
    const auto bot_user_id = envelope["bot_user_id"].get<int64_t>();
    const auto bot = agent_bot_users_.find(bot_user_id);
    if (bot == agent_bot_users_.end()) return false;
    if (!PrepareHermesReply(payload, bot_user_id, bot->second, &reply, &err)) {
        LOG_ERROR << "Reject Hermes reply: " << err;
        MetricsRegistry::Instance().Increment("spark_push_hermes_errors_total");
        return false;  // durable ai_reply DLQ after bounded retries
    }
    const auto& request_id = reply.request_id;
    const auto user_id = reply.user_id;
    const bool ok = reply.ok;
    if (!reply.citation_warning.empty()) {
        MetricsRegistry::Instance().Increment("spark_push_hermes_citations_rejected_total");
    }
    Message message = reply.message;
    bool is_new = false;
    const int64_t user1 = std::min(user_id, bot_user_id);
    const int64_t user2 = std::max(user_id, bot_user_id);
    if (!PersistHermesReply(&message, &is_new,
            [this, &err](Message* msg, bool* fresh) {
                return store_ && store_->AppendMessageHotPath(
                    msg->session_id, msg->sender_id, msg->msg_type, msg->content_json,
                    msg->timestamp_ms, msg->client_msg_id, msg, fresh, &err);
            },
            [this, user1, user2, &err](const Message& msg) {
                return PersistToTopic(msg, "single", user1, user2, 0, &err);
            })) {
        LOG_ERROR << "Persist Hermes reply failed request_id=" << request_id << ": " << err;
        MetricsRegistry::Instance().Increment("spark_push_hermes_errors_total");
        return false;
    }
    if (!is_new) {
        MetricsRegistry::Instance().Increment("spark_push_hermes_duplicate_replies_total");
    }
    // Only Kafka persistence confirmation closes the ephemeral stream and
    // exposes this answer to future Agent context. Job persists asynchronously.
    MarkHermesStreamCompleted(request_id);
    RememberHermesMessage(message);

    std::vector<std::string> comets;
    if (!redis_store_ || !redis_store_->GetUserRoutes(user_id, &comets)) {
        LOG_ERROR << "Get Hermes recipient routes failed user_id="
                  << std::to_string(user_id);
        return false;
    }
    ChatMessage chat_message;
    FillChatMessage(message, &chat_message);
    for (const auto& comet_id : comets) {
        PushToCometRequest request;
        request.set_comet_id(comet_id);
        request.set_request_id(message.msg_id + "@" + comet_id);
        request.set_scene("single");
        *request.mutable_message() = chat_message;
        request.add_targets()->set_user_id(user_id);

        std::string push_payload;
        if (!request.SerializeToString(&push_payload) ||
            !producer_ || !producer_->Send(comet_id, push_payload)) {
            LOG_ERROR << "Enqueue Hermes reply to Comet failed comet_id="
                      << comet_id << ", request_id=" << request_id;
            continue;
        }
        MetricsRegistry::Instance().Increment(
            "spark_push_hermes_realtime_enqueued_total");
    }
    MetricsRegistry::Instance().Increment(
        ok ? "spark_push_hermes_replies_total"
           : "spark_push_hermes_errors_total");
    return true;
}

bool LogicServiceImpl::HandleHermesDelta(const std::string& payload,
                                         const std::string& kafka_key) {
    (void)kafka_key;
    nlohmann::json delta;
    try {
        delta = nlohmann::json::parse(payload);
    } catch (const nlohmann::json::exception& e) {
        LOG_ERROR << "Discard malformed Hermes delta: " << e.what();
        MetricsRegistry::Instance().Increment(
            "spark_push_hermes_stream_delta_errors_total");
        return true;
    }

    const std::string request_id = delta.value("request_id", "");
    const int64_t user_id = delta.value("user_id", 0LL);
    const int64_t bot_user_id = delta.value("bot_user_id", 0LL);
    const std::string text = delta.value("delta", "");
    const int delta_index = delta.value("delta_index", -1);
    if (request_id.empty() || user_id <= 0 ||
        !agent_bot_users_.count(bot_user_id) || text.empty() || delta_index < 0) {
        MetricsRegistry::Instance().Increment(
            "spark_push_hermes_stream_delta_errors_total");
        return true;
    }
    if (IsHermesStreamCompleted(request_id)) return true;

    const std::string delta_id =
        "hermes_delta:" + request_id + ":" + std::to_string(delta_index);
    if (MarkHermesDeltaSeen(delta_id)) {
        MetricsRegistry::Instance().Increment(
            "spark_push_hermes_stream_delta_duplicate_total");
        return true;
    }

    std::string session_id = delta.value("session_id", "");
    const auto expected_session = "s_" + std::to_string(std::min(user_id, bot_user_id)) + "_" +
                                  std::to_string(std::max(user_id, bot_user_id));
    if (session_id.empty()) session_id = expected_session;
    if (session_id != expected_session) return true;
    std::vector<std::string> comets;
    if (!redis_store_ || !redis_store_->GetUserRoutes(user_id, &comets)) {
        LOG_ERROR << "Get Hermes delta recipient routes failed user_id="
                  << std::to_string(user_id);
        return false;
    }
    if (comets.empty()) {
        MetricsRegistry::Instance().Increment(
            "spark_push_hermes_stream_delta_offline_total");
        return true;
    }

    const int64_t now_ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch())
            .count();
    nlohmann::json content = {
        {"type", "hermes_delta"},
        {"request_id", request_id},
        {"session_id", session_id},
        {"from_user_id", bot_user_id},
        {"to_user_id", user_id},
        {"delta_index", delta_index},
        {"delta", text},
        {"progress", delta.value("progress", nlohmann::json()) == true},
        {"source", "hermes"},
    };

    ChatMessage message;
    message.set_msg_id(delta_id);
    message.set_session_id(session_id);
    message.set_msg_seq(0);  // 增量不进入历史游标。
    message.set_sender_id(bot_user_id);
    message.set_timestamp_ms(now_ms);
    message.set_msg_type("hermes_delta");
    message.set_content_json(content.dump());
    message.set_client_msg_id(delta_id);

    for (const auto& comet_id : comets) {
        PushToCometRequest request;
        request.set_comet_id(comet_id);
        request.set_request_id(delta_id + "@" + comet_id);
        request.set_scene("single");
        *request.mutable_message() = message;
        request.add_targets()->set_user_id(user_id);
        std::string push_payload;
        if (!request.SerializeToString(&push_payload) ||
            !producer_ || !producer_->Send(comet_id, push_payload)) {
            LOG_ERROR << "Enqueue Hermes delta to Comet failed comet_id="
                      << comet_id << ", request_id=" << request_id;
            MetricsRegistry::Instance().Increment(
                "spark_push_hermes_stream_delta_errors_total");
            continue;
        }
        MetricsRegistry::Instance().Increment(
            "spark_push_hermes_stream_delta_enqueued_total");
    }
    return true;
}

::grpc::Status LogicServiceImpl::MessageStream(
    ::grpc::ServerContext*,
    ::grpc::ServerReaderWriter<::sparkpush::StreamResponse,
                               ::sparkpush::StreamMessage>* stream) {
    StreamMessage msg;
    while (stream->Read(&msg)) {
        StreamResponse resp;
        resp.set_request_id(msg.request_id());
        switch (msg.payload_case()) {
            case StreamMessage::kUpstream: {
                UpstreamMessageReply reply;
                HandleUpstreamMessage(msg.upstream(), &reply);
                *resp.mutable_error() = reply.error();
                *resp.mutable_upstream_reply() = reply;
                break;
            }
            case StreamMessage::kOffline: {
                SimpleReply reply;
                if (redis_store_) {
                    redis_store_->RemoveRoute(msg.offline().user_id(),
                                             msg.offline().comet_id());
                }
                SetError(reply.mutable_error(), 0, "ok");
                *resp.mutable_error() = reply.error();
                *resp.mutable_simple_reply() = reply;
                break;
            }
            case StreamMessage::kRoomJoin: {
                SimpleReply reply;
                // 复用现有 RPC 逻辑：直接调用成员函数实现
                ReportRoomJoin(nullptr, &msg.room_join(), &reply);
                *resp.mutable_error() = reply.error();
                *resp.mutable_simple_reply() = reply;
                break;
            }
            case StreamMessage::kRoomLeave: {
                SimpleReply reply;
                ReportRoomLeave(nullptr, &msg.room_leave(), &reply);
                *resp.mutable_error() = reply.error();
                *resp.mutable_simple_reply() = reply;
                break;
            }
            default:
                SetError(resp.mutable_error(), 400, "unknown message type");
                break;
        }
        if (!stream->Write(resp)) {
            break;
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
    if (!rate_limiter_.Allow("broadcast", request->scope().empty()
                                      ? "all"
                                      : request->scope())) {
        MetricsRegistry::Instance().Increment(
            "spark_push_rate_limited_total_broadcast");
        SetError(response->mutable_error(), 429,
                 "broadcast rate limit exceeded; retry later");
        return ::grpc::Status::OK;
    }
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
    LOG_INFO << "Sending broadcast task " << task_id
             << ", payload_bytes=" << payload.size();
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

::grpc::Status LogicServiceImpl::SyncMessages(
    ::grpc::ServerContext*, const SyncMessagesRequest* request,
    SyncMessagesReply* response) {
    if (!store_ || !group_member_dao_ || request->user_id() <= 0 ||
        request->session_id().empty() || request->after_seq() < 0) {
        SetError(response->mutable_error(), 400,
                 "user_id/session_id are required");
        return ::grpc::Status::OK;
    }
    Session session;
    std::string err;
    if (!store_->GetSessionById(request->session_id(), &session, &err)) {
        SetError(response->mutable_error(), 404, "session not found");
        return ::grpc::Status::OK;
    }
    bool authorized = false;
    if (session.type == SessionType::kSingle) {
        authorized = session.user1_id == request->user_id() ||
                     session.user2_id == request->user_id();
    } else if (session.group_id > 0) {
        if (!group_member_dao_->IsMember(session.group_id, request->user_id(),
                                         &authorized, &err)) {
            SetError(response->mutable_error(), 500, err);
            return ::grpc::Status::OK;
        }
    }
    if (!authorized) {
        SetError(response->mutable_error(), 403,
                 "user is not a member of this session");
        return ::grpc::Status::OK;
    }
    const int limit = std::max(1, std::min(request->limit() > 0
                                               ? request->limit()
                                               : 100,
                                           1000));
    std::vector<Message> messages;
    if (!store_->GetMessagesAfter(request->session_id(), request->after_seq(),
                                  limit, &messages, &err)) {
        SetError(response->mutable_error(), 500, err);
        return ::grpc::Status::OK;
    }
    for (const auto& message : messages) {
        FillChatMessage(message, response->add_messages());
    }
    response->set_has_more(static_cast<int>(messages.size()) == limit);
    response->set_next_seq(messages.empty() ? request->after_seq()
                                            : messages.back().msg_seq);
    SetError(response->mutable_error(), 0, "ok");
    MetricsRegistry::Instance().Increment("spark_push_cursor_sync_total");
    return ::grpc::Status::OK;
}

::grpc::Status LogicServiceImpl::SyncOffline(
    ::grpc::ServerContext*, const SyncOfflineRequest* request,
    SyncOfflineReply* response) {
    if (!store_ || !group_member_dao_ || request->user_id() <= 0) {
        SetError(response->mutable_error(), 400,
                 "offline sync dependencies/user_id are missing");
        return ::grpc::Status::OK;
    }
    const int limit = std::max(1, std::min(request->limit() > 0
                                               ? request->limit()
                                               : 100,
                                           1000));
    std::vector<Session> sessions;
    std::string err;
    if (!store_->ListUserSingleSessions(request->user_id(), &sessions, &err)) {
        SetError(response->mutable_error(), 500, err);
        return ::grpc::Status::OK;
    }
    std::vector<int64_t> room_ids;
    if (!group_member_dao_->ListUserChatrooms(request->user_id(), &room_ids,
                                              &err)) {
        LOG_WARN << "ListUserChatrooms for offline sync failed: " << err;
    }
    for (int64_t room_id : room_ids) {
        Session room;
        if (store_->GetSessionById("r_" + std::to_string(room_id), &room,
                                   &err)) {
            sessions.push_back(std::move(room));
        }
    }

    int remaining = limit;
    for (const auto& session : sessions) {
        if (remaining <= 0) break;
        int64_t delivered_seq = 0;
        if (!store_->GetDeliveredSeq(request->user_id(), session.id,
                                     &delivered_seq, &err)) {
            LOG_WARN << "GetDeliveredSeq failed for session " << session.id
                     << ": " << err;
            continue;
        }
        std::vector<Message> messages;
        if (!store_->GetMessagesAfter(session.id, delivered_seq, remaining,
                                      &messages, &err)) {
            LOG_WARN << "offline message query failed for session " << session.id
                     << ": " << err;
            continue;
        }
        for (const auto& message : messages) {
            FillChatMessage(message, response->add_messages());
        }
        remaining -= static_cast<int>(messages.size());
    }
    response->set_has_more(remaining == 0 && !sessions.empty());
    SetError(response->mutable_error(), 0, "ok");
    MetricsRegistry::Instance().Increment("spark_push_offline_sync_total");
    return ::grpc::Status::OK;
}

::grpc::Status LogicServiceImpl::MarkDelivered(
    ::grpc::ServerContext*, const MarkDeliveredRequest* request,
    SimpleReply* response) {
    if (!store_ || !group_member_dao_ ||
        (request->user_id() <= 0 && request->user_cursors_size() == 0)) {
        SetError(response->mutable_error(), 400,
                 "user_id or user_cursors is required");
        return ::grpc::Status::OK;
    }
    std::string err;
    auto apply_cursor = [&](int64_t user_id, const std::string& session_id,
                            int64_t msg_seq) -> bool {
        if (user_id <= 0 || session_id.empty() || msg_seq < 0) return true;
        Session session;
        if (!store_->GetSessionById(session_id, &session, &err)) {
            SetError(response->mutable_error(), 404, "session not found");
            return false;
        }
        bool authorized = session.type == SessionType::kSingle &&
                          (session.user1_id == user_id ||
                           session.user2_id == user_id);
        if (!authorized && session.group_id > 0) {
            if (!group_member_dao_->IsMember(session.group_id,
                                             user_id, &authorized, &err)) {
                SetError(response->mutable_error(), 500, err);
                return false;
            }
        }
        if (!authorized) {
            SetError(response->mutable_error(), 403,
                     "user is not a member of this session");
            return false;
        }
        if (!store_->MarkDelivered(user_id, session_id, msg_seq, &err)) {
            SetError(response->mutable_error(), 500, err);
            return false;
        }
        return true;
    };
    for (const auto& cursor : request->cursors()) {
        if (!apply_cursor(request->user_id(), cursor.session_id(),
                          cursor.msg_seq())) {
            return ::grpc::Status::OK;
        }
    }
    for (const auto& cursor : request->user_cursors()) {
        if (!apply_cursor(cursor.user_id(), cursor.session_id(),
                          cursor.msg_seq())) {
            return ::grpc::Status::OK;
        }
    }
    SetError(response->mutable_error(), 0, "ok");
    MetricsRegistry::Instance().Increment("spark_push_delivered_cursor_total");
    return ::grpc::Status::OK;
}

}  // namespace sparkpush
