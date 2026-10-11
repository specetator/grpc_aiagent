#include "services/message_service.h"

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <sstream>
#include <thread>

#include "agent_event.h"
#include "citation_validation.h"
#include "conversation_store.h"
#include "group_dao.h"
#include "hermes_reply.h"
#include "image_attachment.h"
#include "kafka_producer.h"
#include "logging.h"
#include "message_codec.h"
#include "metrics.h"
#include "redis_store.h"
#include "services/attachment_service.h"
#include "services/business_rules.h"
#include "services/user_service.h"
#include "services/room_agent_service.h"
#include "text_metrics.h"

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
        if (content.contains("hermes_context_start_seq") &&
            content["hermes_context_start_seq"].is_number_integer()) {
            entry->context_start_seq =
                content["hermes_context_start_seq"].get<int64_t>();
            entry->has_context_state = entry->context_start_seq >= 0;
        }
        entry->command_reply = content.value("source", "") == "hermes_command";
        // Compatibility only for the exact successful legacy /new reply.
        // New cards and failed/preview commands never infer a boundary from text.
        entry->legacy_new_confirmation =
            entry->command_reply && !entry->has_context_state &&
            !content.contains("agent_event") &&
            content.value("hermes_command", "") == "new" &&
            entry->text.rfind("已开启新的 Pi Agent 上下文。", 0) == 0;
        if (content.value("hermes_model_state_confirmed", true) &&
            content.contains("hermes_model_override") &&
            content.at("hermes_model_override").is_boolean()) {
            entry->has_model_state = true;
            entry->model_override = content.at("hermes_model_override").get<bool>();
            entry->model = content.value("hermes_model", "");
            entry->provider = content.value("hermes_provider", "");
        }
    } catch (const nlohmann::json::exception&) {
    }
}

}  // namespace

// 构造：初始化逻辑服务依赖；持久化由独立 Kafka topic 承担。
MessageService::MessageService(
    ConversationStore* store, GroupMemberDao* group_member_dao,
    UserService* user_service, KafkaProducer* producer, KafkaProducer* group_producer,
    KafkaProducer* broadcast_producer, KafkaProducer* persist_producer,
    RedisStore* redis_store, const RateLimitConfig& rate_limit,
    int persist_kafka_timeout_ms, KafkaProducer* hermes_request_producer,
    bool hermes_enabled, int64_t hermes_bot_user_id,
    std::map<int64_t, std::string> agent_bot_users,
    AttachmentService* attachment_service, RoomAgentService* room_agents,
    MessageEventPublisher* persistence_publisher)
    : store_(store),
      persistence_publisher_(persistence_publisher),
      group_member_dao_(group_member_dao),
      user_service_(user_service),
      producer_(producer),
      group_producer_(group_producer),
      broadcast_producer_(broadcast_producer),
      persist_producer_(persist_producer),
      hermes_request_producer_(hermes_request_producer),
      redis_store_(redis_store),
      attachment_service_(attachment_service),
      room_agent_service_(room_agents),
      rate_limiter_(rate_limit),
      persist_kafka_timeout_ms_(persist_kafka_timeout_ms),
      hermes_enabled_(hermes_enabled),
      hermes_bot_user_id_(hermes_bot_user_id),
      agent_bot_users_(std::move(agent_bot_users)) {
    agent_bot_users_[hermes_bot_user_id] = kHermesBotName;
}

bool MessageService::GetActiveUser(int64_t user, User* result, std::string* error) {
    return user_service_ && user_service_->GetActiveUser(user, result, error);
}
int64_t MessageService::AddAuthenticatedRoute(int64_t user, const std::string& comet, std::string* token) {
    int64_t generation = 0;
    if (redis_store_) redis_store_->AddRouteGeneration(user, comet, &generation, token);
    return generation;
}
void MessageService::UserOffline(int64_t user, const std::string& comet, int64_t generation, const std::string& token) {
    if (redis_store_) redis_store_->RemoveRouteGeneration(user, comet, generation, token);
}

bool MessageService::PersistToTopic(const Message& message, const std::string& scene,
                                    int64_t user1, int64_t user2, int64_t room_id,
                                    std::string* err) {
    if (!persist_producer_ && !persistence_publisher_) {
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
    if (!store_ || !store_->PrepareOutbox(message, payload, err)) return false;
    const bool confirmed = persistence_publisher_
        ? persistence_publisher_->Confirm(message.session_id, payload, persist_kafka_timeout_ms_)
        : persist_producer_->SendAndWait(message.session_id, payload, persist_kafka_timeout_ms_);
    if (!confirmed) {
        if (err) *err = "persist topic delivery failed";
        return false;
    }
    if (!store_->Published(message, err)) return false;
    MetricsRegistry::Instance().Increment("spark_push_persist_events_accepted_total");
    return true;
}

void MessageService::RememberHermesMessage(const Message& message) {
    if (message.session_id.empty() || message.msg_id.empty()) return;
    std::lock_guard<std::mutex> lock(hermes_cache_mu_);
    auto it = hermes_history_cache_.find(message.session_id);
    if (it == hermes_history_cache_.end()) {
        if (hermes_history_cache_.size() >= 1024) {
            hermes_history_cache_.erase(hermes_history_cache_.begin());
        }
        it = hermes_history_cache_.emplace(message.session_id, std::deque<Message>{})
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

bool MessageService::IsHermesStreamCompleted(const std::string& request_id) {
    if (request_id.empty()) return false;
    std::lock_guard<std::mutex> lock(hermes_stream_mu_);
    return hermes_stream_completed_.find(request_id) != hermes_stream_completed_.end();
}

void MessageService::MarkHermesStreamCompleted(const std::string& request_id) {
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
bool MessageService::MarkHermesDeltaSeen(const std::string& delta_id) {
    if (delta_id.empty()) return true;
    std::lock_guard<std::mutex> lock(hermes_stream_mu_);
    if (hermes_stream_completed_.find(delta_id) != hermes_stream_completed_.end()) {
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

bool MessageService::BuildHermesRequest(const Message& current, nlohmann::json* request,
                                        std::string* err) {
    if (current.session_id.rfind("r_", 0) == 0) {
        std::vector<Message> history;
        if (!room_agent_service_ || !store_->GetHistory(current.session_id, 0, 20, &history, err))
            return false;
        {
            std::lock_guard<std::mutex> lock(hermes_cache_mu_);
            const auto cached = hermes_history_cache_.find(current.session_id);
            if (cached != hermes_history_cache_.end())
                for (const auto& item : cached->second) {
                    if (std::none_of(history.begin(), history.end(), [&](const Message& h) { return h.msg_id == item.msg_id; }))
                        history.push_back(item);
                }
        }
        // First input can precede creation of the room session by Job.
        return room_agent_service_->BuildRequest(current, history, request, err);
    }
    if (!request) {
        if (err) *err = "Hermes request output is null";
        return false;
    }
    int64_t bot_user_id = 0;
    for (const auto& bot : agent_bot_users_) {
        const auto expected =
            "s_" + std::to_string(std::min(current.sender_id, bot.first)) + "_" +
            std::to_string(std::max(current.sender_id, bot.first));
        if (current.session_id == expected) {
            bot_user_id = bot.first;
            break;
        }
    }
    if (!bot_user_id) {
        if (err) *err = "invalid Agent conversation";
        return false;
    }

    // ConversationStore::GetHistory 已将 MessageDao 的倒序查询翻转为
    // msg_seq 升序。按这个顺序交给 Hermes，保证多轮上下文与会话顺序一致。
    // 当前消息可能尚未被 Job 落盘，因此最后显式补入 current。
    std::vector<Message> history;
    std::string history_error;
    if (store_->GetHistory(current.session_id, 0, 50, &history, &history_error)) {
    } else {
        // 历史查询失败不应悄悄伪装成“无历史”。当前请求仍可发送，
        // 但把原因记录到日志，方便区分首次对话与数据库异常。
        LOG_WARN << "Build Hermes history failed for session " << current.session_id
                 << ": " << history_error;
    }

    // 当前消息可能已经被 Job 竞争性地写入 MySQL；它会在函数末尾显式追加，
    // 所以先按 msg_id/client_msg_id 排除，避免同一轮用户输入被放进 Prompt 两次。
    const auto same_message = [](const Message& lhs, const Message& rhs) {
        return (!lhs.msg_id.empty() && lhs.msg_id == rhs.msg_id) ||
               (!lhs.client_msg_id.empty() && lhs.client_msg_id == rhs.client_msg_id);
    };
    history.erase(std::remove_if(
                      history.begin(), history.end(),
                      [&](const Message& item) { return same_message(item, current); }),
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
                    history.begin(), history.end(),
                    [&](const Message& item) { return same_message(item, cached); });
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

    std::string current_text = ExtractTextFromContentJson(current.content_json);
    const auto current_images = ExtractImageRefsFromContentJson(current.content_json);
    if (current_text.empty() && current_images.empty()) {
        if (err) *err = "Hermes only supports text or image messages";
        return false;
    }
    if (current_text.empty()) current_text = "请查看这张图片。";

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
    if (!current_images.empty()) {
        nlohmann::json images = nlohmann::json::array();
        for (const auto& image : current_images) {
            images.push_back({{"attachment_id", image.id},
                              {"mime", image.mime},
                              {"name", image.name}});
        }
        (*request)["images"] = std::move(images);
    }
    const auto current_metrics = MeasureText(current_text);
    if (LengthAuditEnabled()) {
        LOG_INFO << "length_audit stage=logic_request request_id=" << current.msg_id
                 << " provider="
                 << (plan.provider.empty() ? "adapter-state" : plan.provider)
                 << " model=" << (plan.model.empty() ? "adapter-state" : plan.model)
                 << " stream=profile-default max_tokens=unset max_output_tokens=unset"
                 << " prompt_bytes=" << plan.prompt_chars
                 << " prompt_chars=" << current_metrics.unicode_chars
                 << " prompt_message_count=" << plan.messages.size()
                 << " context_start_seq=" << plan.context_start_seq
                 << " context_message_count=" << plan.context_message_count;
    }
    return true;
}

bool MessageService::AuthorizeMessageImages(int64_t sender, const std::string& session,
                                            const std::string& content,
                                            std::string* error) {
    if (!attachment_service_) {
        if (ExtractImageRefsFromContentJson(content).empty()) return true;
        if (error) *error = "image attachments are not enabled";
        return false;
    }
    const auto status =
        attachment_service_->AuthorizeMessageImages(sender, session, content);
    if (error) *error = status.ok() ? "" : status.message;
    return status.ok();
}

bool MessageService::EnqueueHermesRequest(const Message& current, std::string* err) {
    if (!hermes_request_producer_) {
        if (err) *err = "Hermes request Kafka producer not initialized";
        return false;
    }
    nlohmann::json request;
    if (!BuildHermesRequest(current, &request, err)) return false;
    const std::string payload = request.dump();
    if (!hermes_request_producer_->SendAndWait(current.session_id, payload,
                                               persist_kafka_timeout_ms_)) {
        if (err) *err = "Hermes request topic delivery failed";
        return false;
    }
    MetricsRegistry::Instance().Increment("spark_push_hermes_requests_total");
    return true;
}

// 处理上行消息：认证、会话顺序、持久化原文/序号预留、Kafka 确认、异步历史落盘。
UpstreamResult MessageService::Submit(const UpstreamCommand& request) {
    UpstreamResult result;
    if (!store_) {
        result.status = ServiceStatus{500, "store not initialized"};
        return result;
    }
    const std::string& scene = request.scene;
    if (request.content_json.size() > 64 * 1024) {
        result.status = {413, "message exceeds 64 KiB"};
        return result;
    }
    const auto input = nlohmann::json::parse(request.content_json, nullptr, false);
    if (input.is_discarded() || !input.is_object()) {
        result.status = {400, "message content must be a JSON object"};
        return result;
    }
    auto submission_mutex = submission_mutexes_.Get(scene == "chatroom"
        ? "room:" + std::to_string(request.group_id)
        : "single:" + std::to_string(std::min(request.from_user_id, request.to_user_id)) + ":" +
          std::to_string(std::max(request.from_user_id, request.to_user_id)));
    std::unique_lock<std::mutex> submission_order(*submission_mutex, std::defer_lock);
    const auto admission_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(8);
    while (!submission_order.try_lock()) {
        if (std::chrono::steady_clock::now() >= admission_deadline) {
            result.status = {503, "session submission queue timed out"};
            return result;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    int64_t from_user = request.from_user_id;
    if (from_user <= 0) {
        result.status = ServiceStatus{400, "from_user_id must be positive"};
        return result;
    }
    if (!user_service_ || !user_service_->UsersAvailable()) {
        result.status = ServiceStatus{500, "user dao not initialized"};
        return result;
    }
    {
        int64_t actor = 0;
        const auto auth = user_service_->Authenticate(request.auth_token,
            UserService::TokenAudience::kRealtimeUser, &actor);
        if (!auth.ok()) { result.status = auth; return result; }
        if (actor != from_user) {
            result.status = {403, "sender does not match authenticated actor"};
            return result;
        }
        from_user = actor;
    }
    User sender;
    std::string sender_error;
    if (!GetActiveUser(from_user, &sender, &sender_error)) {
        result.status = ServiceStatus{403, "sender account is disabled or deleted"};
        return result;
    }
    if (request.client_msg_id.empty() || request.client_msg_id.size() > 128) {
        result.status = ServiceStatus{400, "client_msg_id must contain 1..128 bytes"};
        return result;
    }

    const std::string rate_key = scene == "chatroom" ? std::to_string(request.group_id)
                                                     : std::to_string(from_user);
    if (!rate_limiter_.Allow(scene, rate_key)) {
        MetricsRegistry::Instance().Increment("spark_push_rate_limited_total_" + scene);
        result.status = ServiceStatus{429, "scene rate limit exceeded; retry later"};
        return result;
    }

    std::unordered_map<std::string, std::vector<int64_t>> comet_to_users;
    std::string err;
    std::string session_id;
    int64_t single_u1 = 0, single_u2 = 0, room_id = 0;
    bool hermes_target = false;

    if (scene == "single") {
        int64_t to_user = request.to_user_id;
        if (to_user <= 0) {
            result.status = ServiceStatus{400, "to_user_id must be positive"};
            return result;
        }
        single_u1 = from_user;
        single_u2 = to_user;
        if (single_u1 > single_u2) std::swap(single_u1, single_u2);
        session_id = "s_" + std::to_string(single_u1) + "_" + std::to_string(single_u2);
        hermes_target = agent_bot_users_.count(to_user) != 0;
        if (hermes_target) {
            if (!hermes_enabled_ || !hermes_request_producer_) {
                result.status = ServiceStatus{503, "Hermes Bot is disabled"};
                return result;
            }
            User bot;
            std::string bot_error;
            if (!GetActiveUser(to_user, &bot, &bot_error)) {
                result.status =
                    ServiceStatus{503, "Hermes Bot is disabled or unavailable"};
                return result;
            }
        } else {
            std::vector<std::string> comets;
            if (redis_store_ && !redis_store_->GetUserRoutes(to_user, &comets)) {
                LOG_ERROR << "GetUserRoutes failed for user "
                          << std::to_string(to_user);
            } else {
                for (const auto& cid : comets) {
                    comet_to_users[cid].push_back(to_user);
                }
            }
        }
    } else if (scene == "chatroom") {
        if (!redis_store_) {
            result.status = {503, "room routing unavailable"};
            return result;
        }
        room_id = request.group_id;
        if (room_id <= 0) {
            result.status = ServiceStatus{400, "group_id(room_id) must be positive"};
            return result;
        }
        session_id = "r_" + std::to_string(room_id);
        if (room_agent_service_) {
            const auto access = room_agent_service_->Trigger(from_user, room_id, request.content_json, &hermes_target);
            if (!access.ok()) { result.status = access; return result; }
        }
        std::vector<std::string> room_comets;
        bool use_room_comets = false;
        if (redis_store_->GetRoomComets(room_id, &room_comets) &&
            !room_comets.empty()) {
            use_room_comets = true;
        }
        // Explicit durable members are authoritative when Agent work is enabled;
        // WS presence alone is not authorization to receive workspace messages.
        if (use_room_comets && !room_agent_service_) {
            for (const auto& cid : room_comets) {
                comet_to_users[cid];
            }
        } else {
            std::vector<int64_t> members;
            if (group_member_dao_) {
                if (!group_member_dao_->ListRoomMembers(room_id, &members, &err)) {
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
        result.status = ServiceStatus{400, "unsupported scene"};
        return result;
    }

    int64_t now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                         std::chrono::system_clock::now().time_since_epoch())
                         .count();

    if (!AuthorizeMessageImages(from_user, session_id, request.content_json, &err)) {
        result.status = ServiceStatus{400, err};
        return result;
    }
    const auto image_refs = ExtractImageRefsFromContentJson(request.content_json);
    const char* msg_type = image_refs.empty() ? "text" : "image";

    Message msg;
    bool is_new = false;
    if (!store_->AppendMessageHotPath(session_id, from_user, msg_type,
                                      request.content_json, now_ms,
                                      request.client_msg_id, &msg, &is_new, &err)) {
        result.status = ServiceStatus{500, "hot path append failed: " + err};
        return result;
    }

    std::string push_json = msg.content_json;
    try {
        auto j = nlohmann::json::parse(push_json);
        j["msg_id"] = msg.msg_id;
        j["session_id"] = msg.session_id;
        j["msg_seq"] = msg.msg_seq;
        j["create_time"] = msg.timestamp_ms;
        j["from_user_id"] = from_user;
        if (!j.contains("client_msg_id")) {
            j["client_msg_id"] = request.client_msg_id;
        }
        j["sender"] = {{"uid", from_user}};
        push_json = j.dump();
        msg.content_json = push_json;
    } catch (...) {
    }
    if (hermes_target || scene == "chatroom") RememberHermesMessage(msg);

    result.message = msg;
    result.has_message = true;
    ChatMessage chat_message;
    ChatMessage* cm = &chat_message;
    FillChatMessage(msg, cm);
    cm->set_content_json(push_json);

    // accepted_ack 的边界是持久化事件已经收到 Kafka delivery report；
    // 这一步替代 Logic 本地线程池，Logic 崩溃后仍可由 Job 从 topic 重放。
    if (!PersistToTopic(msg, scene, single_u1, single_u2, room_id, &err)) {
        MetricsRegistry::Instance().Increment("spark_push_persist_events_failed_total");
        result.status = ServiceStatus{503, err};
        return result;
    }
    result.accepted = true;
    result.accepted_at_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                                std::chrono::system_clock::now().time_since_epoch())
                                .count();
    MetricsRegistry::Instance().Increment("spark_push_messages_accepted_total_" +
                                          scene);

    // 相同 sender/session/client_msg_id 复用原文和序号；投递允许同 ID 重发，由下游去重。
    if (!is_new) MetricsRegistry::Instance().Increment("spark_push_duplicate_total_" + scene);
    // Retry the same immutable fanout ID, including after a prior publish failure.

    if (hermes_target) {
        // 输入消息已经越过 accepted_ack 的持久化边界；AI 请求单独进入
        // ai_request，Bridge 崩溃或 Hermes 暂时不可用时由 Kafka 重试。
        if (!EnqueueHermesRequest(msg, &err)) {
            MetricsRegistry::Instance().Increment("spark_push_hermes_errors_total");
            result.status = ServiceStatus{503, err};
            return result;
        }
        result.status = ServiceStatus{0, "Hermes request accepted"};
        if (scene == "single") return result;
    }

    if (comet_to_users.empty()) {
        result.status = ServiceStatus{0, "accepted for offline storage"};
        return result;
    }
    KafkaProducer* realtime_producer = scene == "single" ? producer_ : group_producer_;
    if (!realtime_producer) {
        result.status = ServiceStatus{503, "scene Kafka producer not ready"};
        return result;
    }

    size_t accepted_routes = 0;
    for (const auto& kv : comet_to_users) {
        const std::string& comet_id = kv.first;
        const auto& users = kv.second;
        PushToCometRequest req;
        req.set_comet_id(comet_id);
        req.set_request_id(msg.msg_id + "@" + comet_id);
        req.set_ack_comet_id(request.source_comet_id);
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
        result.status = ServiceStatus{0, "accepted; realtime delivery pending"};
        return result;
    }
    if (accepted_routes != comet_to_users.size()) {
        LOG_ERROR << "Kafka accepted only " << accepted_routes << "/"
                  << comet_to_users.size() << " routes for msg_id=" << msg.msg_id;
    }
    MetricsRegistry::Instance().Increment("spark_push_realtime_enqueued_total_" + scene,
                                          static_cast<int64_t>(accepted_routes));
    result.status = ServiceStatus{0, "ok"};
    return result;
}

bool MessageService::HandleHermesReply(const std::string& payload,
                                       const std::string& kafka_key) {
    (void)kafka_key;
    PreparedHermesReply reply;
    std::string err;
    const auto envelope = nlohmann::json::parse(payload, nullptr, false);
    if (!envelope.is_object() || !envelope.contains("bot_user_id") ||
        !envelope["bot_user_id"].is_number_integer())
        return false;
    const auto bot_user_id = envelope["bot_user_id"].get<int64_t>();
    const auto bot = agent_bot_users_.find(bot_user_id);
    if (bot == agent_bot_users_.end()) return false;
    const bool room_reply = envelope.value("session_id", "").rfind("r_", 0) == 0;
    nlohmann::json room_request;
    if (room_reply && (!room_agent_service_ ||
        !room_agent_service_->ValidateReply(envelope, &room_request, &err))) return false;
    if (!PrepareHermesReply(payload, bot_user_id, bot->second, &reply, &err,
                           room_reply ? &room_request : nullptr)) {
        LOG_ERROR << "Reject Hermes reply: " << err;
        MetricsRegistry::Instance().Increment("spark_push_hermes_errors_total");
        return false;  // durable ai_reply DLQ after bounded retries
    }
    const auto& request_id = reply.request_id;
    const auto user_id = reply.user_id;
    const bool ok = reply.ok;
    if (!reply.citation_warning.empty()) {
        MetricsRegistry::Instance().Increment(
            "spark_push_hermes_citations_rejected_total");
    }
    Message message = reply.message;
    const auto stored_content_metrics = MeasureText(message.content_json);
    const auto stored_text_metrics =
        MeasureText(ExtractTextFromContentJson(message.content_json));
    if (LengthAuditEnabled()) {
        LOG_INFO
            << "length_audit stage=storage_before_enqueue request_id=" << request_id
            << " provider=" << envelope.value("effective_provider", "adapter-state")
            << " model=" << envelope.value("effective_model", "unknown")
            << " stream=" << (envelope.value("streamed", false) ? "true" : "false")
            << " max_tokens=unset max_output_tokens=unset"
            << " provider_bytes=" << envelope.value("audit_provider_output_bytes", 0)
            << " provider_chars=" << envelope.value("audit_provider_output_chars", 0)
            << " content_json_bytes=" << stored_content_metrics.utf8_bytes
            << " content_json_chars=" << stored_content_metrics.unicode_chars
            << " response_bytes="
            << envelope.value("response_bytes", stored_text_metrics.utf8_bytes)
            << " response_chars="
            << envelope.value("response_unicode_chars",
                              stored_text_metrics.unicode_chars)
            << " finish_reason=" << envelope.value("finish_reason", "");
    }
    bool is_new = false;
    const int64_t user1 = std::min(user_id, bot_user_id);
    const int64_t user2 = std::max(user_id, bot_user_id);
    const int64_t reply_room = room_reply ? room_request.at("collaboration").at("room_id").get<int64_t>() : 0;
    if (!PersistHermesReply(
            &message, &is_new,
            [this, &err](Message* msg, bool* fresh) {
                return store_ && store_->AppendMessageHotPath(
                                     msg->session_id, msg->sender_id, msg->msg_type,
                                     msg->content_json, msg->timestamp_ms,
                                     msg->client_msg_id, msg, fresh, &err);
            },
            [this, user1, user2, reply_room, &err](const Message& msg) {
                return PersistToTopic(msg, reply_room ? "chatroom" : "single",
                                      reply_room ? 0 : user1, reply_room ? 0 : user2, reply_room, &err);
            })) {
        LOG_ERROR << "Persist Hermes reply failed request_id=" << request_id << ": "
                  << err;
        MetricsRegistry::Instance().Increment("spark_push_hermes_errors_total");
        return false;
    }
    const auto persisted_content_metrics = MeasureText(message.content_json);
    if (LengthAuditEnabled()) {
        LOG_INFO << "length_audit stage=storage_after_enqueue request_id=" << request_id
                 << " content_json_bytes=" << persisted_content_metrics.utf8_bytes
                 << " content_json_chars=" << persisted_content_metrics.unicode_chars
                 << " is_new=" << (is_new ? "true" : "false");
    }
    if (!is_new) {
        MetricsRegistry::Instance().Increment(
            "spark_push_hermes_duplicate_replies_total");
    }
    // Only Kafka persistence confirmation closes the ephemeral stream and
    // exposes this answer to future Agent context. Job persists asynchronously.
    if (room_reply && !room_agent_service_->Complete(envelope, &err)) return false;
    MarkHermesStreamCompleted(request_id);
    RememberHermesMessage(message);

    std::map<std::string, std::vector<int64_t>> comets;
    if (!AgentRoutes(reply_room, user_id, &comets)) {
        LOG_ERROR << "Get Hermes recipient routes failed user_id="
                  << std::to_string(user_id);
        return false;
    }
    ChatMessage chat_message;
    FillChatMessage(message, &chat_message);
    for (const auto& route : comets) {
        const auto& comet_id = route.first;
        PushToCometRequest request;
        request.set_comet_id(comet_id);
        request.set_request_id(message.msg_id + "@" + comet_id);
        request.set_scene(room_reply ? "group" : "single");
        *request.mutable_message() = chat_message;
        for (auto target : route.second) request.add_targets()->set_user_id(target);

        std::string push_payload;
        auto* producer = room_reply ? group_producer_ : producer_;
        if (!request.SerializeToString(&push_payload) || !producer ||
            !producer->Send(comet_id, push_payload)) {
            LOG_ERROR << "Enqueue Hermes reply to Comet failed comet_id=" << comet_id
                      << ", request_id=" << request_id;
            continue;
        }
        MetricsRegistry::Instance().Increment(
            "spark_push_hermes_realtime_enqueued_total");
    }
    MetricsRegistry::Instance().Increment(ok ? "spark_push_hermes_replies_total"
                                             : "spark_push_hermes_errors_total");
    return true;
}

bool MessageService::HandleHermesDelta(const std::string& payload,
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
    if (request_id.empty() || user_id <= 0 || !agent_bot_users_.count(bot_user_id) ||
        text.empty() || delta_index < 0) {
        MetricsRegistry::Instance().Increment(
            "spark_push_hermes_stream_delta_errors_total");
        return true;
    }
    std::string session_id = delta.value("session_id", "");
    nlohmann::json room_request;
    const bool room_delta = session_id.rfind("r_", 0) == 0;
    std::string room_error;
    if (room_delta && (!room_agent_service_ ||
        !room_agent_service_->ValidateReply(delta, &room_request, &room_error))) return true;
    const int64_t delta_room = room_delta ? room_request.at("collaboration").at("room_id").get<int64_t>() : 0;
    const auto expected_session = "s_" +
                                  std::to_string(std::min(user_id, bot_user_id)) + "_" +
                                  std::to_string(std::max(user_id, bot_user_id));
    if (session_id.empty()) session_id = expected_session;
    if (!room_delta && session_id != expected_session) return true;

    const bool progress = delta.value("progress", nlohmann::json()) == true;
    nlohmann::json normalized_event = {
        {"schema", "sparkpush.agent_event.v1"},
        {"type", progress ? "assistant_progress" : "assistant_delta"},
        {"data", {{"text", text}}}};
    if (delta.contains("agent_event")) {
        if (!NormalizeAgentEvent(delta["agent_event"], &normalized_event) ||
            normalized_event["type"] !=
                (progress ? "assistant_progress" : "assistant_delta") ||
            normalized_event["data"]["text"] != text) {
            MetricsRegistry::Instance().Increment(
                "spark_push_agent_event_invalid_total");
            return true;
        }
        if (normalized_event.contains("envelope")) {
            const auto& envelope = normalized_event["envelope"];
            if (envelope.value("request_id", "") != request_id ||
                envelope.value("conversation_id", "") != session_id ||
                envelope.value("sequence", -1) != delta_index ||
                envelope.value("terminal", true)) {
                MetricsRegistry::Instance().Increment(
                    "spark_push_agent_event_invalid_total");
                return true;
            }
        }
    }
    if (IsHermesStreamCompleted(request_id)) return true;

    const std::string delta_id =
        "hermes_delta:" + request_id + ":" + std::to_string(delta_index);
    if (MarkHermesDeltaSeen(delta_id)) {
        MetricsRegistry::Instance().Increment(
            "spark_push_hermes_stream_delta_duplicate_total");
        return true;
    }

    std::map<std::string, std::vector<int64_t>> comets;
    if (!AgentRoutes(delta_room, user_id, &comets)) {
        LOG_ERROR << "Get Hermes delta recipient routes failed user_id="
                  << std::to_string(user_id);
        return false;
    }
    if (comets.empty()) {
        MetricsRegistry::Instance().Increment(
            "spark_push_hermes_stream_delta_offline_total");
        return true;
    }

    const int64_t now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
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
        {"progress", progress},
        {"source", "hermes"},
        {"agent_event", normalized_event},
    };

    if (room_delta) {
        content["group_id"] = delta_room;
        content["actor_user_id"] = user_id;
        content["task_id"] = room_request.at("collaboration").at("task_id");
        content.erase("to_user_id");
    }

    ChatMessage message;
    message.set_msg_id(delta_id);
    message.set_session_id(session_id);
    message.set_msg_seq(0);  // 增量不进入历史游标。
    message.set_sender_id(bot_user_id);
    message.set_timestamp_ms(now_ms);
    message.set_msg_type("hermes_delta");
    message.set_content_json(content.dump());
    message.set_client_msg_id(delta_id);

    for (const auto& route : comets) {
        const auto& comet_id = route.first;
        PushToCometRequest request;
        request.set_comet_id(comet_id);
        request.set_request_id(delta_id + "@" + comet_id);
        request.set_scene(room_delta ? "group" : "single");
        *request.mutable_message() = message;
        for (auto target : route.second) request.add_targets()->set_user_id(target);
        std::string push_payload;
        auto* producer = room_delta ? group_producer_ : producer_;
        if (!request.SerializeToString(&push_payload) || !producer ||
            !producer->Send(comet_id, push_payload)) {
            LOG_ERROR << "Enqueue Hermes delta to Comet failed comet_id=" << comet_id
                      << ", request_id=" << request_id;
            MetricsRegistry::Instance().Increment(
                "spark_push_hermes_stream_delta_errors_total");
            continue;
        }
        MetricsRegistry::Instance().Increment(
            "spark_push_hermes_stream_delta_enqueued_total");
    }
    return true;
}

bool MessageService::AgentRoutes(int64_t room, int64_t user,
                                std::map<std::string, std::vector<int64_t>>* routes) {
    if (!redis_store_) return false;
    std::vector<int64_t> users{user};
    std::string error;
    if (room && (!group_member_dao_ || !group_member_dao_->ListRoomMembers(room, &users, &error))) return false;
    for (auto id : users) {
        std::vector<std::string> comets;
        if (!redis_store_->GetUserRoutes(id, &comets)) return false;
        for (const auto& comet : comets) (*routes)[comet].push_back(id);
    }
    return true;
}

BroadcastResult MessageService::PublishBroadcast(const std::string& scope,
                                                 int64_t group_id,
                                                 const std::string& content_json,
                                                 bool rate_limit) {
    BroadcastResult result;
    result.scope = scope;
    result.group_id = group_id;
    if (rate_limit &&
        !rate_limiter_.Allow("broadcast", scope.empty() ? "all" : scope)) {
        MetricsRegistry::Instance().Increment(
            "spark_push_rate_limited_total_broadcast");
        result.status =
            ServiceStatus{429, "broadcast rate limit exceeded; retry later"};
        return result;
    }
    if (!broadcast_producer_) {
        result.status = ServiceStatus{500, "broadcast producer not initialized"};
        result.task_id.clear();
        return result;
    }

    // 简单生成一个 task_id：当前毫秒时间戳 + 随机数
    int64_t now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                         std::chrono::system_clock::now().time_since_epoch())
                         .count();
    std::string task_id =
        "bcast-" + std::to_string(now_ms) + "-" + std::to_string(rand());

    BroadcastTaskRequest task;
    task.set_task_id(task_id);
    task.set_scope(scope);
    task.set_group_id(group_id);
    task.set_content_json(content_json);
    if (scope != "all" && scope != "group") {
        result.status = {400, "unsupported broadcast scope"};
        return result;
    }
    if (scope == "group") {
        std::vector<int64_t> members;
        std::string error;
        if (group_id <= 0 || !group_member_dao_ ||
            !group_member_dao_->ListRoomMembers(group_id, &members, &error)) {
            result.status = {400, "cannot resolve broadcast room membership"};
            return result;
        }
        for (auto user : members) task.add_user_ids(user);
    }

    std::string payload;
    if (!task.SerializeToString(&payload)) {
        result.status = ServiceStatus{500, "serialize BroadcastTaskRequest failed"};
        result.task_id.clear();
        return result;
    }
    LOG_INFO << "Sending broadcast task " << task_id
             << ", payload_bytes=" << payload.size();
    if (!broadcast_producer_->Send(task_id, payload)) {
        result.status = ServiceStatus{500, "send broadcast task to kafka failed"};
        result.task_id.clear();
        return result;
    }

    result.status = ServiceStatus{0, "ok"};
    result.task_id = task_id;
    return result;
}

UpstreamResult MessageService::SendLegacyText(int64_t sender, int64_t recipient,
                                              const std::string& content) {
    UpstreamResult result;
    if (sender <= 0 || recipient <= 0 || content.empty()) {
        result.status = {400, "from_user_id/to_user_id/content required"};
        return result;
    }
    if (content.size() > 64 * 1024) {
        result.status = {413, "message exceeds 64 KiB"};
        return result;
    }
    User user;
    std::string user_error;
    if (!GetActiveUser(sender, &user, &user_error) ||
        !GetActiveUser(recipient, &user, &user_error)) {
        result.status = {403, "sender or recipient is unavailable"};
        return result;
    }
    if (!rate_limiter_.Allow("single", std::to_string(sender))) {
        result.status = {429, "single rate limit exceeded; retry later"};
        return result;
    }
    if (!store_) {
        result.status = {500, "conversation store not initialized"};
        return result;
    }
    Session session;
    std::string error;
    if (!store_->GetOrCreateSingleSession(sender, recipient, &session, &error)) {
        result.status = {500, "get session failed: " + error};
        return result;
    }
    const auto now = std::chrono::duration_cast<std::chrono::milliseconds>(
                         std::chrono::system_clock::now().time_since_epoch())
                         .count();
    const auto wire = nlohmann::json{{"type", "single_chat"}, {"session_id", session.id},
        {"from_user_id", sender}, {"to_user_id", recipient}, {"content", {{"text", content}}},
        {"create_time", now}}.dump();
    if (!store_->AppendMessage(session, sender, "text", wire, now, "",
                               &result.message, &error)) {
        result.status = {500, "append message failed: " + error};
        return result;
    }
    result.has_message = true;
    return result;
}
BroadcastResult MessageService::Broadcast(const std::string& scope, int64_t group,
                                          const std::string& content) {
    return PublishBroadcast(scope, group, content, true);
}
BroadcastResult MessageService::BroadcastAdmin(int64_t actor, const std::string& scope,
                                               int64_t room, const std::string& text) {
    const auto access = RequireAdministrator(actor);
    if (!access.ok()) return {access, {}};
    if (text.empty()) return {{400, "text is required"}, {}};
    if (scope != "all" && scope != "chatroom")
        return {{400, "scope must be all or chatroom"}, {}};
    if (scope == "chatroom" && room <= 0)
        return {{400, "room_id required when scope=chatroom"}, {}};
    const std::string real_scope = scope == "all" ? "all" : "group";
    const int64_t group = scope == "chatroom" ? room : 0;
    // Keep the legacy HTTP payload and rate-limit policy unchanged.
    nlohmann::json content = {{"type", "system_broadcast"}, {"scope", real_scope},
                              {"content", {{"text", text}}}};
    if (group > 0) content["group_id"] = group;
    return PublishBroadcast(real_scope, group, content.dump(), false);
}

}  // namespace sparkpush
