#pragma once

#include <chrono>
#include <deque>
#include <map>
#include <mutex>
#include <nlohmann/json.hpp>
#include <string>
#include <unordered_map>
#include <unordered_set>

#include "hermes_command.h"
#include "model.h"
#include "rate_limiter.h"
#include "services/service_status.h"

namespace sparkpush {
class ConversationStore;
class GroupMemberDao;
class UserService;
class KafkaProducer;
class RedisStore;
class AttachmentService;
struct UpstreamCommand {
    std::string scene;
    int64_t from_user_id{0};
    int64_t to_user_id{0};
    int64_t group_id{0};
    std::string content_json;
    std::string client_msg_id;
    std::string source_comet_id;
};
struct UpstreamResult {
    ServiceStatus status;
    Message message;
    bool has_message{false};
    bool accepted{false};
    int64_t accepted_at_ms{0};
};
struct BroadcastResult {
    ServiceStatus status;
    std::string task_id;
    std::string scope;
    int64_t group_id{0};
};

// Business orchestration shared by unary/streaming adapters and Kafka consumers.
// Owns rate limits, idempotent acceptance and Agent lifecycle caches, NOT RPCs.
class MessageService {
   public:
    MessageService(ConversationStore* store, GroupMemberDao* members,
                   UserService* users, KafkaProducer* single_producer,
                   KafkaProducer* group_producer, KafkaProducer* broadcast_producer,
                   KafkaProducer* persist_producer, RedisStore* redis,
                   const RateLimitConfig& rate_limit, int persist_timeout_ms,
                   KafkaProducer* agent_requests, bool agent_enabled,
                   int64_t agent_user_id, std::map<int64_t, std::string> agent_users,
                   AttachmentService* attachments);
    UpstreamResult Submit(const UpstreamCommand& command);
    // Compatibility path: old HTTP API synchronously persists a text message,
    // with no accepted/delivered ACK or realtime fanout. Do not route it through
    // Submit implicitly or claim it has the WS reliability contract.
    UpstreamResult SendLegacyText(int64_t sender, int64_t recipient,
                                  const std::string& content);
    void AddAuthenticatedRoute(int64_t user, const std::string& comet);
    void UserOffline(int64_t user, const std::string& comet);
    BroadcastResult Broadcast(const std::string& scope, int64_t group_id,
                              const std::string& content_json);
    BroadcastResult BroadcastAdmin(int64_t actor, const std::string& scope,
                                   int64_t room_id, const std::string& text);
    bool HandleHermesReply(const std::string& payload, const std::string& kafka_key);
    bool HandleHermesDelta(const std::string& payload, const std::string& kafka_key);

   private:
    BroadcastResult PublishBroadcast(const std::string& scope, int64_t group_id,
                                     const std::string& content_json, bool rate_limit);
    bool PersistToTopic(const Message&, const std::string& scene, int64_t user1,
                        int64_t user2, int64_t room_id, std::string* error);
    bool BuildHermesRequest(const Message&, nlohmann::json*, std::string*);
    bool EnqueueHermesRequest(const Message&, std::string*);
    void RememberHermesMessage(const Message&);
    bool IsHermesStreamCompleted(const std::string&);
    void MarkHermesStreamCompleted(const std::string&);
    bool MarkHermesDeltaSeen(const std::string&);
    bool GetActiveUser(int64_t user, User* result, std::string* error);
    bool AuthorizeMessageImages(int64_t user, const std::string& session,
                                const std::string& content, std::string* error);

    ConversationStore* store_;
    GroupMemberDao* group_member_dao_;
    UserService* user_service_;
    KafkaProducer* producer_;
    KafkaProducer* group_producer_;
    KafkaProducer* broadcast_producer_;
    KafkaProducer* persist_producer_;
    KafkaProducer* hermes_request_producer_;
    RedisStore* redis_store_;
    AttachmentService* attachment_service_;
    SceneRateLimiter rate_limiter_;
    int persist_kafka_timeout_ms_;
    bool hermes_enabled_;
    int64_t hermes_bot_user_id_;
    std::map<int64_t, std::string> agent_bot_users_;
    std::mutex hermes_cache_mu_;
    std::unordered_map<std::string, std::deque<Message>> hermes_history_cache_;
    std::mutex hermes_stream_mu_;
    std::unordered_set<std::string> hermes_stream_completed_;
    std::deque<std::string> hermes_stream_completed_order_;
    std::unordered_set<std::string> hermes_delta_seen_;
    std::deque<std::string> hermes_delta_seen_order_;
};
}  // namespace sparkpush
