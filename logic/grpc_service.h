#pragma once

#include <grpcpp/grpcpp.h>
#include <nlohmann/json.hpp>

#include <memory>
#include <map>
#include <mutex>
#include <chrono>
#include <deque>
#include <string>
#include <unordered_map>
#include <unordered_set>

#include "conversation_store.h"
#include "group_dao.h"
#include "hermes_command.h"
#include "kafka_producer.h"
#include "redis_store.h"
#include "rate_limiter.h"
#include "spark_push.grpc.pb.h"
#include "user_dao.h"

namespace sparkpush {

// LogicService 的具体实现，主要负责鉴权、上行消息处理、路由与调用 JobService
class LogicServiceImpl final : public sparkpush::LogicService::Service {
   public:
    // store / dao / kafka / redis 由外部持有；持久化先写 Kafka topic。
    LogicServiceImpl(ConversationStore* store, GroupMemberDao* group_member_dao,
                     UserDao* user_dao, KafkaProducer* producer,
                     KafkaProducer* group_producer,
                     KafkaProducer* broadcast_producer,
                     KafkaProducer* persist_producer, RedisStore* redis_store,
                     const RateLimitConfig& rate_limit,
                     int persist_kafka_timeout_ms,
                     KafkaProducer* hermes_request_producer,
                     bool hermes_enabled, int64_t hermes_bot_user_id,
                     std::map<int64_t, std::string> agent_bot_users = {});

    ::grpc::Status VerifyToken(
        ::grpc::ServerContext* context,
        const ::sparkpush::VerifyTokenRequest* request,
        ::sparkpush::VerifyTokenReply* response) override;

    ::grpc::Status SendUpstreamMessage(
        ::grpc::ServerContext* context,
        const ::sparkpush::UpstreamMessageRequest* request,
        ::sparkpush::UpstreamMessageReply* response) override;

    // 双向流：Comet 复用连接上行（对齐 06）
    ::grpc::Status MessageStream(
        ::grpc::ServerContext* context,
        ::grpc::ServerReaderWriter<::sparkpush::StreamResponse,
                                   ::sparkpush::StreamMessage>* stream)
        override;

    ::grpc::Status UserOffline(::grpc::ServerContext* context,
                               const ::sparkpush::UserOfflineRequest* request,
                               ::sparkpush::SimpleReply* response) override;

    ::grpc::Status ReportRoomJoin(::grpc::ServerContext* context,
                                  const ::sparkpush::RoomReportRequest* request,
                                  ::sparkpush::SimpleReply* response) override;

    ::grpc::Status ReportRoomLeave(
        ::grpc::ServerContext* context,
        const ::sparkpush::RoomReportRequest* request,
        ::sparkpush::SimpleReply* response) override;

    ::grpc::Status Broadcast(::grpc::ServerContext* context,
                             const ::sparkpush::BroadcastRequest* request,
                             ::sparkpush::BroadcastReply* response) override;

    ::grpc::Status SyncMessages(
        ::grpc::ServerContext* context,
        const ::sparkpush::SyncMessagesRequest* request,
        ::sparkpush::SyncMessagesReply* response) override;

    ::grpc::Status SyncOffline(
        ::grpc::ServerContext* context,
        const ::sparkpush::SyncOfflineRequest* request,
        ::sparkpush::SyncOfflineReply* response) override;

    ::grpc::Status MarkDelivered(
        ::grpc::ServerContext* context,
        const ::sparkpush::MarkDeliveredRequest* request,
        ::sparkpush::SimpleReply* response) override;

    // Hermes Bridge 的最终回复回到普通单聊消息链路：落盘后再投递到 Comet。
    // 返回 false 会让 Kafka consumer 保留消息，等待后续重试。
    bool HandleHermesReply(const std::string& payload,
                           const std::string& kafka_key);

    // Hermes SSE 增量只走实时链路，不写历史；最终 ai_reply 才是权威消息。
    bool HandleHermesDelta(const std::string& payload,
                           const std::string& kafka_key);

   private:
    void SetError(ErrorInfo* e, int code, const std::string& msg);

    // 上行核心（Unary 与双向流共用）
    void HandleUpstreamMessage(const UpstreamMessageRequest& request,
                               UpstreamMessageReply* response);

    void AddRoute(int64_t user_id, const std::string& comet_id);
    void RemoveRoute(int64_t user_id, const std::string& comet_id);
    std::unordered_set<std::string> GetUserComets(int64_t user_id);
    void FillChatMessage(const Message& message, ChatMessage* output);
    bool PersistToTopic(const Message& message, const std::string& scene,
                        int64_t user1, int64_t user2, int64_t room_id,
                        std::string* err);
    bool BuildHermesRequest(const Message& current, nlohmann::json* request,
                            std::string* err);
    bool EnqueueHermesRequest(const Message& current, std::string* err);
    void RememberHermesMessage(const Message& message);
    bool IsHermesStreamCompleted(const std::string& request_id);
    void MarkHermesStreamCompleted(const std::string& request_id);
    bool MarkHermesDeltaSeen(const std::string& delta_id);
    bool GetActiveUser(int64_t user_id, User* user, std::string* err);

    ConversationStore* store_;
    GroupMemberDao* group_member_dao_;
    UserDao* user_dao_;
    KafkaProducer* producer_;
    KafkaProducer* group_producer_;
    KafkaProducer* broadcast_producer_;
    KafkaProducer* persist_producer_;
    KafkaProducer* hermes_request_producer_;
    RedisStore* redis_store_;
    SceneRateLimiter rate_limiter_;
    int persist_kafka_timeout_ms_{5000};
    bool hermes_enabled_{false};
    int64_t hermes_bot_user_id_{0};
    std::map<int64_t, std::string> agent_bot_users_;
    struct UserCacheEntry {
        User user;
        std::chrono::steady_clock::time_point expires_at;
    };
    std::mutex user_cache_mu_;
    std::unordered_map<int64_t, UserCacheEntry> active_user_cache_;
    std::mutex hermes_cache_mu_;
    std::unordered_map<std::string, std::deque<Message>> hermes_history_cache_;
    std::mutex hermes_stream_mu_;
    std::unordered_set<std::string> hermes_stream_completed_;
    std::deque<std::string> hermes_stream_completed_order_;
    std::unordered_set<std::string> hermes_delta_seen_;
    std::deque<std::string> hermes_delta_seen_order_;
};

}  // namespace sparkpush
