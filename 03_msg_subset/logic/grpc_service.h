#pragma once

#include <grpcpp/grpcpp.h>

#include <memory>
#include <mutex>
#include <unordered_map>
#include <unordered_set>

#include "conversation_store.h"
#include "kafka_producer.h"
#include "redis_store.h"
#include "spark_push.grpc.pb.h"
#include "user_dao.h"

namespace sparkpush {

class LogicServiceImpl final : public sparkpush::LogicService::Service {
   public:
    LogicServiceImpl(UserDao* user_dao, KafkaProducer* producer,
                     RedisStore* redis_store, ConversationStore* conversation_store);

    ::grpc::Status VerifyToken(::grpc::ServerContext* context,
                               const ::sparkpush::VerifyTokenRequest* request,
                               ::sparkpush::VerifyTokenReply* response) override;

    ::grpc::Status SendUpstreamMessage(
        ::grpc::ServerContext* context,
        const ::sparkpush::UpstreamMessageRequest* request,
        ::sparkpush::UpstreamMessageReply* response) override;

    ::grpc::Status AckMessage(::grpc::ServerContext* context,
                              const ::sparkpush::AckMessageRequest* request,
                              ::sparkpush::AckMessageReply* response) override;

    ::grpc::Status UserOffline(::grpc::ServerContext* context,
                               const ::sparkpush::UserOfflineRequest* request,
                               ::sparkpush::SimpleReply* response) override;

   private:
    void SetError(ErrorInfo* e, int code, const std::string& msg);

    UserDao* user_dao_;
    KafkaProducer* producer_;
    RedisStore* redis_store_;
    ConversationStore* conversation_store_;
};

}  // namespace sparkpush