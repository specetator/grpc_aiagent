// ============================================================================
// logic 服务的 gRPC 接口实现
//
// 提供给 comet 服务调用的 RPC 接口，当前包含：
// - VerifyToken: 验证客户端提供的 token，返回用户 ID 并记录路由信息
//
// 设计要点：
// 1. 仅实现认证相关的最小接口，后续可扩展消息推送等功能
// 2. 依赖 RedisStore 查询 token 和维护用户路由表
// 3. 采用 gRPC 同步调用模式，适合低延迟的内网服务通信
// ============================================================================
#pragma once

#include <grpcpp/grpcpp.h>

#include <memory>
#include <mutex>
#include <unordered_map>
#include <unordered_set>

#include "kafka_producer.h"
#include "redis_store.h"
#include "spark_push.grpc.pb.h"
#include "user_dao.h"

namespace sparkpush {

// LogicService gRPC 服务实现类
// 继承自 proto 定义生成的 Service 基类，实现具体的业务逻辑
class LogicServiceImpl final : public sparkpush::LogicService::Service {
   public:
    LogicServiceImpl(UserDao* user_dao, KafkaProducer* push_producer,
                     RedisStore* redis_store, int connection_ttl_ms);

    ::grpc::Status VerifyToken(
        ::grpc::ServerContext* context,
        const ::sparkpush::VerifyTokenRequest* request,
        ::sparkpush::VerifyTokenReply* response) override;

    ::grpc::Status SendUpstreamMessage(
        ::grpc::ServerContext* context,
        const ::sparkpush::UpstreamMessageRequest* request,
        ::sparkpush::UpstreamMessageReply* response) override;

    ::grpc::Status UserOffline(::grpc::ServerContext* context,
                               const ::sparkpush::UserOfflineRequest* request,
                               ::sparkpush::SimpleReply* response) override;

   private:
    void SetError(ErrorInfo* e, int code, const std::string& msg);

    UserDao* user_dao_;
    KafkaProducer* push_producer_;
    KafkaProducer* persist_producer_;
    RedisStore* redis_store_;
    int connection_ttl_ms_{60000};
};

}  // namespace sparkpush