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
#include "room_store.h"
#include "spark_push.grpc.pb.h"
#include "user_dao.h"

namespace sparkpush {

// LogicServiceImpl：logic 服务的 gRPC 接口实现
//
// 说明：
// - 继承自 proto 生成的 Service 基类，实现 comet 侧需要调用的 RPC
// - 主要承担：token 校验、上行消息接收、用户离线通知等
class LogicServiceImpl final : public sparkpush::LogicService::Service {
   public:
    // 构造函数：注入依赖与路由 TTL 配置
    LogicServiceImpl(UserDao* user_dao, KafkaProducer* push_producer,
                     RedisStore* redis_store, RoomStore* room_store,
                     int connection_ttl_ms);

    // 验证 token：成功时返回 user_id，并写入/刷新用户路由信息（用于推送定位）
    ::grpc::Status VerifyToken(
        ::grpc::ServerContext* context,
        const ::sparkpush::VerifyTokenRequest* request,
        ::sparkpush::VerifyTokenReply* response) override;

    // 上行消息入口：comet 把客户端消息透传到 logic，由 logic 负责分发/落库/推送
    ::grpc::Status SendUpstreamMessage(
        ::grpc::ServerContext* context,
        const ::sparkpush::UpstreamMessageRequest* request,
        ::sparkpush::UpstreamMessageReply* response) override;

    // 用户离线通知：comet 在连接断开后调用，用于清理路由/统计等
    ::grpc::Status UserOffline(::grpc::ServerContext* context,
                               const ::sparkpush::UserOfflineRequest* request,
                               ::sparkpush::SimpleReply* response) override;

   private:
    // 统一填充错误信息（业务错误码 + 文本消息）
    void SetError(ErrorInfo* e, int code, const std::string& msg);

    // 用户 DAO（MySQL）：用于查询用户信息等
    UserDao* user_dao_;
    // Kafka 推送生产者：写入推送 topic
    KafkaProducer* push_producer_;
    // Kafka 持久化生产者：写入持久化 topic（如有配置）
    KafkaProducer* persist_producer_;
    // Redis 存储：token 校验、路由表、在线信息等
    RedisStore* redis_store_;
    // 房间/话题存储：用于房间消息分发、成员查询等
    RoomStore* room_store_{nullptr};
    // 路由/连接信息 TTL（毫秒）：用于自动过期清理
    int connection_ttl_ms_{60000};
};

}  // namespace sparkpush