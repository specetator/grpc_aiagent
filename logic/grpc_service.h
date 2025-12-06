#pragma once

#include <grpcpp/grpcpp.h>

#include <memory>
#include <mutex>
#include <unordered_map>
#include <unordered_set>

#include "conversation_store.h"
#include "group_dao.h"
#include "kafka_producer.h"
#include "redis_store.h"
#include "spark_push.grpc.pb.h"
#include "user_dao.h"

namespace sparkpush {

// LogicService 的具体实现，主要负责鉴权、上行消息处理、路由与调用 JobService
class LogicServiceImpl final : public sparkpush::LogicService::Service {
   public:
    // 功能：构造逻辑服务实现，注入会话/群组/用户/Kafka/Redis 依赖
    // 参数：store 会话存储；group_member_dao 群成员 DAO；user_dao 用户 DAO；
    //       producer 推送 Kafka；broadcast_producer 广播 Kafka；redis_store
    //       路由存储
    // 返回：无
    LogicServiceImpl(ConversationStore* store, GroupMemberDao* group_member_dao,
                     UserDao* user_dao, KafkaProducer* producer,
                     KafkaProducer* broadcast_producer,
                     RedisStore* redis_store);

    // 功能：校验 token，写入用户路由
    // 参数：context gRPC 上下文；request 请求体；response 返回体
    // 返回：grpc::Status 永远 OK，业务状态写入 response
    ::grpc::Status VerifyToken(
        ::grpc::ServerContext* context,
        const ::sparkpush::VerifyTokenRequest* request,
        ::sparkpush::VerifyTokenReply* response) override;

    // 功能：处理客户端上行消息，写入会话并推送
    // 参数：context 上下文；request 请求；response 返回
    // 返回：grpc::Status OK，错误码在 response 中
    ::grpc::Status SendUpstreamMessage(
        ::grpc::ServerContext* context,
        const ::sparkpush::UpstreamMessageRequest* request,
        ::sparkpush::UpstreamMessageReply* response) override;

    // 功能：用户离线上报，移除路由
    // 参数：context 上下文；request 请求；response 返回
    // 返回：grpc::Status OK
    ::grpc::Status UserOffline(::grpc::ServerContext* context,
                               const ::sparkpush::UserOfflineRequest* request,
                               ::sparkpush::SimpleReply* response) override;

    // 功能：上报用户进入聊天室，维护房间路由与在线数
    // 参数：context 上下文；request 请求；response 返回
    // 返回：grpc::Status OK
    ::grpc::Status ReportRoomJoin(::grpc::ServerContext* context,
                                  const ::sparkpush::RoomReportRequest* request,
                                  ::sparkpush::SimpleReply* response) override;

    // 功能：上报用户离开聊天室，回收路由与在线数
    // 参数：context 上下文；request 请求；response 返回
    // 返回：grpc::Status OK
    ::grpc::Status ReportRoomLeave(
        ::grpc::ServerContext* context,
        const ::sparkpush::RoomReportRequest* request,
        ::sparkpush::SimpleReply* response) override;

    // 功能：触发广播任务，写入广播 Kafka topic
    // 参数：context 上下文；request 请求；response 返回 task_id
    // 返回：grpc::Status OK
    ::grpc::Status Broadcast(::grpc::ServerContext* context,
                             const ::sparkpush::BroadcastRequest* request,
                             ::sparkpush::BroadcastReply* response) override;

   private:
    void SetError(ErrorInfo* e, int code, const std::string& msg);

    // 简单路由结构：user_id -> set<comet_id>
    void AddRoute(int64_t user_id, const std::string& comet_id);
    void RemoveRoute(int64_t user_id, const std::string& comet_id);
    std::unordered_set<std::string> GetUserComets(int64_t user_id);

    ConversationStore* store_;
    GroupMemberDao* group_member_dao_;
    UserDao* user_dao_;
    KafkaProducer* producer_;
    KafkaProducer* broadcast_producer_;
    RedisStore* redis_store_;
};

}  // namespace sparkpush
