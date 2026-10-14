#pragma once

#include <grpcpp/grpcpp.h>

#include "services/conversation_service.h"
#include "services/message_service.h"
#include "services/room_service.h"
#include "services/user_service.h"
#include "spark_push.grpc.pb.h"

namespace sparkpush {

// gRPC transport adapter: protobuf conversion and streaming I/O only.
// Reusable business rules live in logic/services, not in the RPC class.
//
// 译：gRPC 传输适配层：只做 protobuf 转换与流式 I/O。
// 可复用的业务规则住在 logic/services 里，不在本 RPC 类里。
//
// 本类与 http_server.cpp 是**同一组 service 的两个适配层**，分工完全对称：
//   鉴权模型不同：HTTP 用 Bearer token 解出用户后代入 actor；
//                 gRPC 用 AuthorizeInternalRpc 验角色密钥（comet/admin）。
//   调用方不同：  HTTP 面向客户端/管理后台；gRPC 面向 Comet 与管理工具。
// 两边都不应该出现业务判断，否则同一规则会在两处逐渐分叉。
//
// 另一个贯穿全文件的约定：**业务错误走带内（ErrorInfo），不走 gRPC Status**。
// 除了 AuthorizeInternalRpc 失败会返回真的 UNAUTHENTICATED，其余 handler
// 一律 return ::grpc::Status::OK，错误码放在 response 的 error 字段里。
// 理由：MessageStream 是**多路复用**的，一个 gRPC 层面的错误会整条流
// 关掉，连带弄死其他正在飞的请求；而且在流里把“参数非法”当成传输
// 错误会让 comet 误判为“链路断了”而触发重连。把两者分开后，
// 传输层错误 = 重连，带内错误 = 回给客户端。
class LogicServiceImpl final : public sparkpush::LogicService::Service {
   public:
    // 四个 service 都是借用（不拥有），生命周期由 main.cpp 保证。
    // 注意没有注入 AdminService：管理类操作（封禁/建房间）走 HTTP，
    // gRPC 上只有一个 Broadcast，而且它验的是 "admin" 角色密钥。
    LogicServiceImpl(UserService* users, ConversationService* conversations,
                     MessageService* messages, RoomService* rooms)
        : users_(users),
          conversations_(conversations),
          messages_(messages),
          rooms_(rooms) {}

    // Comet 握手时调：验 token + 注册路由（user -> comet）。
    // 回复里除了 user_id 还带 route_token / route_generation，
    // 两者共同构成“只有当前这条连接能撤销自己的路由”的凭证。
    ::grpc::Status VerifyToken(::grpc::ServerContext* context,
                               const ::sparkpush::VerifyTokenRequest* request,
                               ::sparkpush::VerifyTokenReply* response) override;

    // 单请求上行。当 Comet 配置为 use_stream=false 时走这条；
    // 开了流则同样的逻辑走 MessageStream 的 kUpstream 分支。
    ::grpc::Status SendUpstreamMessage(
        ::grpc::ServerContext* context,
        const ::sparkpush::UpstreamMessageRequest* request,
        ::sparkpush::UpstreamMessageReply* response) override;

    // 双向流：Comet 复用连接上行（对齐 06）
    //
    // 一条流上多路复用四种请求（upstream / offline / room_join / room_leave），
    // 靠 request_id 回显做关联。
    //
    // **重要：实现是 Read -> 处理 -> Write 的严格串行循环**，因此单条流上
    // 同一时刻只有一个请求在处理中。而处理里可能包含同步等 Kafka
    // delivery report（messages_->Submit），耗时是毫秒级。
    // 并发因此完全来自 Comet 侧：它建 stream_count_（默认 4）条流，
    // 按 request_id 取模分片（见 comet_server.h 的 InitStreams/NextRequestId）。
    // 这也解释了为什么 Comet 需要 pending 表 + deadline 看门狗：
    // 一条流上排在慢请求后面的所有请求都会被拖住。
    // 若以后要把本 handler 改成并发处理，必须先把“同一用户的上行
    // 必须按序处理”这个不变量重新想清楚（并发会打乱 msg_seq 分配顺序）。
    ::grpc::Status MessageStream(
        ::grpc::ServerContext* context,
        ::grpc::ServerReaderWriter<::sparkpush::StreamResponse,
                                   ::sparkpush::StreamMessage>* stream) override;

    ::grpc::Status UserOffline(::grpc::ServerContext* context,
                               const ::sparkpush::UserOfflineRequest* request,
                               ::sparkpush::SimpleReply* response) override;

    ::grpc::Status ReportRoomJoin(::grpc::ServerContext* context,
                                  const ::sparkpush::RoomReportRequest* request,
                                  ::sparkpush::SimpleReply* response) override;

    ::grpc::Status ReportRoomLeave(::grpc::ServerContext* context,
                                   const ::sparkpush::RoomReportRequest* request,
                                   ::sparkpush::SimpleReply* response) override;

    ::grpc::Status Broadcast(::grpc::ServerContext* context,
                             const ::sparkpush::BroadcastRequest* request,
                             ::sparkpush::BroadcastReply* response) override;

    ::grpc::Status SyncMessages(::grpc::ServerContext* context,
                                const ::sparkpush::SyncMessagesRequest* request,
                                ::sparkpush::SyncMessagesReply* response) override;

    ::grpc::Status SyncOffline(::grpc::ServerContext* context,
                               const ::sparkpush::SyncOfflineRequest* request,
                               ::sparkpush::SyncOfflineReply* response) override;

    ::grpc::Status MarkDelivered(::grpc::ServerContext* context,
                                 const ::sparkpush::MarkDeliveredRequest* request,
                                 ::sparkpush::SimpleReply* response) override;

   private:
    // 上行消息的实际处理，被单请求 RPC 与流式 RPC **共用**。
    // 抽出来的理由不只是去重：两条路径必须给出完全相同的回复字段
    //（尤其是 ack_stage 与 accepted_at_ms），否则 Comet 切换
    // use_stream 开关时客户端行为会变。分开写迟早会分叉。
    // 注意它不做鉴权：鉴权在两个调用方各自的开头完成。
    void HandleUpstreamMessage(const UpstreamMessageRequest&, UpstreamMessageReply*);
    UserService* users_;
    ConversationService* conversations_;
    MessageService* messages_;
    RoomService* rooms_;
};
}  // namespace sparkpush
