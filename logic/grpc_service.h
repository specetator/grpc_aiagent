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
class LogicServiceImpl final : public sparkpush::LogicService::Service {
   public:
    LogicServiceImpl(UserService* users, ConversationService* conversations,
                     MessageService* messages, RoomService* rooms)
        : users_(users),
          conversations_(conversations),
          messages_(messages),
          rooms_(rooms) {}

    ::grpc::Status VerifyToken(::grpc::ServerContext* context,
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
    void HandleUpstreamMessage(const UpstreamMessageRequest&, UpstreamMessageReply*);
    UserService* users_;
    ConversationService* conversations_;
    MessageService* messages_;
    RoomService* rooms_;
};
}  // namespace sparkpush
