#include "grpc_service.h"

#include "logging.h"
#include "message_codec.h"

namespace sparkpush {
namespace {
void SetError(ErrorInfo* error, int code, const std::string& message) {
    if (code != 0) LOG_WARN << "Logic error code=" << code << ", message=" << message;
    error->set_code(code);
    error->set_message(message);
}
void SetError(ErrorInfo* error, const ServiceStatus& status) {
    SetError(error, status.code, status.message);
}
UpstreamCommand DecodeUpstream(const UpstreamMessageRequest& request) {
    return {request.scene(),          request.from_user_id(), request.to_user_id(),
            request.group_id(),       request.content_json(), request.client_msg_id(),
            request.source_comet_id()};
}
}  // namespace

::grpc::Status LogicServiceImpl::VerifyToken(::grpc::ServerContext*,
                                             const VerifyTokenRequest* request,
                                             VerifyTokenReply* response) {
    LOG_INFO << "VerifyToken called for comet_id=" << request->comet_id();
    int64_t user = 0;
    const auto status = users_->Authenticate(
        request->token(), UserService::TokenAudience::kRealtimeUser, &user);
    SetError(response->mutable_error(), status);
    if (status.ok()) {
        response->set_user_id(user);
        messages_->AddAuthenticatedRoute(user, request->comet_id());
    }
    return ::grpc::Status::OK;
}

void LogicServiceImpl::HandleUpstreamMessage(const UpstreamMessageRequest& request,
                                             UpstreamMessageReply* response) {
    const auto result = messages_->Submit(DecodeUpstream(request));
    SetError(response->mutable_error(), result.status);
    if (result.has_message)
        FillChatMessage(result.message, response->mutable_message());
    if (result.accepted) {
        response->set_ack_stage(ACK_STAGE_ACCEPTED);
        response->set_accepted_at_ms(result.accepted_at_ms);
    }
}
::grpc::Status LogicServiceImpl::SendUpstreamMessage(
    ::grpc::ServerContext*, const UpstreamMessageRequest* request,
    UpstreamMessageReply* response) {
    HandleUpstreamMessage(*request, response);
    return ::grpc::Status::OK;
}

::grpc::Status LogicServiceImpl::MessageStream(
    ::grpc::ServerContext*,
    ::grpc::ServerReaderWriter<::sparkpush::StreamResponse, ::sparkpush::StreamMessage>*
        stream) {
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
                messages_->UserOffline(msg.offline().user_id(),
                                       msg.offline().comet_id());
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

::grpc::Status LogicServiceImpl::UserOffline(::grpc::ServerContext*,
                                             const UserOfflineRequest* request,
                                             SimpleReply* response) {
    messages_->UserOffline(request->user_id(), request->comet_id());
    SetError(response->mutable_error(), {});
    return ::grpc::Status::OK;
}
::grpc::Status LogicServiceImpl::ReportRoomJoin(::grpc::ServerContext*,
                                                const RoomReportRequest* request,
                                                SimpleReply* response) {
    SetError(response->mutable_error(),
             rooms_->ReportRoomJoin(request->room_id(), request->comet_id(),
                                    request->user_id()));
    return ::grpc::Status::OK;
}
::grpc::Status LogicServiceImpl::ReportRoomLeave(::grpc::ServerContext*,
                                                 const RoomReportRequest* request,
                                                 SimpleReply* response) {
    SetError(response->mutable_error(),
             rooms_->ReportRoomLeave(request->room_id(), request->comet_id(),
                                     request->user_id()));
    return ::grpc::Status::OK;
}
::grpc::Status LogicServiceImpl::Broadcast(::grpc::ServerContext*,
                                           const BroadcastRequest* request,
                                           BroadcastReply* response) {
    const auto result = messages_->Broadcast(request->scope(), request->group_id(),
                                             request->content_json());
    SetError(response->mutable_error(), result.status);
    response->set_task_id(result.task_id);
    return ::grpc::Status::OK;
}
::grpc::Status LogicServiceImpl::SyncMessages(::grpc::ServerContext*,
                                              const SyncMessagesRequest* request,
                                              SyncMessagesReply* response) {
    MessagePage page;
    const auto status =
        conversations_->SyncAfter(request->user_id(), request->session_id(),
                                  request->after_seq(), request->limit(), &page);
    SetError(response->mutable_error(), status);
    if (status.ok()) {
        for (const auto& message : page.messages)
            FillChatMessage(message, response->add_messages());
        response->set_has_more(page.has_more);
        response->set_next_seq(page.next_seq);
    }
    return ::grpc::Status::OK;
}
::grpc::Status LogicServiceImpl::SyncOffline(::grpc::ServerContext*,
                                             const SyncOfflineRequest* request,
                                             SyncOfflineReply* response) {
    MessagePage page;
    const auto status =
        conversations_->SyncOffline(request->user_id(), request->limit(), &page);
    SetError(response->mutable_error(), status);
    if (status.ok()) {
        for (const auto& message : page.messages)
            FillChatMessage(message, response->add_messages());
        response->set_has_more(page.has_more);
    }
    return ::grpc::Status::OK;
}
::grpc::Status LogicServiceImpl::MarkDelivered(::grpc::ServerContext*,
                                               const MarkDeliveredRequest* request,
                                               SimpleReply* response) {
    std::vector<DeliveryCursor> cursors;
    for (const auto& cursor : request->cursors())
        cursors.push_back({request->user_id(), cursor.session_id(), cursor.msg_seq()});
    for (const auto& cursor : request->user_cursors())
        cursors.push_back({cursor.user_id(), cursor.session_id(), cursor.msg_seq()});
    SetError(response->mutable_error(),
             conversations_->MarkDelivered(request->user_id(),
                                           request->user_cursors_size() > 0, cursors));
    return ::grpc::Status::OK;
}
}  // namespace sparkpush
