#include "grpc_service.h"

#include "logging.h"
#include "internal_rpc.h"
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
            request.source_comet_id(), request.auth_token()};
}
}  // namespace

::grpc::Status LogicServiceImpl::VerifyToken(::grpc::ServerContext* context,
                                             const VerifyTokenRequest* request,
                                             VerifyTokenReply* response) {
    const auto authorized = AuthorizeInternalRpc(context, "comet");
    if (!authorized.ok()) return authorized;
    LOG_INFO << "VerifyToken called for comet_id=" << request->comet_id();
    int64_t user = 0;
    const auto status = users_->Authenticate(
        request->token(), UserService::TokenAudience::kRealtimeUser, &user);
    SetError(response->mutable_error(), status);
    if (status.ok()) {
        response->set_user_id(user);
        std::string token;
        const int64_t generation = messages_->AddAuthenticatedRoute(user, request->comet_id(), &token);
        response->set_route_token(token);
        if (generation <= 0) SetError(response->mutable_error(), 503, "route registration failed");
        else response->set_route_generation(generation);
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
    ::grpc::ServerContext* context, const UpstreamMessageRequest* request,
    UpstreamMessageReply* response) {
    const auto authorized = AuthorizeInternalRpc(context, "comet");
    if (!authorized.ok()) return authorized;
    HandleUpstreamMessage(*request, response);
    return ::grpc::Status::OK;
}

::grpc::Status LogicServiceImpl::MessageStream(
    ::grpc::ServerContext* context,
    ::grpc::ServerReaderWriter<::sparkpush::StreamResponse, ::sparkpush::StreamMessage>*
        stream) {
    const auto authorized = AuthorizeInternalRpc(context, "comet");
    if (!authorized.ok()) return authorized;
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
                                       msg.offline().comet_id(), msg.offline().route_generation(), msg.offline().route_token());
                SetError(reply.mutable_error(), 0, "ok");
                *resp.mutable_error() = reply.error();
                *resp.mutable_simple_reply() = reply;
                break;
            }
            case StreamMessage::kRoomJoin: {
                SimpleReply reply;
                // 复用现有 RPC 逻辑：直接调用成员函数实现
                SetError(reply.mutable_error(), rooms_->ReportRoomJoin(msg.room_join().room_id(), msg.room_join().comet_id(), msg.room_join().user_id()));
                *resp.mutable_error() = reply.error();
                *resp.mutable_simple_reply() = reply;
                break;
            }
            case StreamMessage::kRoomLeave: {
                SimpleReply reply;
                SetError(reply.mutable_error(), rooms_->ReportRoomLeave(msg.room_leave().room_id(), msg.room_leave().comet_id(), msg.room_leave().user_id()));
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

::grpc::Status LogicServiceImpl::UserOffline(::grpc::ServerContext* context,
                                             const UserOfflineRequest* request,
                                             SimpleReply* response) {
    const auto authorized = AuthorizeInternalRpc(context, "comet");
    if (!authorized.ok()) return authorized;
    messages_->UserOffline(request->user_id(), request->comet_id(), request->route_generation(), request->route_token());
    SetError(response->mutable_error(), {});
    return ::grpc::Status::OK;
}
::grpc::Status LogicServiceImpl::ReportRoomJoin(::grpc::ServerContext* context,
                                                const RoomReportRequest* request,
                                                SimpleReply* response) {
    const auto authorized = AuthorizeInternalRpc(context, "comet");
    if (!authorized.ok()) return authorized;
    SetError(response->mutable_error(),
             rooms_->ReportRoomJoin(request->room_id(), request->comet_id(),
                                    request->user_id()));
    return ::grpc::Status::OK;
}
::grpc::Status LogicServiceImpl::ReportRoomLeave(::grpc::ServerContext* context,
                                                 const RoomReportRequest* request,
                                                 SimpleReply* response) {
    const auto authorized = AuthorizeInternalRpc(context, "comet");
    if (!authorized.ok()) return authorized;
    SetError(response->mutable_error(),
             rooms_->ReportRoomLeave(request->room_id(), request->comet_id(),
                                     request->user_id()));
    return ::grpc::Status::OK;
}
::grpc::Status LogicServiceImpl::Broadcast(::grpc::ServerContext* context,
                                           const BroadcastRequest* request,
                                           BroadcastReply* response) {
    const auto authorized = AuthorizeInternalRpc(context, "admin");
    if (!authorized.ok()) return authorized;
    const auto result = messages_->Broadcast(request->scope(), request->group_id(),
                                             request->content_json());
    SetError(response->mutable_error(), result.status);
    response->set_task_id(result.task_id);
    return ::grpc::Status::OK;
}
::grpc::Status LogicServiceImpl::SyncMessages(::grpc::ServerContext* context,
                                              const SyncMessagesRequest* request,
                                              SyncMessagesReply* response) {
    const auto authorized = AuthorizeInternalRpc(context, "comet");
    if (!authorized.ok()) return authorized;
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
        response->set_safe_seq(page.safe_seq);
    }
    return ::grpc::Status::OK;
}
::grpc::Status LogicServiceImpl::SyncOffline(::grpc::ServerContext* context,
                                             const SyncOfflineRequest* request,
                                             SyncOfflineReply* response) {
    const auto authorized = AuthorizeInternalRpc(context, "comet");
    if (!authorized.ok()) return authorized;
    MessagePage page;
    const auto status =
        conversations_->SyncOffline(request->user_id(), request->limit(), &page,
                                     request->after_session_id(), request->after_seq());
    SetError(response->mutable_error(), status);
    if (status.ok()) {
        for (const auto& message : page.messages)
            FillChatMessage(message, response->add_messages());
        response->set_has_more(page.has_more);
        response->set_next_session_id(page.next_session_id);
        response->set_next_seq(page.next_seq);
    }
    return ::grpc::Status::OK;
}
::grpc::Status LogicServiceImpl::MarkDelivered(::grpc::ServerContext* context,
                                               const MarkDeliveredRequest* request,
                                               SimpleReply* response) {
    const auto authorized = AuthorizeInternalRpc(context, "comet");
    if (!authorized.ok()) return authorized;
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
