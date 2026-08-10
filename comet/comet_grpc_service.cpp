#include "comet_grpc_service.h"

#include "logging.h"
#include "metrics.h"

namespace sparkpush {

CometServiceImpl::CometServiceImpl(CometServer* server) : server_(server) {}

size_t CometServiceImpl::ProcessPushRequest(
    const ::sparkpush::PushToCometRequest& request) {
    if (!server_ || !server_->AcceptPushRequest(request.request_id())) {
        return 0;
    }
    if (request.targets_size() > 0) {
        std::vector<int64_t> users;
        users.reserve(request.targets_size());
        for (const auto& target : request.targets()) {
            if (target.user_id() > 0) users.push_back(target.user_id());
        }
        const size_t delivered = server_->PushToUsers(request.message(), users);
        if (delivered > 0) {
            server_->ReportDeliveredToUsers(request.message(), users);
        }
        return delivered;
    }

    const std::string& session_id = request.message().session_id();
    if (session_id.size() > 2 && session_id[0] == 'r' && session_id[1] == '_') {
        try {
            const int64_t room_id = std::stoll(session_id.substr(2));
            if (room_id <= 0) return 0;
            const auto users = server_->GetRoomUserIds(room_id);
            const size_t delivered = server_->PushToRoom(request.message(), room_id);
            if (delivered > 0) {
                server_->ReportDeliveredToUsers(request.message(), users);
            }
            return delivered;
        } catch (...) {
            return 0;
        }
    }
    if (session_id == "broadcast") {
        return server_->PushToAll(request.message());
    }
    return 0;
}

::grpc::Status CometServiceImpl::PushToComet(
    ::grpc::ServerContext*, const ::sparkpush::PushToCometRequest* request,
    ::sparkpush::PushToCometReply* response) {
    const size_t delivered = ProcessPushRequest(*request);
    response->set_request_id(request->request_id());
    response->set_delivered_count(static_cast<int32_t>(delivered));
    response->mutable_error()->set_code(0);
    response->mutable_error()->set_message("ok");
    return ::grpc::Status::OK;
}

::grpc::Status CometServiceImpl::PushDeliveryAck(
    ::grpc::ServerContext*, const ::sparkpush::PushDeliveryAckRequest* request,
    ::sparkpush::SimpleReply* response) {
    if (server_ && request->user_id() > 0) {
        server_->PushDeliveryAck(request->user_id(), request->message());
    }
    response->mutable_error()->set_code(0);
    response->mutable_error()->set_message("ok");
    return ::grpc::Status::OK;
}

::grpc::Status CometServiceImpl::PushStream(
    ::grpc::ServerContext*,
    ::grpc::ServerReaderWriter<::sparkpush::PushToCometReply,
                               ::sparkpush::PushToCometRequest>* stream) {
    // 该 gauge 只在 Job->Comet 长连接真正进入服务端处理函数后置 1。
    // 启动脚本据此判断“可以收发消息”，避免仅凭 9105 端口监听就提前返回。
    MetricsRegistry::Instance().Set("spark_push_comet_push_stream_ready", 1);
    PushToCometRequest request;
    while (stream->Read(&request)) {
        const size_t delivered = ProcessPushRequest(request);
        PushToCometReply reply;
        reply.set_request_id(request.request_id());
        reply.set_delivered_count(static_cast<int32_t>(delivered));
        reply.mutable_error()->set_code(0);
        reply.mutable_error()->set_message("ok");
        if (!stream->Write(reply)) {
            break;
        }
    }
    MetricsRegistry::Instance().Set("spark_push_comet_push_stream_ready", 0);
    MetricsRegistry::Instance().Increment("spark_push_comet_push_stream_closed_total");
    return ::grpc::Status::OK;
}

}  // namespace sparkpush
