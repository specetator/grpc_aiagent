#include "grpc_service.h"

#include "logging.h"

namespace sparkpush {

LogicServiceImpl::LogicServiceImpl(RedisStore* redis_store)
    : redis_store_(redis_store) {}

void LogicServiceImpl::SetError(ErrorInfo* e, int code,
                                const std::string& msg) {
    if (!e) return;
    e->set_code(code);
    e->set_message(msg);
}

::grpc::Status LogicServiceImpl::VerifyToken(
    ::grpc::ServerContext*, const ::sparkpush::VerifyTokenRequest* request,
    ::sparkpush::VerifyTokenReply* response) {
    LOG_INFO << "VerifyToken called with token: " << request->token()
             << ", comet_id: " << request->comet_id();
    if (!redis_store_) {
        SetError(response->mutable_error(), 500, "redis store not initialized");
        return ::grpc::Status::OK;
    }
    int64_t uid = 0;
    if (!redis_store_->GetUserIdByToken(request->token(), &uid)) {
        SetError(response->mutable_error(), 401, "invalid or expired token");
        return ::grpc::Status::OK;
    }
    response->set_user_id(uid);
    SetError(response->mutable_error(), 0, "ok");
    redis_store_->AddRoute(uid, request->comet_id());
    return ::grpc::Status::OK;
}

}  // namespace sparkpush
