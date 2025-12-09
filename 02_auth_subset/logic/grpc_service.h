#pragma once

#include <grpcpp/grpcpp.h>

#include "redis_store.h"
#include "spark_push.grpc.pb.h"

namespace sparkpush {

// 最小鉴权实现：仅覆盖 VerifyToken
class LogicServiceImpl final : public sparkpush::LogicService::Service {
   public:
    explicit LogicServiceImpl(RedisStore* redis_store);

    ::grpc::Status VerifyToken(::grpc::ServerContext* context,
                               const ::sparkpush::VerifyTokenRequest* request,
                               ::sparkpush::VerifyTokenReply* response) override;

   private:
    void SetError(ErrorInfo* e, int code, const std::string& msg);

    RedisStore* redis_store_;
};

}  // namespace sparkpush
