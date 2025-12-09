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

#include "redis_store.h"
#include "spark_push.grpc.pb.h"

namespace sparkpush {

// LogicService gRPC 服务实现类
// 继承自 proto 定义生成的 Service 基类，实现具体的业务逻辑
class LogicServiceImpl final : public sparkpush::LogicService::Service {
   public:
    // 构造函数：注入 Redis 存储依赖
    explicit LogicServiceImpl(RedisStore* redis_store);

    // 验证 token 并返回用户 ID
    // comet 在 WebSocket 握手时调用此接口完成鉴权
    // 成功时会在 Redis 中记录 user_id -> comet_id 的路由映射
    ::grpc::Status VerifyToken(::grpc::ServerContext* context,
                               const ::sparkpush::VerifyTokenRequest* request,
                               ::sparkpush::VerifyTokenReply* response) override;

   private:
    // 辅助函数：设置错误信息到 protobuf 响应消息
    void SetError(ErrorInfo* e, int code, const std::string& msg);

    // Redis 访问接口，用于查询 token 和路由信息
    RedisStore* redis_store_;
};

}  // namespace sparkpush
