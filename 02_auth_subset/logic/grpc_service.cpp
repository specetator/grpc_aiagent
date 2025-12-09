// ============================================================================
// logic 服务 gRPC 接口实现
// ============================================================================
#include "grpc_service.h"

#include "logging.h"

namespace sparkpush {

// 构造函数：保存 Redis 存储的引用
LogicServiceImpl::LogicServiceImpl(RedisStore* redis_store)
    : redis_store_(redis_store) {}

// 设置错误码和错误消息到 protobuf 的 ErrorInfo 字段
// 统一的错误处理逻辑，避免重复代码
void LogicServiceImpl::SetError(ErrorInfo* e, int code,
                                const std::string& msg) {
    if (!e) return;
    e->set_code(code);
    e->set_message(msg);
}

// VerifyToken RPC 实现：验证客户端 token 并记录路由
// 
// 调用流程：
// 1. comet 收到 WebSocket 握手请求，从 URL 参数提取 token
// 2. comet 通过此 RPC 向 logic 验证 token 的有效性
// 3. logic 查询 Redis 中的 token -> user_id 映射
// 4. 验证成功后返回 user_id，并在 Redis 记录 user_id -> comet_id 路由
// 5. comet 根据返回结果决定是否完成 WebSocket 握手
::grpc::Status LogicServiceImpl::VerifyToken(
    ::grpc::ServerContext*, const ::sparkpush::VerifyTokenRequest* request,
    ::sparkpush::VerifyTokenReply* response) {
    LOG_INFO << "VerifyToken called with token: " << request->token()
             << ", comet_id: " << request->comet_id();
    
    // 检查 Redis 存储是否已初始化
    if (!redis_store_) {
        SetError(response->mutable_error(), 500, "redis store not initialized");
        return ::grpc::Status::OK;
    }
    
    // 从 Redis 查询 token 对应的用户 ID
    int64_t uid = 0;
    if (!redis_store_->GetUserIdByToken(request->token(), &uid)) {
        // token 无效或已过期
        SetError(response->mutable_error(), 401, "invalid or expired token");
        return ::grpc::Status::OK;
    }
    
    // 验证成功：设置返回值
    response->set_user_id(uid);
    SetError(response->mutable_error(), 0, "ok");
    
    // 在 Redis 中记录用户路由：user_id -> comet_id
    // 用于后续消息推送时查找用户所在的 comet 节点
    redis_store_->AddRoute(uid, request->comet_id());
    
    return ::grpc::Status::OK;
}

}  // namespace sparkpush
