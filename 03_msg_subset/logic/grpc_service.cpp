// ============================================================================
// logic 服务 gRPC 接口实现
//
// 实现 comet 服务调用的 RPC 接口：
// 1. VerifyToken: token 验证和路由注册
// 2. SendUpstreamMessage: 上行消息处理和推送
// 3. UserOffline: 用户下线和路由清理
// ============================================================================
#include "grpc_service.h"

#include <algorithm>
#include <chrono>
#include <nlohmann/json.hpp>

#include "logging.h"
#include "message_helper.h"

namespace sparkpush {

// 构造函数：初始化 gRPC 服务并保存依赖对象
//
// 实现说明：
// 保存依赖对象的指针，用于后续 RPC 方法调用
//
// 参数说明：
// @param user_dao: 用户数据访问对象（当前未使用，保留用于扩展）
// @param push_producer: Kafka 生产者，用于发送推送消息
// @param redis_store: Redis 存储对象，用于 token 验证和路由管理
// @param connection_ttl_ms: 连接 TTL（毫秒），用于设置路由信息过期时间
LogicServiceImpl::LogicServiceImpl(UserDao* user_dao,
                                   KafkaProducer* push_producer,
                                   RedisStore* redis_store,
                                   int connection_ttl_ms)
    : user_dao_(user_dao),
      push_producer_(push_producer),
      redis_store_(redis_store),
      connection_ttl_ms_(connection_ttl_ms) {}

// 设置错误信息的辅助函数
//
// 功能说明：
// 统一设置 protobuf ErrorInfo 的 code 和 message 字段
// 避免在每个 RPC 方法中重复编写相同的代码
//
// 使用示例：
// SetError(response->mutable_error(), 401, "invalid token");
//
// 参数说明：
// @param e: ErrorInfo 指针，由 response->mutable_error() 获取
// @param code: 错误码，0 表示成功，非 0 表示各种错误
// @param msg: 错误描述信息，用于调试和日志
void LogicServiceImpl::SetError(ErrorInfo* e, int code,
                                const std::string& msg) {
    LOG_INFO << "SetError called with code: " << std::to_string(code)
             << ", message: " << msg;
    e->set_code(code);
    e->set_message(msg);
}

// VerifyToken RPC 实现：验证客户端 token 并记录路由
//
// 调用流程：
// 1. comet 收到 WebSocket 握手请求，从 URL 参数提取 token
// 2. comet 通过此 RPC 向 logic 验证 token 的有效性
// 3. logic 查询 Redis 中的 token -> user_id 映射
// 4. 验证成功后返回 user_id，并在 Redis 记录 user_id -> comet_id:conn_id 路由
// 5. comet 根据返回结果决定是否完成 WebSocket 握手
//
// 业务逻辑：
// - 验证 token 有效性（查询 Redis）
// - 返回对应的 user_id
// - 记录用户连接路由信息（用于消息推送）
//
// 错误处理：
// - Redis 不可用：返回 500 错误
// - token 无效或过期：返回 401 错误
// - 验证成功：返回 0，并设置 user_id
//
// 参数说明：
// @param context: gRPC 上下文（当前未使用）
// @param request: 请求参数，包含 token、comet_id 和 conn_id
// @param response: 响应参数，返回 user_id 和错误信息
// @return: gRPC::Status::OK 表示 RPC 调用成功（不代表业务成功）
::grpc::Status LogicServiceImpl::VerifyToken(
    ::grpc::ServerContext*, const ::sparkpush::VerifyTokenRequest* request,
    ::sparkpush::VerifyTokenReply* response) {
    LOG_INFO << "VerifyToken called with token: " << request->token()
             << ", comet_id: " << request->comet_id();

    // 步骤1：检查 Redis 存储是否已初始化
    if (!redis_store_) {
        SetError(response->mutable_error(), 500, "redis store not initialized");
        return ::grpc::Status::OK;
    }

    // 步骤2：通过 token 查询 user_id
    // 从 Redis 中查询 token:<token> -> <user_id> 映射
    int64_t uid = 0;
    if (!redis_store_->GetUserIdByToken(request->token(), &uid)) {
        // token 不存在或已过期
        SetError(response->mutable_error(), 401, "invalid or expired token");
        return ::grpc::Status::OK;
    }

    // 步骤3：设置响应的 user_id
    response->set_user_id(uid);
    SetError(response->mutable_error(), 0, "ok");

    // 步骤4：记录用户连接路由信息
    // 在 Redis 中记录：user_connections:<user_id> -> {<conn_id>: <comet_id>}
    // 用于后续消息推送时查找用户所在的 comet 节点
    if (redis_store_) {
        redis_store_->UpsertUserConnection(
            uid, request->comet_id(), request->conn_id(), connection_ttl_ms_);
    }

    // 返回 gRPC::Status::OK 表示 RPC 调用成功
    // 业务是否成功由 response->error()->code() 判断
    return ::grpc::Status::OK;
}

// SendUpstreamMessage RPC 实现：处理上行消息并推送
//
// 调用流程：
// 1. 客户端通过 WebSocket 向 comet 发送消息
// 2. comet 将消息通过此 RPC 转发给 logic
// 3. logic 生成消息 ID 和序号
// 4. logic 规范化消息内容 JSON
// 5. logic 构造 ChatMessage 对象
// 6. logic 调用 PostProcessSingleMessage 推送给目标用户
//
// 业务逻辑：
// - 验证发送方和接收方的 user_id
// - 生成全局唯一的消息 ID 和序号
// - 规范化消息内容，补充服务端字段
// - 推送消息到目标用户的 comet 节点
//
// 参数说明：
// @param context: gRPC 上下文（当前未使用）
// @param request: 请求参数，包含发送方、接收方、消息类型和内容
// @param response: 响应参数，返回 ChatMessage 和错误信息
// @return: gRPC::Status::OK 表示 RPC 调用成功
::grpc::Status LogicServiceImpl::SendUpstreamMessage(
    ::grpc::ServerContext*, const ::sparkpush::UpstreamMessageRequest* request,
    ::sparkpush::UpstreamMessageReply* response) {
    // 步骤1：验证发送方 user_id
    // 发送方 ID 必须为正数
    int64_t from_user = request->from_user_id();
    if (from_user <= 0) {
        SetError(response->mutable_error(), 400,
                 "from_user_id must be positive");
        return ::grpc::Status::OK;
    }

    // 步骤2：验证接收方和目标类型
    // 当前只支持单聊（single_chat），不支持群聊
    int64_t to_user = request->target_id();
    const std::string& target_type = request->target_type();
    if (target_type != "single_chat" || to_user <= 0) {
        SetError(response->mutable_error(), 400, "only single_chat supported");
        return ::grpc::Status::OK;
    }

    // 步骤3：计算会话 ID
    // 会话 ID 由两个用户 ID 组成，按大小排序（小的在前）
    // 格式：s_<小uid>:<大uid>
    int64_t user1 = std::min(from_user, to_user);
    int64_t user2 = std::max(from_user, to_user);
    std::string session_id =
        "s_" + std::to_string(user1) + ":" + std::to_string(user2);

    // 步骤4：生成消息 ID 和序号
    // 调用 Redis INCR 生成全局唯一且递增的消息序号
    int64_t msg_seq = 0;
    std::string msg_id;
    if (redis_store_ && redis_store_->NextSingleMsgId(user1, user2, &msg_seq)) {
        // 消息 ID 格式：msgid:<小uid>:<大uid>-<序号>
        msg_id = "msgid:" + std::to_string(user1) + ":" +
                 std::to_string(user2) + "-" + std::to_string(msg_seq);
    } else {
        // Redis 不可用或 INCR 失败
        SetError(response->mutable_error(), 500, "alloc msg_seq failed");
        return ::grpc::Status::OK;
    }
    LOG_INFO << "Allocated msg_id " << msg_id << ", session_id " << session_id;

    // 步骤5：获取服务端时间戳（毫秒）
    // 用于记录消息的创建时间
    int64_t now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                         std::chrono::system_clock::now().time_since_epoch())
                         .count();

    // 步骤6：规范化消息内容 JSON
    // 补充服务端生成的字段，去重同义字段
    std::string final_json = request->content_json();
    std::string preview = "[unsupported]";
    std::string msg_type =
        request->msg_type().empty() ? "single_chat" : request->msg_type();
    try {
        // 解析客户端传来的 content_json
        auto j = nlohmann::json::parse(final_json);

        // 去重同义字段：将 timestamp 重命名为 client_timestamp_ms
        if (j.contains("timestamp")) {
            try {
                j["client_timestamp_ms"] = j["timestamp"];
            } catch (...) {
            }
            j.erase("timestamp");  // 删除旧字段
        }

        // 补充服务端生成的字段
        j["msg_id"] = msg_id;              // 服务端生成的消息 ID
        j["msg_seq"] = msg_seq;            // 服务端生成的消息序号
        j["create_time"] = now_ms;         // 服务端时间戳
        j["from_user_id"] = from_user;     // 发送方用户 ID
        j["target_type"] = "single_chat";  // 目标类型
        j["target_id"] = to_user;          // 接收方用户 ID
        j["msg_type"] = msg_type;          // 消息类型

        // 补充客户端消息 ID（如果 JSON 中没有）
        if (!j.contains("client_msg_id")) {
            j["client_msg_id"] = request->client_msg_id();
        }

        // 提取消息预览文本（用于推送通知等场景）
        if (j.contains("content") && j["content"].is_string()) {
            preview = j["content"].get<std::string>();
            if (preview.size() > 50) preview = preview.substr(0, 50);
        } else if (j.contains("text") && j["text"].is_string()) {
            preview = j["text"].get<std::string>();
            if (preview.size() > 50) preview = preview.substr(0, 50);
        } else {
            preview = msg_type;  // 如果没有文本内容，使用消息类型作为预览
        }

        // 将规范化后的 JSON 对象序列化为字符串
        final_json = j.dump();
    } catch (...) {
        // JSON 解析失败，保持原样
        // 不影响主流程，继续执行
    }

    // 步骤7：构造 ChatMessage protobuf 对象
    // 填充所有必要的字段
    ChatMessage* cm = response->mutable_message();
    cm->set_msg_id(msg_id);
    cm->set_session_id(session_id);
    cm->set_msg_seq(msg_seq);
    cm->set_sender_id(from_user);
    cm->set_timestamp_ms(now_ms);
    cm->set_msg_type(msg_type);
    cm->set_content_json(final_json);  // 存储规范化后的 JSON
    cm->set_client_msg_id(request->client_msg_id());

    // 设置成功响应
    SetError(response->mutable_error(), 0, "ok");

    // 步骤8：检查 Kafka 生产者是否可用
    if (!push_producer_) {
        LOG_ERROR << "Kafka producer not ready";
        // 虽然 Kafka 不可用，但消息已经生成成功，返回给 comet
        // comet 可以先返回给客户端，推送失败不影响消息发送
        return ::grpc::Status::OK;
    }

    // 步骤9：调用后置处理函数推送消息
    // 查询目标用户的 comet 节点，将消息推送过去
    PostProcessSingleMessage(to_user, session_id, *cm, redis_store_,
                             push_producer_);

    // 返回 gRPC::Status::OK 表示 RPC 调用成功
    return ::grpc::Status::OK;
}

// UserOffline RPC 实现：处理用户下线通知
//
// 调用流程：
// 1. 用户断开 WebSocket 连接
// 2. comet 检测到连接断开
// 3. comet 调用此 RPC 通知 logic 清理路由信息
// 4. logic 从 Redis 中删除用户连接记录
//
// 业务逻辑：
// - 从 Redis 中删除用户连接路由信息
// - 如果 conn_id 为空，跳过清理，依赖 TTL 自动过期
//
// 注意事项：
// - conn_id 必须提供，否则无法精确删除 Hash 中的 field
// - 如果删除失败，路由信息会在 TTL 到期后自动删除
//
// 参数说明：
// @param context: gRPC 上下文（当前未使用）
// @param request: 请求参数，包含 user_id、comet_id 和 conn_id
// @param response: 响应参数，返回错误信息
// @return: gRPC::Status::OK 表示 RPC 调用成功
::grpc::Status LogicServiceImpl::UserOffline(
    ::grpc::ServerContext*, const ::sparkpush::UserOfflineRequest* request,
    ::sparkpush::SimpleReply* response) {
    // 步骤1：检查 Redis 存储是否可用
    if (redis_store_) {
        // 步骤2：检查 conn_id 是否提供
        // conn_id 是 Hash 的 field，必须提供才能精确删除
        if (!request->conn_id().empty()) {
            // 步骤3：从 Redis 删除用户连接记录
            // 执行：HDEL user_connections:<user_id> <conn_id>
            redis_store_->RemoveUserConnection(
                request->user_id(), request->comet_id(), request->conn_id());
        } else {
            // conn_id 为空时无法精确删除 Hash 中的 field
            // 这种情况下跳过 Redis 清理，依赖 TTL 自动过期兜底
            //
            // 注意：不能删除整个 Hash（user_connections:<user_id>）
            // 因为用户可能在其他 comet 节点上还有连接
            LOG_WARN
                << "UserOffline without conn_id, skip redis cleanup. user_id="
                << request->user_id() << " comet_id=" << request->comet_id();
        }
    }

    // 步骤4：设置成功响应
    // 即使 Redis 操作失败，也返回成功
    // 因为路由信息有 TTL，会自动过期
    SetError(response->mutable_error(), 0, "ok");

    // 返回 gRPC::Status::OK 表示 RPC 调用成功
    return ::grpc::Status::OK;
}

}  // namespace sparkpush
