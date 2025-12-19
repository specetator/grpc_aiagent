// ============================================================================
// Comet gRPC 服务实现
//
// 本文件实现 CometService 的 gRPC 接口，主要功能：
// - 接收来自 job/logic 服务的下行推送请求
// - 将消息分发到本机（本 comet 进程）管理的 WebSocket 连接
// - 支持精确推送（按 user_id 列表）和广播推送（全本机用户）
//
// 设计说明：
// - 本服务只负责本机连接的推送，不跨 comet 节点转发
// - 调用方需根据用户的在线路由信息（Redis），选择正确的 comet_id 发起调用
// ============================================================================
#include "comet_grpc_service.h"

#include <unordered_set>

#include "logging.h"

namespace sparkpush {

// ============================================================================
// 构造函数：CometServiceImpl
//
// 参数：
// @param server: CometServer
// 指针，用于调用推送方法（PushToUsers/PushBroadcast）
//
// 说明：
// - 不持有 server 的所有权，生命周期由外部（app.cpp）管理
// - server 指针在整个服务运行期间有效，无需担心野指针问题
// ============================================================================
CometServiceImpl::CometServiceImpl(CometServer *server) : server_(server) {}

// ============================================================================
// gRPC 方法：PushToComet
// 功能：接收下行推送请求，将消息分发到本机 WebSocket 连接
//
// 参数：
// @param context: gRPC 服务端上下文（本方法未使用，但接口要求必须有）
// @param request: 推送请求，包含消息内容和目标用户列表
// @param response: 推送响应，返回执行结果（成功/失败）
//
// 返回：
// @return: gRPC 状态码（通常返回 OK，错误信息在 response 中）
//
// 调用流程：
// 1. job/logic 查询 Redis，获取用户的在线路由（comet_id + conn_id）
// 2. job/logic 按 comet_id 分组，向对应的 comet 节点发起 gRPC 调用
// 3. comet 节点收到请求后，查找本机的 user_conns_ 映射表
// 4. 通过 muduo TcpConnection 将 WebSocket 帧发送给客户端
//
// 推送策略：
// - 如果 targets 非空：精确推送给列表中的用户（单聊/多人推送）
// - 如果 targets 为空且 session_id="broadcast"：推送给本机所有在线用户
// - 其他情况：记录警告，不执行推送
// ============================================================================
::grpc::Status CometServiceImpl::PushToComet(
    ::grpc::ServerContext *, const ::sparkpush::PushToCometRequest *request,
    ::sparkpush::PushToCometReply *response) {
    // ========================================================================
    // 步骤 1: 提取请求信息，记录日志
    // ========================================================================
    // comet_id: 目标 comet 节点 ID（通常应该是本机的 comet_id）
    // 这里从 comet_ids 列表中取第一个（PushToCometRequest
    // 支持批量，但通常只有一个）
    std::string comet_id =
        request->comet_ids_size() > 0 ? request->comet_ids(0) : "";

    // 记录日志：便于追踪消息流转和排查问题
    LOG_INFO << "PushToComet received for comet_id=" << comet_id
             << " msg_id=" << request->message().msg_id();

    // ========================================================================
    // 步骤 2: 根据 targets 字段判断推送策略
    // ========================================================================

    // 策略 1: 精确推送（targets 列表非空）
    // 适用场景：单聊、多人推送、群聊（已知成员列表）
    if (request->targets_size() > 0) {
        // 提取所有目标用户的 user_id
        std::vector<int64_t> users;  // 目标用户 ID 列表（去重后）
        users.reserve(request->targets_size());  // 预分配内存，避免多次扩容
        std::unordered_set<int64_t> seen;
        seen.reserve(static_cast<size_t>(request->targets_size()));

        for (const auto &t : request->targets()) {
            // 记录每个目标用户（便于调试）
            LOG_INFO << "PushToComet target user_id=" << t.user_id();
            if (seen.insert(t.user_id()).second) {
                users.push_back(t.user_id());
            }
        }

        // 调用 CometServer::PushToUsers 执行推送
        // 内部会查找 user_conns_ 映射表，向匹配的连接发送 WebSocket 帧
        server_->PushToUsers(request->message(), users);
    }
    // 策略 2: 广播推送（targets 为空，根据 session_id 判断）
    else {
        // session_id: 会话标识，约定用于区分推送类型
        // - "broadcast": 全服广播（本机所有在线用户）
        // - "r_<room_id>": 房间广播（当前版本未实现）
        const std::string &sid = request->message().session_id();

        if (sid == "broadcast") {
            // 全服广播：遍历本机所有连接，逐个推送
            // 注意：只推送给本机连接，不跨 comet 节点
            server_->PushBroadcast(request->message());
        } else {
            // 不支持的 session_id：记录警告，不执行推送
            // 例如：房间广播需要额外实现，当前版本未支持
            LOG_WARN << "PushToComet targets empty but unsupported session_id="
                     << sid;
        }
    }

    // ========================================================================
    // 步骤 3: 构造响应，返回成功
    // ========================================================================
    // 设置错误码为 0（表示成功）
    response->mutable_error()->set_code(0);
    response->mutable_error()->set_message("ok");

    // 返回 gRPC 状态码 OK
    // 注意：即使推送失败（如用户不在线），也返回 OK，具体错误在 response.error
    // 中
    return ::grpc::Status::OK;
}

}  // namespace sparkpush
