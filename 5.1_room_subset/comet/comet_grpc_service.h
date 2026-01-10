// ============================================================================
// Comet gRPC 服务接口定义
//
// 本文件定义 CometService 的 gRPC 服务端实现类，主要职责：
// 1. 接收来自 job/logic 服务的下行推送请求（PushToComet）
// 2. 将消息分发到本机（本 comet 进程）管理的 WebSocket 连接
// 3. 支持多种推送模式：精确推送、广播推送
//
// 架构说明：
// - 本服务只负责本机连接的推送，不跨 comet 节点转发
// - 跨节点路由由 job/logic 服务负责（通过 Redis 查询在线路由）
// - CometServiceImpl 是纯粹的 RPC 适配层，实际推送由 CometServer 完成
//
// 使用场景：
// - 单聊消息：job 查询收件人在线的 comet_id，调用对应节点的 PushToComet
// - 群聊消息：job 查询所有在线成员的 comet_id，按节点分组批量调用
// - 系统广播：job 遍历所有 comet 节点，逐个调用 PushToComet
// ============================================================================
#pragma once

#include <grpcpp/grpcpp.h>

#include "comet_server.h"
#include "spark_push.grpc.pb.h"

namespace sparkpush {

// ============================================================================
// 类：CometServiceImpl
// 功能：CometService gRPC 服务的实现类
//
// 继承关系：
// - 继承自 sparkpush::CometService::Service（protobuf 自动生成的基类）
// - 使用 final 修饰，禁止进一步继承
//
// 设计模式：
// - 适配器模式：将 gRPC 调用适配到 CometServer 的内部方法
// - 依赖注入：通过构造函数注入 CometServer 指针，便于测试和解耦
//
// 线程安全：
// - gRPC 服务是多线程的，可能并发调用 PushToComet
// - CometServer 内部使用互斥锁保护共享状态（user_conns_），线程安全
// ============================================================================
class CometServiceImpl final : public sparkpush::CometService::Service {
   public:
    // ========================================================================
    // 构造函数
    //
    // 参数：
    // @param server: CometServer 指针，用于调用推送方法
    //
    // 说明：
    // - 使用 explicit 防止隐式转换
    // - 不持有 server 的所有权（生命周期由外部管理）
    // - server 指针在整个服务运行期间有效
    // ========================================================================
    explicit CometServiceImpl(CometServer *server);

    // ========================================================================
    // gRPC 方法：PushToComet
    // 功能：向本机 comet 管理的 WebSocket 连接推送消息
    //
    // 参数：
    // @param context: gRPC 服务端上下文（包含元数据、截止时间等）
    // @param request: 推送请求，包含：
    //   - message: 消息内容（ChatMessage）
    //   - targets: 目标用户列表（PushTarget，包含 user_id）
    //   - comet_ids: 目标 comet 节点 ID 列表（通常只有一个）
    // @param response: 推送响应，包含：
    //   - error: 错误信息（code=0 表示成功）
    //
    // 返回：
    // @return: gRPC 状态码（通常返回 OK，具体错误在 response.error 中）
    //
    // 推送策略：
    // 1. 如果 targets 非空：按 user_id 精确推送（单聊/多人推送）
    // 2. 如果 targets 为空且 session_id="broadcast"：推送给本机所有在线用户
    // 3. 其他情况：记录警告，不执行推送
    //
    // 实现细节：
    // - 本方法只是 RPC 适配层，实际推送逻辑在 CometServer 中
    // - 不做跨 comet 节点转发，调用方需根据 comet_id 选择目标实例
    // - 即使用户不在线，也返回 OK（推送失败不视为 RPC 失败）
    // ========================================================================
    ::grpc::Status PushToComet(
        ::grpc::ServerContext *context,
        const ::sparkpush::PushToCometRequest *request,
        ::sparkpush::PushToCometReply *response) override;

   private:
    // ========================================================================
    // 成员变量：server_
    // 类型：CometServer 指针（不持有所有权）
    //
    // 用途：
    // - 调用 CometServer::PushToUsers 执行精确推送
    // - 调用 CometServer::PushBroadcast 执行广播推送
    //
    // 生命周期：
    // - 由外部（app.cpp）管理，在整个服务运行期间有效
    // - CometServiceImpl 不负责创建或销毁 server_
    //
    // 线程安全：
    // - 多个 gRPC 线程可能并发调用 server_ 的方法
    // - CometServer 内部使用互斥锁保护共享状态，线程安全
    // ========================================================================
    CometServer *server_;
};

}  // namespace sparkpush
