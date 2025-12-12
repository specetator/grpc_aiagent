// ============================================================================
// comet WebSocket 服务器
//
// 核心职责：
// 1. 管理客户端的 WebSocket 长连接
// 2. 处理 HTTP -> WebSocket 握手升级
// 3. 在握手时向 logic 验证 token，完成鉴权
// 4. 维护 user_id 到连接的映射（用于消息推送）
//
// WebSocket 实现说明：
// - 基于 RFC 6455 标准实现 WebSocket 协议
// - 使用 muduo TcpServer 处理底层 TCP 连接
// - 手工解析 HTTP 握手请求和 WebSocket 帧格式
// - 当前版本简化实现，仅处理握手和关闭帧
//
// 鉴权流程：
// 1. 客户端发起 WebSocket 握手，URL 参数带 token
// 2. comet 提取 token，通过 gRPC 调用 logic.VerifyToken
// 3. logic 验证 token 有效性，返回 user_id
// 4. comet 完成握手，记录 user_id -> connection 映射
// ============================================================================
#pragma once

#include <muduo/net/Buffer.h>
#include <muduo/net/EventLoop.h>
#include <muduo/net/TcpServer.h>

#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "config.h"
#include "logging.h"
#include "redis_pool.h"
#include "spark_push.grpc.pb.h"
#include "websocket_utils.h"

namespace sparkpush {

using muduo::net::Buffer;
using muduo::net::EventLoop;
using muduo::net::TcpConnectionPtr;
using muduo::net::TcpServer;

// TCP 连接上下文：记录连接状态和关联的用户信息
struct ConnContext {
    enum State {
        kHandshake,  // 握手阶段：等待 WebSocket 握手完成
        kOpen        // 已建立：WebSocket 连接已升级，可收发消息
    };
    State state{kHandshake};  // 当前状态，初始为握手阶段
    int64_t user_id{0};       // 关联的用户 ID，握手成功后设置
    std::string conn_id;      // 本次连接的唯一 ID，用于鉴权/审计
    int64_t last_active_ms{0};  // 最近活动时间
};

// CometServer 类：WebSocket 服务器核心实现
// 管理所有客户端连接，处理握手鉴权和消息转发
class CometServer {
   public:
    // 构造函数：初始化服务器并创建 gRPC 客户端
    // @param loop: muduo 事件循环，用于驱动网络 IO
    // @param cfg: 配置对象，包含监听端口、logic 服务地址等
    CometServer(EventLoop *loop, const Config &cfg);

    // 设置 IO 线程数
    // muduo 使用主线程 accept 新连接，IO 线程池处理已建立连接的读写
    // 必须在 Start() 之前调用
    // @param thread_num: IO 线程数，通常设置为 CPU 核心数
    void SetThreadNum(int thread_num);
    // 启动服务器，开始监听端口和接受连接
    void Start();

    // 下行推送：按 user_id 精确推送（仅推送到本机 comet 管理的连接集合）。
    // @param msg: 业务消息（通常由 job/logic 通过 gRPC 下发）
    // @param user_ids: 目标用户 ID 列表（允许重复，内部会做 map
    // 查找与连接有效性过滤）
    void PushToUsers(const ChatMessage &msg,
                     const std::vector<int64_t> &user_ids);

    // 下行推送：本机广播（仅限本进程内连接，不跨节点）。
    // @param msg: 业务消息
    void PushBroadcast(const ChatMessage &msg);

   private:
    // ========== muduo 网络库回调函数 ==========

    // 连接状态变化回调：新连接建立或连接断开时调用
    // 建立：初始化连接上下文，状态设为握手阶段
    // 断开：清理用户连接映射表
    void OnConnection(const TcpConnectionPtr &conn);

    // 消息到达回调：收到客户端数据时调用
    // 根据连接状态（握手/已建立）分发到不同的处理函数
    void OnMessage(const TcpConnectionPtr &conn, Buffer *buf, muduo::Timestamp);

    // ========== WebSocket 协议处理函数 ==========

    // 处理 WebSocket 握手
    // 1. 解析 HTTP GET 请求，提取 token 参数
    // 2. 通过 gRPC 向 logic 验证 token
    // 3. 验证成功：计算 Sec-WebSocket-Accept，返回 101 响应
    // 4. 更新连接状态为 kOpen，记录用户映射
    void HandleHandshake(const TcpConnectionPtr &conn, Buffer *buf);

    // 处理 WebSocket 数据帧
    // 解析帧头（FIN、Opcode、Mask、Payload Length）
    // 当前简化实现：仅处理 close 帧，其他帧忽略
    void HandleWebSocketFrame(const TcpConnectionPtr &conn, Buffer *buf,
                              ConnContext &ctx);

    // 文本帧业务处理（客户端上行消息入口）：
    // - 做基础校验/解析 UpstreamMessageMeta
    // - 通过 gRPC 转交给 logic 统一处理（落库、路由、幂等等）
    void OnTextMessage(const TcpConnectionPtr &conn, ConnContext &ctx,
                       const std::string &payload);

    // 从 HTTP 握手请求中提取 token 参数
    // 期望格式：GET /ws?token=xxx HTTP/1.1
    // @return: token 字符串，失败返回空字符串
    std::string ParseTokenFromHandshake(const std::string &req);

    // 用户下线通知：当某用户在本 comet 上最后一个连接断开时调用。
    // 注意：该实现通过 detached thread 发起 RPC，避免阻塞 IO 线程。
    void NotifyUserOffline(int64_t user_id);

    // 在 Redis 注册在线路由（user_id -> comet_id:conn_id），供下行路由使用。
    void RegisterUserOnline(int64_t user_id, const std::string &conn_id);

    // 续期在线路由的 TTL（收到心跳/业务消息时调用）。
    void RefreshUserTtl(int64_t user_id, const std::string &conn_id);

    // 从 Redis 移除指定连接的路由字段（连接关闭时调用）。
    void RemoveUserRoute(int64_t user_id, const std::string &conn_id);

    // 定时扫描空闲连接并关闭（避免僵尸连接长期占用资源）。
    void CheckIdleConnections();

    // muduo TcpServer：底层 TCP 服务器
    TcpServer server_;
    EventLoop *loop_{nullptr};

    // 用户连接映射表：user_id -> 该用户的所有连接集合
    // 一个用户可能同时在多个设备登录，因此使用 set 存储多个连接
    // 用途：消息推送时根据 user_id 查找连接
    // 线程安全：多 IO 线程模式下，访问需受 conns_mu_ 保护。
    std::unordered_map<int64_t, std::set<TcpConnectionPtr>> user_conns_;

    // 保护 user_conns_ 的互斥锁（多线程 IO 模式下需要加锁）
    mutable std::mutex conns_mu_;

    // gRPC 客户端：用于调用 logic 服务的 VerifyToken 接口
    // 生命周期：随 CometServer 存活；线程安全：Stub
    // 通常可并发使用，但此处多数调用发生在 IO 线程。
    std::unique_ptr<sparkpush::LogicService::Stub> logic_stub_;

    // 当前 comet 节点的唯一标识（用于路由记录）
    std::string comet_id_;

    // Redis 连接池：用于在线路由注册/续期/删除
    RedisConnectionPool redis_pool_;
    bool redis_ready_{false};

    // 空闲超时阈值（毫秒）：超过该时长未活跃的连接会被关闭
    int64_t idle_timeout_ms_{60000};  // 心跳超时时间

    // Redis 路由信息的 TTL（毫秒）：异常断线时可依靠 TTL 自动清理
    int64_t redis_ttl_ms_{60000};  // 注册信息 TTL

    // 分片惰性扫描参数：
    // - 每次定时 tick 只扫描一个分片，复杂度约为 O(N / idle_scan_shards_)；
    // - 分片按 user_id % idle_scan_shards_ 归属；
    // - idle_scan_shard_idx_ 每 tick 循环推进。
    // 说明：这是“近似”均摊扫描策略，能显著减少全量扫描带来的抖动与锁竞争。
    int idle_scan_shards_{10};
    int idle_scan_shard_idx_{0};
};

}  // namespace sparkpush
