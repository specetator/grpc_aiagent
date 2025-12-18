// ============================================================================
// Comet WebSocket 服务端（头文件）
//
// 核心职责：
// - 管理客户端 WebSocket 长连接（基于 muduo TcpServer）
// - 处理 HTTP 握手升级为 WebSocket（RFC 6455）
// - 握手阶段通过 gRPC 调用 logic 服务校验 token，完成鉴权
// - 维护 user_id -> 连接集合 的映射，支持按用户精确推送/广播
//
// 说明：
// - 本头文件定义接口与成员；实现见 `comet_server.cpp`
// - 线程模型：主线程 accept，新连接/读写由 IO 线程池处理（可配置）
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

// TCP 连接上下文：记录连接状态与该连接绑定的用户信息
struct ConnContext {
    // 连接阶段
    enum State {
        kHandshake,  // 握手阶段：等待 WebSocket 握手完成
        kOpen        // 已建立：WebSocket 已升级完成，可收发消息
    };

    // 当前连接状态（默认握手阶段）
    State state{kHandshake};
    // 绑定的用户 ID（握手鉴权成功后设置；未鉴权时为 0）
    int64_t user_id{0};
    // 连接唯一标识（用于路由登记/审计/排障）
    std::string conn_id;
    // 最近一次活跃时间戳（毫秒），用于空闲连接扫描与超时踢下线
    int64_t last_active_ms{0};
};

// CometServer：WebSocket 服务端核心类
// - 管理所有客户端连接
// - 处理握手鉴权与协议帧收发
// - 维护 user_id -> 连接集合 的映射，用于下行推送
class CometServer {
   public:
    // 构造：初始化 muduo TcpServer、Redis 连接池、gRPC logic stub 等
    // @param loop: muduo 事件循环（驱动网络 IO，不持有其生命周期）
    // @param cfg: 配置（监听端口、logic 地址、redis 等）
    CometServer(EventLoop *loop, const Config &cfg);

    // 设置 IO 线程数（需在 Start() 前调用）
    // - muduo：主线程 accept，新连接的读写由 IO 线程池负责
    // @param thread_num: IO 线程数（通常可设为 CPU 核心数或略小）
    void SetThreadNum(int thread_num);

    // 启动服务：开始监听端口并接受连接
    void Start();

    // 下行推送：按 user_id 精确推送（仅推送到当前进程内维护的连接集合）
    // @param msg: 业务消息（通常由 job/logic 通过 gRPC 下发）
    // @param user_ids: 目标用户 ID 列表（允许重复，内部会去重并过滤无效连接）
    void PushToUsers(const ChatMessage &msg,
                     const std::vector<int64_t> &user_ids);

    // 下行广播：向本进程内所有在线连接广播（不跨节点）
    // @param msg: 业务消息
    void PushBroadcast(const ChatMessage &msg);

   private:
    // ========== muduo 回调 ==========

    // 连接状态变化回调：
    // - 建立：初始化连接上下文（state=握手阶段）
    // - 断开：清理 user_id -> connection 映射，并触发离线通知/路由删除等
    void OnConnection(const TcpConnectionPtr &conn);

    // 消息到达回调：收到客户端数据时触发
    // - 握手阶段：走 HandleHandshake()
    // - 已建立阶段：走 HandleWebSocketFrame()
    void OnMessage(const TcpConnectionPtr &conn, Buffer *buf, muduo::Timestamp);

    // ========== WebSocket 协议处理 ==========

    // 处理 WebSocket 握手：
    // - 解析 HTTP 请求，提取 token
    // - 调用 logic.VerifyToken 验证 token 并获取 user_id
    // - 计算并返回握手响应（101 Switching Protocols）
    // - 状态切换为 kOpen，建立 user_id -> connection 映射，并写入在线路由
    void HandleHandshake(const TcpConnectionPtr &conn, Buffer *buf);

    // 处理 WebSocket 数据帧：
    // - 解析帧头（FIN/Opcode/Mask/PayloadLen）
    // - 按 opcode 分发（例如 text/close/ping/pong）
    // - 维护活跃时间并刷新在线路由 TTL
    void HandleWebSocketFrame(const TcpConnectionPtr &conn, Buffer *buf,
                              ConnContext &ctx);

    // 文本帧业务处理（客户端上行入口）：
    // - 做基础校验与必要的协议/元信息解析
    // - 通过 gRPC 转交给 logic 统一处理（落库、路由等）
    void OnTextMessage(const TcpConnectionPtr &conn, ConnContext &ctx,
                       const std::string &payload);

    // 从 HTTP 握手请求中提取 token 参数
    // 期望格式：GET /ws?token=xxx HTTP/1.1
    // @return token 字符串；解析失败返回空串
    std::string ParseTokenFromHandshake(const std::string &req);

    // 用户下线通知：当用户在本 comet 上“最后一个连接”断开时触发
    // 注意：通常通过异步方式（例如 detached thread）发起 RPC，避免阻塞 IO 线程
    void NotifyUserOffline(int64_t user_id);

    // 在 Redis 注册在线路由（user_id -> comet_id:conn_id），供跨模块/跨节点路由使用
    void RegisterUserOnline(int64_t user_id, const std::string &conn_id);

    // 刷新在线路由 TTL（例如收到心跳/业务消息/任何有效帧后调用）
    void RefreshUserTtl(int64_t user_id, const std::string &conn_id);

    // 从 Redis 移除指定连接的路由字段（连接关闭时调用）
    void RemoveUserRoute(int64_t user_id, const std::string &conn_id);

    // 定时扫描空闲连接并关闭（避免僵尸连接长期占用资源）
    void CheckIdleConnections();

    // muduo TcpServer：底层 TCP 服务端（监听、accept、连接回调等）
    TcpServer server_;
    // muduo EventLoop：事件循环指针（不持有所有权）
    EventLoop *loop_{nullptr};

    // 用户连接映射：user_id -> 该用户在本进程内的连接集合
    // - 一个用户可能多端登录，因此使用 set 存储多个连接
    // - 用于下行推送时按 user_id 找到对应连接集合
    // - 多 IO 线程模式下需要互斥保护
    std::unordered_map<int64_t, std::set<TcpConnectionPtr>> user_conns_;

    // 保护 user_conns_ 的互斥锁（多线程 IO 模式下使用）
    mutable std::mutex conns_mu_;

    // gRPC 客户端 stub：用于调用 logic 服务（例如 VerifyToken 等）
    // 生命周期：随 CometServer 存活
    std::unique_ptr<sparkpush::LogicService::Stub> logic_stub_;

    // 当前 comet 节点唯一标识（用于 Redis 在线路由登记）
    std::string comet_id_;

    // Redis 连接池：用于在线路由注册/续期/删除
    RedisConnectionPool redis_pool_;
    // Redis 是否可用/初始化成功（用于快速降级与保护逻辑）
    bool redis_ready_{false};

    // 空闲超时阈值（毫秒）：超过该时长未活动的连接会被关闭
    int64_t idle_timeout_ms_{60000};

    // Redis 路由信息 TTL（毫秒）：异常断线/未走清理路径时依赖 TTL 自动过期
    int64_t redis_ttl_ms_{60000};

    // 空闲扫描分片参数（近似均摊策略）：
    // - 每次 tick 只扫描一个分片，降低全量扫描抖动与锁竞争
    // - 分片归属：通常按某种 hash（例如 user_id % idle_scan_shards_）划分
    // - shard_idx 每 tick 递增循环
    int idle_scan_shards_{10};
    int idle_scan_shard_idx_{0};
};

}  // namespace sparkpush


