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

#include "config.h"
#include "logging.h"
#include "spark_push.grpc.pb.h"

#include <muduo/net/Buffer.h>
#include <muduo/net/EventLoop.h>
#include <muduo/net/TcpServer.h>

#include <set>
#include <string>
#include <unordered_map>
#include <mutex>

namespace sparkpush {

using muduo::net::Buffer;
using muduo::net::EventLoop;
using muduo::net::TcpConnectionPtr;
using muduo::net::TcpServer;

// TCP 连接上下文：记录连接状态和关联的用户信息
struct ConnContext {
    // 连接状态枚举
    enum State {
        kHandshake,  // 握手阶段：等待 WebSocket 握手完成
        kOpen        // 已建立：WebSocket 连接已升级，可收发消息
    };
    
    State state{kHandshake};  // 当前状态，初始为握手阶段
    int64_t user_id{0};       // 关联的用户 ID，握手成功后设置
};

// CometServer 类：WebSocket 服务器核心实现
// 管理所有客户端连接，处理握手鉴权和消息转发
class CometServer {
public:
    // 构造函数：初始化服务器并创建 gRPC 客户端
    // @param loop: muduo 事件循环，用于驱动网络 IO
    // @param cfg: 配置对象，包含监听端口、logic 服务地址等
    CometServer(EventLoop* loop, const Config& cfg);

    // 设置 IO 线程数
    // muduo 使用主线程 accept 新连接，IO 线程池处理已建立连接的读写
    // 必须在 Start() 之前调用
    // @param thread_num: IO 线程数，通常设置为 CPU 核心数
    void SetThreadNum(int thread_num);

    // 启动服务器，开始监听端口和接受连接
    void Start();

private:
    // ========== muduo 网络库回调函数 ==========
    
    // 连接状态变化回调：新连接建立或连接断开时调用
    // 建立：初始化连接上下文，状态设为握手阶段
    // 断开：清理用户连接映射表
    void OnConnection(const TcpConnectionPtr& conn);
    
    // 消息到达回调：收到客户端数据时调用
    // 根据连接状态（握手/已建立）分发到不同的处理函数
    void OnMessage(const TcpConnectionPtr& conn,
                   Buffer* buf,
                   muduo::Timestamp);

    // ========== WebSocket 协议处理函数 ==========
    
    // 处理 WebSocket 握手
    // 1. 解析 HTTP GET 请求，提取 token 参数
    // 2. 通过 gRPC 向 logic 验证 token
    // 3. 验证成功：计算 Sec-WebSocket-Accept，返回 101 响应
    // 4. 更新连接状态为 kOpen，记录用户映射
    void HandleHandshake(const TcpConnectionPtr& conn, Buffer* buf);
    
    // 处理 WebSocket 数据帧
    // 解析帧头（FIN、Opcode、Mask、Payload Length）
    // 当前简化实现：仅处理 close 帧，其他帧忽略
    void HandleWebSocketFrame(const TcpConnectionPtr& conn,
                              Buffer* buf,
                              ConnContext& ctx);

    // 从 HTTP 握手请求中提取 token 参数
    // 期望格式：GET /ws?token=xxx HTTP/1.1
    // @return: token 字符串，失败返回空字符串
    std::string ParseTokenFromHandshake(const std::string& req);

    // ========== 成员变量 ==========
    
    // muduo TcpServer：底层 TCP 服务器
    TcpServer server_;
    
    // 用户连接映射表：user_id -> 该用户的所有连接集合
    // 一个用户可能同时在多个设备登录，因此使用 set 存储多个连接
    // 用途：消息推送时根据 user_id 查找连接
    std::unordered_map<int64_t, std::set<TcpConnectionPtr>> user_conns_;
    
    // 保护 user_conns_ 的互斥锁（多线程 IO 模式下需要加锁）
    mutable std::mutex conns_mu_;
    
    // gRPC 客户端：用于调用 logic 服务的 VerifyToken 接口
    std::unique_ptr<sparkpush::LogicService::Stub> logic_stub_;
    
    // 当前 comet 节点的唯一标识（用于路由记录）
    std::string comet_id_;
};

}  // namespace sparkpush
