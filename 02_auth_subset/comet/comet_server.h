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

// 每条 TCP 连接的上下文：握手阶段 / 已升级为 WebSocket，以及绑定的 user_id
struct ConnContext {
    enum State { kHandshake, kOpen } state{kHandshake};
    int64_t user_id{0};
};

// CometServer：负责管理 WebSocket 连接，与 logic 通信完成鉴权
class CometServer {
public:
    CometServer(EventLoop* loop, const Config& cfg);

    // 设置 muduo TcpServer 的 IO 线程数，需在 Start() 之前调用
    void SetThreadNum(int thread_num);

    void Start();

private:
    // muduo 回调：新连接建立或关闭
    void OnConnection(const TcpConnectionPtr& conn);
    
    // muduo 回调：收到数据时调用，根据状态机区分握手/帧处理
    void OnMessage(const TcpConnectionPtr& conn,
                   Buffer* buf,
                   muduo::Timestamp);

    // 处理 HTTP -> WebSocket 升级握手；验证 token，回写 Sec-WebSocket-Accept
    void HandleHandshake(const TcpConnectionPtr& conn, Buffer* buf);
    
    // 处理已升级连接的 WebSocket 帧，解析掩码与负载
    void HandleWebSocketFrame(const TcpConnectionPtr& conn,
                              Buffer* buf,
                              ConnContext& ctx);

    // 解析 HTTP GET 请求行中的 token 参数
    std::string ParseTokenFromHandshake(const std::string& req);

    // 事件循环驱动的 TCP 服务器
    TcpServer server_;
    
    // user_id -> 该用户在本 comet 上的所有 WebSocket 连接
    std::unordered_map<int64_t, std::set<TcpConnectionPtr>> user_conns_;
    
    // 保护 user_conns_ 的互斥锁
    mutable std::mutex conns_mu_;
    
    // 访问 LogicService 的 gRPC stub
    std::unique_ptr<sparkpush::LogicService::Stub> logic_stub_;
    
    // 当前 comet 实例标识，用于上报与鉴权
    std::string comet_id_;
};

}  // namespace sparkpush
