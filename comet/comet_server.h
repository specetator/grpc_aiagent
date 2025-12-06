#pragma once

#include "config.h"
#include "logging.h"
#include "spark_push.grpc.pb.h"
#include "websocket_utils.h"

#include <muduo/net/Buffer.h>
#include <muduo/net/EventLoop.h>
#include <muduo/net/TcpServer.h>

#include <boost/any.hpp>

#include <set>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>
#include <mutex>

namespace sparkpush {

using muduo::net::Buffer;
using muduo::net::EventLoop;
using muduo::net::TcpConnectionPtr;
using muduo::net::TcpServer;

// 每条 TCP 连接的上下文：握手阶段 / 已升级为 WebSocket，以及绑定的 user_id。
// 通过 Muduo 的 boost::any 存储在 TcpConnection 上，便于状态机切换。
struct ConnContext {
    enum State { kHandshake, kOpen } state{kHandshake};
    int64_t user_id{0};
};

// CometServer：负责管理 WebSocket 连接，与 logic 通信，
// 以及为 gRPC CometService 提供下行推送能力。
class CometServer {
public:
    CometServer(EventLoop* loop, const Config& cfg);

    // 设置 muduo TcpServer 的 IO 线程数（即 worker EventLoop 数量），需在 Start() 之前调用。
    void SetThreadNum(int thread_num);

    void Start();

    // 供 gRPC CometService 调用：将消息推送给若干用户（注意一个 user_id 可能有多条连接）。
    void PushToUsers(const ChatMessage& msg,
                     const std::vector<int64_t>& user_ids);
    // 通过本机房间成员表，将消息推送给某个 room 的所有本机在线用户。
    void PushToRoom(const ChatMessage& msg, int64_t room_id);
    // 广播给本机所有在线连接。
    void PushToAll(const ChatMessage& msg);

private:
    // muduo 回调：新连接建立或关闭。
    void OnConnection(const TcpConnectionPtr& conn);
    // muduo 回调：收到数据时调用，根据状态机区分握手/帧处理。
    void OnMessage(const TcpConnectionPtr& conn,
                   Buffer* buf,
                   muduo::Timestamp);

    // 处理 HTTP -> WebSocket 升级握手；验证 token，回写 Sec-WebSocket-Accept。
    void HandleHandshake(const TcpConnectionPtr& conn, Buffer* buf);
    // 处理已升级连接的 WebSocket 帧，解析掩码与负载。
    void HandleWebSocketFrame(const TcpConnectionPtr& conn,
                              Buffer* buf,
                              ConnContext& ctx);
    // 处理客户端文本消息，包括路由解析与转发到 logic。
    void OnTextMessage(const TcpConnectionPtr& conn,
                       ConnContext& ctx,
                       const std::string& payload);

    // 解析 HTTP GET 请求行中的 token 参数。
    std::string ParseTokenFromHandshake(const std::string& req);

    // 当某个 user 在本 comet 上的连接数变为 0 时，通知 logic 执行 UserOffline。
    void NotifyUserOffline(int64_t user_id);

    // 聊天室本机房间成员管理（room_id -> user_ids）。
    void AddUserToRoom(int64_t room_id, int64_t user_id);
    void RemoveUserFromRoom(int64_t room_id, int64_t user_id);
    // 通过逻辑服上报房间路由 + 在线人数（WebSocket 控制消息驱动）。
    void NotifyRoomJoin(int64_t room_id, int64_t user_id);
    void NotifyRoomLeave(int64_t room_id, int64_t user_id);

    // 事件循环驱动的 TCP 服务器。
    TcpServer server_;
    // user_id -> 该用户在本 comet 上的所有 WebSocket 连接。
    std::unordered_map<int64_t, std::set<TcpConnectionPtr>> user_conns_;
    // room_id -> 当前在本 comet 上该房间内的 user_id 列表。
    std::unordered_map<int64_t, std::set<int64_t>> room_users_;
    // 保护 user_conns_ / room_users_ 的互斥。
    mutable std::mutex conns_mu_;
    // 访问 LogicService 的 gRPC stub。
    std::unique_ptr<sparkpush::LogicService::Stub> logic_stub_;
    // 当前 comet 实例标识，用于上报与鉴权。
    std::string comet_id_;
};

}  // namespace sparkpush



