#pragma once

#include "config.h"
#include "logging.h"
#include "spark_push.grpc.pb.h"
#include "websocket_utils.h"

#include <muduo/net/Buffer.h>
#include <muduo/net/EventLoop.h>
#include <muduo/net/TcpServer.h>

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

struct ConnContext {
    enum State {
        kHandshake,  // 握手阶段：等待 WebSocket 握手完成
        kOpen        // 已建立：WebSocket 连接已升级，可收发消息
    };
    State state{kHandshake};  // 当前状态，初始为握手阶段
    int64_t user_id{0};
};

class CometServer {
public:
    CometServer(EventLoop* loop, const Config& cfg);

    void SetThreadNum(int thread_num);
    void Start();

    void PushToUsers(const ChatMessage& msg,
                     const std::vector<int64_t>& user_ids);
private:
    void OnConnection(const TcpConnectionPtr& conn);
    void OnMessage(const TcpConnectionPtr& conn,
                   Buffer* buf,
                   muduo::Timestamp);

    void HandleHandshake(const TcpConnectionPtr& conn, Buffer* buf);
    void HandleWebSocketFrame(const TcpConnectionPtr& conn,
                              Buffer* buf,
                              ConnContext& ctx);
    void OnTextMessage(const TcpConnectionPtr& conn,
                       ConnContext& ctx,
                       const std::string& payload);

    std::string ParseTokenFromHandshake(const std::string& req);

    void NotifyUserOffline(int64_t user_id);
 

    TcpServer server_;
    std::unordered_map<int64_t, std::set<TcpConnectionPtr>> user_conns_;
    mutable std::mutex conns_mu_;
    std::unique_ptr<sparkpush::LogicService::Stub> logic_stub_;
    std::string comet_id_;
};

}  // namespace sparkpush

