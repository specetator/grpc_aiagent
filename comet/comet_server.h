#pragma once

#include "config.h"
#include "logging.h"
#include "metrics_http_server.h"
#include "spark_push.grpc.pb.h"
#include "thread_pool.h"
#include "websocket_utils.h"

#include <muduo/net/Buffer.h>
#include <muduo/net/EventLoop.h>
#include <muduo/net/TcpServer.h>

#include <atomic>
#include <condition_variable>
#include <deque>
#include <functional>
#include <mutex>
#include <queue>
#include <set>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <grpcpp/grpcpp.h>

namespace sparkpush {

using muduo::net::Buffer;
using muduo::net::EventLoop;
using muduo::net::TcpConnectionPtr;
using muduo::net::TcpServer;

struct ConnContext {
    enum State { kHandshake, kOpen } state{kHandshake};
    int64_t user_id{0};
};

class CometServer {
public:
    CometServer(EventLoop* loop, const Config& cfg);
    ~CometServer();

    void SetThreadNum(int thread_num);
    void Start();

    size_t PushToUsers(const ChatMessage& msg,
                       const std::vector<int64_t>& user_ids);
    size_t PushToRoom(const ChatMessage& msg, int64_t room_id);
    size_t PushToAll(const ChatMessage& msg);
    void PushDeliveryAck(int64_t user_id, const ChatMessage& msg);
    void ReportDeliveredToUsers(const ChatMessage& msg,
                                const std::vector<int64_t>& user_ids);
    std::vector<int64_t> GetRoomUserIds(int64_t room_id) const;

    // Job 重连重放时使用 request_id 去重；返回 false 表示已经处理过。
    bool AcceptPushRequest(const std::string& request_id);

private:
    void OnConnection(const TcpConnectionPtr& conn);
    void OnMessage(const TcpConnectionPtr& conn, Buffer* buf,
                   muduo::Timestamp);

    void HandleHandshake(const TcpConnectionPtr& conn, Buffer* buf);
    void HandleWebSocketFrame(const TcpConnectionPtr& conn, Buffer* buf,
                              ConnContext& ctx);
    void OnTextMessage(const TcpConnectionPtr& conn, ConnContext& ctx,
                       const std::string& payload);

    std::string ParseTokenFromHandshake(const std::string& req);
    void NotifyUserOffline(int64_t user_id);
    void RequestOfflineSync(int64_t user_id);
    void RequestCursorSync(const TcpConnectionPtr& conn, int64_t user_id,
                           const std::string& session_id, int64_t after_seq,
                           int limit);

    void AddUserToRoom(int64_t room_id, int64_t user_id);
    void RemoveUserFromRoom(int64_t room_id, int64_t user_id);
    void NotifyRoomJoin(int64_t room_id, int64_t user_id);
    void NotifyRoomLeave(int64_t room_id, int64_t user_id);

    // gRPC 双向流
    void InitStreams();
    void StreamWriterLoop(int stream_idx);
    void StreamReaderLoop(int stream_idx);
    void SendToStream(StreamMessage msg,
                      std::function<void(const StreamResponse&)> callback);
    uint64_t NextRequestId();

    TcpServer server_;
    std::unordered_map<int64_t, std::set<TcpConnectionPtr>> user_conns_;
    std::unordered_map<int64_t, std::set<int64_t>> room_users_;
    mutable std::mutex conns_mu_;
    std::unordered_set<std::string> recent_push_ids_;
    std::deque<std::string> recent_push_order_;
    std::shared_ptr<grpc::Channel> channel_;
    std::unique_ptr<sparkpush::LogicService::Stub> logic_stub_;
    std::string comet_id_;
    int metrics_port_{0};
    MetricsHttpServer metrics_server_;
    ThreadPool grpc_pool_;

    bool use_stream_{false};
    int stream_count_{4};
    struct PendingRequest {
        StreamMessage msg;
        std::function<void(const StreamResponse&)> callback;
    };
    struct StreamState {
        std::unique_ptr<grpc::ClientContext> ctx;
        std::unique_ptr<
            grpc::ClientReaderWriter<StreamMessage, StreamResponse>>
            stream;
        std::thread writer_thread;
        std::thread reader_thread;
        std::queue<PendingRequest> send_queue;
        std::mutex send_queue_mutex;
        std::condition_variable send_queue_cv;
    };
    std::vector<std::unique_ptr<StreamState>> streams_;
    std::atomic<bool> stream_running_{false};
    std::unordered_map<std::string,
                       std::function<void(const StreamResponse&)>>
        pending_callbacks_;
    std::mutex callbacks_mutex_;
    std::atomic<uint64_t> request_id_counter_{0};
};

}  // namespace sparkpush
