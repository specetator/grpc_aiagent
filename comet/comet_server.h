#pragma once

#include "config.h"
#include "connection_budget.h"
#include "logging.h"
#include "metrics_http_server.h"
#include "spark_push.grpc.pb.h"
#include "thread_pool.h"
#include "websocket_utils.h"

#include <muduo/net/Buffer.h>
#include <muduo/net/EventLoop.h>
#include <muduo/net/TcpServer.h>
#include <muduo/net/TimerId.h>

#include <atomic>
#include <array>
#include <chrono>
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
    enum State { kHandshake, kAuthenticating, kOpen } state{kHandshake};
    int64_t user_id{0};
    std::string device_id;
    std::string route_generation;
    std::shared_ptr<ConnectionBudget> budget;
    std::shared_ptr<std::atomic<bool>> sync_inflight;
    std::shared_ptr<std::atomic<bool>> sync_more;
};

class CometServer {
public:
    CometServer(EventLoop* loop, const Config& cfg);
    ~CometServer();

    void SetThreadNum(int thread_num);
    void Start();

    int64_t PushToUsers(const ChatMessage& msg,
                       const std::vector<int64_t>& user_ids);
    int64_t PushToRoom(const ChatMessage& msg, int64_t room_id);
    int64_t PushToAll(const ChatMessage& msg);
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
    void NotifyUserOffline(int64_t user_id, const std::string& generation);
    void RequestOfflineSync(int64_t user_id);
    void RequestDeviceSync(const TcpConnectionPtr& conn, const ConnContext& ctx);
    void RecoverDevices();
    void RefreshRoutes();
    bool SubmitRpc(std::function<void()> task, size_t bytes = 4096);
    bool SendFrame(const TcpConnectionPtr& conn,
                   const std::shared_ptr<ConnectionBudget>& budget, const std::string& frame);
    std::shared_ptr<ConnectionBudget> FindBudget(const TcpConnectionPtr& conn);
    int64_t PushConnections(const ChatMessage& msg, const std::vector<TcpConnectionPtr>& conns);
    std::vector<TcpConnectionPtr> UserConnections(const std::vector<int64_t>& users) const;
    void RequestCursorSync(const TcpConnectionPtr& conn, int64_t user_id,
                           const std::string& session_id, int64_t after_seq,
                           int limit);

    void AddUserToRoom(int64_t room_id, int64_t user_id);
    void RemoveUserFromRoom(int64_t room_id, int64_t user_id);
    void NotifyRoomJoin(int64_t room_id, int64_t user_id);
    void NotifyRoomLeave(int64_t room_id, int64_t user_id);

    struct StreamState;

    // gRPC 双向流
    void InitStreams();
    bool ReconnectStream(int stream_idx);
    void FailStreamPending(StreamState* state, const std::string& message);
    void ExpireStreamPending(StreamState* state);
    void UpdateLogicStreamReadyMetric();
    void StreamWriterLoop(int stream_idx);
    void StreamReaderLoop(int stream_idx);
    void SendToStream(StreamMessage msg,
                      std::function<void(const StreamResponse&)> callback);
    uint64_t NextRequestId();

    TcpServer server_;
    struct UserConnectionsState {
        std::set<TcpConnectionPtr> conns;
        std::unordered_map<TcpConnectionPtr, ConnContext> devices;
        std::string generation;
    };
    struct ConnectionBucket {
        mutable std::mutex mu;
        std::unordered_map<int64_t, UserConnectionsState> users;
        std::unordered_map<TcpConnectionPtr, std::shared_ptr<ConnectionBudget>> budgets;
    };
    std::array<ConnectionBucket, 64> buckets_;
    ConnectionBucket& Bucket(int64_t uid) { return buckets_[static_cast<uint64_t>(uid) % buckets_.size()]; }
    const ConnectionBucket& Bucket(int64_t uid) const { return buckets_[static_cast<uint64_t>(uid) % buckets_.size()]; }
    std::unordered_map<int64_t, std::set<int64_t>> room_users_;
    mutable std::mutex rooms_mu_;
    EventLoop* loop_;
    muduo::net::TimerId recovery_timer_, lease_timer_;
    std::shared_ptr<std::atomic<bool>> alive_{std::make_shared<std::atomic<bool>>(true)};
    std::atomic<size_t> rpc_tasks_{0}, rpc_bytes_{0};
    size_t max_pending_bytes_{8 * 1024 * 1024};
    size_t max_rpc_tasks_{256}, max_rpc_bytes_{16 * 1024 * 1024};
    std::string boot_generation_;
    std::atomic<uint64_t> route_epoch_{0};
    std::shared_ptr<grpc::Channel> channel_;
    std::unique_ptr<sparkpush::LogicService::Stub> logic_stub_;
    std::mutex stub_mutex_;
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
    struct PendingCallback {
        std::function<void(const StreamResponse&)> callback;
        std::chrono::steady_clock::time_point deadline;
    };
    struct StreamState {
        std::unique_ptr<grpc::ClientContext> ctx;
        std::unique_ptr<
            grpc::ClientReaderWriter<StreamMessage, StreamResponse>>
            stream;
        std::thread writer_thread;
        std::thread reader_thread;
        std::queue<PendingRequest> send_queue;
        size_t send_queue_bytes{0};
        std::mutex send_queue_mutex;
        std::condition_variable send_queue_cv;
        std::mutex pending_mutex;
        std::unordered_map<std::string, PendingCallback> pending;
        std::mutex reconnect_mutex;
        std::atomic<bool> broken{false};
    };
    std::vector<std::unique_ptr<StreamState>> streams_;
    std::atomic<bool> stream_running_{false};
    std::string logic_grpc_target_;
    int stream_reconnect_base_ms_{200};
    int stream_reconnect_max_ms_{5000};
    int stream_ack_timeout_ms_{8000};
    std::atomic<uint64_t> request_id_counter_{0};
};

}  // namespace sparkpush
