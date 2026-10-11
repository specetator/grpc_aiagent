#pragma once

#include "config.h"
#include "logging.h"
#include "metrics_http_server.h"
#include "spark_push.grpc.pb.h"
#include "thread_pool.h"
#include "periodic_worker.h"
#include "websocket_utils.h"

#include <muduo/net/Buffer.h>
#include <muduo/net/EventLoop.h>
#include <muduo/net/TcpServer.h>

#include <atomic>
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
    std::string auth_token;
    int64_t route_generation{0};
};

class CometServer {
public:
    CometServer(EventLoop* loop, const Config& cfg);
    ~CometServer();

    void SetThreadNum(int thread_num);
    void Start();

    size_t PushToUsers(const ChatMessage& msg,
                       const std::vector<int64_t>& user_ids,
                       std::vector<int64_t>* delivered_users = nullptr);
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
    void NotifyUserOffline(int64_t user_id, int64_t generation, const std::string& token);
    void CleanupAuthenticatedRoute(int64_t user_id, int64_t generation, const std::string& token);
    void RequestOfflineSync(int64_t user_id, const std::string& after_session = "", int64_t after_seq = 0);
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
    void CancelStream(StreamState* state);
    void StreamWriterLoop(int stream_idx);
    void StreamReaderLoop(int stream_idx);
    void SendToStream(StreamMessage msg,
                      std::function<void(const StreamResponse&)> callback);
    uint64_t NextRequestId();

    std::unordered_map<int64_t, std::set<TcpConnectionPtr>> user_conns_;
    std::unordered_map<int64_t, int64_t> user_route_generations_;
    std::unordered_map<int64_t, std::string> user_route_tokens_;
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
        std::chrono::steady_clock::time_point deadline;
    };
    struct PendingCallback {
        std::function<void(const StreamResponse&)> callback;
        std::chrono::steady_clock::time_point deadline;
    };
    struct StreamState {
        std::shared_ptr<grpc::ClientContext> ctx;
        std::mutex context_mutex;
        std::unique_ptr<
            grpc::ClientReaderWriter<StreamMessage, StreamResponse>>
            stream;
        std::thread writer_thread;
        std::thread reader_thread;
        std::queue<PendingRequest> send_queue;
        size_t queued_bytes{0};
        std::mutex send_queue_mutex;
        std::condition_variable send_queue_cv;
        std::mutex pending_mutex;
        std::unordered_map<std::string, PendingCallback> pending;
        std::mutex reconnect_mutex;
        std::atomic<bool> broken{false};
    };
    std::vector<std::unique_ptr<StreamState>> streams_;
    std::atomic<bool> stream_running_{false};
    std::unique_ptr<PeriodicWorker> stream_watchdog_;
    std::string logic_grpc_target_;
    int stream_reconnect_base_ms_{200};
    int stream_reconnect_max_ms_{5000};
    int stream_ack_timeout_ms_{8000};
    std::atomic<uint64_t> request_id_counter_{0};
    std::atomic<bool> shutting_down_{false};
    std::atomic<int> connection_count_{0};
    int max_connections_{10000};
    // Destroy/join transport loops while callback dependencies still exist.
    TcpServer server_;
};

}  // namespace sparkpush
