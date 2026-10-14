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

// 单条 TCP 连接的状态机上下文，随连接存放在 muduo 的 context（std::any）里。
//
// 状态推进路径：
//   kHandshake      刚建 TCP，等待客户端发来 HTTP Upgrade 请求
//   kAuthenticating 已收到 Upgrade，正在线程池里同步调 Logic.VerifyToken
//   kOpen           鉴权通过、已回 101，可以收发 WebSocket 业务帧
//
// OnConnection 里挂了一个 5 秒定时器：没走到 kOpen 的连接会被 forceClose，
// 防止「连上但不握手」的半开连接耗尽 max_connections_ 配额。
struct ConnContext {
    enum State { kHandshake, kAuthenticating, kOpen } state{kHandshake};
    int64_t user_id{0};           // 鉴权成功后由 Logic 回填，0 表示未认证
    std::string auth_token;       // 原始 bearer。上行时透传给 Logic 做二次校验，
                                  // 保证「WS 在线」不等于「有权限」
    int64_t route_generation{0};  // 路由代数。同一 user 多设备/快速重连时用于
                                  // 区分新旧路由，避免旧连接断开时误摘新路由
};

// Comet 进程的核心类，同时扮演三个角色：
//   1) WebSocket 服务端（面向客户端）        —— 本文件
//   2) gRPC 客户端（面向 Logic 上行流）      —— 本文件 MessageStream
//   3) gRPC 服务端（面向 Job 下推流）        —— comet_grpc_service.cpp PushStream
//
// 职责边界（读代码时反复回来看这条）：
//   Comet 不做任何业务裁决、不分配 msg_seq、不写 MySQL、不持久化消息。
//   它只负责：连接管理、协议解析、本机 fanout、离线补推、ACK 回传。
//   因此「socket 已连接」既不代表消息 accepted，也不代表 delivered。
class CometServer {
public:
    CometServer(EventLoop* loop, const Config& cfg);
    ~CometServer();

    // 设置 muduo 的 IO 线程数（不含 main loop）。必须在 Start() 之前调用。
    void SetThreadNum(int thread_num);
    // 启动顺序：metrics HTTP -> gRPC 线程池 -> 双向流(+看门狗) -> TcpServer。
    void Start();

    // ===== 下推入口（由 CometServiceImpl 在 Job 的 PushStream 回调里调用）=====
    // 按用户列表投递。一个 user_id 可能对应多条连接（多标签页/多设备），
    // 返回值是「成功 send 的连接数」，不是用户数。
    // delivered_users 回填真正投递成功的 user_id 集合，供 MarkDelivered 使用。
    size_t PushToUsers(const ChatMessage& msg,
                       const std::vector<int64_t>& user_ids,
                       std::vector<int64_t>* delivered_users = nullptr);
    // 投递给本机 room_users_ 里的所有在线用户；跨节点由 Logic 做 comet 级 fanout
    size_t PushToRoom(const ChatMessage& msg, int64_t room_id);
    // 全量广播：遍历 user_conns_ 的所有 key 后复用 PushToUsers
    size_t PushToAll(const ChatMessage& msg);
    // 给「发送者」回 delivered_ack 帧。注意这不是已读回执，
    // 只表示服务端已把消息交给了目标在线连接的输出缓冲。
    void PushDeliveryAck(int64_t user_id, const ChatMessage& msg);
    // 批量推进 delivered 游标（一次 RPC 带多个 user_cursor），
    // 避免群聊 fanout 被放大成 N 次数据库往返。
    void ReportDeliveredToUsers(const ChatMessage& msg,
                                const std::vector<int64_t>& user_ids);
    // 快照本机房间成员。返回拷贝，调用方不持锁。
    std::vector<int64_t> GetRoomUserIds(int64_t room_id) const;

    // Job 重连重放时使用 request_id 去重；返回 false 表示已经处理过。
    // 内部是 unordered_set + deque 组成的插入序 LRU（上限 10 万条）。
    bool AcceptPushRequest(const std::string& request_id);

private:
    // ===== muduo 回调 =====
    // 连接建立/断开：建时限流+设 TCP_NODELAY+挂 5s 握手超时；
    // 断时清理 user_conns_ 并在本机连接数归零时上报离线。
    void OnConnection(const TcpConnectionPtr& conn);
    // 数据到达：根据 ConnContext::state 分流到握手解析或帧解析。
    void OnMessage(const TcpConnectionPtr& conn, Buffer* buf,
                   muduo::Timestamp);

    // ===== WebSocket 协议处理 =====
    // HTTP Upgrade -> VerifyToken -> 101 响应 -> 立即 SyncOffline
    void HandleHandshake(const TcpConnectionPtr& conn, Buffer* buf);
    // RFC6455 帧解析。只接受 FIN=1 且已 mask 的 text/close/ping/pong，
    // payload 上限 64 KiB，超限或出现分片帧直接断连。
    void HandleWebSocketFrame(const TcpConnectionPtr& conn, Buffer* buf,
                              ConnContext& ctx);
    // 业务上行分发：sync / single_chat / chatroom / chatroom_join / chatroom_leave
    void OnTextMessage(const TcpConnectionPtr& conn, ConnContext& ctx,
                       const std::string& payload);

    // 从 GET 请求行的 query 里取 token=xxx（不做 URL 解码）
    std::string ParseTokenFromHandshake(const std::string& req);
    // 本机该 user 的连接数归零时，异步通知 Logic 摘除路由
    void NotifyUserOffline(int64_t user_id, int64_t generation, const std::string& token);
    // 鉴权成功但连接已经没了（客户端秒断）时的补偿清理，防止路由泄漏
    void CleanupAuthenticatedRoute(int64_t user_id, int64_t generation, const std::string& token);
    // 握手成功后补推离线期间「未交给连接」的消息。权威依据是
    // user_session_state.delivered_seq；只有真正 send 成功才 MarkDelivered。
    // has_more 时按 (next_session_id, next_seq) 递归翻页。
    void RequestOfflineSync(int64_t user_id, const std::string& after_session = "", int64_t after_seq = 0);
    // 客户端主动发 sync 帧时的缺口补齐；回帧序列以 sync_end 结束，
    // 带 safe_seq（已完整落库的前缀）供客户端推进连续游标。
    void RequestCursorSync(const TcpConnectionPtr& conn, int64_t user_id,
                           const std::string& session_id, int64_t after_seq,
                           int limit);

    // ===== 本机房间表（仅内存快照，权威成员表在 MySQL）=====
    void AddUserToRoom(int64_t room_id, int64_t user_id);
    void RemoveUserFromRoom(int64_t room_id, int64_t user_id);
    void NotifyRoomJoin(int64_t room_id, int64_t user_id);
    void NotifyRoomLeave(int64_t room_id, int64_t user_id);

    struct StreamState;

    // ===== Comet -> Logic 的 gRPC 双向流（MessageStream）=====
    // 为什么要用流：消除逐条 Unary RPC 的连接建立与头部开销。
    // 为什么要多条：单条流的 Write 是串行的，多条可按 request_id 取模分片并发。
    void InitStreams();
    // 指数退避重连（base 200ms -> max 5s）。重连会把旧流上所有 pending
    // 请求以 503 失败掉，让客户端重试，绝不静默丢弃。
    bool ReconnectStream(int stream_idx);
    // 把 pending 表整体 swap 出来再以 503 回调，避免持锁执行用户回调
    void FailStreamPending(StreamState* state, const std::string& message);
    // 看门狗：Write 成功但迟迟没有 reply 的请求按 deadline 超时失败，
    // 防止前端气泡永远停在「发送中」。
    void ExpireStreamPending(StreamState* state);
    // 汇总成 spark_push_comet_logic_stream_ready（0/1）。
    // 启动脚本和健康检查都要求它为 1，避免「进程在、但发不出去」。
    void UpdateLogicStreamReadyMetric();
    void CancelStream(StreamState* state);
    // 写线程：从有界队列取请求 -> 必要时重连 -> 登记 pending -> Write。
    // 最多重试 4 次，全部失败则以 503 回调。
    void StreamWriterLoop(int stream_idx);
    // 读线程：Read 到 reply 后按 request_id 找到 pending 回调并执行。
    // Read 返回 false 即流断开，标记 broken 并唤醒写线程去重连。
    void StreamReaderLoop(int stream_idx);
    // 入队 + 背压：队列长度 512、字节预算 8 MiB，超限直接回 429。
    // callback 用 atomic<bool> 包成「只执行一次」，因为超时路径和
    // 正常 reply 路径可能同时触发。
    void SendToStream(StreamMessage msg,
                      std::function<void(const StreamResponse&)> callback);
    // 流内单调递增；同时用于对 stream_count_ 取模分片
    uint64_t NextRequestId();

    // ===== 连接与路由表（全部由 conns_mu_ 保护）=====
    // user_id -> 该用户的所有在线连接。用 set 保证同一连接不重复。
    std::unordered_map<int64_t, std::set<TcpConnectionPtr>> user_conns_;
    // user_id -> 当前最大路由代数，配合 RemoveRouteGeneration 做 CAS 式摘除
    std::unordered_map<int64_t, int64_t> user_route_generations_;
    // user_id -> 路由令牌，Logic 侧校验用，防止别的 Comet 摘掉本机路由
    std::unordered_map<int64_t, std::string> user_route_tokens_;
    // room_id -> 本机在线成员快照（不是权威成员表）
    std::unordered_map<int64_t, std::set<int64_t>> room_users_;
    mutable std::mutex conns_mu_;
    // request_id 幂等去重：set 做查找，deque 做插入序 LRU 淘汰
    std::unordered_set<std::string> recent_push_ids_;
    std::deque<std::string> recent_push_order_;
    std::shared_ptr<grpc::Channel> channel_;
    std::unique_ptr<sparkpush::LogicService::Stub> logic_stub_;
    std::string comet_id_;          // 本节点身份，Logic 据此做路由
    int metrics_port_{0};
    MetricsHttpServer metrics_server_;
    // 所有阻塞 gRPC 调用（VerifyToken/UserOffline/SyncOffline/SyncMessages/
    // MarkDelivered/RoomReport）都丢进这个池，绝不占用 muduo IO 线程。
    ThreadPool grpc_pool_;

    bool use_stream_{false};        // false 时上行退化为 Unary + 线程池
    int stream_count_{4};
    // 已入队但还没 Write 的请求
    struct PendingRequest {
        StreamMessage msg;
        std::function<void(const StreamResponse&)> callback;
        std::chrono::steady_clock::time_point deadline;
    };
    // 已 Write 出去、正在等 reply 的请求
    struct PendingCallback {
        std::function<void(const StreamResponse&)> callback;
        std::chrono::steady_clock::time_point deadline;
    };
    // 一条双向流的完整运行时状态。
    //
    // 三把锁的分工（这是最容易看错的地方）：
    //   context_mutex    保护 ctx，供 CancelStream 与重连时替换 ctx 互斥
    //   send_queue_mutex 保护 send_queue / queued_bytes，配 send_queue_cv 做阻塞唤醒
    //   pending_mutex    保护 pending（request_id -> callback）
    // 写线程持 send_queue_mutex 取任务、持 pending_mutex 登记；
    // 读线程只持 pending_mutex 摘除并回调。两把锁分开避免读写互相阻塞。
    //
    // reconnect_mutex 单独串行化重连，避免写线程和看门狗同时重建流。
    struct StreamState {
        std::shared_ptr<grpc::ClientContext> ctx;
        std::mutex context_mutex;
        std::unique_ptr<
            grpc::ClientReaderWriter<StreamMessage, StreamResponse>>
            stream;
        std::thread writer_thread;
        std::thread reader_thread;
        std::queue<PendingRequest> send_queue;
        size_t queued_bytes{0};                  // 字节级背压预算
        std::mutex send_queue_mutex;
        std::condition_variable send_queue_cv;
        std::mutex pending_mutex;
        std::unordered_map<std::string, PendingCallback> pending;
        std::mutex reconnect_mutex;
        std::atomic<bool> broken{false};         // 读线程置位，写线程消费
    };
    std::vector<std::unique_ptr<StreamState>> streams_;
    std::atomic<bool> stream_running_{false};
    std::unique_ptr<PeriodicWorker> stream_watchdog_;  // 100ms 扫一遍超时
    std::string logic_grpc_target_;
    int stream_reconnect_base_ms_{200};
    int stream_reconnect_max_ms_{5000};
    int stream_ack_timeout_ms_{8000};
    std::atomic<uint64_t> request_id_counter_{0};
    std::atomic<bool> shutting_down_{false};
    std::atomic<int> connection_count_{0};
    int max_connections_{10000};
    // Destroy/join transport loops while callback dependencies still exist.
    // 声明顺序决定析构逆序：server_ 放最后，保证它先于线程池/流被销毁，
    // 析构时不会有 IO 回调还在访问已释放的成员。
    TcpServer server_;
};

}  // namespace sparkpush
