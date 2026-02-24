#pragma once

#include "config.h"
#include "logging.h"
#include "spark_push.grpc.pb.h"
#include "thread_pool.h"
#include "websocket_utils.h"

#include <muduo/net/Buffer.h>
#include <muduo/net/EventLoop.h>
#include <muduo/net/TcpServer.h>

// #include <boost/any.hpp>

#include <set>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>
#include <mutex>
#include <atomic>
#include <queue>
#include <condition_variable>
#include <functional>
#include <grpcpp/grpcpp.h> // 解决部分平台出现编译报错 std::shared_ptr<grpc::Channel> channel_;  有报错，因为muduo库的Channel和grpc的Channel有冲突

namespace sparkpush {

using muduo::net::Buffer;
using muduo::net::EventLoop;
using muduo::net::TcpConnectionPtr;
using muduo::net::TcpServer;

// 每条 TCP 连接的上下文：握手阶段 / 已升级为 WebSocket，以及绑定的 user_id
struct ConnContext {
  enum State { kHandshake, kOpen } state{kHandshake};
  int64_t user_id{0};
  std::string conn_id;  // 连接唯一标识: comet_id:序号
  int64_t last_pong_ms{0};  // 最近一次收到 pong 的时间（毫秒时间戳）
};

// CometServer：负责管理 WebSocket 连接，与 logic 通信，以及为 gRPC CometService 提供下行推送能力
class CometServer {
 public:
  CometServer(EventLoop* loop, const Config& cfg);

  // 设置 muduo TcpServer 的 IO 线程数（即 worker EventLoop 数量），需在 Start() 之前调用
  void SetThreadNum(int thread_num);

  void Start();

  // 供 gRPC CometService 调用：将消息推送给若干用户（注意一个 user_id 可能有多条连接）
  void PushToUsers(const ChatMessage& msg,
                   const std::vector<int64_t>& user_ids);
     // 通过本机房间成员表，将消息推送给某个 room 的所有本机在线用户
  void PushToRoom(const ChatMessage& msg, int64_t room_id);
  // 广播给本机所有在线连接
  void PushToAll(const ChatMessage& msg);
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

  // 当某个 user 在本 comet 上的连接数变为 0 时，通知 logic 执行 UserOffline
  void NotifyUserOffline(int64_t user_id, const std::string& conn_id = "");

  // 聊天室本机房间成员管理（room_id -> user_ids）
  void AddUserToRoom(int64_t room_id, int64_t user_id);
  void RemoveUserFromRoom(int64_t room_id, int64_t user_id);
  // 通过逻辑服上报房间路由 + 在线人数（WebSocket 控制消息驱动）
  void NotifyRoomJoin(int64_t room_id, int64_t user_id);
  void NotifyRoomLeave(int64_t room_id, int64_t user_id);

 

  // WebSocket 心跳检测
  void HeartbeatTick();

  // 双向流相关
  void InitStreams();
  void StreamWriterLoop(int stream_idx);
  void StreamReaderLoop(int stream_idx);
  void SendToStream(StreamMessage msg, std::function<void(const StreamResponse&)> callback);
  uint64_t NextRequestId();

  TcpServer server_;
  // user_id -> 该用户在本 comet 上的所有 WebSocket 连接
  std::unordered_map<int64_t, std::set<TcpConnectionPtr>> user_conns_;
  // room_id -> 当前在本 comet 上该房间内的 user_id 列表
  std::unordered_map<int64_t, std::set<int64_t>> room_users_;
  mutable std::mutex conns_mu_;
  std::unique_ptr<sparkpush::LogicService::Stub> logic_stub_;
  std::string comet_id_;
  // 用于异步执行 gRPC 调用的线程池，避免阻塞 muduo 事件循环
  ThreadPool grpc_pool_;

  // 多流支持
  bool use_stream_;  // 是否启用双向流模式
  int stream_count_;  // 流数量
  std::shared_ptr<grpc::Channel> channel_;
  
  // 发送请求结构
  struct PendingRequest {
    StreamMessage msg;
    std::function<void(const StreamResponse&)> callback;
  };
  
  // 每个流的状态
  struct StreamState {
    std::unique_ptr<grpc::ClientContext> ctx;
    std::unique_ptr<grpc::ClientReaderWriter<StreamMessage, StreamResponse>> stream;
    std::thread writer_thread;
    std::thread reader_thread;
    std::queue<PendingRequest> send_queue;
    std::mutex send_queue_mutex;
    std::condition_variable send_queue_cv;
  };
  std::vector<std::unique_ptr<StreamState>> streams_;
  std::atomic<bool> stream_running_{false};
  
  // 响应回调映射（所有流共享）
  std::unordered_map<std::string, std::function<void(const StreamResponse&)>> pending_callbacks_;
  std::mutex callbacks_mutex_;
  
  std::atomic<uint64_t> request_id_counter_{0};
  std::atomic<uint64_t> conn_id_counter_{0};  // 连接ID生成器
};

}  // namespace sparkpush



