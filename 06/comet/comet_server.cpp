#include "comet_server.h"

#include <grpcpp/grpcpp.h>

#include <chrono>
#include <cstring>
#include <array>
#include <cstdint>
#include <vector>

namespace sparkpush {

namespace {

// 从 HTTP 请求中提取指定 Header 的值（简单实现，按行搜索）
bool ExtractHeader(const std::string& req,
                   const std::string& header_name,
                   std::string* value) {
  if (!value) return false;
  // 按 "Header-Name:" 形式查找
  std::string key = header_name + ":";
  auto pos = req.find(key);
  if (pos == std::string::npos) return false;
  pos += key.size();
  // 跳过空格
  while (pos < req.size() &&
         (req[pos] == ' ' || req[pos] == '\t')) {
    ++pos;
  }
  if (pos >= req.size()) return false;
  auto end = req.find("\r\n", pos);
  if (end == std::string::npos) {
    end = req.size();
  }
  // 去掉末尾空白
  size_t trimmed_end = end;
  while (trimmed_end > pos &&
         (req[trimmed_end - 1] == ' ' ||
          req[trimmed_end - 1] == '\t' ||
          req[trimmed_end - 1] == '\r' ||
          req[trimmed_end - 1] == '\n')) {
    --trimmed_end;
  }
  *value = req.substr(pos, trimmed_end - pos);
  return true;
}

// 简单的 Base64 编码（仅用于 WebSocket 握手）
std::string Base64Encode(const unsigned char* data, size_t len) {
  static const char kTable[] =
      "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
      "abcdefghijklmnopqrstuvwxyz"
      "0123456789+/";
  std::string out;
  out.reserve(((len + 2) / 3) * 4);

  size_t i = 0;
  while (i + 2 < len) {
    unsigned int n = (data[i] << 16) | (data[i + 1] << 8) | data[i + 2];
    out.push_back(kTable[(n >> 18) & 0x3F]);
    out.push_back(kTable[(n >> 12) & 0x3F]);
    out.push_back(kTable[(n >> 6) & 0x3F]);
    out.push_back(kTable[n & 0x3F]);
    i += 3;
  }

  if (i < len) {
    unsigned int n = data[i] << 16;
    if (i + 1 < len) {
      n |= (data[i + 1] << 8);
    }
    out.push_back(kTable[(n >> 18) & 0x3F]);
    out.push_back(kTable[(n >> 12) & 0x3F]);
    if (i + 1 < len) {
      out.push_back(kTable[(n >> 6) & 0x3F]);
      out.push_back('=');
    } else {
      out.push_back('=');
      out.push_back('=');
    }
  }

  return out;
}

inline uint32_t RotL(uint32_t value, unsigned int bits) {
  return (value << bits) | (value >> (32 - bits));
}

// 单块 SHA1 变换
void SHA1Transform(uint32_t state[5],
                   const unsigned char block[64]) {
  uint32_t w[80];
  for (int i = 0; i < 16; ++i) {
    w[i] = (static_cast<uint32_t>(block[i * 4]) << 24) |
           (static_cast<uint32_t>(block[i * 4 + 1]) << 16) |
           (static_cast<uint32_t>(block[i * 4 + 2]) << 8) |
           (static_cast<uint32_t>(block[i * 4 + 3]));
  }
  for (int i = 16; i < 80; ++i) {
    w[i] = RotL(w[i - 3] ^ w[i - 8] ^ w[i - 14] ^ w[i - 16], 1);
  }

  uint32_t a = state[0];
  uint32_t b = state[1];
  uint32_t c = state[2];
  uint32_t d = state[3];
  uint32_t e = state[4];

  for (int i = 0; i < 80; ++i) {
    uint32_t f, k;
    if (i < 20) {
      f = (b & c) | ((~b) & d);
      k = 0x5A827999;
    } else if (i < 40) {
      f = b ^ c ^ d;
      k = 0x6ED9EBA1;
    } else if (i < 60) {
      f = (b & c) | (b & d) | (c & d);
      k = 0x8F1BBCDC;
    } else {
      f = b ^ c ^ d;
      k = 0xCA62C1D6;
    }
    uint32_t temp = RotL(a, 5) + f + e + k + w[i];
    e = d;
    d = c;
    c = RotL(b, 30);
    b = a;
    a = temp;
  }

  state[0] += a;
  state[1] += b;
  state[2] += c;
  state[3] += d;
  state[4] += e;
}

// 计算字符串的 SHA1 摘要
std::array<unsigned char, 20> SHA1(const std::string& data) {
  uint32_t state[5] = {
      0x67452301u,
      0xEFCDAB89u,
      0x98BADCFEu,
      0x10325476u,
      0xC3D2E1F0u,
  };

  // 消息拷贝 + 填充
  std::vector<unsigned char> msg(data.begin(), data.end());
  uint64_t bit_len = static_cast<uint64_t>(data.size()) * 8;

  // 附加 0x80
  msg.push_back(0x80);
  // 填充 0，直到长度 % 64 == 56
  while ((msg.size() % 64) != 56) {
    msg.push_back(0x00);
  }
  // 追加 64bit 大端表示的原始 bit 长度
  for (int i = 7; i >= 0; --i) {
    msg.push_back(static_cast<unsigned char>(
        (bit_len >> (8 * i)) & 0xFF));
  }

  // 分块处理
  for (size_t offset = 0; offset < msg.size(); offset += 64) {
    SHA1Transform(state, &msg[offset]);
  }

  std::array<unsigned char, 20> digest{};
  for (int i = 0; i < 5; ++i) {
    digest[i * 4] =
        static_cast<unsigned char>((state[i] >> 24) & 0xFF);
    digest[i * 4 + 1] =
        static_cast<unsigned char>((state[i] >> 16) & 0xFF);
    digest[i * 4 + 2] =
        static_cast<unsigned char>((state[i] >> 8) & 0xFF);
    digest[i * 4 + 3] =
        static_cast<unsigned char>(state[i] & 0xFF);
  }
  return digest;
}

// 按 RFC6455 计算 Sec-WebSocket-Accept
std::string ComputeWebSocketAccept(const std::string& client_key) {
  static const std::string kGuid =
      "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
  auto digest = SHA1(client_key + kGuid);
  return Base64Encode(digest.data(), digest.size());
}

}  // namespace

// ============================================================================
// CometServer 构造与初始化
// 架构要点：
//   Comet 是 WebSocket 网关层，负责维护客户端长连接。
//   上行消息通过 gRPC 转发给 Logic 处理，下行消息由 Job 通过 gRPC 推送到 Comet。
//   gRPC 通信支持两种模式：传统同步调用（线程池）和双向流（持久连接，高吞吐）。
// ============================================================================

CometServer::CometServer(EventLoop* loop, const Config& cfg)
    : server_(loop,
              muduo::net::InetAddress(cfg.listen_port),
              "comet_server"),
      grpc_pool_(cfg.comet_grpc_pool_size > 0 ? cfg.comet_grpc_pool_size : 4,
                 "comet_grpc_pool"),
      use_stream_(cfg.use_grpc_stream),
      stream_count_(cfg.grpc_stream_count > 0 ? cfg.grpc_stream_count : 1) {
  comet_id_ = cfg.comet_id;
  // gRPC HTTP/2 多路复用：单 channel 可并发多个 RPC 调用
  channel_ = grpc::CreateChannel(cfg.logic_grpc_target,
                                 grpc::InsecureChannelCredentials());
  logic_stub_ = sparkpush::LogicService::NewStub(channel_);

  server_.setConnectionCallback(
      std::bind(&CometServer::OnConnection, this, std::placeholders::_1));
  server_.setMessageCallback(
      std::bind(&CometServer::OnMessage,
                this,
                std::placeholders::_1,
                std::placeholders::_2,
                std::placeholders::_3));
}

void CometServer::SetThreadNum(int thread_num) {
  if (thread_num < 1) thread_num = 1;
  server_.setThreadNum(thread_num);
}

void CometServer::Start() {
  grpc_pool_.Start();
  
  // 启动双向流（如果启用）
  if (use_stream_) {
    InitStreams();
  }
  
  // 启动 WebSocket 心跳定时器：每 30s 发送 ping，60s 无 pong 则断开
  server_.getLoop()->runEvery(30.0, [this]() { HeartbeatTick(); });

  server_.start();
}

// ============================================================================
// 多流实现
// ============================================================================

uint64_t CometServer::NextRequestId() {
  return request_id_counter_.fetch_add(1, std::memory_order_relaxed);
}

void CometServer::InitStreams() {
  streams_.resize(stream_count_);
  
  for (int i = 0; i < stream_count_; ++i) {
    streams_[i] = std::make_unique<StreamState>();
    streams_[i]->ctx = std::make_unique<grpc::ClientContext>();
    streams_[i]->stream = logic_stub_->MessageStream(streams_[i]->ctx.get());
    
    if (!streams_[i]->stream) {
      LogError("Failed to create gRPC stream " + std::to_string(i));
      use_stream_ = false;
      return;
    }
  }
  
  stream_running_ = true;
  
  // 启动每个流的读写线程
  for (int i = 0; i < stream_count_; ++i) {
    streams_[i]->writer_thread = std::thread(&CometServer::StreamWriterLoop, this, i);
    streams_[i]->reader_thread = std::thread(&CometServer::StreamReaderLoop, this, i);
  }
  
  LogInfo("gRPC bidirectional streams initialized, count=" + std::to_string(stream_count_));
}

void CometServer::StreamWriterLoop(int stream_idx) {
  auto& state = streams_[stream_idx];
  if (!state || !state->stream) return;
  
  while (stream_running_) {
    PendingRequest req;
    {
      std::unique_lock<std::mutex> lock(state->send_queue_mutex);
      state->send_queue_cv.wait(lock, [&state, this] {
        return !state->send_queue.empty() || !stream_running_;
      });
      
      if (!stream_running_) break;
      if (state->send_queue.empty()) continue;
      
      req = std::move(state->send_queue.front());
      state->send_queue.pop();
    }
    
    // 注册回调（所有流共享回调映射）
    if (req.callback) {
      std::lock_guard<std::mutex> lock(callbacks_mutex_);
      pending_callbacks_[req.msg.request_id()] = std::move(req.callback);
    }
    
    // 发送到流
    if (!state->stream->Write(req.msg)) {
      LogError("Stream " + std::to_string(stream_idx) + " write failed");
      break;
    }
  }
}

void CometServer::StreamReaderLoop(int stream_idx) {
  auto& state = streams_[stream_idx];
  if (!state || !state->stream) return;
  
  StreamResponse resp;
  while (stream_running_ && state->stream->Read(&resp)) {
    std::function<void(const StreamResponse&)> callback;
    {
      std::lock_guard<std::mutex> lock(callbacks_mutex_);
      auto it = pending_callbacks_.find(resp.request_id());
      if (it != pending_callbacks_.end()) {
        callback = std::move(it->second);
        pending_callbacks_.erase(it);
      }
    }
    
    if (callback) {
      callback(resp);
    }
  }
  
  if (stream_running_) {
    LogError("Stream " + std::to_string(stream_idx) + " read loop ended unexpectedly");
  }
}

void CometServer::SendToStream(StreamMessage msg, 
                               std::function<void(const StreamResponse&)> callback) {
  uint64_t req_id = NextRequestId();
  msg.set_request_id(std::to_string(req_id));
  
  // 轮询分配到不同流
  int stream_idx = req_id % stream_count_;
  auto& state = streams_[stream_idx];
  
  {
    std::lock_guard<std::mutex> lock(state->send_queue_mutex);
    state->send_queue.push({std::move(msg), std::move(callback)});
  }
  state->send_queue_cv.notify_one();
}

// 单聊推送：按 user_id 查找本机连接并发送 WebSocket 帧
// 关键设计：先在锁内拷贝连接列表，再在锁外发送，避免持锁时间过长
void CometServer::PushToUsers(const ChatMessage& msg,
                              const std::vector<int64_t>& user_ids) {
  std::string payload = msg.content_json().empty()
                            ? ("{\"msg_id\":\"" + msg.msg_id() + "\"}")
                            : msg.content_json();
  std::string frame = BuildWebSocketTextFrame(payload);
  // 为避免在持锁状态下执行 send，这里先把连接拷贝到局部向量，然后再逐个发送。
  std::vector<TcpConnectionPtr> conns;
  {
    std::lock_guard<std::mutex> lock(conns_mu_);
    for (int64_t uid : user_ids) {
      auto it = user_conns_.find(uid);
      if (it == user_conns_.end()) continue;
      for (const auto& c : it->second) {
        conns.push_back(c);
      }
    }
  }
  for (const auto& c : conns) {
    if (c->connected()) {
      // muduo 的 send 本身是线程安全的，会在内部切回所属 EventLoop
      c->send(frame);
    }
  }
}

void CometServer::OnConnection(const TcpConnectionPtr& conn) {
  if (conn->connected()) {
    ConnContext ctx;
    ctx.state = ConnContext::kHandshake;
    ctx.conn_id = comet_id_ + ":" + std::to_string(conn_id_counter_.fetch_add(1));
    conn->setContext(ctx);
    LogInfo("New TCP connection from " + conn->peerAddress().toIpPort() +
            " conn_id=" + ctx.conn_id);
  } else {
    // 断开连接，清理 user->conn 映射
    int64_t disconnecting_uid = 0;
    std::string disconnecting_conn_id;
    try {
      auto ctx = std::any_cast<ConnContext>(conn->getContext());
      disconnecting_conn_id = ctx.conn_id;
      if (ctx.user_id > 0) {
        disconnecting_uid = ctx.user_id;
        std::lock_guard<std::mutex> lock(conns_mu_);
        auto it = user_conns_.find(ctx.user_id);
        if (it != user_conns_.end()) {
          it->second.erase(conn);
          if (it->second.empty()) {
            user_conns_.erase(it);
          }
        }
      }
    } catch (const std::bad_any_cast&) {
      // 忽略
    }
    // 每条连接断开都通知 logic 清理该 conn_id 的路由条目
    // （HASH 路由模式下，每条连接有独立的路由记录，必须逐条清理）
    if (disconnecting_uid > 0) {
      NotifyUserOffline(disconnecting_uid, disconnecting_conn_id);
    }
    LogInfo("Connection closed conn_id=" + disconnecting_conn_id);
  }
}

std::string CometServer::ParseTokenFromHandshake(const std::string& req) {
  auto pos = req.find("GET ");
  if (pos == std::string::npos) return {};
  pos += 4;
  auto end = req.find(' ', pos);
  if (end == std::string::npos) return {};
  std::string path = req.substr(pos, end - pos);
  auto qpos = path.find("token=");
  if (qpos == std::string::npos) return {};
  qpos += 6;
  std::string token = path.substr(qpos);
  auto amp = token.find('&');
  if (amp != std::string::npos) token = token.substr(0, amp);
  return token;
}

void CometServer::HandleHandshake(const TcpConnectionPtr& conn, Buffer* buf) {
  const char* crlf2 = "\r\n\r\n";
  const char* data = buf->peek();
  const char* end = static_cast<const char*>(
      memmem(data, buf->readableBytes(), crlf2, 4));
  if (!end) {
    return;  // 头还不完整
  }
  size_t headerLen = end - data + 4;
  std::string req(data, headerLen);

  std::string token = ParseTokenFromHandshake(req);
  if (token.empty()) {
    LogError("No token in WebSocket handshake");
    conn->shutdown();
    return;
  }

  // 解析客户端 Sec-WebSocket-Key
  std::string ws_key;
  if (!ExtractHeader(req, "Sec-WebSocket-Key", &ws_key) ||
      ws_key.empty()) {
    LogError("No Sec-WebSocket-Key in WebSocket handshake");
    conn->shutdown();
    return;
  }
  std::string accept_val = ComputeWebSocketAccept(ws_key);

  // 先从 buffer 中取出已解析的数据，避免重复处理
  buf->retrieve(headerLen);

  // 调用 logic 的 VerifyToken 做鉴权（异步）
  if (!logic_stub_) {
    LogError("Logic stub not initialized");
    conn->shutdown();
    return;
  }

  // 捕获所有需要的变量，提交到线程池异步执行 gRPC 调用
  auto* stub = logic_stub_.get();
  std::string comet_id = comet_id_;

  // 从连接上下文中取 conn_id
  std::string conn_id;
  try {
    auto cctx = std::any_cast<ConnContext>(conn->getContext());
    conn_id = cctx.conn_id;
  } catch (...) {}

  grpc_pool_.Submit([this, conn, stub, token, comet_id, accept_val, conn_id]() {
    VerifyTokenRequest vreq;
    vreq.set_token(token);
    vreq.set_comet_id(comet_id);
    vreq.set_conn_id(conn_id);
    VerifyTokenReply vrep;
    grpc::ClientContext ctx_rpc;
    LogInfo("Verifying token (async): " + token);
    auto status = stub->VerifyToken(&ctx_rpc, vreq, &vrep);

    // 检查连接是否仍然有效
    if (!conn->connected()) {
      LogInfo("Connection closed during VerifyToken, ignoring result");
      return;
    }

    if (!status.ok() || vrep.error().code() != 0) {
      std::string msg = status.ok() ? vrep.error().message()
                                    : status.error_message();
      LogError("VerifyToken failed: " + msg);
      conn->shutdown();
      return;
    }

    int64_t user_id = vrep.user_id();
    LogInfo("VerifyToken reply received, user_id=" + std::to_string(user_id));

    // 标准 WebSocket 握手响应
    std::string resp =
        "HTTP/1.1 101 Switching Protocols\r\n"
        "Upgrade: websocket\r\n"
        "Connection: Upgrade\r\n";
    resp += "Sec-WebSocket-Accept: " + accept_val + "\r\n\r\n";

    // 使用 runInLoop 确保在 EventLoop 线程中执行上下文更新和发送
    conn->getLoop()->runInLoop([this, conn, resp, user_id]() {
      if (!conn->connected()) {
        LogInfo("Connection closed before handshake completion");
        return;
      }

      // muduo TcpConnection::send() 在 EventLoop 线程中调用
      conn->send(resp);

      // 更新连接上下文（必须在 EventLoop 线程中执行）
      try {
        ConnContext ctx = std::any_cast<ConnContext>(conn->getContext());
        ctx.state = ConnContext::kOpen;
        ctx.user_id = user_id;
        ctx.last_pong_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now().time_since_epoch()).count();
        conn->setContext(ctx);
      } catch (const std::bad_any_cast&) {
        LogError("Failed to get connection context");
        conn->shutdown();
        return;
      }

      {
        std::lock_guard<std::mutex> lock(conns_mu_);
        user_conns_[user_id].insert(conn);
      }
      LogInfo("WebSocket handshake done (async), user_id=" + std::to_string(user_id));
    });
  });
}

void CometServer::HandleWebSocketFrame(const TcpConnectionPtr& conn,
                                       Buffer* buf,
                                       ConnContext& ctx) {
  // RFC 6455 WebSocket 帧解析
  // 帧结构: [FIN(1b) RSV(3b) opcode(4b)] [MASK(1b) payload_len(7b)] [ext_len] [mask_key(4B)] [payload]
  // 客户端 → 服务端必须 MASK，服务端 → 客户端不 MASK
  while (buf->readableBytes() >= 2) {
    const unsigned char* data =
        reinterpret_cast<const unsigned char*>(buf->peek());
    bool fin = (data[0] & 0x80) != 0;
    unsigned char opcode = data[0] & 0x0F;
    bool masked = (data[1] & 0x80) != 0;
    uint64_t payloadLen = data[1] & 0x7F;
    size_t headerLen = 2;
    if (!fin || !masked) {
      // demo 中不支持分片帧，也不接受未 mask 的客户端数据
      conn->shutdown();
      return;
    }
    if (payloadLen == 126) {
      if (buf->readableBytes() < headerLen + 2) return;
      const unsigned char* p = data + headerLen;
      payloadLen = (p[0] << 8) | p[1];
      headerLen += 2;
    } else if (payloadLen == 127) {
      if (buf->readableBytes() < headerLen + 8) return;
      payloadLen = 0;
      const unsigned char* p = data + headerLen;
      for (int i = 0; i < 8; ++i) {
        payloadLen = (payloadLen << 8) | p[i];
      }
      headerLen += 8;
    }
    if (buf->readableBytes() < headerLen + 4 + payloadLen) {
      return;  // 数据还不完整
    }
    const unsigned char* mask = data + headerLen;
    headerLen += 4;
    std::string payload;
    payload.resize(payloadLen);
    const unsigned char* payloadData = data + headerLen;
    for (uint64_t i = 0; i < payloadLen; ++i) {
      payload[i] =
          static_cast<char>(payloadData[i] ^ mask[i % 4]);
    }
    buf->retrieve(headerLen + payloadLen);

    if (opcode == 0x8) {  // close
      conn->shutdown();
      return;
    } else if (opcode == 0x9) {  // ping — 回复 pong
      conn->send(BuildWebSocketPongFrame());
    } else if (opcode == 0xA) {  // pong — 更新心跳时间
      ctx.last_pong_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::steady_clock::now().time_since_epoch()).count();
      conn->setContext(ctx);
    } else if (opcode == 0x1) {  // text
      OnTextMessage(conn, ctx, payload);
    }
  }
}

void CometServer::HeartbeatTick() {
  static const int64_t kPongTimeoutMs = 60000;  // 60s 无 pong 则断开
  int64_t now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::steady_clock::now().time_since_epoch()).count();

  std::string ping_frame = BuildWebSocketPingFrame();
  std::vector<TcpConnectionPtr> timeout_conns;

  {
    std::lock_guard<std::mutex> lock(conns_mu_);
    for (const auto& [uid, conns] : user_conns_) {
      for (const auto& conn : conns) {
        try {
          auto ctx = std::any_cast<ConnContext>(conn->getContext());
          if (ctx.state != ConnContext::kOpen) continue;
          if (ctx.last_pong_ms > 0 &&
              (now_ms - ctx.last_pong_ms) > kPongTimeoutMs) {
            timeout_conns.push_back(conn);
          } else {
            conn->send(ping_frame);
          }
        } catch (const std::bad_any_cast&) {}
      }
    }
  }

  for (const auto& conn : timeout_conns) {
    LogInfo("Heartbeat timeout, closing connection: " + conn->peerAddress().toIpPort());
    conn->shutdown();
  }
}

void CometServer::AddUserToRoom(int64_t room_id, int64_t user_id) {
  bool joined = false;
  {
    std::lock_guard<std::mutex> lock(conns_mu_);
    auto& users = room_users_[room_id];
    auto res = users.insert(user_id);
    if (!res.second) {
      // 已在房间内，忽略重复 join
      return;
    }
    joined = true;
  }
  if (joined) {
    // 由 logic 侧在 Redis 中进行 INCRBY 计数 + room:comets 维护
    NotifyRoomJoin(room_id, user_id);
  }
}

void CometServer::RemoveUserFromRoom(int64_t room_id, int64_t user_id) {
  bool left = false;
  {
    std::lock_guard<std::mutex> lock(conns_mu_);
    auto it = room_users_.find(room_id);
    if (it == room_users_.end()) return;
    auto& users = it->second;
    size_t before = users.size();
    users.erase(user_id);
    if (users.size() == before) {
      // 原本就不在房间中
      return;
    }
    if (users.empty()) {
      room_users_.erase(it);
    }
    left = true;
  }
  if (left) {
    NotifyRoomLeave(room_id, user_id);
  }
}

void CometServer::OnTextMessage(const TcpConnectionPtr& conn,
                                ConnContext& ctx,
                                const std::string& payload) {
  // 热路径：不打日志
  UpstreamMessageMeta meta;
  if (!ParseUpstreamMessage(payload, &meta)) {
    std::string frame = BuildWebSocketTextFrame(
        "{\"type\":\"error\",\"message\":\"invalid message format\"}");
    conn->send(frame);
    return;
  }

  const std::string& type = meta.type;
  std::string scene;

  if (type == "single_chat") {
    if (meta.to_user_id <= 0) {
      std::string frame = BuildWebSocketTextFrame(
          "{\"type\":\"error\",\"message\":\"to_user_id must be positive\"}");
      conn->send(frame);
      return;
    }
    scene = "single";
  } else if (type == "chatroom" || type == "chatroom_msg") {
    if (meta.group_id <= 0) {
      std::string frame = BuildWebSocketTextFrame(
          "{\"type\":\"error\",\"message\":\"group_id(room_id) must be positive\"}");
      conn->send(frame);
      return;
    }
    scene = "chatroom";
  } else if (type == "danmaku") {
    if (meta.video_id.empty()) {
      std::string frame = BuildWebSocketTextFrame(
          "{\"type\":\"error\",\"message\":\"video_id required for danmaku\"}");
      conn->send(frame);
      return;
    }
    // 弹幕必须在一个房间内（group_id/room_id 必须指定）
    if (meta.group_id <= 0) {
      std::string frame = BuildWebSocketTextFrame(
          "{\"type\":\"error\",\"message\":\"group_id(room_id) required for danmaku\"}");
      conn->send(frame);
      return;
    }
    scene = "danmaku";
  } else if (type == "chatroom_join" || type == "chatroom_leave") {
    if (meta.group_id <= 0) {
      std::string frame = BuildWebSocketTextFrame(
          "{\"type\":\"error\",\"message\":\"group_id(room_id) must be positive\"}");
      conn->send(frame);
      return;
    }
    int64_t room_id = meta.group_id;
    if (type == "chatroom_join") {
      AddUserToRoom(room_id, ctx.user_id);
      std::string frame = BuildWebSocketTextFrame(
          "{\"type\":\"ack\",\"op\":\"chatroom_join\"}");
      conn->send(frame);
    } else {
      RemoveUserFromRoom(room_id, ctx.user_id);
      std::string frame = BuildWebSocketTextFrame(
          "{\"type\":\"ack\",\"op\":\"chatroom_leave\"}");
      conn->send(frame);
    }
    return;
  } else {
    std::string frame = BuildWebSocketTextFrame(
        "{\"type\":\"error\",\"message\":\"unsupported type\"}");
    conn->send(frame);
    return;
  }

  if (!logic_stub_) {
    std::string frame = BuildWebSocketTextFrame(
        "{\"type\":\"error\",\"message\":\"logic not available\"}");
    conn->send(frame);
    return;
  }

  // 构造上行消息
  int64_t received_time_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::system_clock::now().time_since_epoch()).count();

  UpstreamMessageRequest req;
  req.set_from_user_id(ctx.user_id);
  req.set_scene(scene);
  req.set_client_msg_id(meta.client_msg_id);
  req.set_comet_id(comet_id_);
  req.set_conn_id(ctx.conn_id);
  req.set_received_time_ms(received_time_ms);
  if (scene == "single") {
    req.set_to_user_id(meta.to_user_id);
  } else if (scene == "chatroom") {
    req.set_group_id(meta.group_id);
  } else if (scene == "danmaku") {
    req.set_group_id(meta.group_id);
    req.set_video_id(meta.video_id);
    req.set_timeline_ms(meta.timeline_ms);
  }
  req.set_content_json(payload);

  if (use_stream_ && stream_running_) {
    // 双向流模式：通过流发送
    StreamMessage stream_msg;
    *stream_msg.mutable_upstream() = req;
    
    SendToStream(std::move(stream_msg), [conn](const StreamResponse& resp) {
      if (!conn->connected()) return;
      
      if (resp.error().code() != 0) {
        std::string frame = BuildWebSocketTextFrame(
            "{\"type\":\"error\",\"message\":\"send failed\"}");
        conn->send(frame);
        return;
      }
      
      std::string frame = BuildWebSocketTextFrame(
          "{\"type\":\"ack\",\"msg_id\":\"" + 
          resp.upstream_reply().message().msg_id() + "\"}");
      conn->send(frame);
    });
  } else {
    // 传统模式：线程池 + 同步gRPC调用
    auto* stub = logic_stub_.get();

    grpc_pool_.Submit([conn, stub, req]() {
      UpstreamMessageReply rep;
      grpc::ClientContext rpc_ctx;
      auto status = stub->SendUpstreamMessage(&rpc_ctx, req, &rep);

      if (!conn->connected()) {
        return;
      }

      if (!status.ok() || rep.error().code() != 0) {
        std::string msg = status.ok() ? rep.error().message()
                                      : status.error_message();
        LogError("SendUpstreamMessage failed: " + msg);
        std::string frame = BuildWebSocketTextFrame(
            "{\"type\":\"error\",\"message\":\"send failed\"}");
        conn->send(frame);
        return;
      }

      std::string frame = BuildWebSocketTextFrame(
          "{\"type\":\"ack\",\"msg_id\":\"" + rep.message().msg_id() + "\"}");
      conn->send(frame);
    });
  }
}

void CometServer::OnMessage(const TcpConnectionPtr& conn,
                            Buffer* buf,
                            muduo::Timestamp ts) {
  (void)ts;
  // 热路径：不打日志
  ConnContext ctx = std::any_cast<ConnContext>(conn->getContext());
  if (ctx.state == ConnContext::kHandshake) {
    HandleHandshake(conn, buf);
  } else {
    HandleWebSocketFrame(conn, buf, ctx);
  }
}

void CometServer::NotifyUserOffline(int64_t user_id, const std::string& conn_id) {
  if (!logic_stub_) return;
  auto* stub = logic_stub_.get();
  std::string comet_id = comet_id_;

  // 使用线程池替代 detached thread，避免线程泄漏
  grpc_pool_.Submit([stub, user_id, comet_id, conn_id]() {
    UserOfflineRequest req;
    req.set_user_id(user_id);
    req.set_comet_id(comet_id);
    req.set_conn_id(conn_id);
    SimpleReply rep;
    grpc::ClientContext ctx;
    auto status = stub->UserOffline(&ctx, req, &rep);
    if (!status.ok()) {
      LogError("UserOffline RPC failed for user " +
               std::to_string(user_id) + ": " +
               status.error_message());
      return;
    }
    if (rep.error().code() != 0) {
      LogError("UserOffline logic error for user " +
               std::to_string(user_id) + ": " +
               rep.error().message());
    } else {
      LogInfo("UserOffline reported for user " +
              std::to_string(user_id));
    }
  });
}

void CometServer::NotifyRoomJoin(int64_t room_id, int64_t user_id) {
  if (!logic_stub_) return;
  auto* stub = logic_stub_.get();
  std::string comet_id = comet_id_;
  grpc_pool_.Submit([stub, room_id, user_id, comet_id]() {
    RoomReportRequest req;
    req.set_room_id(room_id);
    req.set_user_id(user_id);
    req.set_comet_id(comet_id);
    SimpleReply rep;
    grpc::ClientContext ctx;
    auto status = stub->ReportRoomJoin(&ctx, req, &rep);
    if (!status.ok()) {
      LogError("ReportRoomJoin RPC failed for room " +
               std::to_string(room_id) + ": " +
               status.error_message());
      return;
    }
    if (rep.error().code() != 0) {
      LogError("ReportRoomJoin logic error for room " +
               std::to_string(room_id) + ": " +
               rep.error().message());
    } else {
      LogInfo("ReportRoomJoin ok for room " +
              std::to_string(room_id));
    }
  });
}

void CometServer::NotifyRoomLeave(int64_t room_id, int64_t user_id) {
  if (!logic_stub_) return;
  auto* stub = logic_stub_.get();
  std::string comet_id = comet_id_;
  grpc_pool_.Submit([stub, room_id, user_id, comet_id]() {
    RoomReportRequest req;
    req.set_room_id(room_id);
    req.set_user_id(user_id);
    req.set_comet_id(comet_id);
    SimpleReply rep;
    grpc::ClientContext ctx;
    auto status = stub->ReportRoomLeave(&ctx, req, &rep);
    if (!status.ok()) {
      LogError("ReportRoomLeave RPC failed for room " +
               std::to_string(room_id) + ": " +
               status.error_message());
      return;
    }
    if (rep.error().code() != 0) {
      LogError("ReportRoomLeave logic error for room " +
               std::to_string(room_id) + ": " +
               rep.error().message());
    } else {
      LogInfo("ReportRoomLeave ok for room " +
              std::to_string(room_id));
    }
  });
}

// 聊天室/弹幕推送：只推送给本 Comet 上属于该房间的用户（精确路由，非全量广播）
void CometServer::PushToRoom(const ChatMessage& msg, int64_t room_id) {
  std::vector<TcpConnectionPtr> conns;
  {
    std::lock_guard<std::mutex> lock(conns_mu_);
    auto rit = room_users_.find(room_id);
    if (rit == room_users_.end()) {
      return;
    }
    const auto& users = rit->second;
    for (int64_t uid : users) {
      auto uit = user_conns_.find(uid);
      if (uit == user_conns_.end()) continue;
      for (const auto& c : uit->second) {
        conns.push_back(c);
      }
    }
  }
  if (conns.empty()) return;

  std::string payload = msg.content_json().empty()
                            ? ("{\"msg_id\":\"" + msg.msg_id() + "\"}")
                            : msg.content_json();
  std::string frame = BuildWebSocketTextFrame(payload);
  for (const auto& c : conns) {
    if (c->connected()) {
      c->send(frame);
    }
  }
}

void CometServer::PushToAll(const ChatMessage& msg) {
  std::vector<TcpConnectionPtr> conns;
  {
    std::lock_guard<std::mutex> lock(conns_mu_);
    for (auto& kv : user_conns_) {
      for (const auto& c : kv.second) {
        conns.push_back(c);
      }
    }
  }
  if (conns.empty()) return;

  std::string payload = msg.content_json().empty()
                            ? ("{\"msg_id\":\"" + msg.msg_id() + "\"}")
                            : msg.content_json();
  std::string frame = BuildWebSocketTextFrame(payload);
  for (const auto& c : conns) {
    if (c->connected()) {
      c->send(frame);
    }
  }
}

}  // namespace sparkpush



