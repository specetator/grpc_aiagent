#include "comet_server.h"

#include <grpcpp/grpcpp.h>
#include <nlohmann/json.hpp>

#include <algorithm>
#include <cstring>
#include <array>
#include <cstdint>
#include <vector>

#include "metrics.h"

namespace sparkpush {

namespace {

// 从 HTTP 请求中提取指定 Header 的值（简单实现，按行搜索）
// 仅用于握手阶段，不做大小写兼容和多值处理。
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
// 计算标准 Base64 编码，满足 WebSocket Accept 计算需求。
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

// 左循环移位，供 SHA1 轮函数使用。
inline uint32_t RotL(uint32_t value, unsigned int bits) {
    return (value << bits) | (value >> (32 - bits));
}

// 单块 SHA1 变换
// 单块 SHA1 变换，按 FIPS PUB 180-1 实现。
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
// 计算字符串 SHA1 摘要，返回 20 字节数组。
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
// 计算 Sec-WebSocket-Accept，拼接 GUID 后做 SHA1+Base64。
std::string ComputeWebSocketAccept(const std::string& client_key) {
    static const std::string kGuid =
            "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
    auto digest = SHA1(client_key + kGuid);
    return Base64Encode(digest.data(), digest.size());
}

}  // namespace

// 构造：TcpServer + Logic gRPC stub；可选双向流模式（对齐 06）
CometServer::CometServer(EventLoop* loop, const Config& cfg)
    : server_(loop, muduo::net::InetAddress(cfg.listen_port), "comet_server"),
      grpc_pool_(cfg.comet_grpc_pool_size > 0 ? cfg.comet_grpc_pool_size : 4,
                 "comet_grpc_pool"),
      use_stream_(cfg.use_grpc_stream),
      stream_count_(cfg.grpc_stream_count > 0 ? cfg.grpc_stream_count : 4),
      metrics_port_(cfg.metrics_port) {
    comet_id_ = cfg.comet_id;
    channel_ = grpc::CreateChannel(cfg.logic_grpc_target,
                                   grpc::InsecureChannelCredentials());
    logic_stub_ = sparkpush::LogicService::NewStub(channel_);

    server_.setConnectionCallback(
        std::bind(&CometServer::OnConnection, this, std::placeholders::_1));
    server_.setMessageCallback(std::bind(&CometServer::OnMessage, this,
                                         std::placeholders::_1,
                                         std::placeholders::_2,
                                         std::placeholders::_3));
}

CometServer::~CometServer() {
    metrics_server_.Stop();
    stream_running_ = false;
    for (auto& st : streams_) {
        if (!st) continue;
        st->send_queue_cv.notify_all();
        if (st->ctx) st->ctx->TryCancel();
        if (st->writer_thread.joinable()) st->writer_thread.join();
        if (st->reader_thread.joinable()) st->reader_thread.join();
    }
    grpc_pool_.Stop();
}

void CometServer::SetThreadNum(int thread_num) {
    if (thread_num < 1) thread_num = 1;
    server_.setThreadNum(thread_num);
}

uint64_t CometServer::NextRequestId() {
    return request_id_counter_.fetch_add(1, std::memory_order_relaxed);
}

void CometServer::InitStreams() {
    streams_.resize(stream_count_);
    for (int i = 0; i < stream_count_; ++i) {
        streams_[i] = std::make_unique<StreamState>();
        streams_[i]->ctx = std::make_unique<grpc::ClientContext>();
        streams_[i]->stream =
            logic_stub_->MessageStream(streams_[i]->ctx.get());
        if (!streams_[i]->stream) {
            LOG_ERROR << "Failed to create gRPC stream " << i;
            use_stream_ = false;
            return;
        }
    }
    stream_running_ = true;
    for (int i = 0; i < stream_count_; ++i) {
        streams_[i]->writer_thread =
            std::thread(&CometServer::StreamWriterLoop, this, i);
        streams_[i]->reader_thread =
            std::thread(&CometServer::StreamReaderLoop, this, i);
    }
    LOG_INFO << "gRPC bidirectional streams ready, count=" << stream_count_;
}

void CometServer::StreamWriterLoop(int stream_idx) {
    auto& state = streams_[stream_idx];
    if (!state || !state->stream) return;
    while (stream_running_) {
        PendingRequest req;
        {
            std::unique_lock<std::mutex> lock(state->send_queue_mutex);
            state->send_queue_cv.wait(lock, [&] {
                return !state->send_queue.empty() || !stream_running_;
            });
            if (!stream_running_) break;
            if (state->send_queue.empty()) continue;
            req = std::move(state->send_queue.front());
            state->send_queue.pop();
        }
        if (req.callback) {
            std::lock_guard<std::mutex> lock(callbacks_mutex_);
            pending_callbacks_[req.msg.request_id()] = std::move(req.callback);
        }
        if (!state->stream->Write(req.msg)) {
            LOG_ERROR << "Stream " << stream_idx << " write failed";
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
        if (callback) callback(resp);
    }
}

void CometServer::SendToStream(
    StreamMessage msg, std::function<void(const StreamResponse&)> callback) {
    uint64_t req_id = NextRequestId();
    msg.set_request_id(std::to_string(req_id));
    int stream_idx =
        static_cast<int>(req_id % static_cast<uint64_t>(stream_count_));
    auto& state = streams_[stream_idx];
    {
        std::lock_guard<std::mutex> lock(state->send_queue_mutex);
        state->send_queue.push({std::move(msg), std::move(callback)});
    }
    state->send_queue_cv.notify_one();
}

void CometServer::Start() {
    if (metrics_port_ > 0 && !metrics_server_.Start(metrics_port_)) {
        LOG_ERROR << "Comet metrics HTTP server start failed on port "
                  << metrics_port_;
    }
    grpc_pool_.Start();
    if (use_stream_) {
        InitStreams();
    }
    server_.start();
}

// 按用户列表推送消息；一个 user_id 可能存在多条连接（多设备/多标签页）。
// 使用局部拷贝规避锁期间的发送开销。
size_t CometServer::PushToUsers(const ChatMessage& msg,
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
    size_t sent = 0;
    for (const auto& c : conns) {
        if (c->connected()) {
            // muduo 的 send 本身是线程安全的，会在内部切回所属 EventLoop
            c->send(frame);
            ++sent;
        }
    }
    return sent;
}

bool CometServer::AcceptPushRequest(const std::string& request_id) {
    if (request_id.empty()) return true;
    std::lock_guard<std::mutex> lock(conns_mu_);
    if (recent_push_ids_.find(request_id) != recent_push_ids_.end()) {
        MetricsRegistry::Instance().Increment(
            "spark_push_delivery_duplicate_total");
        return false;
    }
    recent_push_ids_.insert(request_id);
    recent_push_order_.push_back(request_id);
    constexpr size_t kRecentPushLimit = 100000;
    while (recent_push_order_.size() > kRecentPushLimit) {
        recent_push_ids_.erase(recent_push_order_.front());
        recent_push_order_.pop_front();
    }
    return true;
}

void CometServer::PushDeliveryAck(int64_t user_id, const ChatMessage& msg) {
    if (user_id <= 0) return;
    nlohmann::json ack;
    ack["type"] = "delivered_ack";
    ack["ack_stage"] = "delivered";
    ack["msg_id"] = msg.msg_id();
    ack["client_msg_id"] = msg.client_msg_id();
    ack["session_id"] = msg.session_id();
    ack["msg_seq"] = msg.msg_seq();
    ack["delivered_at_ms"] = std::chrono::duration_cast<std::chrono::milliseconds>(
                                  std::chrono::system_clock::now().time_since_epoch())
                                  .count();
    const std::string frame = BuildWebSocketTextFrame(ack.dump());
    std::vector<TcpConnectionPtr> conns;
    {
        std::lock_guard<std::mutex> lock(conns_mu_);
        auto it = user_conns_.find(user_id);
        if (it != user_conns_.end()) {
            for (const auto& conn : it->second) conns.push_back(conn);
        }
    }
    for (const auto& conn : conns) {
        if (conn->connected()) conn->send(frame);
    }
}

void CometServer::ReportDeliveredToUsers(
    const ChatMessage& msg, const std::vector<int64_t>& user_ids) {
    if (!logic_stub_ || msg.session_id().empty() || msg.msg_seq() <= 0 ||
        user_ids.empty()) {
        return;
    }
    std::vector<int64_t> online_users;
    {
        std::lock_guard<std::mutex> lock(conns_mu_);
        std::unordered_set<int64_t> seen;
        for (int64_t uid : user_ids) {
            if (uid <= 0 || !seen.insert(uid).second) continue;
            auto it = user_conns_.find(uid);
            if (it != user_conns_.end() && !it->second.empty()) {
                online_users.push_back(uid);
            }
        }
    }
    if (online_users.empty()) return;

    // 一条推送可能命中多个在线用户。MarkDelivered 已支持批量游标，
    // 每个用户单独发 RPC 会把群聊 fan-out 放大成 N 次数据库往返，
    // 这里合并成一次调用，降低 Job/Comet/Logic 的线程和连接压力。
    MarkDeliveredRequest request;
    for (int64_t uid : online_users) {
        auto* cursor = request.add_user_cursors();
        cursor->set_user_id(uid);
        cursor->set_session_id(msg.session_id());
        cursor->set_msg_seq(msg.msg_seq());
    }
    auto* stub = logic_stub_.get();
    grpc_pool_.Submit([stub, request = std::move(request)]() {
        SimpleReply reply;
        grpc::ClientContext context;
        context.set_deadline(std::chrono::system_clock::now() +
                             std::chrono::seconds(3));
        const auto status = stub->MarkDelivered(&context, request, &reply);
        if (!status.ok() || reply.error().code() != 0) {
            MetricsRegistry::Instance().Increment(
                "spark_push_delivery_cursor_update_failed_total");
        } else {
            MetricsRegistry::Instance().Increment(
                "spark_push_delivery_cursor_update_total");
        }
    });
}

std::vector<int64_t> CometServer::GetRoomUserIds(int64_t room_id) const {
    std::vector<int64_t> user_ids;
    if (room_id <= 0) return user_ids;
    std::lock_guard<std::mutex> lock(conns_mu_);
    auto it = room_users_.find(room_id);
    if (it == room_users_.end()) return user_ids;
    user_ids.assign(it->second.begin(), it->second.end());
    return user_ids;
}

// 连接生命周期回调：建立时初始化上下文，关闭时清理映射并尝试上报离线。
void CometServer::OnConnection(const TcpConnectionPtr& conn) {
    if (conn->connected()) {
        // IM/WebSocket 消息通常很小，关闭 Nagle 避免小包等待合并，
        // 降低 accepted/delivered 之后的首屏体感延迟。
        conn->setTcpNoDelay(true);
        ConnContext ctx;
        ctx.state = ConnContext::kHandshake;
        conn->setContext(ctx);
        // 首次建立 TCP 连接，先进入握手状态等待 HTTP 升级请求
        LOG_INFO << "New TCP connection from " << conn->peerAddress().toIpPort();
    } else {
        // 断开连接，清理 user->conn 映射
        int64_t offline_uid = 0;
        bool need_offline = false;
        try {
            auto ctx = std::any_cast<ConnContext>(conn->getContext());
            if (ctx.user_id > 0) {
                std::lock_guard<std::mutex> lock(conns_mu_);
                auto it = user_conns_.find(ctx.user_id);
                if (it != user_conns_.end()) {
                    it->second.erase(conn);
                    if (it->second.empty()) {
                        user_conns_.erase(it);
                        offline_uid = ctx.user_id;
                        need_offline = true;
                    }
                }
            }
        } catch (const std::bad_any_cast&) {
            // 忽略
        }
        if (need_offline && offline_uid > 0) {
            NotifyUserOffline(offline_uid);
        }
        LOG_INFO << "Connection closed";
    }
}

// 解析握手 HTTP 请求行中的 query 参数，提取 token=xxx。
// 不解析请求体，也不做 URL 解码，前端需确保明文附带 token。
std::string CometServer::ParseTokenFromHandshake(const std::string& req) {
    // 通过解析 GET 请求行中的 query 参数提取 token
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

// WebSocket 升级处理：
// 1) 读取完整 HTTP 头，提取 token 与 Sec-WebSocket-Key。
// 2) 通过 logic.VerifyToken 校验用户身份，绑定 user_id。
// 3) 生成 Sec-WebSocket-Accept 返回 101 切换协议响应，进入开放状态。
void CometServer::HandleHandshake(const TcpConnectionPtr& conn, Buffer* buf) {
    // 按 HTTP 报文格式解析握手，确保拿到完整头部
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
        LOG_ERROR << "No token in WebSocket handshake";
        conn->shutdown();
        return;
    }

    // 调用 logic 的 VerifyToken 做鉴权
    if (!logic_stub_) {
        LOG_ERROR << "Logic stub not initialized";
        conn->shutdown();
        return;
    }
    VerifyTokenRequest vreq;
    vreq.set_token(token);
    vreq.set_comet_id(comet_id_);
    VerifyTokenReply vrep;
    grpc::ClientContext ctx_rpc;
    // token 属于凭证，日志中只记录鉴权阶段，不输出原文。
    LOG_INFO << "Verifying WebSocket token for comet_id=" << comet_id_;
    auto status = logic_stub_->VerifyToken(&ctx_rpc, vreq, &vrep);
    LOG_INFO << "VerifyToken reply received " << vrep.user_id();
    if (!status.ok() || vrep.error().code() != 0) {
        // 鉴权失败直接断开，避免继续占用连接
        std::string msg = status.ok() ? vrep.error().message()
                                                                    : status.error_message();
        LOG_ERROR << "VerifyToken failed: " << msg;
        conn->shutdown();
        return;
    }
    int64_t user_id = vrep.user_id();

    // 解析客户端 Sec-WebSocket-Key，并计算符合 RFC6455 的 Sec-WebSocket-Accept
    std::string ws_key;
    if (!ExtractHeader(req, "Sec-WebSocket-Key", &ws_key) ||
            ws_key.empty()) {
        LOG_ERROR << "No Sec-WebSocket-Key in WebSocket handshake";
        conn->shutdown();
        return;
    }
    std::string accept_val = ComputeWebSocketAccept(ws_key);

    // 标准 WebSocket 握手响应
    std::string resp =
            "HTTP/1.1 101 Switching Protocols\r\n"
            "Upgrade: websocket\r\n"
            "Connection: Upgrade\r\n";
    resp += "Sec-WebSocket-Accept: " + accept_val + "\r\n\r\n";
    conn->send(resp);
    buf->retrieve(headerLen);

    ConnContext ctx = std::any_cast<ConnContext>(conn->getContext());
    ctx.state = ConnContext::kOpen;
    ctx.user_id = user_id;
    conn->setContext(ctx);

    {
        std::lock_guard<std::mutex> lock(conns_mu_);
        user_conns_[user_id].insert(conn);
    }
    MetricsRegistry::Instance().Increment("spark_push_comet_connections_total");
    // 新连接先补推 delivered_seq 之后的消息；失败时客户端仍可用 sync
    // 控制帧按自己的 msg_seq 游标补偿。
    RequestOfflineSync(user_id);
    // 握手成功：记录用户连接并等待后续 WebSocket 帧
    LOG_INFO << "WebSocket handshake done, user_id=" << user_id;
}

// WebSocket 帧处理：
// - 仅支持 FIN=1 的文本帧，拒绝分片与未 mask 帧（客户端必须 mask）。
// - 解析 payload 长度、掩码后解密负载，并分发到 OnTextMessage。
void CometServer::HandleWebSocketFrame(const TcpConnectionPtr& conn,
                                                                              Buffer* buf,
                                                                              ConnContext& ctx) {
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
        constexpr uint64_t kMaxWebSocketPayloadBytes = 4 * 1024 * 1024;
        if (payloadLen > kMaxWebSocketPayloadBytes) {
            conn->shutdown();
            return;
        }
        if (buf->readableBytes() < headerLen + 4 + payloadLen) {
            return;  // 数据还不完整
        }
        const unsigned char* mask = data + headerLen;
        headerLen += 4;
        std::string payload;
        payload.resize(payloadLen);
        const unsigned char* payloadData = data + headerLen;
        // WebSocket 规范要求客户端数据必须 mask，这里按掩码逐字节解码
        for (uint64_t i = 0; i < payloadLen; ++i) {
            payload[i] =
                    static_cast<char>(payloadData[i] ^ mask[i % 4]);
        }
        buf->retrieve(headerLen + payloadLen);

        if (opcode == 0x8) {  // close
            conn->shutdown();
            return;
        } else if (opcode == 0x1) {  // text
            OnTextMessage(conn, ctx, payload);
        }
    }
}

// 将用户加入本机的房间成员表，并在首次加入时通知 logic 侧路由。
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

// 将用户从本机房间成员表中移除；当成员数为 0 时删除房间记录并上报离开。
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

// 处理客户端文本上行：
// - 解析 type / to_user_id / group_id / client_msg_id。
// - single_chat/chatroom 转发到 logic，获得 message.msg_id 后返回 ACK。
// - chatroom_join/leave 维护本地房间表并 ACK。
void CometServer::OnTextMessage(const TcpConnectionPtr& conn,
                                                                ConnContext& ctx,
                                                                const std::string& payload) {
    // 客户端检测到 msg_seq 缺口时发送：
    // {"type":"sync","session_id":"...","after_seq":12,"limit":100}
    // 服务端按游标返回历史消息，客户端据 msg_seq 去重并推进连续游标。
    try {
        const auto control = nlohmann::json::parse(payload);
        if (control.value("type", "") == "sync") {
            const std::string session_id =
                control.value("session_id", std::string{});
            const int64_t after_seq = control.value("after_seq", 0LL);
            const int limit = control.value("limit", 100);
            if (session_id.empty() || after_seq < 0) {
                conn->send(BuildWebSocketTextFrame(
                    "{\"type\":\"error\",\"message\":\"invalid sync cursor\"}"));
            } else {
                RequestCursorSync(conn, ctx.user_id, session_id, after_seq,
                                  std::max(1, std::min(limit, 1000)));
            }
            return;
        }
    } catch (const nlohmann::json::exception&) {
        // 继续走轻量上行解析，统一返回 invalid message。
    }

    // 热路径：不打印每条消息内容（避免日志拖垮 QPS）
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
    } else if (type == "chatroom") {
        if (meta.group_id <= 0) {
            std::string frame = BuildWebSocketTextFrame(
                    "{\"type\":\"error\",\"message\":\"group_id(room_id) must be positive\"}");
            conn->send(frame);
            return;
        }
        scene = "chatroom";
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
        // 理论上不会发生，防御性返回
        std::string frame = BuildWebSocketTextFrame(
                "{\"type\":\"error\",\"message\":\"logic not available\"}");
        conn->send(frame);
        return;
    }

    UpstreamMessageRequest req;
    req.set_from_user_id(ctx.user_id);
    req.set_scene(scene);
    req.set_client_msg_id(meta.client_msg_id);
    req.set_source_comet_id(comet_id_);
    if (scene == "single") {
        req.set_to_user_id(meta.to_user_id);
    } else if (scene == "chatroom") {
        req.set_group_id(meta.group_id);
    }
    req.set_content_json(payload);

    if (use_stream_ && stream_running_) {
        StreamMessage stream_msg;
        *stream_msg.mutable_upstream() = req;
        const std::string client_msg_id = req.client_msg_id();
        SendToStream(std::move(stream_msg), [conn, client_msg_id](const StreamResponse& resp) {
            if (!conn->connected()) return;
            if (resp.error().code() != 0) {
                nlohmann::json error = {
                    {"type", "error"}, {"message", "send failed"},
                    {"client_msg_id", client_msg_id}};
                conn->send(BuildWebSocketTextFrame(error.dump()));
                return;
            }
            const auto& reply = resp.upstream_reply();
            nlohmann::json ack;
            ack["type"] = "accepted_ack";
            ack["ack_stage"] = "accepted";
            ack["msg_id"] = reply.message().msg_id();
            ack["client_msg_id"] = reply.message().client_msg_id();
            ack["session_id"] = reply.message().session_id();
            ack["msg_seq"] = reply.message().msg_seq();
            ack["accepted_at_ms"] = reply.accepted_at_ms();
            conn->send(BuildWebSocketTextFrame(ack.dump()));
        });
        return;
    }

    // 传统 Unary：丢线程池，避免堵死 muduo IO 线程
    auto* stub = logic_stub_.get();
    grpc_pool_.Submit([conn, stub, req]() {
        UpstreamMessageReply rep;
        grpc::ClientContext rpc_ctx;
        auto status = stub->SendUpstreamMessage(&rpc_ctx, req, &rep);
        if (!conn->connected()) return;
        if (!status.ok() || rep.error().code() != 0) {
            nlohmann::json error = {
                {"type", "error"}, {"message", "send failed"},
                {"client_msg_id", req.client_msg_id()}};
            conn->send(BuildWebSocketTextFrame(error.dump()));
            return;
        }
        nlohmann::json ack;
        ack["type"] = "accepted_ack";
        ack["ack_stage"] = "accepted";
        ack["msg_id"] = rep.message().msg_id();
        ack["client_msg_id"] = rep.message().client_msg_id();
        ack["session_id"] = rep.message().session_id();
        ack["msg_seq"] = rep.message().msg_seq();
        ack["accepted_at_ms"] = rep.accepted_at_ms();
        conn->send(BuildWebSocketTextFrame(ack.dump()));
    });
}

// muduo 数据到达回调：握手阶段解析 HTTP，握手完成后解析 WebSocket 帧。
void CometServer::OnMessage(const TcpConnectionPtr& conn,
                                                        Buffer* buf,
                                                        muduo::Timestamp ts) {
    (void)ts;
    ConnContext ctx = std::any_cast<ConnContext>(conn->getContext());
    if (ctx.state == ConnContext::kHandshake) {
        // 首次阶段处理 HTTP 升级握手
        HandleHandshake(conn, buf);
    } else {
        // 已升级为 WebSocket，按帧协议处理
        HandleWebSocketFrame(conn, buf, ctx);
    }
}

// 当本机用户连接计数变为 0 时，异步通知 logic 执行 UserOffline。
void CometServer::NotifyUserOffline(int64_t user_id) {
    if (!logic_stub_) return;
    auto* stub = logic_stub_.get();
    std::string comet_id = comet_id_;

    // 简单起一个线程，避免在 muduo 事件循环线程里阻塞 gRPC 调用。
    std::thread([stub, user_id, comet_id]() {
        UserOfflineRequest req;
        req.set_user_id(user_id);
        req.set_comet_id(comet_id);
        SimpleReply rep;
        grpc::ClientContext ctx;
        auto status = stub->UserOffline(&ctx, req, &rep);
        if (!status.ok()) {
            LOG_ERROR << "UserOffline RPC failed for user " << user_id
                      << ": " << status.error_message();
            return;
        }
        if (rep.error().code() != 0) {
            LOG_ERROR << "UserOffline logic error for user " << user_id
                      << ": " << rep.error().message();
        } else {
            LOG_INFO << "UserOffline reported for user " << user_id;
        }
    }).detach();
}

void CometServer::RequestOfflineSync(int64_t user_id) {
    if (!logic_stub_ || user_id <= 0) return;
    auto* stub = logic_stub_.get();
    grpc_pool_.Submit([this, stub, user_id]() {
        SyncOfflineRequest request;
        request.set_user_id(user_id);
        request.set_limit(200);
        SyncOfflineReply reply;
        grpc::ClientContext context;
        context.set_deadline(std::chrono::system_clock::now() +
                             std::chrono::seconds(3));
        const auto status = stub->SyncOffline(&context, request, &reply);
        if (!status.ok() || reply.error().code() != 0) {
            MetricsRegistry::Instance().Increment(
                "spark_push_offline_sync_failed_total");
            return;
        }

        std::unordered_map<std::string, int64_t> cursors;
        size_t successful_messages = 0;
        for (const auto& message : reply.messages()) {
            if (PushToUsers(message, std::vector<int64_t>{user_id}) > 0) {
                cursors[message.session_id()] = std::max(
                    cursors[message.session_id()], message.msg_seq());
                ++successful_messages;
                MetricsRegistry::Instance().Increment(
                    "spark_push_offline_repush_messages_total");
            }
        }
        if (successful_messages == 0) return;

        MarkDeliveredRequest mark;
        mark.set_user_id(user_id);
        for (const auto& item : cursors) {
            auto* cursor = mark.add_cursors();
            cursor->set_session_id(item.first);
            cursor->set_msg_seq(item.second);
        }
        SimpleReply mark_reply;
        grpc::ClientContext mark_context;
        mark_context.set_deadline(std::chrono::system_clock::now() +
                                  std::chrono::seconds(3));
        const auto mark_status =
            stub->MarkDelivered(&mark_context, mark, &mark_reply);
        if (!mark_status.ok() || mark_reply.error().code() != 0) {
            MetricsRegistry::Instance().Increment(
                "spark_push_offline_cursor_update_failed_total");
        }
    });
}

void CometServer::RequestCursorSync(const TcpConnectionPtr& conn,
                                    int64_t user_id,
                                    const std::string& session_id,
                                    int64_t after_seq, int limit) {
    if (!logic_stub_ || !conn || user_id <= 0) return;
    auto* stub = logic_stub_.get();
    grpc_pool_.Submit([this, conn, stub, user_id, session_id, after_seq,
                       limit]() {
        SyncMessagesRequest request;
        request.set_user_id(user_id);
        request.set_session_id(session_id);
        request.set_after_seq(after_seq);
        request.set_limit(limit);
        SyncMessagesReply reply;
        grpc::ClientContext context;
        context.set_deadline(std::chrono::system_clock::now() +
                             std::chrono::seconds(3));
        const auto status = stub->SyncMessages(&context, request, &reply);
        if (!conn->connected()) return;
        if (!status.ok() || reply.error().code() != 0) {
            conn->send(BuildWebSocketTextFrame(
                "{\"type\":\"error\",\"message\":\"sync failed\"}"));
            return;
        }
        int64_t last_seq = after_seq;
        for (const auto& message : reply.messages()) {
            if (!conn->connected()) return;
            last_seq = std::max(last_seq, message.msg_seq());
            conn->send(BuildWebSocketTextFrame(message.content_json()));
        }
        nlohmann::json end;
        end["type"] = "sync_end";
        end["session_id"] = session_id;
        end["after_seq"] = after_seq;
        end["next_seq"] = last_seq;
        end["has_more"] = reply.has_more();
        conn->send(BuildWebSocketTextFrame(end.dump()));

        if (last_seq > after_seq) {
            MarkDeliveredRequest mark;
            mark.set_user_id(user_id);
            auto* cursor = mark.add_cursors();
            cursor->set_session_id(session_id);
            cursor->set_msg_seq(last_seq);
            SimpleReply mark_reply;
            grpc::ClientContext mark_context;
            mark_context.set_deadline(std::chrono::system_clock::now() +
                                      std::chrono::seconds(3));
            stub->MarkDelivered(&mark_context, mark, &mark_reply);
        }
        MetricsRegistry::Instance().Increment("spark_push_cursor_sync_success_total");
    });
}

// 上报房间加入事件，logic 侧维护 room->comet 路由与在线人数。
void CometServer::NotifyRoomJoin(int64_t room_id, int64_t user_id) {
    if (!logic_stub_) return;
    auto* stub = logic_stub_.get();
    std::string comet_id = comet_id_;
    std::thread([stub, room_id, user_id, comet_id]() {
        RoomReportRequest req;
        req.set_room_id(room_id);
        req.set_user_id(user_id);
        req.set_comet_id(comet_id);
        SimpleReply rep;
        grpc::ClientContext ctx;
        auto status = stub->ReportRoomJoin(&ctx, req, &rep);
        if (!status.ok()) {
            LOG_ERROR << "ReportRoomJoin RPC failed for room " << room_id
                      << ": " << status.error_message();
            return;
        }
        if (rep.error().code() != 0) {
            LOG_ERROR << "ReportRoomJoin logic error for room " << room_id
                      << ": " << rep.error().message();
        } else {
            LOG_INFO << "ReportRoomJoin ok for room " << room_id;
        }
    }).detach();
}

// 上报房间离开事件，释放路由计数。
void CometServer::NotifyRoomLeave(int64_t room_id, int64_t user_id) {
    if (!logic_stub_) return;
    auto* stub = logic_stub_.get();
    std::string comet_id = comet_id_;
    std::thread([stub, room_id, user_id, comet_id]() {
        RoomReportRequest req;
        req.set_room_id(room_id);
        req.set_user_id(user_id);
        req.set_comet_id(comet_id);
        SimpleReply rep;
        grpc::ClientContext ctx;
        auto status = stub->ReportRoomLeave(&ctx, req, &rep);
        if (!status.ok()) {
            LOG_ERROR << "ReportRoomLeave RPC failed for room " << room_id
                      << ": " << status.error_message();
            return;
        }
        if (rep.error().code() != 0) {
            LOG_ERROR << "ReportRoomLeave logic error for room " << room_id
                      << ": " << rep.error().message();
        } else {
            LOG_INFO << "ReportRoomLeave ok for room " << room_id;
        }
    }).detach();
}

// 将消息推送给本机 room_id 的所有在线用户（不跨节点）。
size_t CometServer::PushToRoom(const ChatMessage& msg, int64_t room_id) {
    std::vector<TcpConnectionPtr> conns;
    {
        std::lock_guard<std::mutex> lock(conns_mu_);
        auto rit = room_users_.find(room_id);
        if (rit == room_users_.end()) {
            return 0;
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
    if (conns.empty()) return 0;

    std::string payload = msg.content_json().empty()
                                                        ? ("{\"msg_id\":\"" + msg.msg_id() + "\"}")
                                                        : msg.content_json();
    std::string frame = BuildWebSocketTextFrame(payload);
    size_t sent = 0;
    for (const auto& c : conns) {
        if (c->connected()) {
            c->send(frame);
            ++sent;
        }
    }
    return sent;
}

// 广播给本机所有在线连接，仅作用于当前 comet。
size_t CometServer::PushToAll(const ChatMessage& msg) {
    std::vector<TcpConnectionPtr> conns;
    {
        std::lock_guard<std::mutex> lock(conns_mu_);
        for (auto& kv : user_conns_) {
            for (const auto& c : kv.second) {
                conns.push_back(c);
            }
        }
    }
    if (conns.empty()) return 0;

    std::string payload = msg.content_json().empty()
                                                        ? ("{\"msg_id\":\"" + msg.msg_id() + "\"}")
                                                        : msg.content_json();
    std::string frame = BuildWebSocketTextFrame(payload);
    size_t sent = 0;
    for (const auto& c : conns) {
        if (c->connected()) {
            c->send(frame);
            ++sent;
        }
    }
    return sent;
}

}  // namespace sparkpush
