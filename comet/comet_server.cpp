#include "comet_server.h"

#include <grpcpp/grpcpp.h>
#include <nlohmann/json.hpp>

#include <algorithm>
#include <cstring>
#include <array>
#include <chrono>
#include <cstdint>
#include <vector>
#include <future>

#include "grpc_keepalive.h"
#include "metrics.h"
#include "message_payload.h"

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
      stream_reconnect_base_ms_(cfg.grpc_stream_reconnect_base_ms > 0
                                    ? cfg.grpc_stream_reconnect_base_ms
                                    : 200),
      stream_reconnect_max_ms_(cfg.grpc_stream_reconnect_max_ms > 0
                                   ? cfg.grpc_stream_reconnect_max_ms
                                   : 5000),
      stream_ack_timeout_ms_(cfg.grpc_stream_ack_timeout_ms > 0
                                 ? cfg.grpc_stream_ack_timeout_ms
                                 : 8000),
      logic_grpc_target_(cfg.logic_grpc_target),
      metrics_port_(cfg.metrics_port) {
    comet_id_ = cfg.comet_id;
    max_connections_ = std::max(1, cfg.comet_max_connections);
    channel_ = CreateKeepaliveChannel(cfg.logic_grpc_target, "comet");
    logic_stub_ = sparkpush::LogicService::NewStub(channel_);

    server_.setConnectionCallback(
        std::bind(&CometServer::OnConnection, this, std::placeholders::_1));
    server_.setMessageCallback(std::bind(&CometServer::OnMessage, this,
                                         std::placeholders::_1,
                                         std::placeholders::_2,
                                         std::placeholders::_3));
}

CometServer::~CometServer() {
    shutting_down_.store(true);
    metrics_server_.Stop();
    stream_running_ = false;
    if (stream_watchdog_) stream_watchdog_->Stop();
    for (auto& st : streams_) {
        if (!st) continue;
        st->send_queue_cv.notify_all();
        CancelStream(st.get());
        if (st->writer_thread.joinable()) st->writer_thread.join();
        if (st->reader_thread.joinable()) st->reader_thread.join();
    }
    grpc_pool_.Stop(false);
}

void CometServer::SetThreadNum(int thread_num) {
    if (thread_num < 1) thread_num = 1;
    server_.setThreadNum(thread_num);
}

uint64_t CometServer::NextRequestId() {
    return request_id_counter_.fetch_add(1, std::memory_order_relaxed);
}

void CometServer::FailStreamPending(StreamState* state,
                                    const std::string& message) {
    if (!state) return;
    std::unordered_map<std::string, PendingCallback> pending;
    {
        std::lock_guard<std::mutex> lock(state->pending_mutex);
        pending.swap(state->pending);
    }
    StreamResponse resp;
    resp.mutable_error()->set_code(503);
    resp.mutable_error()->set_message(message);
    for (auto& item : pending) {
        if (item.second.callback) item.second.callback(resp);
    }
}

void CometServer::ExpireStreamPending(StreamState* state) {
    if (!state || stream_ack_timeout_ms_ <= 0) return;
    const auto now = std::chrono::steady_clock::now();
    std::vector<std::function<void(const StreamResponse&)>> expired;
    {
        std::lock_guard<std::mutex> lock(state->pending_mutex);
        for (auto it = state->pending.begin(); it != state->pending.end();) {
            if (it->second.deadline <= now) {
                if (it->second.callback) {
                    expired.push_back(std::move(it->second.callback));
                }
                it = state->pending.erase(it);
            } else {
                ++it;
            }
        }
    }
    if (expired.empty()) return;
    CancelStream(state);
    StreamResponse resp;
    resp.mutable_error()->set_code(503);
    resp.mutable_error()->set_message("logic stream ack timeout");
    MetricsRegistry::Instance().Increment(
        "spark_push_comet_logic_stream_ack_timeout_total",
        static_cast<int64_t>(expired.size()));
    for (auto& cb : expired) {
        cb(resp);
    }
}

void CometServer::UpdateLogicStreamReadyMetric() {
    int ready = 1;
    if (!use_stream_ || !stream_running_ || streams_.empty()) {
        ready = 0;
    } else {
        for (const auto& state : streams_) {
            if (!state || state->broken.load()) {
                ready = 0;
                break;
            }
        }
    }
    MetricsRegistry::Instance().Set("spark_push_comet_logic_stream_ready",
                                    ready);
}

bool CometServer::ReconnectStream(int stream_idx) {
    if (stream_idx < 0 || stream_idx >= static_cast<int>(streams_.size())) {
        return false;
    }
    auto& state = streams_[stream_idx];
    if (!state) return false;
    std::lock_guard<std::mutex> reconnect_lock(state->reconnect_mutex);
    if (!stream_running_) return false;
    if (state->stream && !state->broken.load()) return true;

    CancelStream(state.get());
    if (state->stream) {
        state->stream->WritesDone();
    }
    if (state->reader_thread.joinable() &&
        state->reader_thread.get_id() != std::this_thread::get_id()) {
        state->reader_thread.join();
    }
    if (state->stream) {
        state->stream->Finish();
        state->stream.reset();
    }
    FailStreamPending(state.get(), "logic stream disconnected");

    {
        std::lock_guard<std::mutex> lock(state->context_mutex);
        if (!stream_running_) return false;
        state->ctx = std::make_shared<grpc::ClientContext>();
    }
    // Channel and stub are stable for the component lifetime; gRPC reconnects the transport.
    // Replacing a stub here would invalidate raw pointers held by in-flight worker RPCs.
    state->stream = logic_stub_->MessageStream(state->ctx.get());
    if (!state->stream) {
        state->broken = true;
        UpdateLogicStreamReadyMetric();
        LOG_ERROR << "Failed to reconnect Logic MessageStream " << stream_idx;
        return false;
    }
    state->broken = false;
    state->reader_thread =
        std::thread(&CometServer::StreamReaderLoop, this, stream_idx);
    MetricsRegistry::Instance().Increment(
        "spark_push_comet_logic_stream_reconnect_total");
    UpdateLogicStreamReadyMetric();
    LOG_INFO << "Reconnected Logic MessageStream " << stream_idx;
    return true;
}

void CometServer::InitStreams() {
    streams_.resize(stream_count_);
    for (int i = 0; i < stream_count_; ++i) {
        streams_[i] = std::make_unique<StreamState>();
        streams_[i]->ctx = std::make_shared<grpc::ClientContext>();
        streams_[i]->stream =
            logic_stub_->MessageStream(streams_[i]->ctx.get());
        if (!streams_[i]->stream) {
            LOG_ERROR << "Failed to create gRPC stream " << i
                      << ", will retry on first send";
            streams_[i]->broken = true;
        }
    }
    stream_running_ = true;
    for (int i = 0; i < stream_count_; ++i) {
        streams_[i]->writer_thread =
            std::thread(&CometServer::StreamWriterLoop, this, i);
        if (streams_[i]->stream && !streams_[i]->broken.load()) {
            streams_[i]->reader_thread =
                std::thread(&CometServer::StreamReaderLoop, this, i);
        }
    }
    UpdateLogicStreamReadyMetric();
    LOG_INFO << "gRPC bidirectional streams ready, count=" << stream_count_;
}

void CometServer::StreamWriterLoop(int stream_idx) {
    auto& state = streams_[stream_idx];
    if (!state) return;
    const int max_backoff =
        std::max(stream_reconnect_base_ms_, stream_reconnect_max_ms_);
    int idle_backoff = std::max(10, stream_reconnect_base_ms_);
    while (stream_running_) {
        ExpireStreamPending(state.get());
        PendingRequest req;
        bool have_req = false;
        {
            std::unique_lock<std::mutex> lock(state->send_queue_mutex);
            const int wait_ms =
                (state->broken.load() || !state->stream ||
                 idle_backoff > stream_reconnect_base_ms_)
                    ? idle_backoff
                    : 200;
            state->send_queue_cv.wait_for(
                lock, std::chrono::milliseconds(wait_ms), [&] {
                    return !state->send_queue.empty() || !stream_running_;
                });
            if (!stream_running_ && state->send_queue.empty()) break;
            if (!state->send_queue.empty()) {
                req = std::move(state->send_queue.front());
                state->send_queue.pop();
                state->queued_bytes -= req.msg.ByteSizeLong();
                have_req = true;
            }
        }
        if (!have_req) {
            if (stream_running_ &&
                (state->broken.load() || !state->stream)) {
                ReconnectStream(stream_idx);
                idle_backoff = std::min(max_backoff, idle_backoff * 2);
            } else {
                idle_backoff = std::max(10, stream_reconnect_base_ms_);
            }
            continue;
        }
        idle_backoff = std::max(10, stream_reconnect_base_ms_);

        bool sent = false;
        int backoff = std::max(10, stream_reconnect_base_ms_);
        for (int attempt = 0; attempt < 4 && stream_running_ &&
             std::chrono::steady_clock::now() < req.deadline; ++attempt) {
            if (state->broken.load() || !state->stream) {
                if (!ReconnectStream(stream_idx)) {
                    std::this_thread::sleep_for(
                        std::chrono::milliseconds(backoff));
                    backoff = std::min(max_backoff, backoff * 2);
                    continue;
                }
            }
            {
                std::lock_guard<std::mutex> lock(state->pending_mutex);
                if (req.callback) {
                    state->pending[req.msg.request_id()] = PendingCallback{req.callback, req.deadline};
                }
            }
            if (state->stream && state->stream->Write(req.msg)) {
                sent = true;
                break;
            }
            MetricsRegistry::Instance().Increment(
                "spark_push_comet_logic_stream_write_failed_total");
            {
                std::lock_guard<std::mutex> lock(state->pending_mutex);
                state->pending.erase(req.msg.request_id());
            }
            state->broken = true;
            UpdateLogicStreamReadyMetric();
            LOG_ERROR << "Stream " << stream_idx
                      << " write failed, reconnecting";
            std::this_thread::sleep_for(std::chrono::milliseconds(backoff));
            ReconnectStream(stream_idx);
            backoff = std::min(max_backoff, backoff * 2);
        }
        if (!sent && req.callback) {
            StreamResponse resp;
            resp.mutable_error()->set_code(503);
            resp.mutable_error()->set_message("logic stream unavailable");
            req.callback(resp);
        }
    }
}

void CometServer::StreamReaderLoop(int stream_idx) {
    auto& state = streams_[stream_idx];
    if (!state || !state->stream) return;
    StreamResponse resp;
    while (stream_running_ && state->stream && state->stream->Read(&resp)) {
        std::function<void(const StreamResponse&)> callback;
        {
            std::lock_guard<std::mutex> lock(state->pending_mutex);
            auto it = state->pending.find(resp.request_id());
            if (it != state->pending.end()) {
                callback = std::move(it->second.callback);
                state->pending.erase(it);
            }
        }
        if (callback) callback(resp);
    }
    if (stream_running_) {
        state->broken = true;
        MetricsRegistry::Instance().Increment(
            "spark_push_comet_logic_stream_read_failed_total");
        FailStreamPending(state.get(), "logic stream disconnected");
        UpdateLogicStreamReadyMetric();
        state->send_queue_cv.notify_one();
        LOG_ERROR << "Stream " << stream_idx
                  << " read failed, waiting to reconnect";
    }
}

void CometServer::CancelStream(StreamState* state) {
    std::lock_guard<std::mutex> lock(state->context_mutex);
    if (state->ctx) state->ctx->TryCancel();
}
void CometServer::SendToStream(StreamMessage msg, std::function<void(const StreamResponse&)> callback) {
    const auto id = NextRequestId();
    msg.set_request_id(std::to_string(id));
    auto once = std::make_shared<std::atomic<bool>>(false);
    auto complete = [once, callback = std::move(callback)](const StreamResponse& reply) {
        if (!once->exchange(true) && callback) callback(reply);
    };
    auto& state = streams_[id % stream_count_];
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(std::max(1, stream_ack_timeout_ms_));
    bool admitted = false;
    {
        std::scoped_lock lock(state->send_queue_mutex, state->pending_mutex);
        if (stream_running_ && state->send_queue.size() + state->pending.size() < 512 &&
            msg.ByteSizeLong() <= 8 * 1024 * 1024 - state->queued_bytes) {
            state->queued_bytes += msg.ByteSizeLong();
            state->send_queue.push({std::move(msg), complete, deadline});
            admitted = true;
        }
    }
    if (!admitted) {
        StreamResponse reply;
        reply.mutable_error()->set_code(429);
        reply.mutable_error()->set_message("upstream queue full");
        complete(reply);
        MetricsRegistry::Instance().Increment("spark_push_comet_queue_rejected_total");
    } else state->send_queue_cv.notify_one();
}

void CometServer::Start() {
    if (metrics_port_ > 0 && !metrics_server_.Start(metrics_port_)) {
        LOG_ERROR << "Comet metrics HTTP server start failed on port "
                  << metrics_port_;
    }
    grpc_pool_.Start();
    if (use_stream_) {
        InitStreams();
        stream_watchdog_ = std::make_unique<PeriodicWorker>([this] {
            for (auto& state : streams_) {
                ExpireStreamPending(state.get());
                std::vector<std::function<void(const StreamResponse&)>> expired;
                {
                    std::lock_guard<std::mutex> lock(state->send_queue_mutex);
                    while (!state->send_queue.empty() && state->send_queue.front().deadline <= std::chrono::steady_clock::now()) {
                        auto& request = state->send_queue.front();
                        state->queued_bytes -= request.msg.ByteSizeLong();
                        expired.push_back(std::move(request.callback));
                        state->send_queue.pop();
                    }
                }
                StreamResponse reply;
                reply.mutable_error()->set_code(504);
                reply.mutable_error()->set_message("queued request timed out");
                for (const auto& callback : expired) callback(reply);
            }
        }, std::chrono::milliseconds(100));
    }
    server_.start();
}

// 按用户列表推送消息；一个 user_id 可能存在多条连接（多设备/多标签页）。
// 使用局部拷贝规避锁期间的发送开销。
size_t CometServer::PushToUsers(const ChatMessage& msg, const std::vector<int64_t>& user_ids,
                               std::vector<int64_t>* delivered_users) {
    if (delivered_users) delivered_users->clear();
    auto frame = std::make_shared<std::string>(BuildWebSocketTextFrame(DownstreamMessageJson(msg)));
    std::vector<std::pair<int64_t, TcpConnectionPtr>> connections;
    {
        std::lock_guard<std::mutex> lock(conns_mu_);
        std::unordered_set<int64_t> seen;
        for (auto user : user_ids) {
            if (!seen.insert(user).second) continue;
            auto it = user_conns_.find(user);
            if (it != user_conns_.end()) for (const auto& c : it->second) connections.emplace_back(user, c);
        }
    }
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(1);
    std::vector<std::pair<int64_t, std::future<bool>>> results;
    for (const auto& item : connections) {
        auto accepted = std::make_shared<std::promise<bool>>();
        results.emplace_back(item.first, accepted->get_future());
        auto c = item.second;
        c->getLoop()->runInLoop([c, frame, accepted, deadline] {
            const bool live = c->connected() && std::chrono::steady_clock::now() < deadline;
            const bool capacity = frame->size() <= 4 * 1024 * 1024 &&
                c->outputBuffer()->readableBytes() <= 4 * 1024 * 1024 - frame->size();
            if (live && capacity) c->send(*frame);
            else if (live) {
                MetricsRegistry::Instance().Increment("spark_push_comet_slow_connection_closed_total");
                c->forceClose();
            }
            accepted->set_value(live && capacity);
        });
    }
    size_t sent = 0;
    std::unordered_set<int64_t> users;
    for (auto& item : results) {
        if (item.second.wait_until(deadline) == std::future_status::ready && item.second.get()) {
            ++sent;
            users.insert(item.first);
        }
    }
    if (delivered_users) delivered_users->assign(users.begin(), users.end());
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
        if (connection_count_.fetch_add(1) >= max_connections_) {
            conn->forceClose();
            return;
        }
        // IM/WebSocket 消息通常很小，关闭 Nagle 避免小包等待合并，
        // 降低 accepted/delivered 之后的首屏体感延迟。
        conn->setTcpNoDelay(true);
        conn->setHighWaterMarkCallback([](const TcpConnectionPtr& c, size_t) {
            MetricsRegistry::Instance().Increment("spark_push_comet_slow_connection_closed_total");
            c->forceClose();
        }, 4 * 1024 * 1024);
        ConnContext ctx;
        ctx.state = ConnContext::kHandshake;
        conn->setContext(ctx);
        std::weak_ptr<muduo::net::TcpConnection> weak = conn;
        conn->getLoop()->runAfter(5.0, [weak] {
            if (auto c = weak.lock()) {
                auto* state = std::any_cast<ConnContext>(c->getMutableContext());
                if (state && state->state != ConnContext::kOpen) c->forceClose();
            }
        });
        // 首次建立 TCP 连接，先进入握手状态等待 HTTP 升级请求
        LOG_INFO << "New TCP connection from " << conn->peerAddress().toIpPort();
    } else {
        // 断开连接，清理 user->conn 映射
        connection_count_.fetch_sub(1);
        int64_t offline_uid = 0;
        bool need_offline = false;
        int64_t offline_generation = 0;
        std::string offline_token;
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
                        offline_generation = user_route_generations_[ctx.user_id];
                        offline_token = user_route_tokens_[ctx.user_id];
                        user_route_tokens_.erase(ctx.user_id);
                        user_route_generations_.erase(ctx.user_id);
                    }
                }
            }
        } catch (const std::bad_any_cast&) {
            // 忽略
        }
        if (need_offline && offline_uid > 0) {
            NotifyUserOffline(offline_uid, offline_generation, offline_token);
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
    const char* end = static_cast<const char*>(memmem(buf->peek(), buf->readableBytes(), "\r\n\r\n", 4));
    if (!end) {
        if (buf->readableBytes() > 16 * 1024) conn->forceClose();
        return;
    }
    const size_t length = end - buf->peek() + 4;
    if (length > 16 * 1024) { conn->forceClose(); return; }
    const std::string request(buf->peek(), length);
    const auto token = ParseTokenFromHandshake(request);
    std::string key;
    if (token.empty() || !logic_stub_ || !ExtractHeader(request, "Sec-WebSocket-Key", &key) || key.empty()) {
        conn->forceClose(); return;
    }
    auto* context = std::any_cast<ConnContext>(conn->getMutableContext());
    context->state = ConnContext::kAuthenticating;
    buf->retrieve(length);
    std::weak_ptr<muduo::net::TcpConnection> weak = conn;
    if (!grpc_pool_.Submit([this, weak, token, key] {
        auto starting = weak.lock();
        if (!starting || !starting->connected() || shutting_down_.load()) return;
        VerifyTokenRequest request;
        request.set_token(token);
        request.set_comet_id(comet_id_);
        VerifyTokenReply reply;
        grpc::ClientContext context;
        context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(3));
        const auto status = logic_stub_->VerifyToken(&context, request, &reply);
        auto connection = weak.lock();
        if (!connection) {
            if (status.ok() && reply.error().code() == 0) CleanupAuthenticatedRoute(reply.user_id(), reply.route_generation(), reply.route_token());
            return;
        }
        connection->getLoop()->queueInLoop([this, connection, token, key, status, reply] {
            if (shutting_down_.load()) return;
            auto* state = std::any_cast<ConnContext>(connection->getMutableContext());
            if (!connection->connected() || !state || state->state != ConnContext::kAuthenticating) {
                if (status.ok() && reply.error().code() == 0) CleanupAuthenticatedRoute(reply.user_id(), reply.route_generation(), reply.route_token());
                return;
            }
            if (!status.ok() || reply.error().code() != 0 || reply.user_id() <= 0) {
                connection->forceClose(); return;
            }
            state->state = ConnContext::kOpen;
            state->user_id = reply.user_id();
            state->auth_token = token;
            state->route_generation = reply.route_generation();
            {
                std::lock_guard<std::mutex> lock(conns_mu_);
                user_conns_[reply.user_id()].insert(connection);
                auto& generation = user_route_generations_[reply.user_id()];
                if (reply.route_generation() >= generation) {
                    generation = reply.route_generation();
                    user_route_tokens_[reply.user_id()] = reply.route_token();
                }
            }
            connection->send("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: " + ComputeWebSocketAccept(key) + "\r\n\r\n");
            MetricsRegistry::Instance().Increment("spark_push_comet_connections_total");
            RequestOfflineSync(reply.user_id());
            // A TCP read may contain both the upgrade and first masked frame.
            HandleWebSocketFrame(connection, connection->inputBuffer(), *state);
        });
    })) conn->forceClose();
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
        if (!fin || !masked || (data[0] & 0x70) ||
            (opcode != 0x1 && opcode != 0x8 && opcode != 0x9 && opcode != 0xA) ||
            ((opcode & 0x8) && payloadLen > 125)) {
            // demo 中不支持分片帧，也不接受未 mask 的客户端数据
            conn->forceClose();
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
        constexpr uint64_t kMaxWebSocketPayloadBytes = 64 * 1024;
        if (payloadLen > kMaxWebSocketPayloadBytes) {
            conn->forceClose();
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

        if (opcode == 0x9) {
            conn->send(std::string(1, static_cast<char>(0x8A)) +
                       std::string(1, static_cast<char>(payload.size())) + payload);
        } else if (opcode == 0x8) {  // close
            conn->send(std::string(1, static_cast<char>(0x88)) +
                       std::string(1, static_cast<char>(payload.size())) + payload);
            conn->shutdown();
            conn->forceCloseWithDelay(1.0);
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
    req.set_auth_token(ctx.auth_token);
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
    const auto deadline = std::chrono::system_clock::now() + std::chrono::seconds(8);
    if (!grpc_pool_.Submit([conn, stub, req, deadline]() {
        UpstreamMessageReply rep;
        grpc::ClientContext rpc_ctx;
        rpc_ctx.set_deadline(deadline);
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
    }, req.ByteSizeLong())) {
        conn->send(BuildWebSocketTextFrame(nlohmann::json{{"type", "error"},
            {"message", "upstream queue full"}, {"client_msg_id", req.client_msg_id()}}.dump()));
    }
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
    } else if (ctx.state == ConnContext::kOpen) {
        // 已升级为 WebSocket，按帧协议处理
        HandleWebSocketFrame(conn, buf, ctx);
    } else if (buf->readableBytes() > 64 * 1024) {
        conn->forceClose();
    }
}

// 当本机用户连接计数变为 0 时，异步通知 logic 执行 UserOffline。
void CometServer::NotifyUserOffline(int64_t user_id, int64_t generation, const std::string& token) {
    if (!logic_stub_) return;
    auto* stub = logic_stub_.get();
    std::string comet_id = comet_id_;

    // 使用有界工作池；组件析构等待这些 RPC，stub 在此期间保持有效。
    grpc_pool_.Submit([stub, user_id, comet_id, generation, token]() {
        UserOfflineRequest req;
        req.set_user_id(user_id);
        req.set_comet_id(comet_id);
        req.set_route_generation(generation);
        req.set_route_token(token);
        SimpleReply rep;
        grpc::ClientContext ctx;
        ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(3));
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
    });
}

void CometServer::CleanupAuthenticatedRoute(int64_t user, int64_t generation, const std::string& token) {
    {
        std::lock_guard<std::mutex> lock(conns_mu_);
        if (user_conns_.count(user)) {
            auto& current = user_route_generations_[user];
            if (generation >= current) { current = generation; user_route_tokens_[user] = token; }
            return;
        }
    }
    NotifyUserOffline(user, generation, token);
}

void CometServer::RequestOfflineSync(int64_t user_id, const std::string& after_session, int64_t after_seq) {
    if (!logic_stub_ || user_id <= 0) return;
    auto* stub = logic_stub_.get();
    grpc_pool_.Submit([this, stub, user_id, after_session, after_seq]() {
        {
            std::lock_guard<std::mutex> lock(conns_mu_);
            if (!user_conns_.count(user_id)) return;
        }
        SyncOfflineRequest request;
        request.set_user_id(user_id);
        request.set_limit(200);
        request.set_after_session_id(after_session);
        request.set_after_seq(after_seq);
        SyncOfflineReply reply;
        grpc::ClientContext context;
        context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(3));
        const auto status = stub->SyncOffline(&context, request, &reply);
        if (!status.ok() || reply.error().code() != 0) {
            MetricsRegistry::Instance().Increment("spark_push_offline_sync_failed_total");
            return;
        }
        MarkDeliveredRequest mark;
        mark.set_user_id(user_id);
        for (const auto& message : reply.messages()) {
            if (PushToUsers(message, {user_id}) == 0) return;
            auto* cursor = mark.add_cursors();
            cursor->set_session_id(message.session_id());
            cursor->set_msg_seq(message.msg_seq());
            MetricsRegistry::Instance().Increment("spark_push_offline_repush_messages_total");
        }
        if (!mark.cursors().empty()) {
            SimpleReply mark_reply;
            grpc::ClientContext mark_context;
            mark_context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(3));
            if (!stub->MarkDelivered(&mark_context, mark, &mark_reply).ok() || mark_reply.error().code() != 0)
                MetricsRegistry::Instance().Increment("spark_push_offline_cursor_update_failed_total");
        }
        if (reply.has_more() && !reply.next_session_id().empty() &&
            (reply.next_session_id() != after_session || reply.next_seq() > after_seq))
            RequestOfflineSync(user_id, reply.next_session_id(), reply.next_seq());
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
        auto frames = std::make_shared<std::vector<std::pair<int64_t, std::string>>>();
        int64_t last_seq = after_seq;
        for (const auto& message : reply.messages()) {
            last_seq = std::max(last_seq, message.msg_seq());
            frames->emplace_back(message.msg_seq(), BuildWebSocketTextFrame(DownstreamMessageJson(message)));
        }
        nlohmann::json end = {{"type", "sync_end"}, {"session_id", session_id},
            {"after_seq", after_seq}, {"next_seq", last_seq},
            {"has_more", reply.has_more()}, {"safe_seq", reply.safe_seq()}};
        const auto end_frame = BuildWebSocketTextFrame(end.dump());
        auto accepted = std::make_shared<std::promise<std::vector<int64_t>>>();
        auto result = accepted->get_future();
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(1);
        conn->getLoop()->runInLoop([conn, frames, end_frame, accepted, deadline] {
            std::vector<int64_t> sequences;
            auto send = [&](const std::string& frame) {
                if (!conn->connected() || std::chrono::steady_clock::now() >= deadline) return false;
                constexpr size_t capacity = 4 * 1024 * 1024;
                if (frame.size() > capacity || conn->outputBuffer()->readableBytes() > capacity - frame.size()) {
                    MetricsRegistry::Instance().Increment("spark_push_comet_slow_connection_closed_total");
                    conn->forceClose();
                    return false;
                }
                conn->send(frame);
                return true;
            };
            for (const auto& frame : *frames) {
                if (!send(frame.second)) break;
                sequences.push_back(frame.first);
            }
            if (sequences.size() == frames->size()) send(end_frame);
            accepted->set_value(std::move(sequences));
        });
        if (result.wait_until(deadline) != std::future_status::ready) return;
        const auto sequences = result.get();
        if (!sequences.empty()) {
            MarkDeliveredRequest mark;
            mark.set_user_id(user_id);
            for (const auto sequence : sequences) {
                auto* cursor = mark.add_cursors();
                cursor->set_session_id(session_id);
                cursor->set_msg_seq(sequence);
            }
            SimpleReply mark_reply;
            grpc::ClientContext mark_context;
            mark_context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(3));
            if (!stub->MarkDelivered(&mark_context, mark, &mark_reply).ok() || mark_reply.error().code() != 0)
                MetricsRegistry::Instance().Increment("spark_push_offline_cursor_update_failed_total");
        }
        MetricsRegistry::Instance().Increment("spark_push_cursor_sync_success_total");
    });
}

// 上报房间加入事件，logic 侧维护 room->comet 路由与在线人数。
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
        ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(3));
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
    });
}

// 上报房间离开事件，释放路由计数。
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
        ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(3));
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
    });
}

// 将消息推送给本机 room_id 的所有在线用户（不跨节点）。
size_t CometServer::PushToRoom(const ChatMessage& msg, int64_t room_id) {
    return PushToUsers(msg, GetRoomUserIds(room_id));
}
size_t CometServer::PushToAll(const ChatMessage& msg) {
    std::vector<int64_t> users;
    {
        std::lock_guard<std::mutex> lock(conns_mu_);
        for (const auto& item : user_conns_) users.push_back(item.first);
    }
    return PushToUsers(msg, users);
}

}  // namespace sparkpush
