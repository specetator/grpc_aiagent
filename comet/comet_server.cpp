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
//
// 关键设计点：
// 1) channel_ 由 CreateKeepaliveChannel 创建，带 TCP keepalive 与 HTTP2 ping，
//    能在 NAT/防火墙静默丢包时主动发现死连接，而不是等 Write 失败才知道。
// 2) 所有超时/数量类配置都做了「<=0 则用默认值」的兜底，配置缺失
//    不会让服务进入 0 超时的忙等。
// 3) 这里只注册 muduo 回调，不启动任何线程；真正启动在 Start()。
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

// 析构顺序至关重要（逆序释放依赖）：
//   1. shutting_down_ 置位，让所有在飞回调快速返回
//   2. 停 metrics，避免外部再读到半死状态
//   3. stream_running_ = false + 停看门狗
//   4. 唤醒并 Cancel 每条流，join 读写线程。此时 grpc_pool_ 还活着，
//      pending 回调里若用到它仍然安全
//   5. 最后停 grpc_pool_（drain=false，不等在途 RPC，避免关闭卡死）
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

// 流内单调递增的 request_id，双重用途：
//   a) 在 pending 表里匹配 reply
//   b) 在 SendToStream 里对 stream_count_ 取模做分片，实现多流并发
uint64_t CometServer::NextRequestId() {
    return request_id_counter_.fetch_add(1, std::memory_order_relaxed);
}

// 把 pending 表整体 swap 到局部再逐个回调。
//
// 必须这样做的两个原因：
// 1) 回调里可能再调 SendToStream（比如 error 帧触发客户端重发），
//    持锁执行会自死锁；
// 2) swap 之后 pending 已空，即使回调耗时也不会阻塞读线程摘除新 reply。
//
// 统一以 503 失败，语义是「服务端暂不可用，请重试」。客户端凭
// client_msg_id 重试时会命中 Redis 幂等键，不会产生重复消息。
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

// 扫描 pending 表，把超过 deadline 的请求摘出来失败掉（由看门狗周期调用）。
//
// 为什么超时后要 CancelStream 整条流：Write 返回 true 只代表写进了 gRPC
// 本地缓冲，长时间没有 reply 说明这条流的传输层已经不健康（半开连接、
// 对端 hang 住）。继续在上面发只会让请求堆积，不如直接取消重建。
//
// 注意：回调在锁外执行，理由同 FailStreamPending。
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

// 把所有流的健康度汇总成单个 0/1 指标。
// 判定为不健康（0）的三种情况：未启用流模式、流已停、任一条流 broken。
// 运维脚本与健康检查都以它为门禁，避免「进程活着但消息发不出去」。
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

// 重建指定下标的双向流。由写线程和看门狗共用，因此全程持 reconnect_mutex 串行化。
//
// 拆除旧流的步骤必须严格按这个顺序，否则会死锁或访问已释放对象：
//   TryCancel   唤醒阻塞在 Read/Write 里的线程，否则下面 join 会永远卡住
//   WritesDone  告知对端不再写入
//   join reader 但排除自己（读线程内部触发重连时不能 join 自己）
//   Finish      回收流资源
//   FailPending 旧流上没收到 reply 的请求全部 503，交客户端重试
//
// 只重建 ClientContext 和 stream，channel_/logic_stub_ 全生命周期不变：
// 替换 stub 会让在飞工作线程持有的裸指针失效。
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

// 创建 stream_count_ 条流并为每条起写线程；创建成功的再起读线程。
//
// 容忍部分流创建失败：标记 broken 后不阻塞启动，首次发送时
// StreamWriterLoop 会自动重连。这样 Logic 还没就绪时 Comet 也能先起来。
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

// 写线程主循环。同时承担两个职责：发送队列消费 + 空闲期流健康维护。
//
// wait_ms 的取值是个容易忽略的细节：流不健康或处于退避期时用 idle_backoff
// 等待（给重连锁留时间），健康时用 200ms 短等待（保证 ExpireStreamPending
// 即使没新消息也能周期跑，不会让超时请求永久挂住）。
//
// 发送重试循环的不变量：每次 Write 前先把 callback 登记进 pending，
// Write 失败立即从 pending 摘除。顺序不能颠倒，否则读线程可能在
// 登记完成前就拿到 reply 而找不到对应项。
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

// 读线程主循环。Read 是阻塞调用，返回 false 意味着流已终止。
//
// 回调在锁外执行（先在锁内 move 出来再调），理由同 FailStreamPending。
// 找不到匹配 request_id 的 reply 不报错，因为超时路径可能已经把它摘掉了，
// 这是正常竞态而非异常。
//
// 退出时置 broken 并 notify 写线程，由写线程去做重连（只有写线程会重建流，
// 避免两个线程同时重连）。
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
// 上行请求入队。这是 Comet 唯一的背压闸门。
//
// 三层保护：
// 1) once 标志：超时路径和正常 reply 路径可能同时触发回调，
//    exchange(true) 保证业务回调只执行一次，不会给客户端发两个 ACK。
// 2) 长度 + 字节双重限额：512 条且总量不超 8 MiB。只限条数会让
//    少量巨型消息耗尽内存，只限字节又会让海量小消息卡住队头。
// 3) 超限直接回 429 而不是阻塞：调用方是 muduo IO 线程，阻塞会拖垮
//    整个节点的所有连接。
//
// scoped_lock 同时拿两把锁：入队与登记 pending 必须原子，否则读线程
// 可能在写线程登记前就处理完 reply。
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

// 启动全部子系统。顺序有讲究：
//   metrics 先起，让运维在业务就绪前就能拉到指标；
//   流与看门狗在 TcpServer 之前，保证客户端连上来时上行通道已可用。
//
// 看门狗除了处理 pending 超时，还会丢弃 send_queue 里已经过期、
// 根本来不及发的请求（回 504），防止队列堆积后发出大量陈旧消息。
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
//
// 本函数是 Comet 下推的核心，三个关键设计：
// 1) 持锁只做「拷贝连接列表」，绝不在锁内 send。send 会进 muduo
//    输出缓冲并可能触发回调，持锁执行会与 OnConnection 争锁甚至死锁。
// 2) 每条连接的实际 send 通过 runInLoop 派到它所属的 IO 线程执行，
//    遵守 muduo 「one loop per thread」约束，避免跳线程写 socket。
// 3) 慢连接保护：输出缓冲超 4 MiB 直接 forceClose。IM 场景下积压的
//    旧消息对客户端已无价值，不如断开让它重连走 SyncOffline 全量补齐。
//
// 返回值语义：成功 send 的连接数（不是用户数）。调用方据此判断
// 是否可以 MarkDelivered。
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

// request_id 幂等去重。Job 重连重放、Unary 兜底与流路径重叠时，
// 同一条消息可能多次到达 Comet；不去重会让客户端看到重复气泡。
//
// 用 set + deque 而不是带 TTL 的缓存：插入序 deque 保证淘汰的是最早
// 的 id，且不需要后台线程扫描过期。上限 10 万条，内存可控。
//
// 空 request_id 直接放行（返回 true）：兼容不带幂等键的旧调用方。
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

// 向发送者回 delivered_ack 帧。
//
// 语义边界（很容易误解）：这**不是**已读回执。它只表示 Job 已确认
// Comet 把消息交给了目标在线连接。客户端是否渲染、用户是否看到，
// 都不在这个 ACK 的保证范围内。
//
// 先持锁拷贝连接列表再锁外 send，理由同 PushToUsers。
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

// 批量上报 delivered 游标，推进 user_session_state.delivered_seq。
//
// 这个游标是 SyncOffline 的唯一依据：下次连接时只会补推 seq 大于它的
// 消息。因此必须在 send 真正成功后才上报，否则会造成消息永久丢失。
//
// 先过滤掉本机不在线的用户（uid<=0、重复、无连接），避免把没投递
// 成功的人也计入游标。
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
//
// 这里最难看懂的是「两次 weak.lock() + 状态回检」的防御结构：
//   VerifyToken 是耗时同步 RPC，被丢到 grpc_pool_ 里执行。在它返回之前，
//   客户端可能已经断开、可能已经超时被 5s 定时器关掉、进程也可能在退出。
//   因此 RPC 前后各检查一次连接存活，回到 IO 线程后再检查一次
//   state 是否仍为 kAuthenticating（防止重复处理）。
//
// 任何一个分支失败时，如果 VerifyToken 其实已经成功（路由已写入 Redis），
// 必须调 CleanupAuthenticatedRoute 把它摘掉，否则 Logic 会永远认为该用户
// 在线，向一个不存在的连接投递消息。这是路由泄漏的主要来源。
//
// 注意：buf->retrieve(length) 在提交异步任务**之前**执行，因为 Buffer
// 属于 IO 线程，不能在工作线程里操作；token/key 已拷成值捕获。
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
            // 握手成功立即补推离线消息，不等客户端开口。
            RequestOfflineSync(reply.user_id());
            // A TCP read may contain both the upgrade and first masked frame.
            // 握手报文后面可能已经黏了第一个业务帧（TCP 粘包），
            // 这里主动再解析一次 inputBuffer，否则会等到下次可读事件才处理。
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

    // 上行通道选择：优先走双向流，流不可用时退化到 Unary。
    //
    // 两条路径的 ACK 帧格式完全一致（都是 accepted_ack），客户端无感知。
    // 流路径：回调在流读线程里执行，conn->send 由 muduo 内部派到对应 IO 线程；
    // 捕获裸 conn（shared_ptr）而非 weak，因为需要在回调里直接发送。
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
    //
    // 与流路径的关键差异：Submit 带了 req.ByteSizeLong() 做字节级配额，
    // 队满时返回 false，这里就能给客户端一个明确的「upstream queue full」
    // 而不是静默丢弃。deadline 固定 8s，比流模式的 ack_timeout 略宽。
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

// 鉴权成功却发现连接已不在时的补偿路径。
//
// 分两种情况：
//   该用户在本机还有别的连接 -> 只更新代数/令牌，不能摘路由
//   本机已无任何连接          -> 走 NotifyUserOffline 摘除路由
// 没有这一步，秒断的客户端会在 Redis 里留下永久脏路由。
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

// 离线补推：握手成功后把「已落库但尚未交给连接」的消息补发给客户端。
//
// 与 RequestCursorSync 的区别：
//   本函数由服务端主动发起，跨会话遍历，依据是 delivered_seq 游标；
//   CursorSync 由客户端发 sync 帧触发，只针对单个会话的已知缺口。
//
// 可靠性关键点：逐条 PushToUsers，一旦返回 0（连接已断或输出缓冲满）
// 就立即 return，**不推进游标也不继续翻页**。这样下次重连会从同
// 一个位置重新补推，宁可重复（客户端按 msg_seq 去重）也不丢失。
//
// 翻页递归带了前进判定（next != after 或 seq 变大），防止 Logic 返回
// 相同游标时无限递归。
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

// 客户端主动缺口补齐。客户端发现本地 msg_seq 不连续时发：
//   {"type":"sync","session_id":"...","after_seq":12,"limit":100}
//
// 回帧结构：N 条消息帧 + 1 条 sync_end 帧。
// sync_end 里带 safe_seq（已完整落库的前缀），这是客户端能安全推进
// 连续游标的依据——next_seq 只代表本页最大值，中间可能还有空洞。
//
// promise/future 的用途：发送在 IO 线程里做，但 MarkDelivered 必须等
// 「哪些帧真正 send 成功了」这个结果，所以用 future 把结果从 IO 线程
// 传回工作线程。只上报成功的 seq，失败的那部分下次重连会再补。
//
// 只有全部帧都发成功才发 sync_end：部分成功时不发，避免客户端
// 误以为补齐已完成而推进游标。
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
