#include "comet_server.h"

#include <grpcpp/grpcpp.h>
#include <hiredis/hiredis.h>

#include <array>
#include <atomic>
#include <cctype>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <functional>
#include <sstream>
#include <vector>

namespace sparkpush {

namespace {

// -----------------------------------------------------------------------------
// 工具函数（仅在本文件内使用）
// -----------------------------------------------------------------------------

// 从 HTTP 请求头中提取指定 header 的值（大小写敏感，按原始文本匹配）。
// @param req: 完整 HTTP 头文本（至少包含 \r\n 分隔的 header 行）
// @param header_name: 需要提取的 header 名（如 "Sec-WebSocket-Key"）
// @param value: 输出参数，返回 header 值（会做首尾空白裁剪）
// @return: 找到并成功解析返回 true，否则 false
bool ExtractHeader(const std::string &req, const std::string &header_name,
                   std::string *value) {
    if (!value) return false;
    std::string key = header_name + ":";
    auto pos = req.find(key);
    if (pos == std::string::npos) return false;
    pos += key.size();
    while (pos < req.size() && (req[pos] == ' ' || req[pos] == '\t')) {
        ++pos;
    }
    if (pos >= req.size()) return false;
    auto end = req.find("\r\n", pos);
    if (end == std::string::npos) {
        end = req.size();
    }
    size_t trimmed_end = end;
    while (trimmed_end > pos &&
           (req[trimmed_end - 1] == ' ' || req[trimmed_end - 1] == '\t' ||
            req[trimmed_end - 1] == '\r' || req[trimmed_end - 1] == '\n')) {
        --trimmed_end;
    }
    *value = req.substr(pos, trimmed_end - pos);
    return true;
}

// 裁剪字符串首尾空白（空格、制表符、CR/LF）。
std::string Trim(const std::string &s) {
    size_t start = s.find_first_not_of(" \t\r\n");
    if (start == std::string::npos) return "";
    size_t end = s.find_last_not_of(" \t\r\n");
    return s.substr(start, end - start + 1);
}

// 转成小写（ASCII），用于做不区分大小写的 header 前缀判断。
std::string ToLower(std::string s) {
    for (auto &c : s) {
        c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    }
    return s;
}

// Base64 编码（标准表，不带换行），用于 WebSocket 握手的 Accept 值计算。
std::string Base64Encode(const unsigned char *data, size_t len) {
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

// 32 位循环左移，SHA1 的基础运算。
inline uint32_t RotL(uint32_t value, unsigned int bits) {
    return (value << bits) | (value >> (32 - bits));
}

// SHA1 的 64 字节块压缩函数（RFC 3174 兼容实现）。
void SHA1Transform(uint32_t state[5], const unsigned char block[64]) {
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

// 计算输入数据的 SHA1 摘要（20 字节）。
std::array<unsigned char, 20> SHA1(const std::string &data) {
    uint32_t state[5] = {
        0x67452301u, 0xEFCDAB89u, 0x98BADCFEu, 0x10325476u, 0xC3D2E1F0u,
    };

    std::vector<unsigned char> msg(data.begin(), data.end());
    uint64_t bit_len = static_cast<uint64_t>(data.size()) * 8;

    msg.push_back(0x80);
    while ((msg.size() % 64) != 56) {
        msg.push_back(0x00);
    }
    for (int i = 7; i >= 0; --i) {
        msg.push_back(static_cast<unsigned char>((bit_len >> (8 * i)) & 0xFF));
    }

    for (size_t offset = 0; offset < msg.size(); offset += 64) {
        SHA1Transform(state, &msg[offset]);
    }

    std::array<unsigned char, 20> digest{};
    for (int i = 0; i < 5; ++i) {
        digest[i * 4] = static_cast<unsigned char>((state[i] >> 24) & 0xFF);
        digest[i * 4 + 1] = static_cast<unsigned char>((state[i] >> 16) & 0xFF);
        digest[i * 4 + 2] = static_cast<unsigned char>((state[i] >> 8) & 0xFF);
        digest[i * 4 + 3] = static_cast<unsigned char>(state[i] & 0xFF);
    }
    return digest;
}

// 计算 WebSocket 握手响应头 Sec-WebSocket-Accept：
// accept = Base64( SHA1( client_key + GUID ) )
std::string ComputeWebSocketAccept(const std::string &client_key) {
    static const std::string kGuid = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
    auto digest = SHA1(client_key + kGuid);
    return Base64Encode(digest.data(), digest.size());
}

}  // namespace

CometServer::CometServer(EventLoop *loop, const Config &cfg)
    : server_(loop, muduo::net::InetAddress(cfg.listen_port), "comet_server"),
      loop_(loop) {
    comet_id_ = cfg.comet_id;
    // 初始化 gRPC 客户端：comet -> logic（鉴权/上行消息）
    auto channel = grpc::CreateChannel(cfg.logic_grpc_target,
                                       grpc::InsecureChannelCredentials());
    logic_stub_ = sparkpush::LogicService::NewStub(channel);

    // muduo 回调：连接建立/断开、收到数据
    server_.setConnectionCallback(
        std::bind(&CometServer::OnConnection, this, std::placeholders::_1));
    server_.setMessageCallback(
        std::bind(&CometServer::OnMessage, this, std::placeholders::_1,
                  std::placeholders::_2, std::placeholders::_3));

    // 初始化 Redis 连接池
    RedisConfig rcfg;
    rcfg.host = cfg.redis_host;
    rcfg.port = cfg.redis_port;
    rcfg.password = cfg.redis_password;
    rcfg.db = cfg.redis_db;
    rcfg.pool_size = cfg.redis_pool_size;
    rcfg.min_pool_size = cfg.redis_pool_size;
    rcfg.max_pool_size = cfg.redis_max_pool_size > 0 ? cfg.redis_max_pool_size
                                                     : cfg.redis_pool_size;
    rcfg.connect_timeout_ms = cfg.redis_connect_timeout_ms;
    rcfg.rw_timeout_ms = cfg.redis_rw_timeout_ms;
    rcfg.idle_timeout_ms = cfg.redis_idle_timeout_ms;
    redis_ready_ = redis_pool_.Init(rcfg);
    if (!redis_ready_) {
        LOG_ERROR << "Redis pool init failed, online registry disabled";
    }

    // 心跳超时和 Redis TTL
    idle_timeout_ms_ = static_cast<int64_t>(cfg.comet_idle_timeout_ms);
    redis_ttl_ms_ = static_cast<int64_t>(cfg.comet_redis_ttl_ms);

    // 时间戳 + 分片惰性扫描：
    // - 连接活跃时只更新 last_active_ms（不为每次活跃创建/取消定时器）；
    // - 每次 tick 只扫描一个分片（约 O(N /
    // idle_scan_shards_)），降低抖动与锁竞争；
    // - 分片按 user_id % idle_scan_shards_ 归属，idle_scan_shard_idx_
    // 循环推进。
    if (loop_) {
        loop_->runEvery(10.0,
                        std::bind(&CometServer::CheckIdleConnections, this));
    }
}

void CometServer::SetThreadNum(int thread_num) {
    if (thread_num < 1) thread_num = 1;
    // muduo：设置 IO 线程池数量（主线程 accept，其它线程处理读写）
    server_.setThreadNum(thread_num);
}

void CometServer::Start() { server_.start(); }

void CometServer::PushToUsers(const ChatMessage &msg,
                              const std::vector<int64_t> &user_ids) {
    // 下行推送：将 protobuf ChatMessage 变为客户端可理解的 JSON 文本帧。
    std::string payload = msg.content_json().empty()
                              ? ("{\"msg_id\":\"" + msg.msg_id() + "\"}")
                              : msg.content_json();
    std::string frame = BuildWebSocketTextFrame(payload);
    std::vector<TcpConnectionPtr> conns;
    std::vector<std::pair<int64_t, TcpConnectionPtr>> stale;
    {
        std::lock_guard<std::mutex> lock(conns_mu_);   // 找到要发送的连接
        for (int64_t uid : user_ids) {
            auto it = user_conns_.find(uid);
            if (it == user_conns_.end()) continue;
            for (const auto &c : it->second) {
                if (c && c->connected()) {
                    conns.push_back(c);
                } else {
                    stale.emplace_back(uid, c);
                }
            }
            // 清理失效连接
            for (const auto &rm : stale) {
                it->second.erase(rm.second);
            }
            stale.clear();
            if (it != user_conns_.end() && it->second.empty()) {
                user_conns_.erase(it);
            }
        }
    }
    // 发送在锁外执行：避免持锁进行 I/O，降低锁竞争和回调重入风险。
    for (const auto &c : conns) {
        if (c->connected()) {
            c->send(frame);     // 真正发给web客户端
        }
    }
}

void CometServer::PushBroadcast(const ChatMessage &msg) {
    // 本机广播：遍历本进程管理的 user_conns_，向所有连接推送。
    std::string payload = msg.content_json().empty()
                              ? ("{\"msg_id\":\"" + msg.msg_id() + "\"}")
                              : msg.content_json();
    std::string frame = BuildWebSocketTextFrame(payload);
    std::vector<TcpConnectionPtr> conns;
    {
        std::lock_guard<std::mutex> lock(conns_mu_);
        for (auto &kv : user_conns_) {
            for (const auto &c : kv.second) {
                conns.push_back(c);
            }
        }
    }
    for (const auto &c : conns) {
        if (c->connected()) {
            c->send(frame);
        }
    }
}

void CometServer::OnConnection(const TcpConnectionPtr &conn) {
    if (conn->connected()) {
        // 新连接：进入握手状态，记录 last_active_ms 用于空闲扫描/心跳超时。
        ConnContext ctx;
        ctx.state = ConnContext::kHandshake;
        ctx.last_active_ms =
            std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now().time_since_epoch())
                .count();
        conn->setContext(ctx);
        LOG_INFO << "New TCP connection from "
                 << conn->peerAddress().toIpPort();
    } else {
        // 连接断开：从 user_conns_ 移除，必要时通知 logic 用户下线并清理 Redis
        // 路由。
        int64_t offline_uid = 0;
        std::string conn_id;
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
                    conn_id = ctx.conn_id;
                }
            }
        } catch (const std::bad_any_cast &) {
        }
        if (need_offline && offline_uid > 0) {
            NotifyUserOffline(offline_uid);
        }
        if (!conn_id.empty() && offline_uid > 0) {
            RemoveUserRoute(offline_uid, conn_id);
        }
        LOG_INFO << "Connection closed";
    }
}

// 生成连接唯一 ID：comet_id + 时间戳 + 递增序号
// 用途：鉴权追踪、审计、Redis 路由字段区分同一用户多连接。
std::string GenerateConnId(const std::string &comet_id) {
    static std::atomic<uint64_t> seq{1};
    uint64_t cur = seq.fetch_add(1, std::memory_order_relaxed);
    auto now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                      std::chrono::system_clock::now().time_since_epoch())
                      .count();
    std::ostringstream oss;
    oss << comet_id << "-" << now_ms << "-" << cur;
    return oss.str();
}

std::string CometServer::ParseTokenFromHandshake(const std::string &req) {
    // 握手鉴权 token 提取策略：
    // 1) 推荐：Authorization: Bearer <token>
    // 2) 兼容旧逻辑：URL query 参数 token=xxx
    // 优先从 Authorization: Bearer <token> 读取
    std::string auth;
    if (ExtractHeader(req, "Authorization", &auth)) {
        auth = Trim(auth);
        std::string lower = ToLower(auth);
        const std::string prefix = "bearer ";
        if (lower.size() > prefix.size() &&
            lower.compare(0, prefix.size(), prefix) == 0) {
            return Trim(auth.substr(prefix.size()));
        }
    }

    // 兼容旧逻辑：URL query token=xxx
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

void CometServer::HandleHandshake(const TcpConnectionPtr &conn, Buffer *buf) {
    // 解析 HTTP 头（找到 "\r\n\r\n" 作为 header
    // 结束标志），若未收齐则等待更多数据。
    const char *crlf2 = "\r\n\r\n";
    const char *data = buf->peek();
    const char *end =
        static_cast<const char *>(memmem(data, buf->readableBytes(), crlf2, 4));
    if (!end) {
        return;
    }
    size_t headerLen = end - data + 4;
    std::string req(data, headerLen);

    // 1) 提取 token 并向 logic 验证（鉴权 + 绑定 conn_id）
    std::string token = ParseTokenFromHandshake(req);
    if (token.empty()) {
        LOG_ERROR << "No token in WebSocket handshake";
        conn->shutdown();
        return;
    }

    if (!logic_stub_) {
        LOG_ERROR << "Logic stub not initialized";
        conn->shutdown();
        return;
    }
    VerifyTokenRequest vreq;
    vreq.set_token(token);
    vreq.set_comet_id(comet_id_);
    std::string conn_id = GenerateConnId(comet_id_);
    vreq.set_conn_id(conn_id);
    VerifyTokenReply vrep;
    grpc::ClientContext ctx_rpc;
    LOG_INFO << "Verifying token for conn_id=" << conn_id;
    auto status = logic_stub_->VerifyToken(&ctx_rpc, vreq, &vrep);
    LOG_INFO << "VerifyToken reply received " << vrep.user_id();
    if (!status.ok() || vrep.error().code() != 0) {
        std::string msg =
            status.ok() ? vrep.error().message() : status.error_message();
        LOG_ERROR << "VerifyToken failed: " << msg;
        conn->shutdown();
        return;
    }
    int64_t user_id = vrep.user_id();

    // 2) 计算 Sec-WebSocket-Accept 并返回 101 Switching Protocols
    std::string ws_key;
    if (!ExtractHeader(req, "Sec-WebSocket-Key", &ws_key) || ws_key.empty()) {
        LOG_ERROR << "No Sec-WebSocket-Key in WebSocket handshake";
        conn->shutdown();
        return;
    }
    std::string accept_val = ComputeWebSocketAccept(ws_key);

    std::string resp =
        "HTTP/1.1 101 Switching Protocols\r\n"
        "Upgrade: websocket\r\n"
        "Connection: Upgrade\r\n";
    resp += "Sec-WebSocket-Accept: " + accept_val + "\r\n\r\n";
    conn->send(resp);
    buf->retrieve(headerLen);

    // 3) 切换连接状态为 Open，并写回上下文
    ConnContext ctx = std::any_cast<ConnContext>(conn->getContext());
    ctx.state = ConnContext::kOpen;
    ctx.user_id = user_id;
    ctx.conn_id = conn_id;
    ctx.last_active_ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch())
            .count();
    conn->setContext(ctx);

    {
        // 4) 建立 user_id -> connection 的映射（可能一人多端，多连接用 set 存）
        std::lock_guard<std::mutex> lock(conns_mu_);
        user_conns_[user_id].insert(conn);
    }
    // 5) 将在线路由注册到 Redis（供 job/logic 按 comet_id 路由下行）
    RegisterUserOnline(user_id, conn_id);
    LOG_INFO << "WebSocket handshake done, user_id=" << user_id
             << " conn_id=" << conn_id;
}

void CometServer::HandleWebSocketFrame(const TcpConnectionPtr &conn,
                                       Buffer *buf, ConnContext &ctx) {
    // RFC6455 帧解析（简化实现）：
    // - 仅接受 FIN=1 的单帧消息，不支持分片（FIN=0 的 continuation）。
    // - 客户端发来的帧必须 masked（协议规定客户端->服务端必须掩码）。
    while (buf->readableBytes() >= 2) {
        const unsigned char *data =
            reinterpret_cast<const unsigned char *>(buf->peek());
        bool fin = (data[0] & 0x80) != 0;
        unsigned char opcode = data[0] & 0x0F;
        bool masked = (data[1] & 0x80) != 0;
        uint64_t payloadLen = data[1] & 0x7F;
        size_t headerLen = 2;
        if (!fin || !masked) {
            LOG_WARN << "Close connection from user " << ctx.user_id
                     << " conn_id=" << ctx.conn_id << " due to invalid frame";
            conn->shutdown();
            return;
        }
        // 处理扩展长度：126 => 16bit，127 => 64bit（网络字节序）
        if (payloadLen == 126) {
            if (buf->readableBytes() < headerLen + 2) return;
            const unsigned char *p = data + headerLen;
            payloadLen = (p[0] << 8) | p[1];
            headerLen += 2;
        } else if (payloadLen == 127) {
            if (buf->readableBytes() < headerLen + 8) return;
            payloadLen = 0;
            const unsigned char *p = data + headerLen;
            for (int i = 0; i < 8; ++i) {
                payloadLen = (payloadLen << 8) | p[i];
            }
            headerLen += 8;
        }
        // 控制帧（close/ping/pong）负载最大 125 字节
        if ((opcode == 0x9 || opcode == 0xA) && payloadLen > 125) {
            LOG_WARN << "Close connection from user " << ctx.user_id
                     << " conn_id=" << ctx.conn_id
                     << " due to control frame payload too large";
            // 控制帧负载不能超过 125 字节
            conn->shutdown();
            return;
        }
        if (buf->readableBytes() < headerLen + 4 + payloadLen) {
            return;
        }
        const unsigned char *mask = data + headerLen;
        headerLen += 4;
        std::string payload;
        payload.resize(payloadLen);
        const unsigned char *payloadData = data + headerLen;
        // mask 解码：payload[i] = data[i] XOR mask[i % 4]
        for (uint64_t i = 0; i < payloadLen; ++i) {
            payload[i] = static_cast<char>(payloadData[i] ^ mask[i % 4]);
        }
        buf->retrieve(headerLen + payloadLen);

        if (opcode == 0x8) {
            LOG_WARN << "Close connection from user " << ctx.user_id
                     << " conn_id=" << ctx.conn_id;
            conn->shutdown();
            return;
        } else if (opcode == 0x9) {  // ping
            // 收到 ping：立即回 pong（携带相同 payload），并视作一次活跃（刷新
            // TTL）
            std::string frame;
            frame.reserve(2 + payloadLen);
            frame.push_back(static_cast<char>(0x8A));  // FIN + PONG
            frame.push_back(static_cast<char>(payloadLen));
            frame.append(payload);
            conn->send(frame);
            ctx.last_active_ms =
                std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::system_clock::now().time_since_epoch())
                    .count();
            LOG_INFO << "Ping from user " << ctx.user_id
                     << " conn_id=" << ctx.conn_id;
            RefreshUserTtl(ctx.user_id, ctx.conn_id);
            conn->setContext(ctx);
        } else if (opcode == 0xA) {  // pong
            // 忽略，视为保活
            ctx.last_active_ms =
                std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::system_clock::now().time_since_epoch())
                    .count();
            LOG_INFO << "Pong from user " << ctx.user_id
                     << " conn_id=" << ctx.conn_id;
            RefreshUserTtl(ctx.user_id, ctx.conn_id);
            conn->setContext(ctx);
        } else if (opcode == 0x1) {
            // 文本帧：作为上行消息处理
            OnTextMessage(conn, ctx, payload);
            // OnTextMessage 内部已负责刷新 ctx 并回写
            // setContext()，这里无需重复设置。
        }
    }
}

void CometServer::OnTextMessage(const TcpConnectionPtr &conn, ConnContext &ctx,
                                const std::string &payload) {
    // 任意业务消息都视为活跃：刷新 last_active 和 Redis TTL
    ctx.last_active_ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch())
            .count();
    RefreshUserTtl(ctx.user_id, ctx.conn_id);
    LOG_INFO << "Recv text from user " << ctx.user_id << ": " << payload;
    conn->setContext(ctx);

    UpstreamMessageMeta meta; //解析json
    if (!ParseUpstreamMessage(payload, &meta)) {
        // 客户端消息格式错误：返回 error frame，不断开（便于客户端纠错）
        std::string frame = BuildWebSocketTextFrame(
            "{\"type\":\"error\",\"code\":400,\"message\":"
            "\"invalid message format\"}");
        conn->send(frame);
        return;
    }
    // 简单校验目标类型和 ID 
    if (meta.target_type != "single_chat" || meta.target_id <= 0) {
        // 当前 comet 只支持单聊上行（room/广播等可按需要扩展）
        std::string frame = BuildWebSocketTextFrame(
            "{\"type\":\"error\",\"code\":400,\"message\":"
            "\"unsupported target_type or target_id\"}");
        conn->send(frame);
        return;
    }

    if (!logic_stub_) {
        std::string frame = BuildWebSocketTextFrame(
            "{\"type\":\"error\",\"message\":\"logic not available\"}");
        conn->send(frame);
        return;
    }

    // 通过 gRPC 将上行消息转交给 logic（统一做落库/路由/幂等/风控等）
    UpstreamMessageRequest req;
    req.set_from_user_id(ctx.user_id);
    req.set_client_msg_id(meta.client_msg_id);
    req.set_content_json(payload);
    req.set_comet_id(comet_id_);
    req.set_conn_id(ctx.conn_id);
    req.set_received_time_ms(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch())
            .count());
    req.set_msg_type(meta.msg_type);
    req.set_target_type(meta.target_type);
    req.set_target_id(meta.target_id);

    UpstreamMessageReply rep;
    grpc::ClientContext rpc_ctx;
    auto status =
        logic_stub_->SendUpstreamMessage(&rpc_ctx, req, &rep);  //真正 发送
    if (!status.ok() || rep.error().code() != 0) {
        std::string msg =
            status.ok() ? rep.error().message() : status.error_message();
        LOG_ERROR << "SendUpstreamMessage failed: " << msg;
        std::string frame = BuildWebSocketTextFrame(
            "{\"type\":\"error\",\"code\":500,\"message\":\"send failed\"}");
        conn->send(frame);
        return;
    }

    // 发送 ack：带回服务端生成的 msg_id，客户端可用于对账/去重/状态机推进
    std::string frame = BuildWebSocketTextFrame(
        "{\"type\":\"ack\",\"msg_id\":\"" + rep.message().msg_id() +
        "\",\"client_msg_id\":\"" + rep.message().client_msg_id() + "\"}");
    conn->send(frame);
}

void CometServer::OnMessage(const TcpConnectionPtr &conn, Buffer *buf,
                            muduo::Timestamp ts) {
    (void)ts;
    LOG_DEBUG << "OnMessage called, bytes=" << buf->readableBytes();
    ConnContext ctx = std::any_cast<ConnContext>(conn->getContext());
    if (ctx.state == ConnContext::kHandshake) {
        // 握手阶段：尝试完成 HTTP->WebSocket 升级
        HandleHandshake(conn, buf);
    } else {
        // 已升级：解析 WebSocket 帧
        HandleWebSocketFrame(conn, buf, ctx);
    }
}

void CometServer::NotifyUserOffline(int64_t user_id) {
    if (!logic_stub_) return;
    auto *stub = logic_stub_.get();
    std::string comet_id = comet_id_;
    std::string conn_id;
    {
        // 取一个连接 ID 用于日志/路由清理（如有）
        std::lock_guard<std::mutex> lock(conns_mu_);
        auto it = user_conns_.find(user_id);
        if (it != user_conns_.end() && !it->second.empty()) {
            try {
                auto ctx = std::any_cast<ConnContext>(
                    (*it->second.begin())->getContext());
                conn_id = ctx.conn_id;
            } catch (...) {
            }
        }
    }

    // 下线通知可能较慢（RPC），用 detached thread 避免阻塞 muduo IO 线程。
    std::thread([stub, user_id, comet_id, conn_id]() {
        UserOfflineRequest req;
        req.set_user_id(user_id);
        req.set_comet_id(comet_id);
        if (!conn_id.empty()) req.set_conn_id(conn_id);
        SimpleReply rep;
        grpc::ClientContext ctx;
        auto status = stub->UserOffline(&ctx, req, &rep);
        if (!status.ok()) {
            LOG_ERROR << "UserOffline RPC failed for user " << user_id << ": "
                      << status.error_message();
            return;
        }
        if (rep.error().code() != 0) {
            LOG_ERROR << "UserOffline logic error for user " << user_id << ": "
                      << rep.error().message();
        } else {
            LOG_INFO << "UserOffline reported for user " << user_id;
        }
    }).detach();
}

void CometServer::RegisterUserOnline(int64_t user_id,
                                     const std::string &conn_id) {
    if (!redis_ready_ || user_id <= 0 || conn_id.empty()) return;
    auto guard = redis_pool_.Acquire();
    redisContext *ctx = guard.get();
    if (!ctx) {
        LOG_ERROR << "Redis Acquire failed for RegisterUserOnline";
        return;
    }
    // Redis 路由结构（示例）：
    // key:   user_connections:<user_id>
    // field: <comet_id>:<conn_id>
    // value: {}（预留扩展）
    std::string key = "user_connections:" + std::to_string(user_id);
    std::string field = comet_id_ + ":" + conn_id;
    redisReply *reply = (redisReply *)redisCommand(ctx, "HSET %s %s {}",
                                                   key.c_str(), field.c_str());
    if (reply) freeReplyObject(reply);
    // 设置 TTL：用于自动剔除异常断线/进程崩溃未清理的脏数据
    reply = (redisReply *)redisCommand(ctx, "PEXPIRE %s %lld", key.c_str(),
                                       static_cast<long long>(redis_ttl_ms_));
    if (reply) freeReplyObject(reply);
}

void CometServer::RefreshUserTtl(int64_t user_id, const std::string &conn_id) {
    if (!redis_ready_ || user_id <= 0 || conn_id.empty()) return;
    // 轻量续期：用非阻塞/快速路径获取连接（Acquire(0)）
    auto guard = redis_pool_.Acquire(0);
    redisContext *ctx = guard.get();
    if (!ctx) return;
    std::string key = "user_connections:" + std::to_string(user_id);
    redisReply *reply =
        (redisReply *)redisCommand(ctx, "PEXPIRE %s %lld", key.c_str(),
                                   static_cast<long long>(redis_ttl_ms_));
    if (reply) freeReplyObject(reply);
}

void CometServer::RemoveUserRoute(int64_t user_id, const std::string &conn_id) {
    if (!redis_ready_ || user_id <= 0 || conn_id.empty()) return;
    auto guard = redis_pool_.Acquire();
    redisContext *ctx = guard.get();
    if (!ctx) return;
    // 主动清理该连接的路由字段（连接断开时调用）
    std::string key = "user_connections:" + std::to_string(user_id);
    std::string field = comet_id_ + ":" + conn_id;
    redisReply *reply = (redisReply *)redisCommand(ctx, "HDEL %s %s",
                                                   key.c_str(), field.c_str());
    if (reply) freeReplyObject(reply);
}

void CometServer::CheckIdleConnections() {
    // 获取当前时间戳（毫秒）。
    // 用于与连接上下文中的 last_active_ms 做差，判断连接是否“长时间无活动”。
    int64_t now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                         std::chrono::system_clock::now().time_since_epoch())
                         .count();

    // 待关闭的连接列表：
    // 这里先“收集”需要关闭的连接，再在锁外真正执行 shutdown()。
    // 目的：
    // - 避免在持有 conns_mu_ 时做 I/O/回调等潜在耗时操作，减少锁竞争；
    // - 避免 shutdown() 过程中触发回调（如连接关闭回调）再次访问 user_conns_
    // 导致死锁。
    std::vector<TcpConnectionPtr> to_close;
    {
        // 保护 user_conns_（用户 -> 连接列表）的共享访问。
        std::lock_guard<std::mutex> lock(conns_mu_);

        // 分片惰性扫描：本次只扫描一个分片，避免全量 O(N) 扫描造成抖动。
        // 分片归属：user_id % idle_scan_shards_ == idle_scan_shard_idx_
        for (auto &kv : user_conns_) {
            const int64_t user_id = kv.first;
            if (idle_scan_shards_ > 1) {
                int shard = static_cast<int>(user_id % idle_scan_shards_);
                if (shard < 0) shard += idle_scan_shards_;
                if (shard != idle_scan_shard_idx_) continue;
            }
            for (auto &c : kv.second) {
                try {
                    // 从连接对象里取出我们保存的上下文（ConnContext），里面记录了连接状态与活跃时间。
                    // 注意：getContext() 返回的是
                    // std::any，类型不匹配会抛异常。
                    auto ctx = std::any_cast<ConnContext>(c->getContext());

                    // 只处理已完成握手/已打开的连接；处于半开、关闭中等状态的连接跳过。
                    if (ctx.state != ConnContext::kOpen) continue;

                    // 判断“空闲超时”：
                    // -
                    // ctx.last_active_ms：最后一次活动时间（例如收到客户端消息/心跳，或成功发送等时刻刷新）
                    // - idle_timeout_ms_：允许的最大空闲时长（毫秒）
                    // 若 (当前时间 - 最后活跃时间)
                    // 超过阈值，则认为连接应被关闭释放资源。
                    if (now_ms - ctx.last_active_ms > idle_timeout_ms_) {
                        to_close.push_back(c);
                    }
                } catch (...) {
                    // 兜底：上下文缺失或类型不匹配等情况，不影响整体扫描。
                    // 这里选择吞掉异常继续处理其他连接，避免定时扫描线程被异常打断。
                }
            }
        }

        // 循环推进分片游标（只在持锁区内更新，避免竞态）。
        if (idle_scan_shards_ > 1) {
            idle_scan_shard_idx_ =
                (idle_scan_shard_idx_ + 1) % idle_scan_shards_;
        }
    }

    // 在锁外逐个关闭连接（见上方注释：避免长时间持锁/潜在回调死锁）。
    for (auto &c : to_close) {
        // connected() 再确认一次连接当前仍处于连接状态，防止重复关闭或竞态。
        if (c->connected()) {
            LOG_WARN << "Close idle connection from user "
                     << c->peerAddress().toIpPort();
            // 触发优雅关闭：通常会发送 FIN，并在底层完成后回收资源。
            c->shutdown();
        }
    }
}

}  // namespace sparkpush
