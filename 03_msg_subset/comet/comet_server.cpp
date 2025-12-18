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
#include <unordered_set>
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

// ============================================================================
// 构造函数：CometServer
//
// 功能：
// 1. 初始化 muduo TcpServer（监听 WebSocket 连接）
// 2. 创建 gRPC 客户端连接到 logic 服务（用于鉴权和上行消息转发）
// 3. 初始化 Redis 连接池（用于在线路由注册）
// 4. 设置定时器（定期扫描空闲连接）
//
// 参数：
// @param loop: muduo 事件循环指针（主线程的事件循环）
// @param cfg: 配置对象，包含监听端口、线程数、Redis 配置等
// ============================================================================
CometServer::CometServer(EventLoop *loop, const Config &cfg)
    : server_(loop, muduo::net::InetAddress(cfg.listen_port), "comet_server"),
      loop_(loop) {
    // 记录当前 comet 节点的唯一标识（用于在线路由注册）
    comet_id_ = cfg.comet_id;

    // ========================================================================
    // 步骤 1: 初始化 gRPC 客户端（comet -> logic）
    // ========================================================================
    // 用途：
    // - 握手时调用 logic.VerifyToken 验证客户端 token
    // - 上行消息时调用 logic.SendUpstreamMessage 转发消息
    // - 用户下线时调用 logic.UserOffline 通知状态变更
    auto channel = grpc::CreateChannel(cfg.logic_grpc_target,
                                       grpc::InsecureChannelCredentials());
    logic_stub_ = sparkpush::LogicService::NewStub(channel);

    // ========================================================================
    // 步骤 2: 设置 muduo 网络库回调函数
    // ========================================================================
    // 连接回调：当新连接建立或连接断开时调用
    // - 新连接：初始化 ConnContext，状态设为握手阶段
    // - 断开：从 user_conns_ 移除映射，必要时通知 logic 用户下线
    server_.setConnectionCallback(
        std::bind(&CometServer::OnConnection, this, std::placeholders::_1));

    // 消息回调：当收到客户端数据时调用
    // - 握手阶段：处理 HTTP -> WebSocket 升级
    // - 已建立阶段：解析 WebSocket 帧，处理业务消息
    server_.setMessageCallback(
        std::bind(&CometServer::OnMessage, this, std::placeholders::_1,
                  std::placeholders::_2, std::placeholders::_3));

    // ========================================================================
    // 步骤 3: 初始化 Redis 连接池
    // ========================================================================
    // 用途：
    // - 注册在线路由：user_connections:<user_id> -> {comet_id:conn_id}
    // - 续期 TTL：心跳时刷新路由信息的过期时间
    // - 清理路由：连接断开时删除对应的路由字段
    RedisConfig rcfg;
    rcfg.host = cfg.redis_host;          // Redis 服务器地址
    rcfg.port = cfg.redis_port;          // Redis 服务器端口
    rcfg.password = cfg.redis_password;  // Redis 密码（可选）
    rcfg.db = cfg.redis_db;              // Redis 数据库编号（默认 0）
    rcfg.pool_size = cfg.redis_pool_size;      // 初始连接池大小
    rcfg.min_pool_size = cfg.redis_pool_size;  // 最小连接池大小
    rcfg.max_pool_size = cfg.redis_max_pool_size > 0
                             ? cfg.redis_max_pool_size
                             : cfg.redis_pool_size;  // 最大连接池大小
    rcfg.connect_timeout_ms = cfg.redis_connect_timeout_ms;  // 连接超时（毫秒）
    rcfg.rw_timeout_ms = cfg.redis_rw_timeout_ms;  // 读写超时（毫秒）
    rcfg.idle_timeout_ms = cfg.redis_idle_timeout_ms;  // 空闲超时（毫秒）

    // 初始化连接池，如果失败则禁用在线路由注册功能
    redis_ready_ = redis_pool_.Init(rcfg);
    if (!redis_ready_) {
        LOG_ERROR << "Redis pool init failed, online registry disabled";
    }

    // ========================================================================
    // 步骤 4: 设置超时参数
    // ========================================================================
    // idle_timeout_ms_: 心跳超时时间（毫秒）
    // - 连接超过该时长未活跃，会被定时器扫描并关闭
    // - 客户端需定期发送 ping/pong 或业务消息保持活跃
    idle_timeout_ms_ = static_cast<int64_t>(cfg.comet_idle_timeout_ms);

    // redis_ttl_ms_: Redis 路由信息的 TTL（毫秒）
    // - 用于自动剔除异常断线/进程崩溃未清理的脏数据
    // - 应该略大于 idle_timeout_ms_，确保正常连接不会被误清理
    redis_ttl_ms_ = static_cast<int64_t>(cfg.comet_redis_ttl_ms);

    // ========================================================================
    // 步骤 5: 启动定时器，定期扫描空闲连接
    // ========================================================================
    // 时间戳 + 分片惰性扫描策略：
    // - 连接活跃时只更新 last_active_ms（不为每次活跃创建/取消定时器）
    // - 每次 tick 只扫描一个分片（约 O(N /
    // idle_scan_shards_)），降低抖动与锁竞争
    // - 分片按 user_id % idle_scan_shards_ 归属，idle_scan_shard_idx_ 循环推进
    // - 定时器间隔 10 秒（runEvery），适合大部分场景
    if (loop_) {
        loop_->runEvery(10.0,  // 每 10 秒执行一次
                        std::bind(&CometServer::CheckIdleConnections, this));
    }
}

// ============================================================================
// 函数：SetThreadNum
// 功能：设置 IO 线程池的线程数量
//
// 参数：
// @param thread_num: 线程数量（必须 >= 1）
//
// 说明：
// - muduo 网络库的线程模型：
//   * 主线程（EventLoop 线程）：负责 accept 新连接
//   * IO 线程池：负责处理已建立连接的读写、业务逻辑
// - 线程数量选择建议：
//   * CPU 密集型：设置为 CPU 核心数
//   * IO 密集型：设置为 CPU 核心数 * 2
//   * 默认值：通常设置为 4-8
// - 必须在 Start() 之前调用
// ============================================================================
void CometServer::SetThreadNum(int thread_num) {
    // 参数校验：至少需要 1 个 IO 线程
    if (thread_num < 1) thread_num = 1;

    // 设置 muduo TcpServer 的 IO 线程池大小
    // 注意：主线程不包含在 thread_num 中
    server_.setThreadNum(thread_num);
}

// ============================================================================
// 函数：Start
// 功能：启动 WebSocket 服务器，开始监听端口和接受连接
//
// 说明：
// - 调用后，服务器开始监听配置的端口（cfg.listen_port）
// - 当客户端连接到来时，触发 OnConnection 回调
// - 当收到客户端数据时，触发 OnMessage 回调
// - 本方法立即返回（非阻塞），实际监听在 loop.loop() 中进行
// ============================================================================
void CometServer::Start() {
    // 启动 muduo TcpServer
    // 内部会创建监听 socket，绑定端口，开始 listen
    server_.start();
}

// ============================================================================
// 函数：PushToUsers
// 功能：向指定用户列表推送消息（精确推送）
//
// 参数：
// @param msg: 消息内容（protobuf ChatMessage）
// @param user_ids: 目标用户 ID 列表（可能包含重复，内部会去重）
//
// 实现流程：
// 1. 将 protobuf 消息转换为 JSON 格式的 WebSocket 文本帧
// 2. 遍历 user_ids，查找 user_conns_ 映射表，收集所有在线连接
// 3. 清理失效连接（已断开但未及时清理的）
// 4. 在锁外逐个发送，避免持锁进行 I/O 操作
//
// 设计说明：
// - 只推送给本机管理的连接，不跨 comet 节点
// - 一个用户可能有多个连接（多端登录），会全部推送
// - 用户不在线时静默忽略，不返回错误
// ============================================================================
void CometServer::PushToUsers(const ChatMessage &msg,
                              const std::vector<int64_t> &user_ids) {
    // ========================================================================
    // 步骤 1: 构造 WebSocket 文本帧
    // ========================================================================
    // 将 protobuf ChatMessage 转换为客户端可理解的 JSON 格式
    // - 如果 content_json 非空：直接使用（已经是 JSON）
    // - 如果 content_json 为空：构造最小 JSON（只包含 msg_id）
    std::string payload = msg.content_json().empty()
                              ? ("{\"msg_id\":\"" + msg.msg_id() + "\"}")
                              : msg.content_json();

    // 按照 RFC 6455 规范构造 WebSocket 文本帧（包含帧头和负载）
    std::string frame = BuildWebSocketTextFrame(payload);

    // ========================================================================
    // 步骤 2: 查找目标连接（需要加锁访问 user_conns_）
    // ========================================================================
    std::vector<TcpConnectionPtr> conns;  // 待发送的连接列表
    std::vector<std::pair<int64_t, TcpConnectionPtr>> stale;  // 失效连接列表

    {
        // 加锁保护 user_conns_（多 IO 线程可能并发访问）
        std::lock_guard<std::mutex> lock(conns_mu_);

        // 目标用户去重：避免上游 targets / user_ids 含重复导致同连接发多次
        std::unordered_set<int64_t> uniq_uids;
        uniq_uids.reserve(user_ids.size());

        // 连接去重：同一个 TcpConnectionPtr 不应被重复加入发送列表
        std::unordered_set<const void *> uniq_conns;
        uniq_conns.reserve(user_ids.size());

        // 遍历所有目标用户 ID
        for (int64_t uid : user_ids) {
            if (!uniq_uids.insert(uid).second) continue;

            // 查找该用户的所有连接
            auto it = user_conns_.find(uid);  //比如找到用户userid=1的set
            if (it == user_conns_.end()) continue;  // 用户不在线，跳过

            // 遍历该用户的所有连接（多端登录,一个端也可以多个连接）
            for (const auto &c : it->second) {
                // 检查连接是否仍然有效
                if (c && c->connected()) {
                    // 有效连接：加入待发送列表
                    const void *key = static_cast<const void *>(c.get());
                    if (uniq_conns.insert(key).second) {
                        conns.push_back(c);
                    }
                } else {
                    // 失效连接：标记为待清理（已断开但未及时移除）
                    stale.emplace_back(uid, c);
                }
            }

            // 清理失效连接（从 user_conns_ 中移除）
            for (const auto &rm : stale) {
                it->second.erase(rm.second);
            }
            stale.clear();

            // 如果该用户的所有连接都已清理，删除整个映射项
            if (it != user_conns_.end() && it->second.empty()) {
                user_conns_.erase(it);
            }
        }
    }
    // 锁已释放，下面在锁外执行 I/O 操作

    // ========================================================================
    // 步骤 3: 发送消息（在锁外执行，避免持锁进行 I/O）
    // ========================================================================
    // 为什么在锁外发送：
    // - send() 可能阻塞或触发回调，持锁会导致死锁或性能问题
    // - 在锁外发送可以降低锁竞争，提高并发性能
    for (const auto &c : conns) {
        // 再次检查连接状态（可能在锁释放后断开）
        if (c->connected()) {
            // 通过 muduo TcpConnection 发送 WebSocket 帧
            // 内部会将数据写入发送缓冲区，由 IO 线程异步发送
            c->send(frame);  // 真正发给 web 客户端
        }
    }
}

// ============================================================================
// 函数：PushBroadcast
// 功能：向本机所有在线用户广播消息
//
// 参数：
// @param msg: 消息内容（protobuf ChatMessage）
//
// 实现流程：
// 1. 将 protobuf 消息转换为 JSON 格式的 WebSocket 文本帧
// 2. 遍历 user_conns_，收集本机所有在线连接
// 3. 在锁外逐个发送，避免持锁进行 I/O 操作
//
// 使用场景：
// - 系统公告：向所有在线用户推送通知
// - 运维操作：维护通知、版本更新提示等
// - 全服活动：节日活动、限时活动等
//
// 设计说明：
// - 只推送给本机管理的连接，不跨 comet 节点
// - 如需全服广播，调用方需遍历所有 comet 节点逐个调用
// - 不检查用户权限，所有在线用户都会收到
// ============================================================================
void CometServer::PushBroadcast(const ChatMessage &msg) {
    // ========================================================================
    // 步骤 1: 构造 WebSocket 文本帧
    // ========================================================================
    // 将 protobuf ChatMessage 转换为 JSON 格式
    std::string payload = msg.content_json().empty()
                              ? ("{\"msg_id\":\"" + msg.msg_id() + "\"}")
                              : msg.content_json();

    // 按照 RFC 6455 规范构造 WebSocket 文本帧
    std::string frame = BuildWebSocketTextFrame(payload);

    // ========================================================================
    // 步骤 2: 收集所有在线连接（需要加锁访问 user_conns_）
    // ========================================================================
    std::vector<TcpConnectionPtr> conns;  // 待发送的连接列表
    {
        // 加锁保护 user_conns_
        std::lock_guard<std::mutex> lock(conns_mu_);

        // 遍历所有在线用户
        for (auto &kv : user_conns_) {
            // 遍历该用户的所有连接（多端登录）
            for (const auto &c : kv.second) {
                conns.push_back(c);
            }
        }
    }
    // 锁已释放，下面在锁外执行 I/O 操作

    // ========================================================================
    // 步骤 3: 发送消息（在锁外执行，避免持锁进行 I/O）
    // ========================================================================
    for (const auto &c : conns) {
        // 检查连接状态（可能在锁释放后断开）
        if (c->connected()) {
            // 通过 muduo TcpConnection 发送 WebSocket 帧
            c->send(frame);
        }
    }
}

// ============================================================================
// 函数：OnConnection
// 功能：连接状态变化回调（muduo 网络库回调）
//
// 参数：
// @param conn: TCP 连接对象（智能指针）
//
// 触发时机：
// - 新连接建立：conn->connected() == true
// - 连接断开：conn->connected() == false
//
// 实现说明：
// - 新连接：初始化连接上下文（ConnContext），状态设为握手阶段
// - 断开：清理用户连接映射，通知 logic 用户下线，清理 Redis 路由
// ============================================================================
void CometServer::OnConnection(const TcpConnectionPtr &conn) {
    // ========================================================================
    // 分支 1: 新连接建立
    // ========================================================================
    if (conn->connected()) {
        // 初始化连接上下文（ConnContext）
        ConnContext ctx;

        // 状态设为握手阶段（等待 WebSocket 握手请求）
        ctx.state = ConnContext::kHandshake;

        // 记录连接建立时间（用于空闲检测和心跳超时）
        ctx.last_active_ms =
            std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now().time_since_epoch())
                .count();

        // 将上下文绑定到连接对象（muduo 的 Context 机制）
        conn->setContext(ctx);

        // 记录日志：便于监控和调试
        LOG_INFO << "New TCP connection from "
                 << conn->peerAddress().toIpPort();
    }
    // ========================================================================
    // 分支 2: 连接断开
    // ========================================================================
    else {
        // 需要清理的信息
        int64_t offline_uid = 0;    // 下线的用户 ID
        std::string conn_id;        // 连接唯一标识
        bool need_offline = false;  // 是否需要通知 logic 用户下线

        // --------------------------------------------------------------------
        // 步骤 1: 从连接上下文中提取用户信息
        // --------------------------------------------------------------------
        try {
            // 从连接对象中取出上下文（存储在 std::any 中）
            auto ctx = std::any_cast<ConnContext>(conn->getContext());

            // 只有已完成鉴权的连接才需要清理映射
            if (ctx.user_id > 0) {
                // 加锁保护 user_conns_
                std::lock_guard<std::mutex> lock(conns_mu_);

                // 查找该用户的连接集合
                auto it = user_conns_.find(ctx.user_id);
                if (it != user_conns_.end()) {
                    // 从集合中移除当前连接
                    it->second.erase(conn);

                    // 如果这是该用户的最后一个连接，标记需要通知下线
                    if (it->second.empty()) {
                        user_conns_.erase(it);      // 移除整个映射项
                        offline_uid = ctx.user_id;  // 记录用户 ID
                        need_offline = true;        // 标记需要下线通知
                    }

                    // 记录连接 ID（用于 Redis 路由清理）
                    conn_id = ctx.conn_id;
                }
            }
        } catch (const std::bad_any_cast &) {
            // 上下文不存在或类型不匹配（可能是握手失败的连接）
            // 静默忽略，不影响其他连接的清理
        }

        // --------------------------------------------------------------------
        // 步骤 2: 通知 logic 服务用户下线
        // --------------------------------------------------------------------
        // 条件：这是该用户在本 comet 上的最后一个连接
        // 用途：logic 可以更新用户在线状态、触发离线消息推送等
        if (need_offline && offline_uid > 0) {
            NotifyUserOffline(offline_uid);
        }

        // --------------------------------------------------------------------
        // 步骤 3: 清理 Redis 在线路由
        // --------------------------------------------------------------------
        // 从 Redis 中删除该连接的路由信息（user_connections:<user_id>）
        // 避免 job 服务向已断开的连接推送消息
        if (!conn_id.empty() && offline_uid > 0) {
            RemoveUserRoute(offline_uid, conn_id);
        }

        // 记录日志
        LOG_INFO << "Connection closed";
    }
}

// ============================================================================
// 函数：GenerateConnId（辅助函数）
// 功能：生成连接的唯一标识符
//
// 参数：
// @param comet_id: 当前 comet 节点的唯一标识
//
// 返回：
// @return: 连接唯一 ID，格式：<comet_id>-<时间戳>-<序号>
//          例如：comet-001-1703123456789-42
//
// 用途：
// - 鉴权追踪：在 VerifyToken 调用中传递，便于 logic 关联日志
// - 审计日志：记录每个连接的生命周期和操作历史
// - Redis 路由：区分同一用户的多个连接（多端登录）
// - 问题排查：根据 conn_id 定位具体连接的问题
//
// 设计说明：
// - 全局唯一：comet_id + 时间戳 + 原子递增序号
// - 可读性强：人类可读的格式，便于日志分析
// - 线程安全：使用 atomic<uint64_t> 保证多线程安全
// ============================================================================
std::string GenerateConnId(const std::string &comet_id) {
    // 静态原子变量：全局递增序号（从 1 开始）
    // - 使用 atomic 保证多线程安全
    // - memory_order_relaxed：只保证原子性，不保证顺序（足够用）
    static std::atomic<uint64_t> seq{1};

    // 获取当前序号并递增（原子操作）
    uint64_t cur = seq.fetch_add(1, std::memory_order_relaxed);

    // 获取当前时间戳（毫秒）
    auto now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                      std::chrono::system_clock::now().time_since_epoch())
                      .count();

    // 组装连接 ID：<comet_id>-<时间戳>-<序号>
    std::ostringstream oss;
    oss << comet_id << "-" << now_ms << "-" << cur;
    return oss.str();
}

// ============================================================================
// 函数：ParseTokenFromHandshake
// 功能：从 WebSocket 握手请求中提取鉴权 token
//
// 参数：
// @param req: HTTP 握手请求的完整文本（包含请求行和头部）
//
// 返回：
// @return: 提取到的 token 字符串，失败返回空字符串
//
// 支持的 token 传递方式（按优先级）：
// 1. Authorization 头部（推荐）：Authorization: Bearer <token>
//    - 符合 HTTP 标准，安全性更好
//    - 不会出现在 URL 日志中，减少泄露风险
// 2. URL 查询参数（兼容旧客户端）：GET /ws?token=<token>
//    - 简单易用，但 token 可能出现在日志中
//    - 仅作为向后兼容，不推荐新客户端使用
//
// 示例请求：
// ```
// GET /ws?token=abc123 HTTP/1.1
// Host: example.com
// Upgrade: websocket
// Connection: Upgrade
// Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==
// Authorization: Bearer xyz789
// ```
// 上述请求会优先返回 "xyz789"（Authorization 头部的值）
// ============================================================================
std::string CometServer::ParseTokenFromHandshake(const std::string &req) {
    // ========================================================================
    // 方式 1（推荐）：从 Authorization 头部提取 token
    // ========================================================================
    // 格式：Authorization: Bearer <token>
    // 说明：Bearer 是 OAuth 2.0 标准的认证方案，广泛用于 API 鉴权
    std::string auth;
    if (ExtractHeader(req, "Authorization", &auth)) {
        // 去除首尾空白字符
        auth = Trim(auth);

        // 转换为小写，便于不区分大小写匹配 "bearer"
        std::string lower = ToLower(auth);

        // 检查是否以 "bearer " 开头（注意有空格）
        const std::string prefix = "bearer ";
        if (lower.size() > prefix.size() &&
            lower.compare(0, prefix.size(), prefix) == 0) {
            // 提取 "bearer " 后面的 token 部分
            // 注意：从原始字符串 auth 中提取（保留原始大小写）
            return Trim(auth.substr(prefix.size()));
        }
    }

    // ========================================================================
    // 方式 2（兼容）：从 URL 查询参数提取 token
    // ========================================================================
    // 格式：GET /ws?token=<token>&other=value HTTP/1.1
    // 说明：简单但不够安全，token 可能出现在访问日志中

    // 查找 "GET " 字符串（HTTP 请求行的开始）
    auto pos = req.find("GET ");
    if (pos == std::string::npos) return {};  // 不是 GET 请求
    pos += 4;                                 // 跳过 "GET "

    // 查找请求行的结束（下一个空格）
    auto end = req.find(' ', pos);
    if (end == std::string::npos) return {};  // 格式错误

    // 提取 URL 路径（例如：/ws?token=abc123）
    std::string path = req.substr(pos, end - pos);

    // 查找 "token=" 参数
    auto qpos = path.find("token=");
    if (qpos == std::string::npos) return {};  // 未找到 token 参数
    qpos += 6;                                 // 跳过 "token="

    // 提取 token 值（从 "token=" 到字符串结束或下一个 '&'）
    std::string token = path.substr(qpos);

    // 如果有多个参数（用 '&' 分隔），只取第一个参数的值
    auto amp = token.find('&');
    if (amp != std::string::npos) token = token.substr(0, amp);

    return token;
}

// ============================================================================
// 函数：HandleHandshake
// 功能：处理 WebSocket 握手请求（HTTP -> WebSocket 升级）
//
// 参数：
// @param conn: TCP 连接对象
// @param buf: 接收缓冲区（muduo Buffer）
//
// 握手流程：
// 1. 解析 HTTP 请求，提取 token 和 Sec-WebSocket-Key
// 2. 通过 gRPC 调用 logic.VerifyToken 验证 token，获取 user_id
// 3. 计算 Sec-WebSocket-Accept 响应值（SHA1 + Base64）
// 4. 返回 HTTP 101 Switching Protocols 响应
// 5. 更新连接状态为 kOpen，记录用户映射
// 6. 在 Redis 中注册在线路由
//
// 错误处理：
// - token 缺失或验证失败：关闭连接
// - Sec-WebSocket-Key 缺失：关闭连接
// - HTTP 头未接收完整：等待更多数据
//
// 参考标准：
// - RFC 6455: WebSocket 协议
// - RFC 2616: HTTP/1.1 协议
// ============================================================================
void CometServer::HandleHandshake(const TcpConnectionPtr &conn, Buffer *buf) {
    // ========================================================================
    // 步骤 1: 检查 HTTP 头是否接收完整
    // ========================================================================
    // HTTP 头以 "\r\n\r\n" 结束（两个连续的 CRLF）
    // 如果未找到，说明数据未接收完整，等待下次 OnMessage 回调
    const char *crlf2 = "\r\n\r\n";
    const char *data = buf->peek();
    const char *end =
        static_cast<const char *>(memmem(data, buf->readableBytes(), crlf2, 4));
    if (!end) {
        // HTTP 头未接收完整，直接返回（不消费缓冲区）
        // muduo 会在下次收到数据时再次调用 OnMessage
        return;
    }

    // 计算 HTTP 头的长度（包括 "\r\n\r\n"）
    size_t headerLen = end - data + 4;

    // 提取完整的 HTTP 头文本
    std::string req(data, headerLen);

    // ========================================================================
    // 步骤 2: 提取 token 并验证（调用 logic 服务鉴权）
    // ========================================================================

    // 2.1 从 HTTP 请求中提取 token
    // 支持两种方式：Authorization: Bearer <token> 或 URL 参数 token=xxx
    std::string token = ParseTokenFromHandshake(req);
    if (token.empty()) {
        // token 缺失：拒绝握手，关闭连接
        LOG_ERROR << "No token in WebSocket handshake";
        conn->shutdown();
        return;
    }

    // 2.2 检查 gRPC 客户端是否已初始化
    if (!logic_stub_) {
        LOG_ERROR << "Logic stub not initialized";
        conn->shutdown();
        return;
    }

    // 2.3 构造 VerifyToken 请求
    VerifyTokenRequest vreq;
    vreq.set_token(token);         // 客户端提供的 token
    vreq.set_comet_id(comet_id_);  // 当前 comet 节点 ID

    // 生成连接唯一标识（用于追踪和路由）
    std::string conn_id = GenerateConnId(comet_id_);
    vreq.set_conn_id(conn_id);  // 连接唯一 ID

    // 2.4 调用 logic.VerifyToken 进行鉴权
    VerifyTokenReply vrep;
    grpc::ClientContext ctx_rpc;
    LOG_INFO << "Verifying token for conn_id=" << conn_id;
    auto status = logic_stub_->VerifyToken(&ctx_rpc, vreq, &vrep);
    LOG_INFO << "VerifyToken reply received, user_id=" << vrep.user_id();

    // 2.5 检查验证结果
    if (!status.ok() || vrep.error().code() != 0) {
        // 验证失败：可能是 token 过期、无效，或 logic 服务异常
        std::string msg =
            status.ok() ? vrep.error().message() : status.error_message();
        LOG_ERROR << "VerifyToken failed: " << msg;
        conn->shutdown();  // 关闭连接，拒绝握手
        return;
    }

    // 验证成功：提取 user_id
    int64_t user_id = vrep.user_id();

    // ========================================================================
    // 步骤 3: 计算 Sec-WebSocket-Accept 并返回 101 响应
    // ========================================================================

    // 3.1 从 HTTP 头中提取 Sec-WebSocket-Key
    // 这是客户端生成的随机值（Base64 编码），用于防止缓存污染攻击
    std::string ws_key;
    if (!ExtractHeader(req, "Sec-WebSocket-Key", &ws_key) || ws_key.empty()) {
        // Sec-WebSocket-Key 缺失：不是合法的 WebSocket 握手请求
        LOG_ERROR << "No Sec-WebSocket-Key in WebSocket handshake";
        conn->shutdown();
        return;
    }

    // 3.2 计算 Sec-WebSocket-Accept
    // 算法（RFC 6455）：
    //   1. 将 Sec-WebSocket-Key 与固定的 GUID 字符串拼接
    //   2. 计算 SHA1 摘要（20 字节）
    //   3. Base64 编码
    std::string accept_val = ComputeWebSocketAccept(ws_key);

    // 3.3 构造 HTTP 101 Switching Protocols 响应
    // 状态码 101 表示协议切换（从 HTTP 升级到 WebSocket）
    std::string resp =
        "HTTP/1.1 101 Switching Protocols\r\n"
        "Upgrade: websocket\r\n"    // 升级到 WebSocket 协议
        "Connection: Upgrade\r\n";  // 连接需要升级
    resp += "Sec-WebSocket-Accept: " + accept_val + "\r\n\r\n";

    // 3.4 发送响应给客户端
    conn->send(resp);

    // 3.5 从缓冲区中移除已处理的 HTTP 头
    buf->retrieve(headerLen);

    // ========================================================================
    // 步骤 4: 更新连接状态为 Open（握手完成）
    // ========================================================================

    // 从连接对象中取出上下文
    ConnContext ctx = std::any_cast<ConnContext>(conn->getContext());

    // 更新状态：从握手阶段切换到已建立阶段
    ctx.state = ConnContext::kOpen;  //握手成功，后续可以收发消息

    // 绑定用户 ID 和连接 ID
    ctx.user_id = user_id;
    ctx.conn_id = conn_id;

    // 记录握手完成时间（用于空闲检测）
    ctx.last_active_ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch())
            .count();

    // 将更新后的上下文写回连接对象
    conn->setContext(ctx);

    // ========================================================================
    // 步骤 5: 建立 user_id -> connection 的映射
    // ========================================================================
    {
        // 加锁保护 user_conns_（多线程访问）
        std::lock_guard<std::mutex> lock(conns_mu_);

        // 将连接加入该用户的连接集合
        // 注意：一个用户可能有多个连接（多端登录），使用 set 存储
        user_conns_[user_id].insert(conn);
    }

    // ========================================================================
    // 步骤 6: 在 Redis 中注册在线路由
    // ========================================================================
    // 用途：供 job/logic 服务查询用户在线的 comet_id，进行下行路由
    // 格式：user_connections:<user_id> -> {<comet_id>:<conn_id>: {}}
    RegisterUserOnline(user_id, conn_id);

    // 记录日志：握手成功
    LOG_INFO << "WebSocket handshake done, user_id=" << user_id
             << " conn_id=" << conn_id;
}

// ============================================================================
// 函数：HandleWebSocketFrame
// 功能：解析和处理 WebSocket 帧（RFC 6455）
//
// 参数：
// @param conn: TCP 连接对象
// @param buf: 接收缓冲区（可能包含多个完整帧或部分帧）
// @param ctx: 连接上下文（引用传递，会更新 last_active_ms 等字段）
//
// 支持的帧类型：
// - 0x1: 文本帧（业务消息）
// - 0x8: 关闭帧（客户端主动断开）
// - 0x9: Ping 帧（心跳请求，需回复 Pong）
// - 0xA: Pong 帧（心跳响应，客户端对服务端 Ping 的回复）
//
// 实现说明：
// - 仅接受 FIN=1 的单帧消息，不支持分片（FIN=0 的 continuation 帧）
// - 客户端发来的帧必须 masked（RFC 6455 规定客户端->服务端必须掩码）
// - 使用循环处理多个帧（TCP 粘包情况）
// - 帧不完整时等待更多数据（return 不消费缓冲区）
//
// WebSocket 帧格式（RFC 6455）：
// ```
//  0                   1                   2                   3
//  0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
// +-+-+-+-+-------+-+-------------+-------------------------------+
// |F|R|R|R| opcode|M| Payload len |    Extended payload length    |
// |I|S|S|S|  (4)  |A|     (7)     |             (16/64)           |
// |N|V|V|V|       |S|             |   (if payload len==126/127)   |
// | |1|2|3|       |K|             |                               |
// +-+-+-+-+-------+-+-------------+ - - - - - - - - - - - - - - - +
// |     Extended payload length continued, if payload len == 127  |
// + - - - - - - - - - - - - - - - +-------------------------------+
// |                               |Masking-key, if MASK set to 1  |
// +-------------------------------+-------------------------------+
// | Masking-key (continued)       |          Payload Data         |
// +-------------------------------- - - - - - - - - - - - - - - - +
// :                     Payload Data continued ...                :
// + - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - +
// |                     Payload Data continued ...                |
// +---------------------------------------------------------------+
// ```
// ============================================================================
void CometServer::HandleWebSocketFrame(const TcpConnectionPtr &conn,
                                       Buffer *buf, ConnContext &ctx) {
    // ========================================================================
    // 循环处理缓冲区中的所有完整帧
    // ========================================================================
    // 为什么使用 while 循环：
    // - TCP 是流式协议，可能一次收到多个 WebSocket 帧（粘包）
    // - 需要循环解析，直到缓冲区中没有完整的帧
    while (buf->readableBytes() >= 2) {  // 至少需要 2 字节（帧头的前两个字节）
        // ====================================================================
        // 步骤 1: 解析帧头的前两个字节
        // ====================================================================
        const unsigned char *data =
            reinterpret_cast<const unsigned char *>(buf->peek());

        // 第 1 字节：FIN + RSV + Opcode
        bool fin = (data[0] & 0x80) != 0;  // 第 7 位：FIN（是否最后一帧）
        unsigned char opcode = data[0] & 0x0F;  // 第 0-3 位：Opcode（帧类型）

        // 第 2 字节：MASK + Payload Length
        bool masked = (data[1] & 0x80) != 0;  // 第 7 位：MASK（是否有掩码）
        uint64_t payloadLen = data[1] & 0x7F;  // 第 0-6 位：负载长度（初始值）

        // 帧头长度（初始为 2 字节，扩展长度和掩码字段会增加）
        size_t headerLen = 2;

        // ====================================================================
        // 步骤 2: 校验 FIN 和 MASK 标志位
        // ====================================================================
        // 检查 1: FIN 必须为 1（不支持分片）
        // 检查 2: MASK 必须为 1（RFC 6455 规定客户端->服务端必须掩码）
        if (!fin || !masked) {
            // 不符合协议要求：关闭连接
            LOG_WARN << "Close connection from user " << ctx.user_id
                     << " conn_id=" << ctx.conn_id << " due to invalid frame"
                     << " (FIN=" << fin << ", MASK=" << masked << ")";
            conn->shutdown();
            return;
        }
        // ====================================================================
        // 步骤 3: 处理扩展长度字段
        // ====================================================================
        // Payload Length 的表示方式（RFC 6455）：
        // - 0-125: 直接表示实际长度（不需要扩展字段）
        // - 126: 实际长度在接下来的 2 字节（16 位，网络字节序）
        // - 127: 实际长度在接下来的 8 字节（64 位，网络字节序）

        if (payloadLen == 126) {
            // 中等长度：使用 16 位扩展字段
            if (buf->readableBytes() < headerLen + 2)
                return;  // 数据不足，等待更多
            const unsigned char *p = data + headerLen;
            payloadLen = (p[0] << 8) | p[1];  // 网络字节序（大端）转主机字节序
            headerLen += 2;
        } else if (payloadLen == 127) {
            // 长消息：使用 64 位扩展字段
            if (buf->readableBytes() < headerLen + 8)
                return;  // 数据不足，等待更多
            payloadLen = 0;
            const unsigned char *p = data + headerLen;
            // 逐字节读取（网络字节序）
            for (int i = 0; i < 8; ++i) {
                payloadLen = (payloadLen << 8) | p[i];
            }
            headerLen += 8;
        }

        // ====================================================================
        // 步骤 4: 校验控制帧的负载长度
        // ====================================================================
        // RFC 6455 规定：控制帧（close/ping/pong）的负载最大 125 字节
        // Opcode: 0x8=close, 0x9=ping, 0xA=pong
        if ((opcode == 0x8 || opcode == 0x9 || opcode == 0xA) &&
            payloadLen > 125) {
            LOG_WARN << "Close connection from user " << ctx.user_id
                     << " conn_id=" << ctx.conn_id
                     << " due to control frame payload too large"
                     << " (opcode=" << static_cast<int>(opcode)
                     << ", payload_len=" << payloadLen << ")";
            conn->shutdown();
            return;
        }

        // ====================================================================
        // 步骤 5: 检查帧是否接收完整
        // ====================================================================
        // 完整帧大小 = headerLen + 4（掩码） + payloadLen（负载）
        if (buf->readableBytes() < headerLen + 4 + payloadLen) {
            // 帧不完整：等待更多数据
            // 注意：这里 return 不消费缓冲区，下次 OnMessage 会继续处理
            return;
        }

        // ====================================================================
        // 步骤 6: 提取掩码和负载数据
        // ====================================================================

        // 6.1 提取 4 字节掩码（紧跟在帧头后面）
        const unsigned char *mask = data + headerLen;
        headerLen += 4;

        // 6.2 提取负载数据并解码
        std::string payload;
        payload.resize(payloadLen);
        const unsigned char *payloadData = data + headerLen;

        // 6.3 掩码解码：payload[i] = encoded_data[i] XOR mask[i % 4]
        // 为什么需要掩码：
        // - 防止恶意客户端构造特殊帧，污染中间代理的缓存
        // - RFC 6455 强制要求客户端->服务端必须掩码
        for (uint64_t i = 0; i < payloadLen; ++i) {
            payload[i] = static_cast<char>(payloadData[i] ^ mask[i % 4]);
        }

        // 6.4 从缓冲区中移除已处理的帧数据
        buf->retrieve(headerLen + payloadLen);

        // ====================================================================
        // 步骤 7: 根据 Opcode 分发到不同的处理逻辑
        // ====================================================================
        // Opcode 定义（RFC 6455）：
        // - 0x0: Continuation 帧（分片消息的后续帧，当前未实现）
        // - 0x1: Text 帧（UTF-8 文本消息）
        // - 0x2: Binary 帧（二进制消息，当前未实现）
        // - 0x8: Close 帧（关闭连接）
        // - 0x9: Ping 帧（心跳请求）
        // - 0xA: Pong 帧（心跳响应）

        if (opcode == 0x8) {
            // ================================================================
            // Opcode 0x8: Close 帧（客户端主动关闭连接）
            // ================================================================
            // 处理：优雅关闭连接
            // 注意：应该先发送 Close 帧响应，再关闭 TCP 连接（RFC 6455）
            //      但简化实现中直接关闭 TCP 连接
            LOG_WARN << "Recv close frame from user " << ctx.user_id
                     << " conn_id=" << ctx.conn_id;
            conn->shutdown();
            return;  // 关闭连接后不再处理后续帧

        } else if (opcode == 0x9) {
            // ================================================================
            // Opcode 0x9: Ping 帧（心跳请求）
            // ================================================================
            // 处理：立即回复 Pong 帧（携带相同的 payload）
            // 用途：保持连接活跃，防止被防火墙/负载均衡器断开

            // 构造 Pong 帧
            std::string frame;
            frame.reserve(2 + payloadLen);
            frame.push_back(
                static_cast<char>(0x8A));  // FIN=1, Opcode=0xA (Pong)
            frame.push_back(
                static_cast<char>(payloadLen));  // 负载长度（服务端无需掩码）
            frame.append(payload);  // 原样返回 Ping 的 payload

            // 发送 Pong 帧
            conn->send(frame);

            // 更新最后活跃时间（防止空闲超时）
            ctx.last_active_ms =
                std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::system_clock::now().time_since_epoch())
                    .count();

            LOG_INFO << "Recv ping from user " << ctx.user_id
                     << " conn_id=" << ctx.conn_id << ", replied pong";

            // 刷新 Redis 中的在线路由 TTL
            RefreshUserTtl(ctx.user_id, ctx.conn_id);

            // 将更新后的上下文写回连接对象
            conn->setContext(ctx);

        } else if (opcode == 0xA) {
            // ================================================================
            // Opcode 0xA: Pong 帧（心跳响应）
            // ================================================================
            // 处理：视为保活消息，更新活跃时间
            // 说明：通常是客户端对服务端 Ping 的响应（如果实现了服务端主动
            // Ping）

            // 更新最后活跃时间（防止空闲超时）
            ctx.last_active_ms =
                std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::system_clock::now().time_since_epoch())
                    .count();

            LOG_INFO << "Recv pong from user " << ctx.user_id
                     << " conn_id=" << ctx.conn_id;

            // 刷新 Redis 中的在线路由 TTL
            RefreshUserTtl(ctx.user_id, ctx.conn_id);

            // 将更新后的上下文写回连接对象
            conn->setContext(ctx);

        } else if (opcode == 0x1) {
            // ================================================================
            // Opcode 0x1: Text 帧（业务消息）
            // ================================================================
            // 处理：解析 JSON 消息，转发给 logic 服务
            // 注意：OnTextMessage 内部会更新 ctx 并调用 setContext()
            OnTextMessage(conn, ctx, payload);

        } else {
            // ================================================================
            // 其他 Opcode：不支持（例如 Binary 帧、Continuation 帧）
            // ================================================================
            // 处理：记录警告，继续处理后续帧（不关闭连接）
            LOG_WARN << "Unsupported opcode " << static_cast<int>(opcode)
                     << " from user " << ctx.user_id
                     << " conn_id=" << ctx.conn_id;
        }
    }  // end while
}

// ============================================================================
// 函数：OnTextMessage
// 功能：处理客户端上行的文本消息（业务消息）
//
// 参数：
// @param conn: TCP 连接对象
// @param ctx: 连接上下文（引用传递，会更新 last_active_ms）
// @param payload: 文本消息内容（UTF-8 JSON 字符串）
//
// 处理流程：
// 1. 更新连接活跃时间，刷新 Redis TTL
// 2. 解析 JSON 消息，提取元信息（msg_type、target_type、target_id 等）
// 3. 校验消息格式和目标类型
// 4. 通过 gRPC 转发给 logic 服务（落库、路由、幂等、风控等）
// 5. 返回 ACK 确认（包含服务端生成的 msg_id）
//
// 消息格式示例：
// {
//   "msg_type": "text",
//   "target_type": "single_chat",
//   "target_id": 12345,
//   "client_msg_id": "uuid-...",
//   "content": "Hello World"
// }
// ============================================================================
void CometServer::OnTextMessage(const TcpConnectionPtr &conn, ConnContext &ctx,
                                const std::string &payload) {
    // ========================================================================
    // 步骤 1: 更新连接活跃时间
    // ========================================================================
    // 任意业务消息都视为活跃：刷新 last_active 和 Redis TTL
    ctx.last_active_ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch())
            .count();
    RefreshUserTtl(ctx.user_id, ctx.conn_id);
    LOG_INFO << "Recv text from user " << ctx.user_id << ": " << payload;
    conn->setContext(ctx);

    // ========================================================================
    // 步骤 2: 解析 JSON 消息，提取元信息
    // ========================================================================
    UpstreamMessageMeta meta;  //解析json
    if (!ParseUpstreamMessage(payload, &meta)) {
        // 客户端消息格式错误：返回 error frame，不断开（便于客户端纠错）
        std::string frame = BuildWebSocketTextFrame(
            "{\"type\":\"error\",\"code\":400,\"message\":"
            "\"invalid message format\"}");
        conn->send(frame);
        return;
    }

    // ========================================================================
    // 步骤 3: 校验目标类型和 ID
    // ========================================================================
    // 简单校验目标类型和 ID
    if (meta.target_type != "single_chat" || meta.target_id <= 0) {
        // 当前 comet 只支持单聊上行（room/广播等可按需要扩展）
        std::string frame = BuildWebSocketTextFrame(
            "{\"type\":\"error\",\"code\":400,\"message\":"
            "\"unsupported target_type or target_id\"}");
        conn->send(frame);
        return;
    }

    // ========================================================================
    // 步骤 4: 检查 gRPC 客户端是否可用
    // ========================================================================
    if (!logic_stub_) {
        std::string frame = BuildWebSocketTextFrame(
            "{\"type\":\"error\",\"message\":\"logic not available\"}");
        conn->send(frame);
        return;
    }

    // ========================================================================
    // 步骤 5: 构造 gRPC 请求，转发给 logic 服务
    // ========================================================================
    // 通过 gRPC 将上行消息转交给 logic（统一做落库/路由/幂等/风控等）
    UpstreamMessageRequest req;
    req.set_from_user_id(ctx.user_id);          // 发送者 ID
    req.set_client_msg_id(meta.client_msg_id);  // 客户端消息 ID（幂等）
    req.set_content_json(payload);              // 完整的 JSON 消息
    req.set_comet_id(comet_id_);                // 当前 comet 节点 ID
    req.set_conn_id(ctx.conn_id);               // 连接唯一标识
    req.set_received_time_ms(  // 服务端接收时间（毫秒）
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch())
            .count());
    req.set_msg_type(meta.msg_type);        // 消息类型（text/image/...）
    req.set_target_type(meta.target_type);  // 目标类型（single_chat/room）
    req.set_target_id(meta.target_id);  // 目标 ID（收件人 ID 或房间 ID）

    // ========================================================================
    // 步骤 6: 调用 logic.SendUpstreamMessage
    // ========================================================================
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

    // ========================================================================
    // 步骤 7: 返回 ACK 确认
    // ========================================================================
    // 发送 ack：带回服务端生成的 msg_id，客户端可用于对账/去重/状态机推进
    std::string frame = BuildWebSocketTextFrame(
        "{\"type\":\"ack\",\"msg_id\":\"" + rep.message().msg_id() +
        "\",\"msg_seq\":" + std::to_string(rep.message().msg_seq()) +
        ",\"client_msg_id\":\"" + rep.message().client_msg_id() + "\"}");
    conn->send(frame);
}

// ============================================================================
// 函数：OnMessage
// 功能：消息到达回调（muduo 网络库回调）
//
// 参数：
// @param conn: TCP 连接对象
// @param buf: 接收缓冲区（包含新到达的数据）
// @param ts: 时间戳（本函数未使用）
//
// 触发时机：
// - 当连接上有数据到达时，muduo 网络库会调用此回调
// - 可能包含部分数据（TCP 流式传输），需要处理分包和粘包
//
// 实现说明：
// - 根据连接状态分发到不同的处理函数：
//   * kHandshake 状态：处理 HTTP -> WebSocket 握手
//   * kOpen 状态：解析 WebSocket 帧，处理业务消息
// ============================================================================
void CometServer::OnMessage(const TcpConnectionPtr &conn, Buffer *buf,
                            muduo::Timestamp ts) {
    // 忽略时间戳参数（本函数不需要）
    (void)ts;

    // 记录调试日志：收到多少字节数据
    LOG_DEBUG << "OnMessage called, bytes=" << buf->readableBytes();

    // 从连接对象中获取上下文（包含连接状态、用户 ID 等信息）
    ConnContext ctx = std::any_cast<ConnContext>(conn->getContext());

    // ========================================================================
    // 根据连接状态分发到不同的处理函数
    // ========================================================================

    if (ctx.state == ConnContext::kHandshake) {
        // ====================================================================
        // 状态 1: 握手阶段（等待 HTTP -> WebSocket 升级请求）
        // ====================================================================
        // 期望收到的数据：HTTP GET 请求（包含 Upgrade: websocket 头）
        // 例如：
        //   GET /ws?token=abc123 HTTP/1.1
        //   Host: example.com
        //   Upgrade: websocket
        //   Connection: Upgrade
        //   Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==
        //   ...
        HandleHandshake(conn, buf);
    } else {
        // ====================================================================
        // 状态 2: 已建立阶段（WebSocket 连接已升级，接收业务消息）
        // ====================================================================
        // 期望收到的数据：WebSocket 帧（RFC 6455 格式）
        // 帧类型：文本帧、二进制帧、ping/pong 帧、关闭帧等
        HandleWebSocketFrame(conn, buf, ctx);
    }
}

// ============================================================================
// 函数：NotifyUserOffline
// 功能：通知 logic 服务用户下线（该用户在本 comet 上的最后一个连接断开）
//
// 参数：
// @param user_id: 下线的用户 ID
//
// 实现说明：
// - 使用 detached thread 异步发起 RPC，避免阻塞 muduo IO 线程
// - 只在用户的最后一个连接断开时调用（由 OnConnection 判断）
// - logic 服务收到通知后可以：
//   * 更新用户在线状态
//   * 触发离线消息推送
//   * 记录用户活跃日志等
//
// 注意事项：
// - 使用 detached thread 意味着无法等待 RPC 完成或获取返回值
// - 如果 RPC 失败，只记录日志，不影响连接关闭流程
// - 为避免野指针，需要在 lambda 中捕获必要的值（不捕获 this）
// ============================================================================
void CometServer::NotifyUserOffline(int64_t user_id) {
    // 检查 gRPC 客户端是否可用
    if (!logic_stub_) return;

    // 获取 stub 的原始指针（在 lambda 中使用）
    auto *stub = logic_stub_.get();

    // 拷贝 comet_id（避免在 lambda 中捕获 this）
    std::string comet_id = comet_id_;

    // 尝试获取一个连接 ID 用于日志/路由清理
    std::string conn_id;
    {
        std::lock_guard<std::mutex> lock(conns_mu_);
        auto it = user_conns_.find(user_id);
        if (it != user_conns_.end() && !it->second.empty()) {
            try {
                // 从该用户的任一连接中提取 conn_id
                auto ctx = std::any_cast<ConnContext>(
                    (*it->second.begin())->getContext());
                conn_id = ctx.conn_id;
            } catch (...) {
                // 忽略异常（上下文不存在或类型不匹配）
            }
        }
    }

    // ========================================================================
    // 在独立线程中发起 RPC 调用
    // ========================================================================
    // 为什么使用 detached thread：
    // - UserOffline RPC 可能较慢（网络延迟、logic 服务繁忙等）
    // - 在 muduo IO 线程中同步调用会阻塞该线程，影响其他连接的处理
    // - detached thread 可以异步执行，不阻塞主流程
    std::thread([stub, user_id, comet_id, conn_id]() {
        // 构造 RPC 请求
        UserOfflineRequest req;
        req.set_user_id(user_id);
        req.set_comet_id(comet_id);
        if (!conn_id.empty()) req.set_conn_id(conn_id);

        // 发起 RPC 调用
        SimpleReply rep;
        grpc::ClientContext ctx;
        auto status = stub->UserOffline(&ctx, req, &rep);

        // 检查 RPC 调用结果
        if (!status.ok()) {
            // RPC 层失败（网络问题、服务不可用等）
            LOG_ERROR << "UserOffline RPC failed for user " << user_id << ": "
                      << status.error_message();
            return;
        }

        // 检查业务层错误码
        if (rep.error().code() != 0) {
            // 业务层失败（logic 服务内部错误）
            LOG_ERROR << "UserOffline logic error for user " << user_id << ": "
                      << rep.error().message();
        } else {
            // 成功通知用户下线
            LOG_INFO << "UserOffline reported for user " << user_id;
        }
    }).detach();  // 分离线程，不等待完成
}

// ============================================================================
// 函数：RegisterUserOnline
// 功能：在 Redis 中注册用户的在线路由信息
//
// 参数：
// @param user_id: 用户 ID
// @param conn_id: 连接唯一标识
//
// Redis 数据结构：
// - 类型：Hash
// - Key：user_connections:<user_id>
// - Field：<comet_id>:<conn_id>
// - Value：{}（预留扩展，可存储设备类型、IP 等信息）
//
// 用途：
// - job/logic 服务查询用户在线的 comet_id，进行下行路由
// - 支持多端登录：一个用户可以有多个 field（不同设备、不同 comet 节点）
// - 通过 TTL 自动清理异常断线未清理的脏数据
//
// 调用时机：
// - WebSocket 握手成功后立即调用
//
// 示例：
// HSET user_connections:12345 comet-001:12345-1703123456789-1 {}
// PEXPIRE user_connections:12345 60000
// ============================================================================
void CometServer::RegisterUserOnline(int64_t user_id,
                                     const std::string &conn_id) {
    // 参数校验
    if (!redis_ready_ || user_id <= 0 || conn_id.empty()) return;

    // 从连接池获取 Redis 连接
    auto guard = redis_pool_.Acquire();
    redisContext *ctx = guard.get();
    if (!ctx) {
        LOG_ERROR << "Redis Acquire failed for RegisterUserOnline";
        return;
    }

    // ========================================================================
    // 步骤 1: 使用 HSET 注册路由信息
    // ========================================================================
    // Redis 路由结构（示例）：
    // key:   user_connections:<user_id>
    // field: <comet_id>:<conn_id>
    // value: {}（预留扩展，可存储设备类型、IP 等）
    std::string key = "user_connections:" + std::to_string(user_id);
    std::string field = comet_id_ + ":" + conn_id;

    // 执行 HSET 命令
    redisReply *reply = (redisReply *)redisCommand(ctx, "HSET %s %s {}",
                                                   key.c_str(), field.c_str());
    if (reply) freeReplyObject(reply);  // 释放回复对象

    // ========================================================================
    // 步骤 2: 设置 TTL（过期时间）
    // ========================================================================
    // 用途：自动剔除异常断线/进程崩溃未清理的脏数据
    // 说明：每次心跳/业务消息都会刷新 TTL（RefreshUserTtl）
    reply = (redisReply *)redisCommand(ctx, "PEXPIRE %s %lld", key.c_str(),
                                       static_cast<long long>(redis_ttl_ms_));
    if (reply) freeReplyObject(reply);
}

// ============================================================================
// 函数：RefreshUserTtl
// 功能：刷新 Redis 中用户在线路由信息的 TTL（续期）
//
// 参数：
// @param user_id: 用户 ID
// @param conn_id: 连接唯一标识（当前未使用，预留）
//
// 用途：
// - 防止正常在线的用户路由信息因 TTL 过期而被自动删除
// - 每次收到心跳（ping/pong）或业务消息时调用
//
// 实现说明：
// - 使用 Acquire(0) 非阻塞获取连接（快速路径）
// - 如果获取失败（连接池繁忙），直接返回（不强制等待）
// - 刷新失败不影响业务流程（下次心跳会重试）
//
// 调用时机：
// - 收到 Ping 帧时
// - 收到 Pong 帧时
// - 收到文本消息时
//
// 注意事项：
// - 高频调用，需要保证性能
// - 使用非阻塞获取，避免阻塞 IO 线程
// ============================================================================
void CometServer::RefreshUserTtl(int64_t user_id, const std::string &conn_id) {
    // 参数校验
    if (!redis_ready_ || user_id <= 0 || conn_id.empty()) return;

    // 轻量续期：用非阻塞/快速路径获取连接（Acquire(0)）
    // 参数 0 表示：如果没有空闲连接，立即返回 nullptr（不等待）
    auto guard = redis_pool_.Acquire(0);
    redisContext *ctx = guard.get();
    if (!ctx) return;  // 获取失败，直接返回（不影响业务）

    // 构造 Redis key
    std::string key = "user_connections:" + std::to_string(user_id);

    // 执行 PEXPIRE 命令，刷新 TTL（毫秒）
    redisReply *reply =
        (redisReply *)redisCommand(ctx, "PEXPIRE %s %lld", key.c_str(),
                                   static_cast<long long>(redis_ttl_ms_));
    if (reply) freeReplyObject(reply);  // 释放回复对象
}

// ============================================================================
// 函数：RemoveUserRoute
// 功能：从 Redis 中删除用户的在线路由信息（连接断开时清理）
//
// 参数：
// @param user_id: 用户 ID
// @param conn_id: 连接唯一标识
//
// 用途：
// - 连接断开时主动清理路由信息，避免 job/logic 向已断开的连接推送消息
// - 即使清理失败，Redis TTL 也会自动删除过期的路由信息
//
// Redis 操作：
// - 使用 HDEL 删除指定的 field（<comet_id>:<conn_id>）
// - 如果该用户在本 comet 上还有其他连接，只删除当前连接的 field
// - 如果是最后一个连接，hash key 会保留（由 TTL 自动删除）
//
// 调用时机：
// - OnConnection 回调中检测到连接断开时
//
// 注意事项：
// - 使用阻塞获取连接（Acquire()），因为清理操作不能丢失
// - 如果获取连接失败，依赖 Redis TTL 兜底清理
// ============================================================================
void CometServer::RemoveUserRoute(int64_t user_id, const std::string &conn_id) {
    // 参数校验
    if (!redis_ready_ || user_id <= 0 || conn_id.empty()) return;

    // 从连接池获取 Redis 连接（阻塞等待）
    auto guard = redis_pool_.Acquire();
    redisContext *ctx = guard.get();
    if (!ctx) return;  // 获取失败，依赖 TTL 兜底

    // ========================================================================
    // 使用 HDEL 删除路由字段
    // ========================================================================
    // 主动清理该连接的路由字段（连接断开时调用）
    std::string key = "user_connections:" + std::to_string(user_id);
    std::string field = comet_id_ + ":" + conn_id;

    // 执行 HDEL 命令
    redisReply *reply = (redisReply *)redisCommand(ctx, "HDEL %s %s",
                                                   key.c_str(), field.c_str());
    if (reply) freeReplyObject(reply);  // 释放回复对象
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
