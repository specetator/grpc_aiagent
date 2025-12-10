// ============================================================================
// CometServer 实现
// 
// 包含：
// 1. WebSocket 握手协议实现（HTTP 升级 + SHA1 + Base64）
// 2. WebSocket 帧格式解析
// 3. 与 logic 服务的 gRPC 通信
// ============================================================================
#include "comet_server.h"

#include <grpcpp/grpcpp.h>

#include <cstring>
#include <array>
#include <cstdint>
#include <vector>

namespace sparkpush {

namespace {

// ============================================================================
// 内部辅助函数：HTTP 头解析、Base64 编码、SHA1 计算
// ============================================================================

// 从 HTTP 请求中提取指定 Header 的值
// @param req: 完整的 HTTP 请求文本
// @param header_name: Header 名称（如 "Sec-WebSocket-Key"）
// @param value: 输出参数，返回 Header 值
// @return: 找到返回 true，未找到返回 false
bool ExtractHeader(const std::string& req,
                   const std::string& header_name,
                   std::string* value) {
    if (!value) return false;
    
    // 查找 "Header-Name:" 字符串
    std::string key = header_name + ":";
    auto pos = req.find(key);
    if (pos == std::string::npos) return false;
    pos += key.size();
    
    // 跳过冒号后的空格
    while (pos < req.size() && (req[pos] == ' ' || req[pos] == '\t')) {
        ++pos;
    }
    if (pos >= req.size()) return false;
    
    // 查找行尾（\r\n）
    auto end = req.find("\r\n", pos);
    if (end == std::string::npos) {
        end = req.size();
    }
    
    // 去掉末尾空白字符
    size_t trimmed_end = end;
    while (trimmed_end > pos &&
           (req[trimmed_end - 1] == ' ' || req[trimmed_end - 1] == '\t' ||
            req[trimmed_end - 1] == '\r' || req[trimmed_end - 1] == '\n')) {
        --trimmed_end;
    }
    
    *value = req.substr(pos, trimmed_end - pos);
    return true;
}

// Base64 编码函数
// 用于 WebSocket 握手中计算 Sec-WebSocket-Accept 值
// @param data: 待编码的二进制数据
// @param len: 数据长度
// @return: Base64 编码后的字符串
std::string Base64Encode(const unsigned char* data, size_t len) {
    // Base64 编码表
    static const char kTable[] =
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "abcdefghijklmnopqrstuvwxyz"
        "0123456789+/";
    std::string out;
    out.reserve(((len + 2) / 3) * 4);  // 预分配空间，每 3 字节编码为 4 字符

    size_t i = 0;
    // 每次处理 3 个字节，编码为 4 个字符
    while (i + 2 < len) {
        unsigned int n = (data[i] << 16) | (data[i + 1] << 8) | data[i + 2];
        out.push_back(kTable[(n >> 18) & 0x3F]);  // 高 6 位
        out.push_back(kTable[(n >> 12) & 0x3F]);
        out.push_back(kTable[(n >> 6) & 0x3F]);
        out.push_back(kTable[n & 0x3F]);          // 低 6 位
        i += 3;
    }

    // 处理剩余字节（1-2 个），使用 '=' 填充
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

// 左循环移位辅助函数（SHA1 算法使用）
inline uint32_t RotL(uint32_t value, unsigned int bits) {
    return (value << bits) | (value >> (32 - bits));
}

// SHA1 核心变换函数：处理一个 512 位（64 字节）的数据块
// @param state: 当前的 160 位（5 个 32 位整数）哈希状态
// @param block: 64 字节的输入数据块
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

// 计算 SHA1 摘要
// @param data: 输入数据
// @return: 20 字节的 SHA1 哈希值
std::array<unsigned char, 20> SHA1(const std::string& data) {
    // SHA1 初始状态（标准值）
    uint32_t state[5] = {
        0x67452301u, 0xEFCDAB89u, 0x98BADCFEu,
        0x10325476u, 0xC3D2E1F0u,
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

// 计算 Sec-WebSocket-Accept 响应头的值
// WebSocket 协议规定：Accept = Base64(SHA1(client_key + GUID))
// @param client_key: 客户端发送的 Sec-WebSocket-Key 值
// @return: 服务端应返回的 Sec-WebSocket-Accept 值
std::string ComputeWebSocketAccept(const std::string& client_key) {
    // WebSocket 协议规定的魔术字符串（RFC 6455）
    static const std::string kGuid = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
    auto digest = SHA1(client_key + kGuid);
    return Base64Encode(digest.data(), digest.size());
}

}  // namespace

// ============================================================================
// CometServer 类实现
// ============================================================================

// 构造函数：初始化服务器和 gRPC 客户端
CometServer::CometServer(EventLoop* loop, const Config& cfg)
    : server_(loop, muduo::net::InetAddress(cfg.listen_port), "comet_server") {
    // 保存当前 comet 节点的标识
    comet_id_ = cfg.comet_id;
    
    // 创建 gRPC 客户端，连接到 logic 服务
    auto channel = grpc::CreateChannel(
        cfg.logic_grpc_target,
        grpc::InsecureChannelCredentials()  // 内网通信，不使用 TLS
    );
    logic_stub_ = sparkpush::LogicService::NewStub(channel);

    // 设置 muduo 回调函数
    server_.setConnectionCallback(
        std::bind(&CometServer::OnConnection, this, std::placeholders::_1));
    server_.setMessageCallback(
        std::bind(&CometServer::OnMessage, this,
                  std::placeholders::_1,
                  std::placeholders::_2,
                  std::placeholders::_3));
}

// 设置 IO 线程数（转发给 muduo TcpServer）
void CometServer::SetThreadNum(int thread_num) {
    if (thread_num < 1) thread_num = 1;
    server_.setThreadNum(thread_num);
}

// 启动服务器（转发给 muduo TcpServer）
void CometServer::Start() {
    server_.start();
}

// 连接建立/断开回调
// 建立时：初始化连接上下文，设置为握手状态
// 断开时：清理用户连接映射表
void CometServer::OnConnection(const TcpConnectionPtr& conn) {
    if (conn->connected()) {
        // 新连接建立：初始化上下文
        ConnContext ctx;
        ctx.state = ConnContext::kHandshake;  // 初始状态为握手阶段
        conn->setContext(ctx);
        LOG_INFO << "New TCP connection from " << conn->peerAddress().toIpPort();
    } else {
        // 连接断开：清理用户映射表
        try {
            auto ctx = std::any_cast<ConnContext>(conn->getContext());
            if (ctx.user_id > 0) {
                // 加锁保护共享数据结构
                std::lock_guard<std::mutex> lock(conns_mu_);
                auto it = user_conns_.find(ctx.user_id);
                if (it != user_conns_.end()) {
                    it->second.erase(conn);
                    // 如果该用户没有其他连接了，删除映射项
                    if (it->second.empty()) {
                        user_conns_.erase(it);
                    }
                }
            }
        } catch (const std::bad_any_cast&) {
            // 上下文未设置或类型错误，忽略
        }
        LOG_INFO << "Connection closed";
    }
}

// 从 WebSocket 握手请求中解析 token 参数
// 期望格式：GET /ws?token=xxx HTTP/1.1
// @param req: HTTP 请求文本
// @return: token 字符串，失败返回空字符串
std::string CometServer::ParseTokenFromHandshake(const std::string& req) {
    // 查找 "GET " 字符串
    auto pos = req.find("GET ");
    if (pos == std::string::npos) return {};
    pos += 4;  // 跳过 "GET "
    
    // 提取请求路径（到空格为止）
    auto end = req.find(' ', pos);
    if (end == std::string::npos) return {};
    std::string path = req.substr(pos, end - pos);
    
    // 查找 "token=" 参数
    auto qpos = path.find("token=");
    if (qpos == std::string::npos) return {};
    qpos += 6;  // 跳过 "token="
    
    // 提取 token 值（到 & 或字符串结尾）
    std::string token = path.substr(qpos);
    auto amp = token.find('&');
    if (amp != std::string::npos) token = token.substr(0, amp);
    
    return token;
}

// 处理 WebSocket 握手请求
// 流程：解析 HTTP 请求 -> 提取 token -> gRPC 鉴权 -> 返回 101 响应
void CometServer::HandleHandshake(const TcpConnectionPtr& conn, Buffer* buf) {
    // 1. 查找 HTTP 头部结束标志（\r\n\r\n）
    const char* crlf2 = "\r\n\r\n";
    const char* data = buf->peek();
    const char* end = static_cast<const char*>(
        memmem(data, buf->readableBytes(), crlf2, 4)
    );
    if (!end) {
        return;  // 还没收到完整的 HTTP 头部，继续等待
    }

    LOG_INFO << "data: " << std::string(data, end + 4 - data);
    
    // 2. 提取完整的 HTTP 请求头
    size_t headerLen = end - data + 4;
    std::string req(data, headerLen);
    
    // 3. 从 URL 参数中提取 token
    std::string token = ParseTokenFromHandshake(req);
    if (token.empty()) {
        LOG_ERROR << "No token in WebSocket handshake";
        conn->shutdown();
        return;
    }
    
    // 4. 通过 gRPC 向 logic 服务验证 token
    if (!logic_stub_) {
        LOG_ERROR << "Logic stub not initialized";
        conn->shutdown();
        return;
    }
    
    // 构造 gRPC 请求
    VerifyTokenRequest vreq;
    vreq.set_token(token);
    vreq.set_comet_id(comet_id_);  // 告知 logic 用户在哪个 comet 节点
    
    VerifyTokenReply vrep;
    grpc::ClientContext ctx_rpc;
    
    LOG_INFO << "Verifying token: " << token;
    
    // 调用 logic.VerifyToken（同步调用）
    auto status = logic_stub_->VerifyToken(&ctx_rpc, vreq, &vrep);
    LOG_INFO << "VerifyToken reply received " << vrep.user_id();
    
    // 检查 gRPC 调用是否成功，以及业务错误码
    if (!status.ok() || vrep.error().code() != 0) {
        std::string msg = status.ok() ? vrep.error().message()
                                      : status.error_message();
        LOG_ERROR << "VerifyToken failed: " << msg;
        conn->shutdown();
        return;
    }
    
    // 验证成功，获取用户 ID
    int64_t user_id = vrep.user_id();
    
    // 5. 提取 Sec-WebSocket-Key（客户端发送的随机值）
    std::string ws_key;
    if (!ExtractHeader(req, "Sec-WebSocket-Key", &ws_key) || ws_key.empty()) {
        LOG_ERROR << "No Sec-WebSocket-Key in WebSocket handshake";
        conn->shutdown();
        return;
    }
    
    // 6. 计算 Sec-WebSocket-Accept（服务端需返回的值）
    std::string accept_val = ComputeWebSocketAccept(ws_key);
    
    // 7. 构造并发送 101 Switching Protocols 响应
    std::string resp =
        "HTTP/1.1 101 Switching Protocols\r\n"
        "Upgrade: websocket\r\n"
        "Connection: Upgrade\r\n"
        "Sec-WebSocket-Accept: " + accept_val + "\r\n\r\n";
    conn->send(resp);
    
    // 8. 从缓冲区移除已处理的 HTTP 头部数据
    buf->retrieve(headerLen);
    
    // 9. 更新连接状态：从握手阶段切换到已建立
    ConnContext ctx = std::any_cast<ConnContext>(conn->getContext());
    ctx.state = ConnContext::kOpen;
    ctx.user_id = user_id;
    conn->setContext(ctx);
    
    // 10. 记录用户连接映射（用于后续消息推送）
    {
        std::lock_guard<std::mutex> lock(conns_mu_);
        user_conns_[user_id].insert(conn);
    }
    
    LOG_INFO << "WebSocket handshake done, user_id=" << user_id;
}

// 处理 WebSocket 数据帧
// 简化实现：仅处理 close 帧，其他帧忽略（课程演示版本）
// 完整实现需要处理：text/binary/ping/pong 等帧类型
void CometServer::HandleWebSocketFrame(const TcpConnectionPtr& conn,
                                       Buffer* buf,
                                       ConnContext& ctx) {
    // 循环处理缓冲区中的所有完整帧
    while (buf->readableBytes() >= 2) {
        const unsigned char* data =
            reinterpret_cast<const unsigned char*>(buf->peek());
        
        // 解析帧头第一个字节：FIN 标志位和 Opcode
        bool fin = (data[0] & 0x80) != 0;        // FIN=1 表示最后一个分片
        unsigned char opcode = data[0] & 0x0F;   // Opcode：0x1=text, 0x2=binary, 0x8=close
        
        // 解析帧头第二个字节：MASK 标志位和 Payload Length
        bool masked = (data[1] & 0x80) != 0;     // 客户端发送的帧必须有掩码
        uint64_t payloadLen = data[1] & 0x7F;    // 低 7 位是长度
        size_t headerLen = 2;                     // 基础帧头长度
        
        // 简化处理：要求帧必须是完整的（FIN=1）且有掩码
        if (!fin || !masked) {
            conn->shutdown();
            return;
        }
        
        // 解析扩展的 Payload Length（根据初始长度值）
        if (payloadLen == 126) {
            // 长度用接下来的 2 字节表示（16 位）
            if (buf->readableBytes() < headerLen + 2) return;
            const unsigned char* p = data + headerLen;
            payloadLen = (p[0] << 8) | p[1];
            headerLen += 2;
        } else if (payloadLen == 127) {
            // 长度用接下来的 8 字节表示（64 位）
            if (buf->readableBytes() < headerLen + 8) return;
            payloadLen = 0;
            const unsigned char* p = data + headerLen;
            for (int i = 0; i < 8; ++i) {
                payloadLen = (payloadLen << 8) | p[i];
            }
            headerLen += 8;
        }
        
        // 检查是否接收到完整的帧（包括 4 字节掩码和负载数据）
        if (buf->readableBytes() < headerLen + 4 + payloadLen) {
            return;  // 等待更多数据
        }
        
        // 跳过 4 字节的掩码
        headerLen += 4;
        
        // 从缓冲区移除整个帧（本示例不解析负载内容）
        buf->retrieve(headerLen + payloadLen);
        
        // 处理 close 帧：关闭连接
        if (opcode == 0x8) {
            conn->shutdown();
            return;
        }
        
        // 其他帧类型（text/binary/ping/pong）暂时忽略
        // 完整实现需要：解掩码、处理消息、发送 pong 响应等
    }
}

// 消息到达回调：根据连接状态分发处理
// 状态机设计：kHandshake（握手阶段）-> kOpen（已建立）
void CometServer::OnMessage(const TcpConnectionPtr& conn,
                            Buffer* buf,
                            muduo::Timestamp ts) {
    (void)ts;  // 忽略时间戳参数
    LOG_INFO << "OnMessage called, bytes=" << buf->readableBytes();
    
    // 获取连接上下文，判断当前状态
    ConnContext ctx = std::any_cast<ConnContext>(conn->getContext());
    
    if (ctx.state == ConnContext::kHandshake) {
        // 握手阶段：处理 HTTP -> WebSocket 升级
        HandleHandshake(conn, buf);
    } else {
        // 已建立阶段：处理 WebSocket 数据帧
        HandleWebSocketFrame(conn, buf, ctx);
    }
}

}  // namespace sparkpush
