#include "comet_server.h"

#include <grpcpp/grpcpp.h>

#include <cstring>
#include <array>
#include <cstdint>
#include <vector>

namespace sparkpush {

namespace {

bool ExtractHeader(const std::string& req,
                                      const std::string& header_name,
                                      std::string* value) {
    if (!value) return false;
    std::string key = header_name + ":";
    auto pos = req.find(key);
    if (pos == std::string::npos) return false;
    pos += key.size();
    while (pos < req.size() &&
                  (req[pos] == ' ' || req[pos] == '\t')) {
        ++pos;
    }
    if (pos >= req.size()) return false;
    auto end = req.find("\r\n", pos);
    if (end == std::string::npos) {
        end = req.size();
    }
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

std::array<unsigned char, 20> SHA1(const std::string& data) {
    uint32_t state[5] = {
            0x67452301u,
            0xEFCDAB89u,
            0x98BADCFEu,
            0x10325476u,
            0xC3D2E1F0u,
    };

    std::vector<unsigned char> msg(data.begin(), data.end());
    uint64_t bit_len = static_cast<uint64_t>(data.size()) * 8;

    msg.push_back(0x80);
    while ((msg.size() % 64) != 56) {
        msg.push_back(0x00);
    }
    for (int i = 7; i >= 0; --i) {
        msg.push_back(static_cast<unsigned char>(
                (bit_len >> (8 * i)) & 0xFF));
    }

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

std::string ComputeWebSocketAccept(const std::string& client_key) {
    static const std::string kGuid =
            "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
    auto digest = SHA1(client_key + kGuid);
    return Base64Encode(digest.data(), digest.size());
}

}  // namespace

CometServer::CometServer(EventLoop* loop, const Config& cfg)
        : server_(loop,
                            muduo::net::InetAddress(cfg.listen_port),
                            "comet_server") {
    comet_id_ = cfg.comet_id;
    auto channel =
            grpc::CreateChannel(cfg.logic_grpc_target,
                                                    grpc::InsecureChannelCredentials());
    logic_stub_ = sparkpush::LogicService::NewStub(channel);

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
    server_.start();
}

void CometServer::PushToUsers(const ChatMessage& msg,
                                                            const std::vector<int64_t>& user_ids) {
    std::string payload = msg.content_json().empty()
                                                        ? ("{\"msg_id\":\"" + msg.msg_id() + "\"}")
                                                        : msg.content_json();
    std::string frame = BuildWebSocketTextFrame(payload);
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
            c->send(frame);
        }
    }
}

void CometServer::OnConnection(const TcpConnectionPtr& conn) {
    if (conn->connected()) {
        ConnContext ctx;
        ctx.state = ConnContext::kHandshake;
        conn->setContext(ctx);
        LOG_INFO << "New TCP connection from " << conn->peerAddress().toIpPort();
    } else {
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
        }
        if (need_offline && offline_uid > 0) {
            NotifyUserOffline(offline_uid);
        }
        LOG_INFO << "Connection closed";
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
        return;
    }
    size_t headerLen = end - data + 4;
    std::string req(data, headerLen);

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
    VerifyTokenReply vrep;
    grpc::ClientContext ctx_rpc;
    LOG_INFO << "Verifying token: " << token;
    auto status = logic_stub_->VerifyToken(&ctx_rpc, vreq, &vrep);
    LOG_INFO << "VerifyToken reply received " << vrep.user_id();
    if (!status.ok() || vrep.error().code() != 0) {
        std::string msg = status.ok() ? vrep.error().message()
                                                                    : status.error_message();
        LOG_ERROR << "VerifyToken failed: " << msg;
        conn->shutdown();
        return;
    }
    int64_t user_id = vrep.user_id();

    std::string ws_key;
    if (!ExtractHeader(req, "Sec-WebSocket-Key", &ws_key) ||
            ws_key.empty()) {
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

    ConnContext ctx = std::any_cast<ConnContext>(conn->getContext());
    ctx.state = ConnContext::kOpen;
    ctx.user_id = user_id;
    conn->setContext(ctx);

    {
        std::lock_guard<std::mutex> lock(conns_mu_);
        user_conns_[user_id].insert(conn);
    }
    LOG_INFO << "WebSocket handshake done, user_id=" << user_id;
}

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
            return;
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

        if (opcode == 0x8) {
            conn->shutdown();
            return;
        } else if (opcode == 0x1) {
            OnTextMessage(conn, ctx, payload);
        }
    }
}

void CometServer::OnTextMessage(const TcpConnectionPtr& conn,
                                                                ConnContext& ctx,
                                                                const std::string& payload) {
    LOG_INFO << "Recv text from user " << ctx.user_id << ": " << payload;

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

    UpstreamMessageRequest req;
    req.set_from_user_id(ctx.user_id);
    req.set_scene(scene);
    req.set_client_msg_id(meta.client_msg_id);
    if (scene == "single") {
        req.set_to_user_id(meta.to_user_id);
    } 
    req.set_content_json(payload);

    UpstreamMessageReply rep;
    grpc::ClientContext rpc_ctx;
    auto status = logic_stub_->SendUpstreamMessage(&rpc_ctx, req, &rep);
    if (!status.ok() || rep.error().code() != 0) {
        std::string msg = status.ok() ? rep.error().message()
                                                                    : status.error_message();
        LOG_ERROR << "SendUpstreamMessage failed: " << msg;
        std::string frame = BuildWebSocketTextFrame(
                "{\"type\":\"error\",\"message\":\"send failed\"}");
        conn->send(frame);
        return;
    }

    std::string frame = BuildWebSocketTextFrame(
            "{\"type\":\"ack\",\"msg_id\":\"" + rep.message().msg_id() +
            "\"}");
    conn->send(frame);
}

void CometServer::OnMessage(const TcpConnectionPtr& conn,
                                                        Buffer* buf,
                                                        muduo::Timestamp ts) {
    (void)ts;
    LOG_INFO << "OnMessage called, bytes=" << buf->readableBytes();
    ConnContext ctx = std::any_cast<ConnContext>(conn->getContext());
    if (ctx.state == ConnContext::kHandshake) {
        HandleHandshake(conn, buf);
    } else {
        HandleWebSocketFrame(conn, buf, ctx);
    }
}

void CometServer::NotifyUserOffline(int64_t user_id) {
    if (!logic_stub_) return;
    auto* stub = logic_stub_.get();
    std::string comet_id = comet_id_;

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
 
  

}  // namespace sparkpush
