#include "ws_client_lib.h"

#include <arpa/inet.h>
#include <netdb.h>
#include <sys/socket.h>
#include <unistd.h>

#include <chrono>
#include <cstring>
#include <iostream>
#include <nlohmann/json.hpp>
#include <sstream>
#include <thread>

/// @brief 安全解析字符串为 int，确保指针有效且完全转换。
bool ParseInt(const std::string& s, int* out) {
    if (!out) return false;
    char* end = nullptr;
    long v = std::strtol(s.c_str(), &end, 10);
    if (end == s.c_str() || *end != '\0') return false;
    *out = static_cast<int>(v);
    return true;
}

/// @brief 安全解析字符串为 long long，失败返回 false。
bool ParseLongLong(const std::string& s, long long* out) {
    if (!out) return false;
    char* end = nullptr;
    long long v = std::strtoll(s.c_str(), &end, 10);
    if (end == s.c_str() || *end != '\0') return false;
    *out = v;
    return true;
}

/// @brief 通用命令行解析，将网络/账号参数落到 Config。
bool ParseArgs(int argc, char* argv[], Config* cfg) {
    if (!cfg) return false;
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        auto next_val = [&](std::string* v) -> bool {
            if (i + 1 >= argc) return false;
            *v = argv[++i];
            return true;
        };

        if (arg == "--logic-host") {
            std::string v;
            if (!next_val(&v)) return false;
            cfg->logic_host = v;
        } else if (arg == "--logic-port") {
            std::string v;
            if (!next_val(&v) || !ParseInt(v, &cfg->logic_port)) return false;
        } else if (arg == "--comet-host") {
            std::string v;
            if (!next_val(&v)) return false;
            cfg->comet_host = v;
        } else if (arg == "--comet-port") {
            std::string v;
            if (!next_val(&v) || !ParseInt(v, &cfg->comet_port)) return false;
        } else if (arg == "--account") {
            std::string v;
            if (!next_val(&v)) return false;
            cfg->account = v;
        } else if (arg == "--password") {
            std::string v;
            if (!next_val(&v)) return false;
            cfg->password = v;
        } else if (arg == "--to-user") {
            std::string v;
            if (!next_val(&v) || !ParseLongLong(v, &cfg->to_user_id)) {
                return false;
            }
        } else if (arg == "--room-id") {
            std::string v;
            if (!next_val(&v) || !ParseLongLong(v, &cfg->room_id)) return false;
            cfg->is_chatroom = true;
        }
    }
    return true;
}

/// @brief 使用 getaddrinfo + connect 建立 TCP 连接。
int ConnectTcp(const std::string& host, int port) {
    struct addrinfo hints;
    std::memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;

    struct addrinfo* res = nullptr;
    std::string port_str = std::to_string(port);
    int ret = getaddrinfo(host.c_str(), port_str.c_str(), &hints, &res);
    if (ret != 0) {
        std::cerr << "getaddrinfo 失败: " << gai_strerror(ret) << "\n";
        return -1;
    }

    int sock = -1;
    for (struct addrinfo* p = res; p; p = p->ai_next) {
        sock = ::socket(p->ai_family, p->ai_socktype, p->ai_protocol);
        if (sock < 0) continue;
        if (::connect(sock, p->ai_addr, p->ai_addrlen) == 0) {
            break;  // 成功连接立即退出循环
        }
        ::close(sock);
        sock = -1;
    }
    freeaddrinfo(res);
    if (sock < 0) {
        std::cerr << "连接 " << host << ":" << port << " 失败\n";
    }
    return sock;
}

namespace {

/// @brief 阻塞读取指定长度字节，直到读满或出错。
bool ReadExact(int fd, void* buf, size_t len) {
    char* p = static_cast<char*>(buf);
    size_t n = 0;
    while (n < len) {
        ssize_t r = ::recv(fd, p + n, len - n, 0);
        if (r <= 0) return false;
        n += static_cast<size_t>(r);
    }
    return true;
}

}  // namespace

/// @brief 调用 logic 的 /api/login，解析返回 JSON，提取 user_id/token。
bool HttpPostLogin(const Config& cfg, long long* user_id, std::string* token) {
    if (!user_id || !token) return false;
    int fd = ConnectTcp(cfg.logic_host, cfg.logic_port);
    if (fd < 0) return false;

    nlohmann::json body_json;
    body_json["account"] = cfg.account;
    body_json["password"] = cfg.password;
    std::string body = body_json.dump();

    std::ostringstream oss;
    oss << "POST /api/login HTTP/1.1\r\n";
    oss << "Host: " << cfg.logic_host << ":" << cfg.logic_port << "\r\n";
    oss << "Content-Type: application/json\r\n";
    oss << "Connection: close\r\n";
    oss << "Content-Length: " << body.size() << "\r\n";
    oss << "\r\n";
    oss << body;

    std::string req = oss.str();
    ssize_t sent = ::send(fd, req.data(), req.size(), 0);
    if (sent != static_cast<ssize_t>(req.size())) {
        std::cerr << "发送 HTTP 请求失败\n";
        ::close(fd);
        return false;
    }

    // 读取响应头
    std::string header;
    char buf[1024];
    while (header.find("\r\n\r\n") == std::string::npos) {
        ssize_t r = ::recv(fd, buf, sizeof(buf), 0);
        if (r <= 0) {
            std::cerr << "读取 HTTP 响应失败\n";
            ::close(fd);
            return false;
        }
        header.append(buf, buf + r);
        if (header.size() > 64 * 1024) {
            std::cerr << "HTTP 头过大\n";
            ::close(fd);
            return false;
        }
    }
    auto pos = header.find("\r\n\r\n");
    std::string header_part = header.substr(0, pos + 4);
    std::string body_part = header.substr(pos + 4);

    // 简单解析状态码
    if (header_part.find("200") == std::string::npos) {
        std::cerr << "登录 HTTP 状态异常:\n" << header_part << "\n";
        ::close(fd);
        return false;
    }

    // Content-Length
    size_t content_length = 0;
    {
        std::string key = "Content-Length:";
        auto p = header_part.find(key);
        if (p != std::string::npos) {
            p += key.size();
            while (p < header_part.size() &&
                   (header_part[p] == ' ' || header_part[p] == '\t')) {
                ++p;
            }
            size_t end = p;
            while (end < header_part.size() && std::isdigit(header_part[end])) {
                ++end;
            }
            if (end > p) {
                content_length = std::stoul(header_part.substr(p, end - p));
            }
        }
    }

    std::string full_body = body_part;
    while (full_body.size() < content_length) {
        ssize_t r = ::recv(fd, buf, sizeof(buf), 0);
        if (r <= 0) break;
        full_body.append(buf, buf + r);
    }
    ::close(fd);

    try {
        auto j = nlohmann::json::parse(full_body);
        if (!j.contains("code") || j["code"].get<int>() != 0) {
            std::cerr << "登录失败, 响应: " << full_body << "\n";
            return false;
        }
        auto data = j["data"];
        if (!data.contains("user_id") || !data.contains("token")) {
            std::cerr << "登录响应缺少 user_id/token: " << full_body << "\n";
            return false;
        }
        *user_id = data["user_id"].get<long long>();
        *token = data["token"].get<std::string>();
        return true;
    } catch (const std::exception& e) {
        std::cerr << "解析登录 JSON 失败: " << e.what() << "\n";
        return false;
    }
}

/// @brief 构造最小合规的文本帧，固定掩码方便客户端快速发包。
std::string BuildClientWebSocketTextFrame(const std::string& payload) {
    std::string frame;
    unsigned char b1 = 0x81;  // FIN=1, text frame
    frame.push_back(static_cast<char>(b1));

    size_t len = payload.size();
    unsigned char b2 = 0x80;  // MASK=1
    if (len < 126) {
        b2 |= static_cast<unsigned char>(len);
        frame.push_back(static_cast<char>(b2));
    } else if (len <= 0xFFFF) {
        b2 |= 126;
        frame.push_back(static_cast<char>(b2));
        frame.push_back(static_cast<char>((len >> 8) & 0xFF));
        frame.push_back(static_cast<char>(len & 0xFF));
    } else {
        b2 |= 127;
        frame.push_back(static_cast<char>(b2));
        for (int i = 7; i >= 0; --i) {
            frame.push_back(static_cast<char>((len >> (8 * i)) & 0xFF));
        }
    }

    // 简单用固定 mask key，足够满足协议要求
    unsigned char mask[4] = {0x12, 0x34, 0x56, 0x78};
    frame.append(reinterpret_cast<char*>(mask),
                 reinterpret_cast<char*>(mask) + 4);

    for (size_t i = 0; i < len; ++i) {
        char c = static_cast<char>(payload[i] ^ mask[i % 4]);
        frame.push_back(c);
    }
    return frame;
}

/// @brief 发送 WebSocket 握手请求并验证 101 Switching Protocols。
bool WebSocketHandshake(int fd,
                        const std::string& host,
                        int port,
                        const std::string& token) {
    std::ostringstream oss;
    oss << "GET /ws?token=" << token << " HTTP/1.1\r\n";
    oss << "Host: " << host << ":" << port << "\r\n";
    oss << "Upgrade: websocket\r\n";
    oss << "Connection: Upgrade\r\n";
    // 使用固定 Sec-WebSocket-Key 即可，服务端只用来计算 Accept
    oss << "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n";
    oss << "Sec-WebSocket-Version: 13\r\n";
    oss << "\r\n";

    std::string req = oss.str();
    ssize_t sent = ::send(fd, req.data(), req.size(), 0);
    if (sent != static_cast<ssize_t>(req.size())) {
        std::cerr << "发送 WebSocket 握手请求失败\n";
        return false;
    }

    std::string resp;
    char buf[1024];
    while (resp.find("\r\n\r\n") == std::string::npos) {
        ssize_t r = ::recv(fd, buf, sizeof(buf), 0);
        if (r <= 0) {
            std::cerr << "读取 WebSocket 握手响应失败\n";
            return false;
        }
        resp.append(buf, buf + r);
        if (resp.size() > 64 * 1024) {
            std::cerr << "WebSocket 握手响应过大\n";
            return false;
        }
    }
    if (resp.find("101") == std::string::npos ||
        resp.find("Switching Protocols") == std::string::npos) {
        std::cerr << "WebSocket 握手失败，响应:\n" << resp << "\n";
        return false;
    }
    std::cout << "WebSocket 握手成功\n";
    return true;
}

/// @brief 简易接收循环，假定服务器只发送小文本帧且不分片。
void ReceiveLoop(int fd) {
    // 仅做非常简单的输出：假设服务端只发小文本帧且不分片
    while (true) {
        unsigned char hdr[2];
        if (!ReadExact(fd, hdr, 2)) {
            std::cerr << "接收 WebSocket 头失败，连接可能已关闭\n";
            break;
        }
        bool fin = (hdr[0] & 0x80) != 0;
        unsigned char opcode = hdr[0] & 0x0F;
        bool masked = (hdr[1] & 0x80) != 0;
        uint64_t payload_len = hdr[1] & 0x7F;
        if (!fin) {
            std::cerr << "暂不支持分片帧，断开连接\n";
            break;
        }
        if (payload_len == 126) {
            unsigned char ext[2];
            if (!ReadExact(fd, ext, 2)) break;
            payload_len = (ext[0] << 8) | ext[1];
        } else if (payload_len == 127) {
            unsigned char ext[8];
            if (!ReadExact(fd, ext, 8)) break;
            payload_len = 0;
            for (int i = 0; i < 8; ++i) {
                payload_len = (payload_len << 8) | ext[i];
            }
        }
        unsigned char mask[4] = {0, 0, 0, 0};
        if (masked) {
            if (!ReadExact(fd, mask, 4)) break;
        }
        if (payload_len > 10 * 1024 * 1024) {
            std::cerr << "Payload 太大，停止接收\n";
            break;
        }
        std::string payload;
        payload.resize(payload_len);
        if (!ReadExact(fd, &payload[0], payload_len)) break;
        if (masked) {
            for (uint64_t i = 0; i < payload_len; ++i) {
                payload[i] =
                    static_cast<char>(payload[i] ^ mask[i % 4]);
            }
        }
        if (opcode == 0x8) {
            std::cout << "收到 close 帧，退出接收循环\n";
            break;
        } else if (opcode == 0x1) {
            std::cout << "[RECV] " << payload << "\n";
        }
    }
}


