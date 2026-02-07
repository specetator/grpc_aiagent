/**
 * async_ws_bench.cpp - 高性能异步WebSocket压测工具
 * 
 * 设计原则：
 * 1. 零等待：发送不阻塞，立即入队
 * 2. 事件驱动：epoll管理所有IO
 * 3. 批量发送：一次性填充发送队列
 * 4. 流水线：发送和接收并行
 */

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/tcp.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstring>
#include <deque>
#include <functional>
#include <iomanip>
#include <iostream>
#include <memory>
#include <random>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include <nlohmann/json.hpp>

// ============================================================================
// 工具函数
// ============================================================================

namespace {

bool SetNonBlocking(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags < 0) return false;
    return fcntl(fd, F_SETFL, flags | O_NONBLOCK) >= 0;
}

bool SetTcpNoDelay(int fd) {
    int opt = 1;
    return setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &opt, sizeof(opt)) == 0;
}

bool SetSendBuf(int fd, int size) {
    return setsockopt(fd, SOL_SOCKET, SO_SNDBUF, &size, sizeof(size)) == 0;
}

uint32_t RandomMask() {
    static thread_local std::mt19937 gen(std::random_device{}());
    return gen();
}

std::string Base64Encode(const unsigned char* data, size_t len) {
    static const char kTable[] =
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
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
        if (i + 1 < len) n |= (data[i + 1] << 8);
        out.push_back(kTable[(n >> 18) & 0x3F]);
        out.push_back(kTable[(n >> 12) & 0x3F]);
        out.push_back(i + 1 < len ? kTable[(n >> 6) & 0x3F] : '=');
        out.push_back('=');
    }
    return out;
}

std::string GenerateWebSocketKey() {
    unsigned char bytes[16];
    std::mt19937 gen(std::random_device{}());
    for (int i = 0; i < 16; ++i) bytes[i] = static_cast<unsigned char>(gen() & 0xFF);
    return Base64Encode(bytes, 16);
}

// 预生成WebSocket帧，避免运行时构造开销
std::string BuildWebSocketFrame(const std::string& payload) {
    std::string frame;
    size_t len = payload.size();
    frame.reserve(len + 14);
    
    frame.push_back(static_cast<char>(0x81));  // FIN + Text
    
    if (len < 126) {
        frame.push_back(static_cast<char>(0x80 | len));
    } else if (len < 65536) {
        frame.push_back(static_cast<char>(0xFE));
        frame.push_back(static_cast<char>((len >> 8) & 0xFF));
        frame.push_back(static_cast<char>(len & 0xFF));
    } else {
        frame.push_back(static_cast<char>(0xFF));
        for (int i = 7; i >= 0; --i)
            frame.push_back(static_cast<char>((len >> (i * 8)) & 0xFF));
    }
    
    uint32_t mask = RandomMask();
    unsigned char mb[4] = {
        (unsigned char)((mask >> 24) & 0xFF),
        (unsigned char)((mask >> 16) & 0xFF),
        (unsigned char)((mask >> 8) & 0xFF),
        (unsigned char)(mask & 0xFF)
    };
    frame.append(reinterpret_cast<char*>(mb), 4);
    
    for (size_t i = 0; i < len; ++i)
        frame.push_back(payload[i] ^ mb[i % 4]);
    
    return frame;
}

}  // namespace

// ============================================================================
// 统计
// ============================================================================

struct Stats {
    std::atomic<int64_t> conn_attempted{0};
    std::atomic<int64_t> conn_established{0};
    std::atomic<int64_t> conn_failed{0};
    std::atomic<int64_t> msg_sent{0};
    std::atomic<int64_t> msg_recv{0};
    std::atomic<int64_t> bytes_sent{0};
    std::atomic<int64_t> bytes_recv{0};
    std::atomic<int64_t> send_eagain{0};  // EAGAIN次数
    std::atomic<int64_t> send_errors{0};
};

// ============================================================================
// 高性能异步客户端
// ============================================================================

class AsyncClient {
public:
    enum State { kInit, kConnecting, kHandshaking, kConnected, kSending, kDone, kClosed };

    AsyncClient(int id, Stats* stats)
        : id_(id), stats_(stats), fd_(-1), state_(kInit),
          send_offset_(0), msg_to_send_(0), msg_sent_(0) {}

    ~AsyncClient() { Close(); }

    int fd() const { return fd_; }
    State state() const { return state_; }
    int id() const { return id_; }

    bool StartConnect(const std::string& host, int port, const std::string& token) {
        host_ = host;
        port_ = port;
        token_ = token;

        fd_ = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0);
        if (fd_ < 0) return false;

        SetTcpNoDelay(fd_);
        SetSendBuf(fd_, 256 * 1024);  // 256KB发送缓冲区

        struct sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port = htons(port);
        inet_pton(AF_INET, host.c_str(), &addr.sin_addr);

        stats_->conn_attempted++;
        int ret = connect(fd_, (struct sockaddr*)&addr, sizeof(addr));
        if (ret == 0 || errno == EINPROGRESS) {
            state_ = kConnecting;
            return true;
        }
        close(fd_);
        fd_ = -1;
        stats_->conn_failed++;
        return false;
    }

    // 连接完成，发送握手
    void OnConnected() {
        int error = 0;
        socklen_t len = sizeof(error);
        if (getsockopt(fd_, SOL_SOCKET, SO_ERROR, &error, &len) < 0 || error != 0) {
            state_ = kClosed;
            stats_->conn_failed++;
            return;
        }
        stats_->conn_established++;

        // 构造握手请求
        std::string key = GenerateWebSocketKey();
        handshake_req_ =
            "GET /ws?token=" + token_ + " HTTP/1.1\r\n"
            "Host: " + host_ + ":" + std::to_string(port_) + "\r\n"
            "Upgrade: websocket\r\n"
            "Connection: Upgrade\r\n"
            "Sec-WebSocket-Key: " + key + "\r\n"
            "Sec-WebSocket-Version: 13\r\n\r\n";
        
        send_buf_ = handshake_req_;
        send_offset_ = 0;
        state_ = kHandshaking;
    }

    // 准备发送消息（预先填充队列）
    void PrepareMessages(int count, int to_user_id, int room_id = 0) {
        msg_to_send_ = count;
        msg_sent_ = 0;
        to_user_id_ = to_user_id;
        room_id_ = room_id;
        
        // 预生成所有消息帧
        msg_frames_.clear();
        msg_frames_.reserve(count + (room_id > 0 ? 1 : 0));
        
        // 如果是房间模式，先发送加入房间消息
        if (room_id > 0) {
            nlohmann::json join_msg;
            join_msg["type"] = "chatroom_join";
            join_msg["group_id"] = room_id;
            msg_frames_.push_back(BuildWebSocketFrame(join_msg.dump()));
            msg_to_send_++;  // 加入房间也算一条消息
        }
        
        for (int i = 0; i < count; ++i) {
            nlohmann::json j;
            if (room_id > 0) {
                // 房间聊天模式
                j["type"] = "chatroom";
                j["group_id"] = room_id;
            } else {
                // 单聊模式
                j["type"] = "single_chat";
                j["to_user_id"] = to_user_id;
            }
            j["client_msg_id"] = std::to_string(id_) + "-" + std::to_string(i);
            j["content"] = {{"text", "b"}};  // 最小payload
            msg_frames_.push_back(BuildWebSocketFrame(j.dump()));
        }
    }

    // 处理可写事件 - 核心发送逻辑
    // 返回: true=继续, false=关闭连接
    bool OnWritable() {
        // 先发送缓冲区中的数据
        while (send_offset_ < send_buf_.size()) {
            ssize_t n = ::send(fd_, send_buf_.data() + send_offset_,
                               send_buf_.size() - send_offset_, MSG_NOSIGNAL);
            if (n > 0) {
                stats_->bytes_sent += n;
                send_offset_ += n;
            } else if (n == 0) {
                return false;
            } else {
                if (errno == EAGAIN || errno == EWOULDBLOCK) {
                    stats_->send_eagain++;
                    return true;  // 等待下次EPOLLOUT
                }
                stats_->send_errors++;
                return false;
            }
        }

        // 缓冲区发完了
        if (state_ == kHandshaking) {
            // 握手请求已发送，等待响应
            send_buf_.clear();
            send_offset_ = 0;
            return true;
        }

        if (state_ == kConnected || state_ == kSending) {
            state_ = kSending;
            
            // 批量填充发送缓冲区
            send_buf_.clear();
            send_offset_ = 0;
            
            // 一次最多填充64KB或剩余所有消息
            const size_t kMaxBatch = 64 * 1024;
            while (msg_sent_ < msg_to_send_ && send_buf_.size() < kMaxBatch) {
                send_buf_ += msg_frames_[msg_sent_];
                msg_sent_++;
                stats_->msg_sent++;
            }

            if (send_buf_.empty()) {
                // 所有消息已发送
                state_ = kDone;
                return true;
            }

            // 立即尝试发送
            return OnWritable();
        }

        return true;
    }

    // 处理可读事件
    bool OnReadable() {
        char buf[8192];
        while (true) {
            ssize_t n = ::recv(fd_, buf, sizeof(buf), 0);
            if (n > 0) {
                stats_->bytes_recv += n;
                recv_buf_.append(buf, n);
                
                if (state_ == kHandshaking) {
                    if (recv_buf_.find("\r\n\r\n") != std::string::npos) {
                        if (recv_buf_.find("101") != std::string::npos) {
                            state_ = kConnected;
                            auto pos = recv_buf_.find("\r\n\r\n");
                            recv_buf_ = recv_buf_.substr(pos + 4);
                        } else {
                            return false;
                        }
                    }
                } else {
                    ParseFrames();
                }
            } else if (n == 0) {
                return false;
            } else {
                if (errno == EAGAIN || errno == EWOULDBLOCK) {
                    return true;
                }
                return false;
            }
        }
    }

    bool HasPendingWrite() const {
        return send_offset_ < send_buf_.size() || 
               (state_ == kConnected && msg_sent_ < msg_to_send_);
    }

    bool IsDone() const { return state_ == kDone; }

    void Close() {
        if (fd_ >= 0) {
            close(fd_);
            fd_ = -1;
        }
        state_ = kClosed;
    }

private:
    void ParseFrames() {
        while (recv_buf_.size() >= 2) {
            const unsigned char* d = (const unsigned char*)recv_buf_.data();
            uint64_t plen = d[1] & 0x7F;
            size_t hlen = 2;
            
            if (plen == 126) {
                if (recv_buf_.size() < 4) return;
                plen = (d[2] << 8) | d[3];
                hlen = 4;
            } else if (plen == 127) {
                if (recv_buf_.size() < 10) return;
                plen = 0;
                for (int i = 0; i < 8; ++i)
                    plen = (plen << 8) | d[2 + i];
                hlen = 10;
            }
            
            if (recv_buf_.size() < hlen + plen) return;
            
            unsigned char opcode = d[0] & 0x0F;
            if (opcode == 0x01 || opcode == 0x02) {
                stats_->msg_recv++;
            }
            recv_buf_ = recv_buf_.substr(hlen + plen);
        }
    }

    int id_;
    Stats* stats_;
    int fd_;
    State state_;
    
    std::string host_;
    int port_;
    std::string token_;
    int to_user_id_;
    int room_id_;
    
    std::string handshake_req_;
    std::string send_buf_;
    size_t send_offset_;
    std::string recv_buf_;
    
    int msg_to_send_;
    int msg_sent_;
    std::vector<std::string> msg_frames_;  // 预生成的帧
};

// ============================================================================
// Worker线程
// ============================================================================

class Worker {
public:
    Worker(int id, const std::string& host, int port,
           const std::vector<std::string>& tokens,
           int msgs_per_conn, int to_user_id, int room_id, Stats* stats)
        : id_(id), host_(host), port_(port), tokens_(tokens),
          msgs_per_conn_(msgs_per_conn), to_user_id_(to_user_id),
          room_id_(room_id), stats_(stats), running_(false) {}

    void Start() {
        running_ = true;
        thread_ = std::thread(&Worker::Run, this);
    }

    void Stop() {
        running_ = false;
        if (thread_.joinable()) thread_.join();
    }

private:
    void Run() {
        int epfd = epoll_create1(0);
        if (epfd < 0) return;

        std::unordered_map<int, std::unique_ptr<AsyncClient>> clients;
        int done_count = 0;
        int total = tokens_.size();

        // 创建所有连接
        for (size_t i = 0; i < tokens_.size(); ++i) {
            auto c = std::make_unique<AsyncClient>(id_ * 10000 + i, stats_);
            if (c->StartConnect(host_, port_, tokens_[i])) {
                c->PrepareMessages(msgs_per_conn_, to_user_id_, room_id_);
                
                struct epoll_event ev;
                ev.events = EPOLLIN | EPOLLOUT | EPOLLET;
                ev.data.ptr = c.get();
                epoll_ctl(epfd, EPOLL_CTL_ADD, c->fd(), &ev);
                
                clients[c->fd()] = std::move(c);
            }
        }

        struct epoll_event events[512];

        while (running_ && done_count < total) {
            int n = epoll_wait(epfd, events, 512, 1);  // 1ms超时，快速响应
            
            for (int i = 0; i < n; ++i) {
                AsyncClient* c = (AsyncClient*)events[i].data.ptr;
                uint32_t ev = events[i].events;
                bool should_close = false;

                if (ev & (EPOLLERR | EPOLLHUP)) {
                    should_close = true;
                } else {
                    // 处理连接完成
                    if (c->state() == AsyncClient::kConnecting && (ev & EPOLLOUT)) {
                        c->OnConnected();
                    }

                    // 处理读
                    if (ev & EPOLLIN) {
                        if (!c->OnReadable()) should_close = true;
                    }

                    // 处理写
                    if (ev & EPOLLOUT) {
                        if (!c->OnWritable()) should_close = true;
                    }

                    // 检查是否完成
                    if (c->IsDone()) {
                        done_count++;
                        should_close = true;
                    }
                }

                if (should_close) {
                    epoll_ctl(epfd, EPOLL_CTL_DEL, c->fd(), nullptr);
                    clients.erase(c->fd());
                }
            }

            // 主动触发写（处理EAGAIN后的重试）
            for (auto& kv : clients) {
                AsyncClient* c = kv.second.get();
                if (c->HasPendingWrite() && c->state() >= AsyncClient::kConnected) {
                    c->OnWritable();
                }
            }
        }

        close(epfd);
    }

    int id_;
    std::string host_;
    int port_;
    std::vector<std::string> tokens_;
    int msgs_per_conn_;
    int to_user_id_;
    int room_id_;
    Stats* stats_;
    std::atomic<bool> running_;
    std::thread thread_;
};

// ============================================================================
// HTTP登录
// ============================================================================

bool HttpLogin(const std::string& host, int port,
               const std::string& account, const std::string& password,
               int64_t* uid, std::string* token) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return false;

    struct sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    inet_pton(AF_INET, host.c_str(), &addr.sin_addr);

    if (connect(fd, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        close(fd);
        return false;
    }

    nlohmann::json body;
    body["account"] = account;
    body["password"] = password;
    std::string bs = body.dump();

    std::string req =
        "POST /api/login HTTP/1.1\r\n"
        "Host: " + host + "\r\n"
        "Content-Type: application/json\r\n"
        "Content-Length: " + std::to_string(bs.size()) + "\r\n"
        "Connection: close\r\n\r\n" + bs;

    send(fd, req.data(), req.size(), 0);

    std::string resp;
    char buf[1024];
    while (true) {
        ssize_t n = recv(fd, buf, sizeof(buf), 0);
        if (n <= 0) break;
        resp.append(buf, n);
    }
    close(fd);

    auto pos = resp.find("\r\n\r\n");
    if (pos == std::string::npos) return false;
    
    try {
        auto j = nlohmann::json::parse(resp.substr(pos + 4));
        if (j["code"].get<int>() != 0) return false;
        *uid = j["data"]["user_id"].get<int64_t>();
        *token = j["data"]["token"].get<std::string>();
        return true;
    } catch (...) {
        return false;
    }
}

bool HttpRegister(const std::string& host, int port,
                  const std::string& account, const std::string& password) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return false;

    struct sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    inet_pton(AF_INET, host.c_str(), &addr.sin_addr);

    if (connect(fd, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        close(fd);
        return false;
    }

    nlohmann::json body;
    body["account"] = account;
    body["password"] = password;
    body["name"] = account;
    std::string bs = body.dump();

    std::string req =
        "POST /api/register HTTP/1.1\r\n"
        "Host: " + host + "\r\n"
        "Content-Type: application/json\r\n"
        "Content-Length: " + std::to_string(bs.size()) + "\r\n"
        "Connection: close\r\n\r\n" + bs;

    send(fd, req.data(), req.size(), 0);
    char buf[1024];
    recv(fd, buf, sizeof(buf), 0);
    close(fd);
    return true;
}

// ============================================================================
// Main
// ============================================================================

void PrintUsage(const char* prog) {
    std::cout << "用法: " << prog << " [选项]\n"
              << "  --logic-host HOST     Logic地址 (127.0.0.1)\n"
              << "  --logic-port PORT     Logic端口 (9101)\n"
              << "  --comet-host HOST     Comet地址 (127.0.0.1)\n"
              << "  --comet-port PORT     Comet端口 (9200)\n"
              << "  --connections N       连接数 (100)\n"
              << "  --workers N           线程数 (4)\n"
              << "  --messages N          每连接消息数 (1000)\n"
              << "  --to-user ID          目标用户ID，单聊模式 (1)\n"
              << "  --room-id ID          房间ID，房间聊天模式 (0=禁用)\n"
              << "  --account PREFIX      账号前缀 (bench_)\n"
              << "  --password PWD        密码 (bench123)\n";
}

int main(int argc, char* argv[]) {
    std::string logic_host = "127.0.0.1";
    int logic_port = 9101;
    std::string comet_host = "127.0.0.1";
    int comet_port = 9200;
    int connections = 100;
    int workers = 4;
    int messages = 1000;
    int to_user_id = 1;
    int room_id = 0;  // 0=单聊模式，>0=房间聊天模式
    std::string account_prefix = "bench_";
    std::string password = "bench123";

    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        auto next = [&]() -> std::string { return i + 1 < argc ? argv[++i] : ""; };
        
        if (arg == "--logic-host") logic_host = next();
        else if (arg == "--logic-port") logic_port = std::stoi(next());
        else if (arg == "--comet-host") comet_host = next();
        else if (arg == "--comet-port") comet_port = std::stoi(next());
        else if (arg == "--connections") connections = std::stoi(next());
        else if (arg == "--workers") workers = std::stoi(next());
        else if (arg == "--messages") messages = std::stoi(next());
        else if (arg == "--to-user") to_user_id = std::stoi(next());
        else if (arg == "--room-id") room_id = std::stoi(next());
        else if (arg == "--account") account_prefix = next();
        else if (arg == "--password") password = next();
        else if (arg == "-h" || arg == "--help") { PrintUsage(argv[0]); return 0; }
    }

    std::string mode = room_id > 0 ? "房间聊天(room_id=" + std::to_string(room_id) + ")" 
                                   : "单聊(to_user=" + std::to_string(to_user_id) + ")";
    std::cout << "========================================\n"
              << "   高性能异步WebSocket压测 (epoll)\n"
              << "========================================\n"
              << "Comet: " << comet_host << ":" << comet_port << "\n"
              << "模式: " << mode << "\n"
              << "连接数: " << connections << ", 线程数: " << workers << "\n"
              << "每连接消息: " << messages << "\n"
              << "预期总消息: " << (int64_t)connections * messages << "\n"
              << "========================================\n\n";

    // 1. 准备用户
    std::cout << "[1/3] 准备用户...\n";
    std::vector<std::string> tokens;
    tokens.reserve(connections);
    
    for (int i = 0; i < connections; ++i) {
        std::string acc = account_prefix + std::to_string(i);
        HttpRegister(logic_host, logic_port, acc, password);
        
        int64_t uid;
        std::string tok;
        if (HttpLogin(logic_host, logic_port, acc, password, &uid, &tok)) {
            tokens.push_back(tok);
        }
        
        if ((i + 1) % 50 == 0 || i + 1 == connections) {
            std::cout << "\r  进度: " << (i + 1) << "/" << connections << std::flush;
        }
    }
    std::cout << "\n  获得 " << tokens.size() << " tokens\n\n";

    if (tokens.empty()) {
        std::cerr << "无可用token\n";
        return 1;
    }

    // 2. 启动压测
    std::cout << "[2/3] 开始压测...\n\n";
    Stats stats;
    std::vector<std::unique_ptr<Worker>> ws;
    
    int per = (tokens.size() + workers - 1) / workers;
    int idx = 0;
    for (int w = 0; w < workers && idx < (int)tokens.size(); ++w) {
        std::vector<std::string> wt;
        for (int j = 0; j < per && idx < (int)tokens.size(); ++j)
            wt.push_back(tokens[idx++]);
        ws.push_back(std::make_unique<Worker>(w, comet_host, comet_port, wt, messages, to_user_id, room_id, &stats));
    }

    auto t0 = std::chrono::steady_clock::now();
    for (auto& w : ws) w->Start();

    int64_t expected = (int64_t)tokens.size() * messages;
    
    // 实时统计
    while (true) {
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
        
        auto now = std::chrono::steady_clock::now();
        double sec = std::chrono::duration<double>(now - t0).count();
        
        int64_t sent = stats.msg_sent.load();
        int64_t recv = stats.msg_recv.load();
        int64_t conns = stats.conn_established.load();
        int64_t eagain = stats.send_eagain.load();
        
        std::cout << "\r[" << std::fixed << std::setprecision(1) << sec << "s] "
                  << "连接:" << conns << " "
                  << "发送:" << sent << "(" << (int)(sent/sec) << "/s) "
                  << "接收:" << recv << "(" << (int)(recv/sec) << "/s) "
                  << "EAGAIN:" << eagain << "   " << std::flush;
        
        if (sent >= expected) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
            break;
        }
        if (sec > 120) break;  // 2分钟超时
    }

    // 3. 统计
    std::cout << "\n\n[3/3] 统计结果\n";
    for (auto& w : ws) w->Stop();

    auto t1 = std::chrono::steady_clock::now();
    double total_sec = std::chrono::duration<double>(t1 - t0).count();

    std::cout << "\n========================================\n"
              << "              压测结果\n"
              << "========================================\n"
              << "连接成功: " << stats.conn_established.load() << "/" << stats.conn_attempted.load() << "\n"
              << "消息发送: " << stats.msg_sent.load() << "\n"
              << "消息接收: " << stats.msg_recv.load() << "\n"
              << "EAGAIN数: " << stats.send_eagain.load() << "\n"
              << "发送错误: " << stats.send_errors.load() << "\n"
              << "发送字节: " << stats.bytes_sent.load() / 1024 << " KB\n"
              << "接收字节: " << stats.bytes_recv.load() / 1024 << " KB\n"
              << "总耗时: " << std::fixed << std::setprecision(2) << total_sec << " s\n"
              << "----------------------------------------\n"
              << "发送QPS: " << (int)(stats.msg_sent.load() / total_sec) << "\n"
              << "接收QPS: " << (int)(stats.msg_recv.load() / total_sec) << "\n"
              << "========================================\n";

    return 0;
}
