/**
 * HTTP消息推送性能测试工具 (高性能版)
 * 
 * 特性：
 * - 多线程 + 多连接
 * - HTTP流水线（pipelining）：连续发送请求，异步接收响应
 * - epoll事件驱动
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
#include <iomanip>
#include <iostream>
#include <mutex>
#include <queue>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

// 配置参数
struct Config {
    std::string host = "127.0.0.1";
    int port = 9101;
    int connections = 100;        // 总连接数
    int messages_per_conn = 100;  // 每连接消息数
    int threads = 4;              // 工作线程数
    int pipeline_depth = 10;      // 流水线深度（同时在途请求数）
    std::string endpoint = "/api/message/send";
    int64_t from_user_id = 1;
    int64_t to_user_id = 2;
    std::string token;            // Bearer token (鉴权用)
};

// 全局统计
std::atomic<int64_t> g_requests_sent{0};
std::atomic<int64_t> g_responses_received{0};
std::atomic<int64_t> g_errors{0};
std::atomic<int64_t> g_bytes_sent{0};
std::atomic<int64_t> g_bytes_received{0};
std::atomic<int64_t> g_active_conns{0};

// 连接状态
struct Connection {
    int fd = -1;
    bool connected = false;
    
    // 发送状态
    std::string send_buffer;
    size_t send_offset = 0;
    
    // 接收状态
    std::string recv_buffer;
    int pending_responses = 0;  // 等待响应数
    
    // 消息计数
    int messages_sent = 0;
    int messages_to_send = 0;
    int64_t from_user_id = 0;
    int64_t to_user_id = 0;
    
    // 流水线配置
    int pipeline_depth = 10;
};

// 构建HTTP请求
std::string BuildHttpRequest(const std::string& host, int port, 
                              const std::string& endpoint,
                              int64_t from_user, int64_t to_user,
                              int msg_id,
                              const std::string& token = "") {
    std::ostringstream body;
    body << "{\"from_user_id\":" << from_user 
         << ",\"to_user_id\":" << to_user
         << ",\"content\":\"bench_" << msg_id << "\"}";
    std::string body_str = body.str();
    
    std::ostringstream req;
    req << "POST " << endpoint << " HTTP/1.1\r\n"
        << "Host: " << host << ":" << port << "\r\n"
        << "Content-Type: application/json\r\n"
        << "Content-Length: " << body_str.size() << "\r\n"
        << "Connection: keep-alive\r\n";
    if (!token.empty()) {
        req << "Authorization: Bearer " << token << "\r\n";
    }
    req << "\r\n"
        << body_str;
    return req.str();
}

bool SetNonBlocking(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags == -1) return false;
    return fcntl(fd, F_SETFL, flags | O_NONBLOCK) != -1;
}

int CreateConnection(const std::string& host, int port) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;
    
    SetNonBlocking(fd);
    
    int flag = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(flag));
    
    // 增大发送缓冲区
    int sndbuf = 256 * 1024;
    setsockopt(fd, SOL_SOCKET, SO_SNDBUF, &sndbuf, sizeof(sndbuf));
    
    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    inet_pton(AF_INET, host.c_str(), &addr.sin_addr);
    
    int ret = connect(fd, (struct sockaddr*)&addr, sizeof(addr));
    if (ret < 0 && errno != EINPROGRESS) {
        close(fd);
        return -1;
    }
    
    return fd;
}

// 尝试发送数据
bool TrySend(Connection* conn) {
    while (conn->send_offset < conn->send_buffer.size()) {
        ssize_t n = send(conn->fd, 
                         conn->send_buffer.data() + conn->send_offset,
                         conn->send_buffer.size() - conn->send_offset, 
                         MSG_NOSIGNAL);
        if (n > 0) {
            conn->send_offset += n;
            g_bytes_sent += n;
        } else if (n < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                return true;  // 需要等待可写
            }
            return false;  // 错误
        }
    }
    
    // 发送完成，清空缓冲区
    conn->send_buffer.clear();
    conn->send_offset = 0;
    return true;
}

// 填充发送缓冲区（流水线）
void FillSendBuffer(Connection* conn, const Config& cfg) {
    // 计算可以发送多少请求（流水线深度限制）
    int can_send = conn->pipeline_depth - conn->pending_responses;
    int remaining = conn->messages_to_send - conn->messages_sent;
    int to_send = std::min(can_send, remaining);
    
    for (int i = 0; i < to_send; ++i) {
        std::string req = BuildHttpRequest(cfg.host, cfg.port, cfg.endpoint,
                                           conn->from_user_id, conn->to_user_id,
                                           conn->messages_sent + i,
                                           cfg.token);
        conn->send_buffer += req;
    }
    
    if (to_send > 0) {
        conn->messages_sent += to_send;
        conn->pending_responses += to_send;
        g_requests_sent += to_send;
    }
}

// 解析响应，返回完整响应数
int ParseResponses(Connection* conn) {
    int count = 0;
    
    while (true) {
        // 查找响应头结束
        size_t header_end = conn->recv_buffer.find("\r\n\r\n");
        if (header_end == std::string::npos) break;
        
        // 解析Content-Length
        size_t content_length = 0;
        std::string header = conn->recv_buffer.substr(0, header_end);
        
        // 查找 Content-Length
        size_t cl_pos = header.find("Content-Length:");
        if (cl_pos == std::string::npos) {
            cl_pos = header.find("content-length:");
        }
        
        if (cl_pos != std::string::npos) {
            size_t cl_start = cl_pos + 15;
            while (cl_start < header.size() && header[cl_start] == ' ') cl_start++;
            size_t cl_end = header.find("\r\n", cl_start);
            if (cl_end != std::string::npos) {
                try {
                    content_length = std::stoul(header.substr(cl_start, cl_end - cl_start));
                } catch (...) {
                    content_length = 0;
                }
            }
        }
        
        // 检查是否收到完整响应
        size_t total_len = header_end + 4 + content_length;
        if (conn->recv_buffer.size() < total_len) break;
        
        // 移除已处理的响应
        conn->recv_buffer.erase(0, total_len);
        count++;
    }
    
    return count;
}

// 工作线程
void WorkerThread(int thread_id, const Config& cfg, 
                  int conns_start, int conns_count) {
    if (conns_count <= 0) return;
    
    int epfd = epoll_create1(0);
    if (epfd < 0) {
        std::cerr << "Thread " << thread_id << ": epoll_create failed\n";
        return;
    }
    
    std::vector<Connection> conns(conns_count);
    int active_conns = 0;
    
    // 创建连接
    for (int i = 0; i < conns_count; ++i) {
        int fd = CreateConnection(cfg.host, cfg.port);
        if (fd < 0) {
            g_errors++;
            continue;
        }
        
        conns[i].fd = fd;
        conns[i].connected = false;
        conns[i].messages_to_send = cfg.messages_per_conn;
        conns[i].from_user_id = cfg.from_user_id + conns_start + i;
        conns[i].to_user_id = cfg.to_user_id;
        conns[i].pipeline_depth = cfg.pipeline_depth;
        
        struct epoll_event ev;
        ev.events = EPOLLOUT | EPOLLIN;
        ev.data.ptr = &conns[i];
        epoll_ctl(epfd, EPOLL_CTL_ADD, fd, &ev);
        active_conns++;
        g_active_conns++;
    }
    
    struct epoll_event events[256];
    
    while (active_conns > 0) {
        int n = epoll_wait(epfd, events, 256, 100);
        
        for (int i = 0; i < n; ++i) {
            Connection* conn = (Connection*)events[i].data.ptr;
            
            if (events[i].events & (EPOLLERR | EPOLLHUP)) {
                epoll_ctl(epfd, EPOLL_CTL_DEL, conn->fd, nullptr);
                close(conn->fd);
                conn->fd = -1;
                g_errors++;
                active_conns--;
                g_active_conns--;
                continue;
            }
            
            // 检查连接是否建立
            if (!conn->connected) {
                int err = 0;
                socklen_t len = sizeof(err);
                getsockopt(conn->fd, SOL_SOCKET, SO_ERROR, &err, &len);
                if (err != 0) {
                    epoll_ctl(epfd, EPOLL_CTL_DEL, conn->fd, nullptr);
                    close(conn->fd);
                    conn->fd = -1;
                    g_errors++;
                    active_conns--;
                    g_active_conns--;
                    continue;
                }
                conn->connected = true;
            }
            
            // 可读：接收响应
            if (events[i].events & EPOLLIN) {
                char buf[8192];
                while (true) {
                    ssize_t n = recv(conn->fd, buf, sizeof(buf), 0);
                    if (n > 0) {
                        conn->recv_buffer.append(buf, n);
                        g_bytes_received += n;
                    } else if (n == 0) {
                        // 连接关闭
                        epoll_ctl(epfd, EPOLL_CTL_DEL, conn->fd, nullptr);
                        close(conn->fd);
                        conn->fd = -1;
                        active_conns--;
                        g_active_conns--;
                        break;
                    } else {
                        if (errno == EAGAIN || errno == EWOULDBLOCK) break;
                        epoll_ctl(epfd, EPOLL_CTL_DEL, conn->fd, nullptr);
                        close(conn->fd);
                        conn->fd = -1;
                        g_errors++;
                        active_conns--;
                        g_active_conns--;
                        break;
                    }
                }
                
                if (conn->fd >= 0) {
                    // 解析响应
                    int resp_count = ParseResponses(conn);
                    if (resp_count > 0) {
                        conn->pending_responses -= resp_count;
                        g_responses_received += resp_count;
                    }
                    
                    // 收到响应后继续填充流水线
                    if (conn->send_buffer.empty() &&
                        conn->messages_sent < conn->messages_to_send &&
                        conn->pending_responses < conn->pipeline_depth) {
                        FillSendBuffer(conn, cfg);
                        if (!conn->send_buffer.empty()) {
                            TrySend(conn);
                        }
                    }
                    
                    // 检查是否完成（所有消息已发且响应已收）
                    if (conn->fd >= 0 &&
                        conn->messages_sent >= conn->messages_to_send &&
                        conn->pending_responses == 0) {
                        epoll_ctl(epfd, EPOLL_CTL_DEL, conn->fd, nullptr);
                        close(conn->fd);
                        conn->fd = -1;
                        active_conns--;
                        g_active_conns--;
                    }
                }
            }
            
            // 可写：发送请求
            if (conn->fd >= 0 && (events[i].events & EPOLLOUT)) {
                // 填充发送缓冲区
                if (conn->send_buffer.empty() && 
                    conn->messages_sent < conn->messages_to_send) {
                    FillSendBuffer(conn, cfg);
                }
                
                // 发送
                if (!conn->send_buffer.empty()) {
                    if (!TrySend(conn)) {
                        epoll_ctl(epfd, EPOLL_CTL_DEL, conn->fd, nullptr);
                        close(conn->fd);
                        conn->fd = -1;
                        g_errors++;
                        active_conns--;
                        g_active_conns--;
                        continue;
                    }
                }
                
                // 检查是否完成
                if (conn->messages_sent >= conn->messages_to_send && 
                    conn->pending_responses == 0) {
                    epoll_ctl(epfd, EPOLL_CTL_DEL, conn->fd, nullptr);
                    close(conn->fd);
                    conn->fd = -1;
                    active_conns--;
                    g_active_conns--;
                }
            }
        }
        
        // 主动尝试填充和发送（流水线）
        for (int i = 0; i < conns_count; ++i) {
            Connection* conn = &conns[i];
            if (conn->fd < 0 || !conn->connected) continue;
            
            // 填充发送缓冲区
            if (conn->send_buffer.empty() && 
                conn->messages_sent < conn->messages_to_send &&
                conn->pending_responses < conn->pipeline_depth) {
                FillSendBuffer(conn, cfg);
            }
            
            // 尝试发送
            if (!conn->send_buffer.empty()) {
                if (!TrySend(conn)) {
                    epoll_ctl(epfd, EPOLL_CTL_DEL, conn->fd, nullptr);
                    close(conn->fd);
                    conn->fd = -1;
                    g_errors++;
                    active_conns--;
                    g_active_conns--;
                }
            }
        }
    }
    
    close(epfd);
}

void PrintUsage(const char* prog) {
    std::cout << "用法: " << prog << " [选项]\n"
              << "  --host HOST         Logic HTTP地址 (127.0.0.1)\n"
              << "  --port PORT         Logic HTTP端口 (9101)\n"
              << "  --connections N     并发连接数 (100)\n"
              << "  --messages N        每连接消息数 (100)\n"
              << "  --threads N         工作线程数 (4)\n"
              << "  --pipeline N        流水线深度 (10)\n"
              << "  --from-user ID      发送者用户ID起始 (1)\n"
              << "  --to-user ID        接收者用户ID (2)\n"
              << "  --endpoint PATH     API路径 (/api/message/send)\n"
              << "  --token TOKEN       Bearer鉴权Token\n";
}

int main(int argc, char* argv[]) {
    Config cfg;
    
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--help" || arg == "-h") {
            PrintUsage(argv[0]);
            return 0;
        } else if (arg == "--host" && i + 1 < argc) {
            cfg.host = argv[++i];
        } else if (arg == "--port" && i + 1 < argc) {
            cfg.port = std::stoi(argv[++i]);
        } else if (arg == "--connections" && i + 1 < argc) {
            cfg.connections = std::stoi(argv[++i]);
        } else if (arg == "--messages" && i + 1 < argc) {
            cfg.messages_per_conn = std::stoi(argv[++i]);
        } else if (arg == "--threads" && i + 1 < argc) {
            cfg.threads = std::stoi(argv[++i]);
        } else if (arg == "--pipeline" && i + 1 < argc) {
            cfg.pipeline_depth = std::stoi(argv[++i]);
        } else if (arg == "--from-user" && i + 1 < argc) {
            cfg.from_user_id = std::stoll(argv[++i]);
        } else if (arg == "--to-user" && i + 1 < argc) {
            cfg.to_user_id = std::stoll(argv[++i]);
        } else if (arg == "--endpoint" && i + 1 < argc) {
            cfg.endpoint = argv[++i];
        } else if (arg == "--token" && i + 1 < argc) {
            cfg.token = argv[++i];
        }
    }
    
    int64_t total_messages = (int64_t)cfg.connections * cfg.messages_per_conn;
    
    std::cout << "========================================\n"
              << "   HTTP消息推送性能测试 (epoll+流水线)\n"
              << "========================================\n"
              << "目标: " << cfg.host << ":" << cfg.port << cfg.endpoint << "\n"
              << "连接数: " << cfg.connections << ", 线程数: " << cfg.threads << "\n"
              << "流水线深度: " << cfg.pipeline_depth << "\n"
              << "每连接消息: " << cfg.messages_per_conn << "\n"
              << "预期总请求: " << total_messages << "\n"
              << "========================================\n\n";
    
    auto start_time = std::chrono::steady_clock::now();
    
    // 启动工作线程
    std::vector<std::thread> threads;
    int conns_per_thread = cfg.connections / cfg.threads;
    int extra_conns = cfg.connections % cfg.threads;
    int conn_offset = 0;
    
    for (int i = 0; i < cfg.threads; ++i) {
        int conns_count = conns_per_thread + (i < extra_conns ? 1 : 0);
        threads.emplace_back(WorkerThread, i, std::ref(cfg), conn_offset, conns_count);
        conn_offset += conns_count;
    }
    
    // 等待并显示进度
    while (true) {
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
        
        int64_t sent = g_requests_sent.load();
        int64_t received = g_responses_received.load();
        int64_t errors = g_errors.load();
        int64_t active = g_active_conns.load();
        
        auto now = std::chrono::steady_clock::now();
        double elapsed = std::chrono::duration<double>(now - start_time).count();
        
        std::cout << "\r[" << std::fixed << std::setprecision(1) << elapsed << "s] "
                  << "请求:" << sent << " 响应:" << received 
                  << " 活跃:" << active
                  << " 错误:" << errors
                  << " QPS:" << (int)(received / elapsed) << "   " << std::flush;
        
        if (received + errors >= total_messages) break;
        if (elapsed > 300) {
            std::cout << "\n超时退出\n";
            break;
        }
    }
    
    // 等待线程结束
    for (auto& t : threads) {
        t.join();
    }
    
    auto end_time = std::chrono::steady_clock::now();
    double total_time = std::chrono::duration<double>(end_time - start_time).count();
    
    int64_t sent = g_requests_sent.load();
    int64_t received = g_responses_received.load();
    int64_t errors = g_errors.load();
    int64_t bytes_sent = g_bytes_sent.load();
    int64_t bytes_received = g_bytes_received.load();
    
    std::cout << "\n\n========================================\n"
              << "              测试结果\n"
              << "========================================\n"
              << "请求发送: " << sent << "\n"
              << "响应接收: " << received << "\n"
              << "错误数: " << errors << "\n"
              << "发送字节: " << bytes_sent / 1024 << " KB\n"
              << "接收字节: " << bytes_received / 1024 << " KB\n"
              << "总耗时: " << std::fixed << std::setprecision(2) << total_time << " s\n"
              << "----------------------------------------\n"
              << "QPS: " << (int)(received / total_time) << "\n"
              << "========================================\n";
    
    return 0;
}
