#include "mysql_pool.h"

#include "logging.h"

#include <chrono>

namespace sparkpush {

// 构造守卫，记录池指针与连接句柄，便于析构时归还。
MySqlConnGuard::MySqlConnGuard(MySqlConnectionPool* pool, MYSQL* conn)
    : pool_(pool), conn_(conn) {}

// 移动构造，转移归还责任。
MySqlConnGuard::MySqlConnGuard(MySqlConnGuard&& other) noexcept
    : pool_(other.pool_), conn_(other.conn_) {
    other.pool_ = nullptr;
    other.conn_ = nullptr;
}

// 移动赋值，确保旧连接先归还。
MySqlConnGuard& MySqlConnGuard::operator=(MySqlConnGuard&& other) noexcept {
    if (this == &other) {
        return *this;
    }

    if (pool_ && conn_) {
        pool_->Release(conn_);
    }

    pool_ = other.pool_;
    conn_ = other.conn_;

    other.pool_ = nullptr;
    other.conn_ = nullptr;

    return *this;
}

// 析构时自动归还连接到连接池。
MySqlConnGuard::~MySqlConnGuard() {
    if (pool_ && conn_) {
        pool_->Release(conn_);
    }
}

MySqlConnectionPool::~MySqlConnectionPool() {
    std::unique_lock<std::mutex> lock(mutex_);
    stopping_ = true;
    cv_.notify_all();
    while (!idle_.empty()) {
        mysql_close(idle_.front().conn);
        idle_.pop_front();
    }
    total_conns_ = 0;
}

// 创建真实 MySQL 连接，设置超时、字符集与自动重连。
MYSQL* MySqlConnectionPool::CreateConnectionUnlocked() {
    MYSQL* conn = mysql_init(nullptr);
    if (!conn) {
        LogError("mysql_init failed");
        return nullptr;
    }

    // 连接超时
    if (cfg_.connect_timeout_ms > 0) {
        unsigned int sec =
            static_cast<unsigned int>((cfg_.connect_timeout_ms + 999) / 1000);
        mysql_options(conn, MYSQL_OPT_CONNECT_TIMEOUT, &sec);
    }

    // 读写超时
    if (cfg_.rw_timeout_ms > 0) {
        unsigned int sec =
            static_cast<unsigned int>((cfg_.rw_timeout_ms + 999) / 1000);
        mysql_options(conn, MYSQL_OPT_READ_TIMEOUT, &sec);
        mysql_options(conn, MYSQL_OPT_WRITE_TIMEOUT, &sec);
    }

    // 自动重连
    bool reconnect = 1;
    mysql_options(conn, MYSQL_OPT_RECONNECT, &reconnect);
    // 字符集
    if (!cfg_.charset.empty()) {
        mysql_options(conn, MYSQL_SET_CHARSET_NAME, cfg_.charset.c_str());
    }

    if (!mysql_real_connect(conn,
                            cfg_.host.c_str(),
                            cfg_.user.c_str(),
                            cfg_.password.c_str(),
                            cfg_.db.c_str(),
                            cfg_.port,
                            nullptr,
                            0)) {
        LogError(std::string("mysql_real_connect failed: ") + mysql_error(conn));
        mysql_close(conn);
        return nullptr;
    }

    return conn;
}

// 初始化连接池，预创建最小连接数，失败时回滚。
bool MySqlConnectionPool::Init(const MySqlConfig& cfg) {
    std::unique_lock<std::mutex> lock(mutex_);
    if (inited_) {
        return true;
    }

    cfg_ = cfg;
    if (cfg_.min_pool_size <= 0) {
        cfg_.min_pool_size = cfg_.pool_size > 0 ? cfg_.pool_size : 1;
    }
    if (cfg_.max_pool_size <= 0) {
        cfg_.max_pool_size = std::max(cfg_.min_pool_size, cfg_.pool_size);
    }
    cfg_.max_pool_size = std::max(cfg_.max_pool_size, cfg_.min_pool_size);
    idle_.clear();
    total_conns_ = 0;

    for (int i = 0; i < cfg_.min_pool_size; ++i) {
        MYSQL* conn = CreateConnectionUnlocked();
        if (!conn) {
            // 回滚已经创建的连接
            while (!idle_.empty()) {
                mysql_close(idle_.front().conn);
                idle_.pop_front();
            }
            total_conns_ = 0;
            return false;
        }
        idle_.push_back({conn, std::chrono::steady_clock::now()});
        ++total_conns_;
    }

    inited_ = true;
    stopping_ = false;
    return true;
}

// 按需获取连接，支持超时等待与自动扩容。
MySqlConnGuard MySqlConnectionPool::Acquire(int timeout_ms) {
    std::unique_lock<std::mutex> lock(mutex_);
    if (!inited_) {
        LogError("MySqlConnectionPool::Acquire called before Init");
        return MySqlConnGuard(nullptr, nullptr);
    }

    MYSQL* conn = nullptr;
    auto wait_timeout = std::chrono::milliseconds(timeout_ms);

    while (!stopping_) {
        conn = PopIdleConnectionUnlocked();
        if (conn) {
            break;
        }

        // 自动扩容
        if (cfg_.enable_auto_grow && total_conns_ < cfg_.max_pool_size) {
            conn = CreateConnectionUnlocked();
            if (conn) {
                ++total_conns_;
                break;
            }
        }

        // 等待或超时
        if (timeout_ms < 0) {
            cv_.wait(lock);
        } else if (timeout_ms == 0) {
            return MySqlConnGuard(nullptr, nullptr);
        } else {
            if (cv_.wait_for(lock, wait_timeout) == std::cv_status::timeout) {
                return MySqlConnGuard(nullptr, nullptr);
            }
        }
    }

    while (true) {
        if (!conn || stopping_) {
            return MySqlConnGuard(nullptr, nullptr);
        }

        lock.unlock();
        if (ValidateConnection(&conn)) {
            return MySqlConnGuard(this, conn);
        }

        // 连接失效，更新计数并再次尝试
        {
            std::lock_guard<std::mutex> relock(mutex_);
            if (total_conns_ > 0) {
                --total_conns_;
            }
        }
        lock.lock();
        conn = PopIdleConnectionUnlocked();
        if (!conn && cfg_.enable_auto_grow && total_conns_ < cfg_.max_pool_size) {
            conn = CreateConnectionUnlocked();
            if (conn) {
                ++total_conns_;
            }
        }
    }
}

// 归还连接到空闲队列，若正在停止则直接关闭。
void MySqlConnectionPool::Release(MYSQL* conn) {
    std::unique_lock<std::mutex> lock(mutex_);
    if (stopping_) {
        if (conn) {
            mysql_close(conn);
            if (total_conns_ > 0) {
                --total_conns_;
            }
        }
        return;
    }
    if (conn) {
        idle_.push_back({conn, std::chrono::steady_clock::now()});
        cv_.notify_one();
    }
}

// 置停止标志，唤醒等待者并拒绝新请求。
void MySqlConnectionPool::Stop() {
    std::unique_lock<std::mutex> lock(mutex_);
    if (stopping_) {
        return;
    }
    stopping_ = true;
    cv_.notify_all();
}

// 弹出一个空闲连接，弹出前会执行空闲清理。
MYSQL* MySqlConnectionPool::PopIdleConnectionUnlocked() {
    CleanupIdleUnlocked();
    if (idle_.empty()) {
        return nullptr;
    }
    MYSQL* conn = idle_.front().conn;
    idle_.pop_front();
    return conn;
}

// 回收超过空闲时间的连接，保持至少 min_pool_size。
void MySqlConnectionPool::CleanupIdleUnlocked() {
    if (cfg_.idle_timeout_ms <= 0) {
        return;
    }

    auto now = std::chrono::steady_clock::now();
    while (!idle_.empty()) {
        auto& entry = idle_.front();
        auto idle_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
            now - entry.last_used)
                           .count();
        if (idle_ms < cfg_.idle_timeout_ms ||
            total_conns_ <= cfg_.min_pool_size) {
            break;
        }
        mysql_close(entry.conn);
        idle_.pop_front();
        --total_conns_;
    }
}

// 校验连接有效性，必要时重建；若启用 ping_on_borrow 则执行 mysql_ping。
bool MySqlConnectionPool::ValidateConnection(MYSQL** conn) {
    if (!conn || !(*conn)) {
        return false;
    }
    if (!cfg_.ping_on_borrow) {
        return true;
    }
    if (mysql_ping(*conn) == 0) {
        return true;
    }
    LogError("mysql_ping failed, dropping connection");
    mysql_close(*conn);
    *conn = nullptr;
    return false;
}

}  // namespace sparkpush


