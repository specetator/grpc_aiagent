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
//
// 命名里的 Unlocked 有点误导：本函数实际上**要求调用方已释锁**
// （Acquire 里先 unlock 再调它），因为 mysql_real_connect 会阻塞
// 数秒，持锁建连会把整个池卡死。它不访问任何共享状态，只读 cfg_。
MYSQL* MySqlConnectionPool::CreateConnectionUnlocked() {
    MYSQL* conn = mysql_init(nullptr);
    if (!conn) {
        LOG_ERROR << "mysql_init failed";
        return nullptr;
    }

    // 连接超时
    // mysql_options 只接受秒级粒度，因此用 (ms + 999) / 1000 向上取整：
    // 配 500ms 会变成 1s 而不是 0s。0 在 MySQL 里意味着「无超时」，
    // 向下取整会静默地把超时保护关掉。
    if (cfg_.connect_timeout_ms > 0) {
        unsigned int sec =
            static_cast<unsigned int>((cfg_.connect_timeout_ms + 999) / 1000);
        mysql_options(conn, MYSQL_OPT_CONNECT_TIMEOUT, &sec);
    }

    // 读写超时。这是防止服务假死的关键：没有它，一条卡住的 SQL
    // （表锁、网络半开）会永久占用连接，池逐渐耗尽后所有 DAO 都拿不到连接。
    if (cfg_.rw_timeout_ms > 0) {
        unsigned int sec =
            static_cast<unsigned int>((cfg_.rw_timeout_ms + 999) / 1000);
        mysql_options(conn, MYSQL_OPT_READ_TIMEOUT, &sec);
        mysql_options(conn, MYSQL_OPT_WRITE_TIMEOUT, &sec);
    }

    // 自动重连
    // bool reconnect = 1;
    // mysql_options(conn, MYSQL_OPT_RECONNECT, &reconnect);
    //
    // 上面两行是故意注释掉的，不要恢复：MYSQL_OPT_RECONNECT 会在
    // 连接断开时静默重建，但**不会恢复会话状态**——事务、临时表、
    // 用户变量、字符集全丢。对本项目而言最危险的是：一个未提交的
    // 事务会在重连后静默变成自动提交模式，SqlTransaction 的原子性
    // 就没了。因此宁可让 SQL 报错，由 ValidateConnection 显式丢弃死连接。
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
        LOG_ERROR << "mysql_real_connect failed: " << mysql_error(conn);
        mysql_close(conn);   // 失败也必须 close，mysql_init 已分配了句柄
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
//
// 这是本文件最难读的函数，核心是「释锁做阻塞操作」的三段式：
//   建连与 ping 都可能耗时数秒，绝不能持 mutex_ 做，否则所有
//   其他 Acquire/Release 全部卡住。因此每段都是 unlock -> 阻塞操作 -> lock。
//
// total_conns_ 的预占很关键（代码里的 Reserve capacity 注释）：
// 先 ++ 再释锁建连。若不预占，释锁期间其他线程会看到「未达 max」
// 而同时建连，瞬间冲破 max_pool_size 上限。建连失败时要 -- 回去。
//
// fresh 标志：刚新建的连接不需要 ping（mysql_real_connect 成功就是活的），
// 只有从 idle_ 里取出的旧连接才需要验活。这省掉了新连接的一次往返。
//
// 末尾的 250ms 定时唤醒（而不是纯等 notify）：建连失败时没有任何人
// 会归还连接，也就没有 notify。若只等通知，调用方会白白卡到超时。
MySqlConnGuard MySqlConnectionPool::Acquire(int timeout_ms) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(std::max(0, timeout_ms));
    std::unique_lock<std::mutex> lock(mutex_);
    while (inited_ && !stopping_) {
        MYSQL* connection = PopIdleConnectionUnlocked();
        bool fresh = false;
        if (!connection && (total_conns_ < cfg_.min_pool_size ||
                            (cfg_.enable_auto_grow && total_conns_ < cfg_.max_pool_size))) {
            ++total_conns_;  // Reserve capacity before leaving the lock.
            lock.unlock();
            connection = CreateConnectionUnlocked();
            lock.lock();
            if (!connection) --total_conns_;
            fresh = true;
        }
        if (connection) {
            lock.unlock();
            const bool valid = fresh || ValidateConnection(&connection);
            lock.lock();
            // 释锁期间可能已经 Stop，因此拿到锁后必须重查 stopping_，
            // 否则会把连接交出去，而池正在析构。
            if (valid && !stopping_) return MySqlConnGuard(this, connection);
            if (connection) mysql_close(connection);
            --total_conns_;      // 与上面的预占配对，把名额还回去
            cv_.notify_all();    // 名额释放了，唤醒其他等待者去建连
        }
        if (stopping_ || timeout_ms == 0 || (timeout_ms > 0 && std::chrono::steady_clock::now() >= deadline)) break;
        // A failed creation has no owner to release a connection. Retry independently of notifications.
        auto wake = std::chrono::steady_clock::now() + std::chrono::milliseconds(250);
        if (timeout_ms > 0) wake = std::min(wake, deadline);   // 不能超过调用方的 deadline
        cv_.wait_until(lock, wake);
    }
    // 返回空 guard 而不是抛异常：调用方统一用 if (!conn) 判失败并回 503。
    return MySqlConnGuard(nullptr, nullptr);
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
//
// 采用惰性清理（在 Pop 时顺便做）而不是后台线程：避开额外线程与
// 定时器，代价是长时间无人借连接时过期连接不会被及时关。
//
// 从队首开始扫且遇到未超时就 break：Release 总是 push_back，因此
// idle_ 按归还时间有序，队首就是最旧的。这使清理是 O(过期数)
// 而不是 O(n)。
//
// total_conns_ <= min_pool_size 时停止回收：保证池里永远有预热连接，
// 避开低谷期把连接全关光、高峰来临时又集体重建的抖动。
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
//
// 注意本函数并**不重建**连接（尽管头文件注释这么写）：ping 失败时
// 只是关掉并置空，由 Acquire 的下一轮循环去建新连接。
//
// 为何需要 ping：MySQL 服务端会在 wait_timeout 后单方面关连接，
// 客户端并不会收到通知。不验活就会把一个已死的句柄交给 DAO，
// 表现为「第一条 SQL 莫名其妙失败」。
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
    LOG_ERROR << "mysql_ping failed, dropping connection";
    mysql_close(*conn);
    *conn = nullptr;
    return false;
}

}  // namespace sparkpush


