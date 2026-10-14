#pragma once

#include <mysql/mysql.h>

#include <condition_variable>
#include <deque>
#include <mutex>
#include <string>
#include <chrono>

namespace sparkpush {

// MySQL 连接池配置
//
// 三个尺寸参数的关系（在 Init 里被规范化）：
//   min_pool_size  预热连接数，启动时就建好，避开首请求的建连延迟
//   pool_size      基准容量
//   max_pool_size  硬上限，达到后 Acquire 只能等，不会再建
// 传 0 表示「用 pool_size 推导」，logic/main.cpp 就是这么用的。
struct MySqlConfig {
    std::string host;
    int port{3306};
    std::string user;
    std::string password;
    std::string db;
    int pool_size{4};
    int min_pool_size{0};
    int max_pool_size{0};
    // 空闲连接超时回收（ms）。必须小于 MySQL 服务端的 wait_timeout，
    // 否则借出来的可能是已被服务端单方面关掉的死连接。
    int idle_timeout_ms{60000};
    // 池空且未达 max 时是否允许新建连接
    bool enable_auto_grow{true};
    // 借出前是否先 ping 验活。多一次往返，但能避开拿到死连接后
    // 业务 SQL 报错（尤其是 MySQL 重启或网络闪断之后）。
    bool ping_on_borrow{true};
    // 连接超时（毫秒）
    int connect_timeout_ms{2000};
    // 读/写超时（毫秒）。必须设：不设的话一条卡住的 SQL 会永久占用
    // 连接，池逐渐耗尽后整个服务假死。
    int rw_timeout_ms{5000};
    // 默认字符集。utf8mb4 而不是 utf8：MySQL 的 utf8 是残缺的
    // 3 字节实现，存不了 emoji 和部分生僻字，会在插入时静默截断。
    std::string charset{"utf8mb4"};
};

class MySqlConnectionPool;

// 简单的 RAII 封装，析构时自动归还连接
//
// 为何必需：DAO 函数里有大量提前 return 分支，手写「借-还」必漏。
// 漏还的后果不是内存泄漏，而是连接永久离开池，最终池空后所有
// 数据库操作卡死。用 RAII 把归还变成不可遗漏的不变量。
//
// 禁拷贝但允许移动：拷贝会造成同一连接被归还两次（double release），
// 进而被两个调用方同时使用，引发数据错乱。
class MySqlConnGuard {
public:
    // 移动构造，转移连接归还责任。
    MySqlConnGuard(MySqlConnGuard&& other) noexcept;
    // 移动赋值，先归还旧连接再接管新连接。
    MySqlConnGuard& operator=(MySqlConnGuard&& other) noexcept;

    MySqlConnGuard(const MySqlConnGuard&) = delete;
    MySqlConnGuard& operator=(const MySqlConnGuard&) = delete;

    // 析构时自动归还连接到连接池。
    ~MySqlConnGuard();

    // 获取底层 MYSQL 指针。可能为 nullptr（Acquire 超时时），
    // 因此所有调用方都必须先判空再用。
    MYSQL* get() const { return conn_; }
    // 便捷访问，等价于 get。
    MYSQL* operator->() const { return conn_; }

private:
    // 只允许池构造：防止外部伪造一个不归属任何池的 guard。
    friend class MySqlConnectionPool;

    MySqlConnGuard(MySqlConnectionPool* pool, MYSQL* conn);

    MySqlConnectionPool* pool_{nullptr};
    MYSQL* conn_{nullptr};
};

// 线程安全的 MySQL 连接池，支持自动扩缩容与空闲回收
//
// Acquire 的策略顺序（实现见 .cpp）：
//   1) 池里有空闲连接 -> 验活后直接给
//   2) 池空但未达 max_pool_size 且允许增长 -> 新建
//   3) 否则在 cv_ 上等，直到有连接归还或超时
// 超时时返回一个空 guard（get() == nullptr），而不是抛异常。
class MySqlConnectionPool {
public:
    MySqlConnectionPool() = default;
    ~MySqlConnectionPool();

    // 按配置初始化连接池，并预热 min_pool_size 个连接。
    bool Init(const MySqlConfig& cfg);

    // 阻塞获取连接；timeout_ms < 0 表示一直等待，=0 表示立即返回，大于 0 表示等待指定毫秒
    //
    // 默认 3s 而不是无限等：上游 RPC 都有自己的 deadline（Logic 的
    // gRPC handler、Comet 的 8s 上行超时），无限等会让请求堆积而
    // 不是快速失败。拿到空 guard 时应返回 503。
    MySqlConnGuard Acquire(int timeout_ms = 3000);

    // 优雅停止，拒绝新的获取请求并回收空闲连接
    void Stop();

private:
    // 下面四个 *Unlocked 方法都要求调用方已持 mutex_。
    // 命名里显式标出这一点，避开在已加锁的路径上重复加锁而死锁。
    MYSQL* CreateConnectionUnlocked();
    MYSQL* PopIdleConnectionUnlocked();
    // 验活：ping 失败则关连接并返回 false。输出参数是双重指针，
    // 因为重连成功时需要把新连接写回调用方。
    bool ValidateConnection(MYSQL** conn);
    void CleanupIdleUnlocked();
    // 由 MySqlConnGuard 析构时调用，外部不应直接调。
    void Release(MYSQL* conn);

    friend class MySqlConnGuard;

    std::mutex mutex_;
    std::condition_variable cv_;
    // 空闲连接带最后使用时间，供 idle_timeout_ms 回收判定。
    // 用 steady_clock：不受系统时间调整影响，否则一次 NTP 向后跳
    // 就能让所有连接瞬间「过期」而被集体销毁。
    struct ConnEntry {
        MYSQL* conn{nullptr};
        std::chrono::steady_clock::time_point last_used;
    };
    std::deque<ConnEntry> idle_;
    int total_conns_{0};   // 已借出 + 空闲，用于判定是否达到 max_pool_size
    bool stopping_{false};
    bool inited_{false};
    MySqlConfig cfg_;
};

}  // namespace sparkpush


