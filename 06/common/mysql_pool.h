#pragma once

#include <mysql/mysql.h>

#include <condition_variable>
#include <deque>
#include <mutex>
#include <string>
#include <chrono>

namespace sparkpush {

struct MySqlConfig {
  std::string host;
  int port{3306};
  std::string user;
  std::string password;
  std::string db;
  int pool_size{4};
  int min_pool_size{0};
  int max_pool_size{0};
  int idle_timeout_ms{60000};
  bool enable_auto_grow{true};
  bool ping_on_borrow{true};
  // 连接超时（毫秒）
  int connect_timeout_ms{2000};
  // 读/写超时（毫秒）
  int rw_timeout_ms{5000};
  // 默认字符集
  std::string charset{"utf8mb4"};
};

class MySqlConnectionPool;

class MySqlConnGuard {
 public:
  MySqlConnGuard(MySqlConnGuard&& other) noexcept;
  MySqlConnGuard& operator=(MySqlConnGuard&& other) noexcept;

  MySqlConnGuard(const MySqlConnGuard&) = delete;
  MySqlConnGuard& operator=(const MySqlConnGuard&) = delete;

  ~MySqlConnGuard();

  MYSQL* get() const { return conn_; }
  MYSQL* operator->() const { return conn_; }

 private:
  friend class MySqlConnectionPool;

  MySqlConnGuard(MySqlConnectionPool* pool, MYSQL* conn);

  MySqlConnectionPool* pool_{nullptr};
  MYSQL* conn_{nullptr};
};

// MySQL 连接池
// 设计与 RedisConnectionPool 对称：RAII Guard + 自动扩容/回收 + 超时等待
// 区别：MySQL 连接使用 mysql_ping() 做健康检查（ping_on_borrow 可配置）
class MySqlConnectionPool {
 public:
  MySqlConnectionPool() = default;
  ~MySqlConnectionPool();

  bool Init(const MySqlConfig& cfg);

  // 阻塞获取连接；timeout_ms < 0 表示一直等待，=0 表示立即返回，大于 0 表示等待指定毫秒
  MySqlConnGuard Acquire(int timeout_ms = -1);

  // 优雅停止，拒绝新的获取请求并回收空闲连接
  void Stop();

 private:
  MYSQL* CreateConnectionUnlocked();
  MYSQL* PopIdleConnectionUnlocked();
  bool ValidateConnection(MYSQL** conn);
  void CleanupIdleUnlocked();
  void Release(MYSQL* conn);

  friend class MySqlConnGuard;

  std::mutex mutex_;
  std::condition_variable cv_;
  struct ConnEntry {
    MYSQL* conn{nullptr};
    std::chrono::steady_clock::time_point last_used;
  };
  std::deque<ConnEntry> idle_;
  int total_conns_{0};
  bool stopping_{false};
  bool inited_{false};
  MySqlConfig cfg_;
};

}  // namespace sparkpush


