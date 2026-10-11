#include <atomic>
#include <future>
#include <thread>
#include <stdexcept>
#include "mysql_pool.h"
std::atomic<bool> online{true};
extern "C" MYSQL* __wrap_mysql_real_connect(MYSQL* connection, const char*, const char*, const char*,
                                           const char*, unsigned int, const char*, unsigned long) {
    return online ? connection : nullptr;
}
extern "C" int __wrap_mysql_ping(MYSQL*) { return online ? 0 : 1; }
void Require(bool value) { if (!value) throw std::runtime_error("pool recovery regression"); }
int main() {
    using namespace std::chrono_literals;
    sparkpush::MySqlConnectionPool pool;
    sparkpush::MySqlConfig config;
    config.pool_size = config.min_pool_size = config.max_pool_size = 1;
    config.enable_auto_grow = false;
    Require(pool.Init(config));
    online = false;
    Require(!pool.Acquire(0).get());
    auto recovery = std::async(std::launch::async, [&] { return pool.Acquire().get() != nullptr; });
    std::this_thread::sleep_for(100ms);
    online = true;
    Require(recovery.wait_for(1500ms) == std::future_status::ready && recovery.get());
    {
        auto held = pool.Acquire();
        auto timeout = std::async(std::launch::async, [&] { return pool.Acquire(100).get() == nullptr; });
        Require(timeout.wait_for(500ms) == std::future_status::ready && timeout.get());
        auto stopped = std::async(std::launch::async, [&] { return pool.Acquire(-1).get() == nullptr; });
        pool.Stop();
        Require(stopped.wait_for(500ms) == std::future_status::ready && stopped.get());
    }
}
