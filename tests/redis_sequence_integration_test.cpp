#include "redis_pool.h"
#include "redis_store.h"

#include <hiredis/hiredis.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdlib>
#include <iostream>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <vector>

namespace {

std::string EnvOr(const char* name, const char* fallback) {
    const char* value = std::getenv(name);
    return value ? value : fallback;
}

}  // namespace

int main() {
    if (EnvOr("SPARK_PUSH_RUN_REDIS_TESTS", "0") != "1") return 77;

    sparkpush::RedisConfig cfg;
    cfg.host = EnvOr("SPARK_PUSH_REDIS_HOST", "127.0.0.1");
    cfg.port = std::stoi(EnvOr("SPARK_PUSH_REDIS_PORT", "6379"));
    cfg.password = EnvOr("SPARK_PUSH_REDIS_PASSWORD", "");
    cfg.pool_size = 8;
    cfg.min_pool_size = 8;
    cfg.max_pool_size = 16;

    sparkpush::RedisConnectionPool pool;
    if (!pool.Init(cfg)) {
        std::cerr << "Redis pool init failed\n";
        return 1;
    }
    sparkpush::RedisStore store(&pool);
    const std::string session_id =
        "ctest_" + std::to_string(getpid()) + "_" +
        std::to_string(std::chrono::steady_clock::now()
                           .time_since_epoch()
                           .count());

    int64_t seq = 0;
    bool is_new = false;
    if (!store.AllocateSessionMsgSeq(session_id, 7, "first", 100, 60, &seq,
                                     &is_new) ||
        seq != 101 || !is_new) {
        std::cerr << "floor calibration failed\n";
        return 1;
    }
    int64_t duplicate_seq = 0;
    bool duplicate_is_new = true;
    if (!store.AllocateSessionMsgSeq(session_id, 7, "first", 0, 60,
                                     &duplicate_seq, &duplicate_is_new) ||
        duplicate_seq != seq || duplicate_is_new) {
        std::cerr << "client_msg_id idempotency failed\n";
        return 1;
    }

    constexpr int kThreads = 8;
    constexpr int kPerThread = 100;
    std::mutex result_mutex;
    std::vector<int64_t> results;
    std::atomic<int> failures{0};
    std::vector<std::thread> workers;
    for (int t = 0; t < kThreads; ++t) {
        workers.emplace_back([&, t]() {
            for (int i = 0; i < kPerThread; ++i) {
                int64_t value = 0;
                bool fresh = false;
                const std::string client_id =
                    "worker-" + std::to_string(t) + "-" + std::to_string(i);
                if (!store.AllocateSessionMsgSeq(session_id, 7, client_id, 0,
                                                 60, &value, &fresh) ||
                    !fresh) {
                    ++failures;
                    continue;
                }
                std::lock_guard<std::mutex> lock(result_mutex);
                results.push_back(value);
            }
        });
    }
    for (auto& worker : workers) worker.join();

    std::set<int64_t> unique(results.begin(), results.end());
    if (failures != 0 || results.size() != kThreads * kPerThread ||
        unique.size() != results.size() || *unique.begin() != 102 ||
        *unique.rbegin() != 101 + kThreads * kPerThread) {
        std::cerr << "concurrent sequence allocation failed\n";
        return 1;
    }

    const std::string token_a = "ctest-token-a-" + std::to_string(getpid());
    const std::string token_b = "ctest-token-b-" + std::to_string(getpid());
    if (!store.SetToken(token_a, 7788, 60) ||
        !store.SetToken(token_b, 7788, 60)) {
        std::cerr << "token setup failed\n";
        return 1;
    }
    int revoked_tokens = 0;
    if (!store.RevokeUserTokens(7788, &revoked_tokens) ||
        revoked_tokens != 2) {
        std::cerr << "batch token revoke failed\n";
        return 1;
    }
    int64_t revoked_uid = 0;
    if (store.GetUserIdByToken(token_a, &revoked_uid) ||
        store.GetUserIdByToken(token_b, &revoked_uid)) {
        std::cerr << "revoked token is still valid\n";
        return 1;
    }

    redisContext* cleanup = redisConnect(cfg.host.c_str(), cfg.port);
    if (cleanup && !cleanup->err) {
        redisReply* reply = static_cast<redisReply*>(redisCommand(
            cleanup, "DEL session:msg_seq:%s session:last_seq:%s",
            session_id.c_str(), session_id.c_str()));
        if (reply) freeReplyObject(reply);
        reply = static_cast<redisReply*>(redisCommand(
            cleanup, "DEL message:dedup:%s:7:first", session_id.c_str()));
        if (reply) freeReplyObject(reply);
        for (int t = 0; t < kThreads; ++t) {
            for (int i = 0; i < kPerThread; ++i) {
                const std::string client_id =
                    "worker-" + std::to_string(t) + "-" + std::to_string(i);
                reply = static_cast<redisReply*>(redisCommand(
                    cleanup, "DEL message:dedup:%s:7:%s", session_id.c_str(),
                    client_id.c_str()));
                if (reply) freeReplyObject(reply);
            }
        }
    }
    if (cleanup) redisFree(cleanup);
    return 0;
}
