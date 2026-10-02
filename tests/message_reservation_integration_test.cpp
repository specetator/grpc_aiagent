#include "message_reservation.h"
#include "sql_helpers.h"
#include <atomic>
#include <chrono>
#include <cstdlib>
#include <iostream>
#include <mutex>
#include <set>
#include <stdexcept>
#include <thread>
#include <unistd.h>
#include <vector>

namespace {
std::string Env(const char* name, const char* fallback) {
    const auto* value = std::getenv(name);
    return value ? value : fallback;
}
void Require(bool condition, const std::string& message) {
    if (!condition) throw std::runtime_error(message);
}
sparkpush::Message Candidate(const std::string& session, const std::string& client) {
    sparkpush::Message message;
    message.session_id = session;
    message.sender_id = 812;
    message.timestamp_ms = 1789000000123LL;
    message.msg_type = "text";
    message.content_json = std::string("中文 original\0payload", 23);
    message.client_msg_id = client;
    return message;
}
void Same(const sparkpush::Message& a, const sparkpush::Message& b) {
    Require(a.msg_id == b.msg_id && a.session_id == b.session_id && a.msg_seq == b.msg_seq &&
        a.sender_id == b.sender_id && a.timestamp_ms == b.timestamp_ms && a.msg_type == b.msg_type &&
        a.content_json == b.content_json && a.client_msg_id == b.client_msg_id, "reservation did not preserve first full message");
}
struct Cleanup {
    sparkpush::MySqlConnectionPool& pool;
    std::string prefix;
    ~Cleanup() {
        auto guard = pool.Acquire(2000);
        auto* conn = guard.get();
        if (!conn) return;
        const auto suffix = " WHERE LEFT(session_id," + std::to_string(prefix.size()) + ")=" + sparkpush::SqlQuote(conn, prefix);
        sparkpush::SqlExec(conn, "DELETE FROM message_reservation" + suffix, nullptr);
        sparkpush::SqlExec(conn, "DELETE FROM session_sequence_reservation" + suffix, nullptr);
    }
};
}  // namespace

int main() {
    if (Env("SPARK_PUSH_RUN_MYSQL_TESTS", "0") != "1") {
        std::cout << "SKIP (77): SQL reservation requires an isolated SPARK_PUSH_MYSQL_TEST_DB\n";
        return 77;
    }
    try {
        sparkpush::MySqlConfig config;
        config.host = Env("SPARK_PUSH_MYSQL_HOST", "127.0.0.1");
        config.port = std::stoi(Env("SPARK_PUSH_MYSQL_PORT", "3306"));
        config.user = Env("SPARK_PUSH_MYSQL_USER", "root");
        config.password = Env("SPARK_PUSH_MYSQL_PASSWORD", "");
        config.db = Env("SPARK_PUSH_MYSQL_TEST_DB", "spark_push_test");
        config.pool_size = 8;
        config.max_pool_size = 8;
        Require(config.db != "spark_push", "test requires a separate database");
        sparkpush::MySqlConnectionPool pool;
        Require(pool.Init(config), "test MySQL pool init failed");
        sparkpush::MessageReservation first(&pool);
        std::string error;
        Require(first.EnsureSchema(&error), error);
        const std::string prefix = "reservation_test_" + std::to_string(getpid()) + "_" +
            std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
        Cleanup cleanup{pool, prefix};
        const auto initial = Candidate(prefix + "a", "pending");
        sparkpush::Message reserved, duplicate;
        bool fresh = false;
        Require(first.Reserve(initial, 100, &reserved, &fresh, &error) && fresh && reserved.msg_seq == 101,
                "durable floor not respected");
        // No message history or Redis write has happened. A new DAO represents
        // process/Redis restart while Job has not yet persisted the Kafka event.
        sparkpush::MessageReservation restarted(&pool);
        auto changed = initial;
        changed.timestamp_ms += 5000;
        changed.content_json = "retry replacement";
        Require(restarted.Reserve(changed, 0, &duplicate, &fresh, &error) && !fresh, error);
        Same(reserved, duplicate);
        auto next = Candidate(initial.session_id, "next");
        Require(restarted.Reserve(next, 0, &duplicate, &fresh, &error) && fresh && duplicate.msg_seq == 102,
                "pending sequence was reused after Redis loss");
        auto upper = Candidate(initial.session_id, "Case");
        auto lower = Candidate(initial.session_id, "case");
        sparkpush::Message upper_reserved, lower_reserved;
        Require(first.Reserve(upper, 0, &upper_reserved, &fresh, &error), error);
        Require(first.Reserve(lower, 0, &lower_reserved, &fresh, &error) && fresh &&
                lower_reserved.msg_seq == upper_reserved.msg_seq + 1, "client ID case was collapsed");

        const auto concurrent = Candidate(prefix + "b", "shared");
        std::atomic<int> failures{0}, fresh_count{0};
        std::mutex mutex;
        std::vector<sparkpush::Message> originals;
        std::vector<std::thread> workers;
        for (int worker = 0; worker < 8; ++worker) workers.emplace_back([&, worker] {
            auto request = concurrent;
            request.content_json += std::to_string(worker);
            sparkpush::Message result;
            bool is_new = false;
            std::string local_error;
            if (!first.Reserve(request, 500, &result, &is_new, &local_error)) ++failures;
            else {
                if (is_new) ++fresh_count;
                std::lock_guard<std::mutex> lock(mutex);
                originals.push_back(result);
            }
        });
        for (auto& worker : workers) worker.join();
        Require(failures == 0 && fresh_count == 1 && originals.size() == 8,
                "concurrent retries created multiple first reservations");
        for (const auto& original : originals) Same(originals.front(), original);
        Require(originals.front().msg_seq == 501, "concurrent reservation floor was not atomic");

        workers.clear();
        std::set<int64_t> sequences;
        for (int worker = 0; worker < 8; ++worker) workers.emplace_back([&, worker] {
            for (int index = 0; index < 12; ++index) {
                auto request = Candidate(prefix + "c", std::to_string(worker) + "-" + std::to_string(index));
                sparkpush::Message result;
                bool is_new = false;
                std::string local_error;
                if (!first.Reserve(request, 0, &result, &is_new, &local_error) || !is_new) ++failures;
                else { std::lock_guard<std::mutex> lock(mutex); sequences.insert(result.msg_seq); }
            }
        });
        for (auto& worker : workers) worker.join();
        Require(failures == 0 && sequences.size() == 96 && *sequences.begin() == 1 && *sequences.rbegin() == 96,
                "cross-process session allocation lost or duplicated a sequence");

        auto invalid = Candidate(prefix + "d", "invalid");
        invalid.content_json.assign(1, static_cast<char>(0xff));
        Require(!first.Reserve(invalid, 1000, &duplicate, &fresh, &error),
                "invalid UTF-8 original unexpectedly reserved");
        Require(first.Reserve(Candidate(prefix + "d", "valid"), 0, &duplicate, &fresh, &error) && fresh && duplicate.msg_seq == 1,
                "failed reservation left its floor/counter committed");
        auto empty = Candidate(prefix + "e", "");
        Require(first.Reserve(empty, 10, &reserved, &fresh, &error) && fresh && reserved.msg_seq == 11, error);
        Require(first.Reserve(empty, 0, &duplicate, &fresh, &error) && fresh && duplicate.msg_seq == 12,
                "empty client IDs were incorrectly deduplicated");
        std::cout << "PASS reservation: pending restart, first full payload, case-sensitive ID, concurrent retry/allocation, transaction rollback, empty ID\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
}
