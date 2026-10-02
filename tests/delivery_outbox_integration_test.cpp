#include "delivery_outbox.h"
#include "sql_helpers.h"
#include <mysql/mysql.h>
#include <atomic>
#include <chrono>
#include <cstdlib>
#include <iostream>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>
#include <unistd.h>

namespace {
std::string Env(const char* name, const char* fallback) {
    const auto* value = std::getenv(name);
    return value ? value : fallback;
}
void Require(bool ok, const std::string& reason) {
    if (!ok) throw std::runtime_error(reason);
}
void Sql(sparkpush::MySqlConnectionPool& pool, const std::string& query) {
    auto guard = pool.Acquire(2000);
    auto* conn = guard.get();
    Require(conn && mysql_query(conn, query.c_str()) == 0,
            conn ? mysql_error(conn) : "no mysql connection");
}
int64_t Scalar(sparkpush::MySqlConnectionPool& pool, const std::string& query) {
    auto guard = pool.Acquire(2000);
    std::vector<std::vector<std::string>> rows;
    std::string error;
    Require(sparkpush::SqlRows(guard.get(), query, &rows, &error) && rows.size() == 1, error);
    return std::stoll(rows[0][0]);
}
sparkpush::PersistMessageRequest Event(const std::string& session, int64_t seq) {
    sparkpush::PersistMessageRequest result;
    result.set_scene("single");
    result.set_user1_id(101);
    result.set_user2_id(102);
    auto* message = result.mutable_message();
    message->set_msg_id(session + "-" + std::to_string(seq));
    message->set_session_id(session);
    message->set_msg_seq(seq);
    message->set_sender_id(101);
    message->set_msg_type("text");
    // Verify the BLOB preserves both UTF-8 and embedded zero bytes.
    message->set_content_json(std::string("\"中文\"\0tail", 13));
    message->set_timestamp_ms(123);
    message->set_client_msg_id("test-original");
    return result;
}
void Persist(sparkpush::MySqlConnectionPool& pool, const sparkpush::PersistMessageRequest& event) {
    const auto& message = event.message();
    Sql(pool, "INSERT INTO message(session_id,msg_seq,sender_id,msg_type,content_json,timestamp_ms,client_msg_id) "
        "VALUES('" + message.session_id() + "'," + std::to_string(message.msg_seq()) +
        ",101,'text','{}',123,'test')");
}
struct Cleanup {
    sparkpush::MySqlConnectionPool& pool;
    std::string prefix;
    ~Cleanup() {
        try {
            const std::string owned = " WHERE LEFT(session_id," + std::to_string(prefix.size()) + ")='" + prefix + "'";
            Sql(pool, "DELETE FROM delivery_outbox" + owned);
            Sql(pool, "DELETE FROM message" + owned);
            Sql(pool, "DELETE FROM `session`" + owned);
        } catch (...) {}
    }
};
}  // namespace

int main() {
    if (Env("SPARK_PUSH_RUN_MYSQL_TESTS", "0") != "1") {
        std::cout << "SKIP (77): set SPARK_PUSH_RUN_MYSQL_TESTS=1 and an isolated SPARK_PUSH_MYSQL_TEST_DB\n";
        return 77;
    }
    try {
        sparkpush::MySqlConfig config;
        config.host = Env("SPARK_PUSH_MYSQL_HOST", "127.0.0.1");
        config.port = std::stoi(Env("SPARK_PUSH_MYSQL_PORT", "3306"));
        config.user = Env("SPARK_PUSH_MYSQL_USER", "root");
        config.password = Env("SPARK_PUSH_MYSQL_PASSWORD", "");
        config.db = Env("SPARK_PUSH_MYSQL_TEST_DB", "spark_push_test");
        Require(config.db != "spark_push", "integration test requires a separate test database");
        config.pool_size = 4;
        config.max_pool_size = 8;
        sparkpush::MySqlConnectionPool pool;
        Require(pool.Init(config), "test MySQL pool init failed");
        sparkpush::DeliveryOutbox first(&pool), second(&pool);
        std::string error;
        Require(first.EnsureSchema(&error), error);
        Sql(pool, "CREATE TABLE IF NOT EXISTS message (session_id VARCHAR(128) NOT NULL,"
            "msg_seq BIGINT NOT NULL,sender_id BIGINT NOT NULL,msg_type VARCHAR(32) NOT NULL,"
            "content_json MEDIUMTEXT NOT NULL,timestamp_ms BIGINT NOT NULL,client_msg_id VARCHAR(128) NOT NULL,"
            "PRIMARY KEY(session_id,msg_seq)) ENGINE=InnoDB");
        Sql(pool, "CREATE TABLE IF NOT EXISTS `session` (session_id VARCHAR(128) NOT NULL PRIMARY KEY,"
            "type TINYINT NOT NULL,user1_id BIGINT NOT NULL,user2_id BIGINT NOT NULL,group_id BIGINT NOT NULL,"
            "last_msg_seq BIGINT NOT NULL DEFAULT 0) ENGINE=InnoDB");
        int64_t backlog = -1;
        Require(first.Backlog(&backlog, &error) && backlog == 0,
                "test database must have no unrelated unfinished delivery tasks");
        const std::string prefix = "outbox_test_" + std::to_string(getpid()) + "_" +
            std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
        Cleanup cleanup{pool, prefix};
        const auto a1 = Event(prefix + "a", 1);
        const auto a2 = Event(prefix + "a", 2);
        const auto b1 = Event(prefix + "b", 1);
        Require(first.InsertTask(a1, &error) && first.InsertTask(a2, &error), error);
        sparkpush::DeliveryOutbox::Task task, other;
        bool found = true;
        Require(first.ClaimReady(800, &task, &found, &error) && !found,
                "task became visible before its message was committed");
        Persist(pool, a1);
        Persist(pool, a2);
        auto changed = a1;
        changed.mutable_message()->set_content_json("replacement");
        Require(second.InsertTask(changed, &error), error);
        Require(first.ClaimReady(800, &task, &found, &error) && found && task.msg_id == a1.message().msg_id(),
                "session head was not claimed");
        Require(task.request.SerializeAsString() == a1.SerializeAsString(), "replay overwrote original event");
        Require(second.ClaimReady(800, &other, &found, &error) && !found,
                "parallel dispatcher overtook an unfinished session head");
        Require(first.InsertTask(b1, &error), error);
        Persist(pool, b1);
        Require(second.ClaimReady(800, &other, &found, &error) && found && other.msg_id == b1.message().msg_id(),
                "one blocked session prevented independent progress");
        Require(second.Complete(other, &error), error);
        Require(first.Retry(task, 250, &error), error);
        Require(!first.Complete(task, &error), "released lease still completed the task");
        Require(second.ClaimReady(100, &other, &found, &error) && !found, "retry ignored its backoff");
        std::this_thread::sleep_for(std::chrono::milliseconds(300));
        Require(second.ClaimReady(100, &other, &found, &error) && found && other.attempts == 2,
                "retry was not automatically claimable");
        Require(second.Renew(other, 200, &error), error);
        std::this_thread::sleep_for(std::chrono::milliseconds(250));
        Require(!second.Complete(other, &error), "expired owner completed a task");
        Require(first.ClaimReady(2000, &task, &found, &error) && found && task.attempts == 3,
                "abandoned lease did not recover after restart");
        Require(!second.Retry(other, 1000, &error), "stale owner modified a successor lease");
        Require(first.Complete(task, &error), error);
        Require(first.ClaimReady(2000, &task, &found, &error) && found && task.msg_id == a2.message().msg_id(),
                "session successor did not become ready after completion");
        Require(first.Complete(task, &error), error);
        Require(first.InsertTask(a1, &error), error);
        Require(first.ClaimReady(2000, &task, &found, &error) && !found,
                "completed replay recreated delivery work");

        const auto c1 = Event(prefix + "c", 1);
        Require(first.InsertTask(c1, &error), error);
        Persist(pool, c1);
        std::atomic<bool> start{false};
        std::atomic<int> winners{0}, failures{0};
        std::mutex winner_mutex;
        sparkpush::DeliveryOutbox::Task winner;
        auto compete = [&](sparkpush::DeliveryOutbox& store) {
            while (!start.load()) std::this_thread::yield();
            sparkpush::DeliveryOutbox::Task candidate;
            bool claimed = false;
            std::string local_error;
            if (!store.ClaimReady(2000, &candidate, &claimed, &local_error)) ++failures;
            else if (claimed) { std::lock_guard<std::mutex> lock(winner_mutex); winner = candidate; ++winners; }
        };
        std::vector<std::thread> dispatchers;
        for (int worker = 0; worker < 8; ++worker) {
            dispatchers.emplace_back(compete, std::ref(worker % 2 == 0 ? first : second));
        }
        start = true;
        for (auto& dispatcher : dispatchers) dispatcher.join();
        Require(failures == 0 && winners == 1, "concurrent Job instances both owned one task");
        Require(first.Complete(winner, &error), error);
        Require(first.Backlog(&backlog, &error) && backlog == 0, "completed tasks remained in backlog");
        const auto poison = Event(prefix + "d", 1);
        const auto valid = Event(prefix + "d", 2);
        Require(first.InsertTask(poison, &error) && first.InsertTask(valid, &error), error);
        Persist(pool, valid);
        Require(first.ClaimReady(2000, &task, &found, &error) && found && task.msg_id == valid.message().msg_id(),
                "an unpersisted poison record blocked valid session delivery");
        Require(first.Complete(task, &error), error);
        Require(first.Backlog(&backlog, &error) && backlog == 1, "unpersisted task should remain visible to diagnosis");
        auto atomic = Event(prefix + "atomic", 21);
        atomic.set_ack_comet_id("original-comet");
        atomic.set_ack_user_id(101);
        atomic.add_recipient_user_ids(101);
        atomic.add_recipient_user_ids(102);
        Require(first.PersistAtomically(atomic, &error), error);
        auto replay = atomic;
        replay.set_ack_comet_id("replacement-comet");
        Require(second.PersistAtomically(replay, &error), error);
        Require(first.ClaimReady(2000, &task, &found, &error) && found &&
                task.request.SerializeAsString() == atomic.SerializeAsString(),
                "atomic replay replaced first delivery payload");
        Require(first.Complete(task, &error), error);
        Require(Scalar(pool, "SELECT COUNT(*) FROM message WHERE session_id='" + atomic.message().session_id() + "'") == 1,
                "atomic replay duplicated message history");
        Require(Scalar(pool, "SELECT last_msg_seq FROM `session` WHERE session_id='" + atomic.message().session_id() + "'") == 21,
                "atomic session sequence did not advance");
        auto collision = atomic;
        collision.mutable_message()->set_content_json("different-content");
        Require(!first.PersistAtomically(collision, &error) && error.find("message sequence collision") == 0,
                "immutable content collision was silently accepted");
        Require(Scalar(pool, "SELECT COUNT(*) FROM delivery_outbox WHERE session_id='" + atomic.message().session_id() + "'") == 1,
                "collision exposed a second delivery task");

        // A failure after session/outbox insertion rolls back all new rows.
        auto rollback = Event(prefix + "rollback", 99);
        rollback.mutable_message()->set_timestamp_ms(-1); // validate failures also leave no session
        Require(!first.PersistAtomically(rollback, &error), "invalid atomic event accepted");
        rollback.mutable_message()->set_timestamp_ms(123);
        rollback.mutable_message()->set_msg_type(std::string(32, static_cast<char>(0xff)));
        Require(!first.PersistAtomically(rollback, &error), "invalid SQL text should fail after inserts");
        const auto rollback_sid = rollback.message().session_id();
        Require(Scalar(pool, "SELECT COUNT(*) FROM `session` WHERE session_id='" + rollback_sid + "'") == 0 &&
                Scalar(pool, "SELECT COUNT(*) FROM delivery_outbox WHERE session_id='" + rollback_sid + "'") == 0 &&
                Scalar(pool, "SELECT COUNT(*) FROM message WHERE session_id='" + rollback_sid + "'") == 0,
                "failed atomic persistence exposed session/history/task rows");

        auto existing_collision = Event(prefix + "existing", 5);
        Persist(pool, existing_collision); // preexisting immutable history with '{}'
        Require(!first.PersistAtomically(existing_collision, &error) && error.find("message sequence collision") == 0,
                "collision against existing history was accepted");
        Require(Scalar(pool, "SELECT COUNT(*) FROM `session` WHERE session_id='" + existing_collision.message().session_id() + "'") == 0 &&
                Scalar(pool, "SELECT COUNT(*) FROM delivery_outbox WHERE session_id='" + existing_collision.message().session_id() + "'") == 0,
                "existing history collision committed new session/outbox");
        auto next_atomic = Event(atomic.message().session_id(), 23);
        Require(first.PersistAtomically(next_atomic, &error), error);
        Require(first.PersistAtomically(atomic, &error), error);
        Require(Scalar(pool, "SELECT last_msg_seq FROM `session` WHERE session_id='" + atomic.message().session_id() + "'") == 23,
                "replay moved last message sequence backwards");
        Require(first.ClaimReady(2000, &task, &found, &error) && found && first.Complete(task, &error), error);
        auto room = Event(prefix + "room", 1);
        room.set_scene("chatroom"); room.set_room_id(99);
        Require(first.PersistAtomically(room, &error), error);
        Require(Scalar(pool, "SELECT type FROM `session` WHERE session_id='" + room.message().session_id() + "'") == 2 &&
                Scalar(pool, "SELECT group_id FROM `session` WHERE session_id='" + room.message().session_id() + "'") == 99,
                "atomic room metadata differs from SessionDao");
        Require(first.ClaimReady(2000, &task, &found, &error) && found && first.Complete(task, &error), error);
        std::cout << "PASS outbox: gate/replay/ordering/retry/renewal/fencing/concurrency/poison/atomic rollback/immutable collisions/room metadata\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
}
