#include <cstdlib>
#include <iostream>
#include <mutex>
#include <set>
#include <stdexcept>
#include <thread>
#include <unistd.h>
#include "conversation_store.h"
#include "mysql_statement.h"
#include "group_dao.h"
#include "repositories/dao_repositories.h"
#include "services/conversation_service.h"
using namespace sparkpush;
void Require(bool value, const std::string& reason) { if (!value) throw std::runtime_error(reason); }
int main() {
    if (!std::getenv("SPARK_PUSH_RUN_MYSQL_TESTS")) return 77;
    MySqlConfig config;
    config.host = "127.0.0.1";
    config.port = std::stoi(std::getenv("SPARK_TEST_MYSQL_PORT"));
    config.user = "root";
    config.password = std::getenv("SPARK_TEST_MYSQL_PASSWORD");
    config.db = "spark_review_test";
    config.pool_size = config.min_pool_size = config.max_pool_size = 8;
    MySqlConnectionPool pool;
    Require(pool.Init(config), "test pool init");
    MySqlMessageReservations reservations(&pool);
    Message proposed;
    proposed.session_id = "reservation_" + std::to_string(getpid());
    proposed.sender_id = 7;
    proposed.client_msg_id = "same-id";
    proposed.content_json = R"({"content":{"text":"hello"}})";
    proposed.msg_type = "text";
    proposed.timestamp_ms = 1000;
    Message first, retry;
    bool fresh = false;
    std::string error;
    Require(reservations.Reserve(proposed, &first, &fresh, &error) && fresh && first.msg_seq == 1, error);
    proposed.timestamp_ms = 2000;
    Require(reservations.Reserve(proposed, &retry, &fresh, &error) && !fresh && retry.timestamp_ms == 1000, error);
    proposed.content_json = "changed";
    Require(!reservations.Reserve(proposed, &retry, &fresh, &error), "reject changed payload for same ID");
    proposed.content_json = first.content_json;
    const std::string binary("payload\0binary", 14);
    Require(reservations.PrepareOutbox(first, binary, &error), error);
    std::vector<std::pair<Message, std::string>> pending;
    Require(reservations.Pending(&pending, 10, &error) && pending.size() == 1 && pending[0].second == binary, error);
    int64_t safe = -1;
    {
        auto guard = pool.Acquire();
        Require(MySqlMessageReservations::SafePrefix(guard.get(), first.session_id, &safe, &error) && safe == 0, error);
    }
    Require(reservations.Published(first, &error) && reservations.Persisted(first, &error), error);
    Require(reservations.Published(first, &error), "late publication report cannot lower persisted stage");
    std::set<int64_t> sequences;
    std::mutex mutex;
    std::vector<std::thread> threads;
    bool concurrent_ok = true;
    for (int i = 0; i < 16; ++i) threads.emplace_back([&, i] {
        Message m = proposed;
        m.client_msg_id = "parallel-" + std::to_string(i);
        Message stored;
        bool new_record;
        std::string failure;
        const bool ok = reservations.Reserve(m, &stored, &new_record, &failure);
        std::lock_guard<std::mutex> lock(mutex);
        concurrent_ok = concurrent_ok && ok && new_record;
        if (ok) sequences.insert(stored.msg_seq);
    });
    for (auto& thread : threads) thread.join();
    Require(concurrent_ok && sequences.size() == 16 && *sequences.begin() == 2 && *sequences.rbegin() == 17,
            "concurrent durable allocation");
    // Legacy HTTP and WS reserve from the same authority, even without any Redis connection.
    SessionDao sessions(&pool);
    MessageDao messages(&pool);
    UserSessionStateDao states(&pool);
    ConversationStore store(&sessions, &messages, &states, nullptr);
    Session session;
    Require(store.GetOrCreateSingleSession(7, 8, &session, &error), error);
    Message ws, legacy;
    Require(store.AppendMessageHotPath(session.id, 7, "text", "first", 1000, "ws-first", &ws, &fresh, &error), error);
    Require(store.AppendMessage(session, 7, "text", "legacy", 2000, "", &legacy, &error) && legacy.msg_seq == ws.msg_seq + 1, error);
    Require(messages.InsertMessage(ws, &error) && reservations.Persisted(ws, &error), error);
    Require(states.UpsertDeliveredSeq(8, session.id, legacy.msg_seq, &error), error);
    int64_t cursor;
    Require(states.GetDeliveredSeq(8, session.id, &cursor, &error) && cursor == 0,
            "later delivery cannot skip a lower undelivered message");
    Require(states.UpsertDeliveredSeq(8, session.id, ws.msg_seq, &error), error);
    Require(states.GetDeliveredSeq(8, session.id, &cursor, &error) && cursor == legacy.msg_seq,
            "receipt gap closes after the missing delivery");
    // An unfinished lower reservation blocks the safe prefix despite a later SQL row.
    Message lower, upper;
    Require(store.AppendMessageHotPath(session.id, 7, "text", "lower", 3000, "lower", &lower, &fresh, &error), error);
    Require(store.AppendMessage(session, 7, "text", "upper", 4000, "upper", &upper, &error), error);
    GroupMemberDao members(&pool);
    DaoConversationRepository conversation_repository(&store, &members);
    ConversationService conversations(&conversation_repository);
    MessagePage page;
    Require(conversations.SyncAfter(8, session.id, legacy.msg_seq, 10, &page).ok() &&
            page.messages.size() == 1 && page.next_seq == upper.msg_seq && page.safe_seq == legacy.msg_seq,
            "real repository must not call next_seq a safe prefix during SQL lag");
    Require(states.UpsertDeliveredSeq(8, session.id, upper.msg_seq, &error), error);
    Require(states.GetDeliveredSeq(8, session.id, &cursor, &error) && cursor == legacy.msg_seq, "SQL lag blocks cursor");
    Require(messages.InsertMessage(lower, &error) && reservations.Persisted(lower, &error), error);
    Require(states.UpsertDeliveredSeq(8, session.id, lower.msg_seq, &error), error);
    Require(states.GetDeliveredSeq(8, session.id, &cursor, &error) && cursor == upper.msg_seq, "lag recovery advances cursor");
    Require(conversations.SyncAfter(8, session.id, legacy.msg_seq, 1, &page).ok() &&
            page.messages.size() == 1 && page.has_more && page.next_seq == lower.msg_seq && page.safe_seq == lower.msg_seq,
            "safe prefix bounded by actually returned page");
    // Concurrent receipts for the same user/session must serialize without lock upgrades.
    threads.clear();
    concurrent_ok = true;
    for (int i = 0; i < 16; ++i) threads.emplace_back([&, i] {
        std::string failure;
        const bool ok = states.UpsertDeliveredSeq(9, session.id, (i % 4) + 1, &failure);
        std::lock_guard<std::mutex> lock(mutex);
        concurrent_ok = concurrent_ok && ok;
    });
    for (auto& thread : threads) thread.join();
    Require(concurrent_ok && states.GetDeliveredSeq(9, session.id, &cursor, &error) && cursor == 4,
            "concurrent delivery receipts");
    std::cout << "canonical retries, parallel allocation, legacy coexistence, outbox and safe delivery passed\n";
}
