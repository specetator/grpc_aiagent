#pragma once
#include "model.h"
#include "mysql_pool.h"
#include <cstdint>
#include <string>

namespace sparkpush {

// The allocation ledger survives Redis loss while Kafka persistence is pending.
// A reservation is not accepted_ack: the caller must still confirm Kafka publish.
class MessageReservation {
public:
    explicit MessageReservation(MySqlConnectionPool* pool) : pool_(pool) {}
    bool EnsureSchema(std::string* error);
    // candidate.msg_seq/msg_id are ignored. floor includes durable history and,
    // during rollout, any preexisting Redis allocation frontier.
    // Empty client_msg_id allocates a unique sequence without request deduplication.
    bool Reserve(const Message& candidate, int64_t floor, Message* message,
                 bool* is_new, std::string* error);
private:
    MySqlConnectionPool* pool_;
};
}  // namespace sparkpush
