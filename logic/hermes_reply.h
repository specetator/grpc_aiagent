#pragma once

#include <functional>
#include <nlohmann/json.hpp>
#include "model.h"

namespace sparkpush {

struct PreparedHermesReply {
    std::string request_id;
    int64_t user_id{0};
    bool ok{false};
    Message message;
    std::string citation_warning;
};

// No wall clock reads: replaying a Kafka record must produce identical bytes.
bool PrepareHermesReply(const std::string& payload, int64_t bot_user_id,
                       const std::string& bot_name, PreparedHermesReply* reply,
                       std::string* error,
                       const nlohmann::json* authorized_room_request = nullptr);

// allocate fills msg_id/msg_seq, even for an existing client_msg_id. persist
// confirms Kafka delivery, not the later database write or WebSocket delivery.
bool PersistHermesReply(
    Message* message, bool* is_new,
    const std::function<bool(Message*, bool*)>& allocate,
    const std::function<bool(const Message&)>& persist);

}  // namespace sparkpush
