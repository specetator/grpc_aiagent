#pragma once
#include "model.h"
#include "spark_push.pb.h"

namespace sparkpush {
// Wire conversion belongs to adapters/publishers, not business DTOs.
inline void FillChatMessage(const Message& message, ChatMessage* output) {
    output->set_msg_id(message.msg_id);
    output->set_session_id(message.session_id);
    output->set_msg_seq(message.msg_seq);
    output->set_sender_id(message.sender_id);
    output->set_timestamp_ms(message.timestamp_ms);
    output->set_msg_type(message.msg_type);
    output->set_content_json(message.content_json);
    output->set_client_msg_id(message.client_msg_id);
}
}  // namespace sparkpush
