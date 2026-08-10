#include "websocket_utils.h"

#include <iostream>
#include <string>

int main() {
    sparkpush::UpstreamMessageMeta meta;
    if (!sparkpush::ParseUpstreamMessage(
            R"({"type":"single_chat","to_user_id":42,"client_msg_id":"c-1"})",
            &meta) ||
        meta.type != "single_chat" || meta.to_user_id != 42 ||
        meta.client_msg_id != "c-1") {
        std::cerr << "single chat parse failed\n";
        return 1;
    }
    if (sparkpush::ParseUpstreamMessage(
            R"({"type":"single_chat","to_user_id":999999999999999999999999})",
            &meta)) {
        std::cerr << "overflowing numeric field accepted\n";
        return 1;
    }

    const std::string short_frame = sparkpush::BuildWebSocketTextFrame("hello");
    const std::string medium_payload(126, 'x');
    const std::string medium_frame =
        sparkpush::BuildWebSocketTextFrame(medium_payload);
    if (short_frame.size() != 7 ||
        static_cast<unsigned char>(short_frame[0]) != 0x81 ||
        static_cast<unsigned char>(short_frame[1]) != 5 ||
        medium_frame.size() != medium_payload.size() + 4 ||
        static_cast<unsigned char>(medium_frame[1]) != 126) {
        std::cerr << "WebSocket frame encoding failed\n";
        return 1;
    }
    return 0;
}
