#pragma once
#include <chrono>
#include <optional>
#include "spark_push.pb.h"
namespace sparkpush {
inline std::optional<PushToCometRequest> BuildBroadcastPush(const BroadcastTaskRequest& task,
                                                          const std::string& comet) {
    if (task.scope() != "all" && task.scope() != "group") return {};
    PushToCometRequest request;
    request.set_comet_id(comet);
    request.set_request_id(task.task_id() + "@" + comet);
    request.set_scene("broadcast");
    if (task.scope() == "group") {
        for (auto user : task.user_ids()) if (user > 0) request.add_targets()->set_user_id(user);
        if (request.targets().empty()) return {};
    }
    auto* message = request.mutable_message();
    message->set_msg_id(task.task_id());
    message->set_session_id("broadcast");
    message->set_msg_type("broadcast");
    message->set_content_json(task.content_json());
    message->set_timestamp_ms(std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count());
    return request;
}
}  // namespace sparkpush
