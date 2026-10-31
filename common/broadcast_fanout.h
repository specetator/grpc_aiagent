#pragma once
#include <chrono>
#include <optional>
#include "spark_push.pb.h"
namespace sparkpush {

// 把一条广播任务投影成「发往指定 Comet」的下推请求。
//
// 放在 common/ 而不是 job/ 里的原因：让它能被单测直接驱动。
// tests/broadcast_fanout_test.cpp 不启任何服务就能验证 scope 分支与
// 幂等键约定；若把逻辑内联在 JobRunner 里，这些回归就只能靠端到端跑。
// 实际生产调用方是 job/service.cpp::HandleBroadcastTask。
//
// 返回 nullopt 的三种情况（调用方应跳过该节点，不算错误）：
//   scope 不是 all/group、group 广播但目标列表为空。
inline std::optional<PushToCometRequest> BuildBroadcastPush(const BroadcastTaskRequest& task,
                                                          const std::string& comet) {
    if (task.scope() != "all" && task.scope() != "group") return {};
    PushToCometRequest request;
    request.set_comet_id(comet);
    // 幂等键 = task_id + "@" + comet，与普通消息的 msg_id + "@" + comet 同构。
    // 必须是**确定性**的：广播重发时同一节点要算出相同键，
    // Comet 侧 AcceptPushRequest 才能识别重复，否则全量广播重试
    // 会让每个客户端看到 N 份相同公告。
    request.set_request_id(task.task_id() + "@" + comet);
    request.set_scene("broadcast");
    if (task.scope() == "group") {
        request.set_access_room_id(task.group_id());
        // 定向广播：user_ids 是 Logic 从**持久化成员表**解出的快照，
        // 不是 WebSocket 在线列表。安全约束：绝不能把「在线」当成
        // 「有权接收」，否则非成员连上房间就能窃听工作区消息。
        for (auto user : task.user_ids()) if (user > 0) request.add_targets()->set_user_id(user);
        if (request.targets().empty()) return {};
    }
    // scope == "all" 时故意不设 targets：空目标列表就是「该节点全量」
    // 的约定，Comet 会走 PushToAll 分支。
    auto* message = request.mutable_message();
    message->set_msg_id(task.task_id());
    // 广播不属于任何会话，因此不分配 msg_seq、不进历史、不参与
    // 未读/游标计算。session_id 固定为字面量 "broadcast" 仅作标识。
    message->set_session_id("broadcast");
    message->set_msg_type("broadcast");
    message->set_content_json(task.content_json());
    message->set_timestamp_ms(std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count());
    return request;
}
}  // namespace sparkpush
