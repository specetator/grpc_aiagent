#include <stdexcept>
#include "broadcast_fanout.h"
using namespace sparkpush;
void Require(bool v) { if (!v) throw std::runtime_error("broadcast scope regression"); }
int main() {
    BroadcastTaskRequest task;
    task.set_task_id("test");
    task.set_scope("group");
    task.set_group_id(42);
    Require(!BuildBroadcastPush(task, "comet"));
    task.add_user_ids(-1);
    Require(!BuildBroadcastPush(task, "comet"));
    task.add_user_ids(7);
    task.add_user_ids(8);
    auto scoped = BuildBroadcastPush(task, "comet");
    Require(scoped && scoped->targets_size() == 2 && scoped->targets(0).user_id() == 7 && scoped->targets(1).user_id() == 8);
    task.set_scope("unsupported");
    Require(!BuildBroadcastPush(task, "comet"));
    task.set_scope("all");
    auto global = BuildBroadcastPush(task, "comet");
    Require(global && global->targets().empty() && global->message().session_id() == "broadcast");
}
