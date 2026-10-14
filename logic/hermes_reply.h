#pragma once

#include <functional>
#include <nlohmann/json.hpp>
#include "model.h"

namespace sparkpush {

// PrepareHermesReply 的输出：一条已经组装好、只差分配序号就能发的回复。
struct PreparedHermesReply {
    // 必须回带的请求 id，用于把回复归到正确的请求上（见 ValidateReply）。
    std::string request_id;
    int64_t user_id{0};
    // Agent 本次调用是否成功。失败时 message 仍然会被发出，
    // 因为用户需要看到「出错了」而不是永久等一个不会来的回复。
    bool ok{false};
    Message message;
    // 引用校验的警告文本（见 citation_validation.h）。
    // 不合格的引用已被丢弃，这里只记下「丢了几条」供日志/排查用。
    std::string citation_warning;
};

// No wall clock reads: replaying a Kafka record must produce identical bytes.
//
// 译：不读墙上时钟：重放一条 Kafka 记录必须产出完全相同的字节。
//
// 这是本函数最重要的约束，也是一个很容易不小心破坏的约束。
// 理由：hermes_reply 走 Kafka，而 Kafka 的语义是「至少一次」，
// 同一条记录可能被消费多次。下游幂等靠的是 client_msg_id
//（= "hermes:" + request_id）与内容比对：若两次生成的字节不同，
// InsertMessage 的内容比对就会判定为「序号冲突」并把消息丢进 DLQ。
//
// 具体落地在两处（修改本文件时不要破坏）：
//   timestamp_ms   取自载荷里的 completed_at_ms，不是 time(nullptr)
//   client_msg_id  由 request_id 确定性拼出，不是随机生成
// 因此本函数内部**不得**出现任何 time()/now()/chrono 调用。
//
// authorized_room_request 非空时额外校验回复归属（房间共享 Agent 场景），
// 比对 request_id / session_id / user_id / bot_user_id / collaboration 五个字段。
bool PrepareHermesReply(const std::string& payload, int64_t bot_user_id,
                       const std::string& bot_name, PreparedHermesReply* reply,
                       std::string* error,
                       const nlohmann::json* authorized_room_request = nullptr);

// allocate fills msg_id/msg_seq, even for an existing client_msg_id. persist
// confirms Kafka delivery, not the later database write or WebSocket delivery.
//
// 译：allocate 会填上 msg_id/msg_seq，即使命中的是一个已存在的 client_msg_id。
// persist 确认的是 **Kafka 投递**，不是后续的数据库写入或 WebSocket 送达。
//
// 两句话各对应一个容易踩的坑：
//   第一句：幂等重放时 allocate 仍会返回一个完整的 Message（带旧的 seq），
//          而不是报错。调用方靠 is_new 区分「新分配」与「命中旧预约」，
//          is_new=false 时不应该再推送一次。
//   第二句：persist 返回 true 只意味着 broker 已确认，对应客户端看到的
//          accepted_ack。它**不等于**消息已落库，也**不等于**用户已收到。
//          把这三者混为一谈是读本项目最容易犯的错。
//
// 用 std::function 而不是直接依赖 ConversationStore：这让本函数能在单测里
// 被驱动（传两个 lambda 即可），而且把「分配」与「持久化」两步的
// 顺序契约显式地写在签名里：必须先 allocate 再 persist，不能颠倒。
bool PersistHermesReply(
    Message* message, bool* is_new,
    const std::function<bool(Message*, bool*)>& allocate,
    const std::function<bool(const Message&)>& persist);

}  // namespace sparkpush
