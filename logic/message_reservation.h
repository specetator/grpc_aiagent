#pragma once
#include "model.h"
#include "mysql_pool.h"
namespace sparkpush {

// 消息「预约表」抽象接口。
//
// 抽出来只有一个目的：让 ConversationStore 能被注入假实现。
// tests/message_acceptance_test.cpp 里就有一个内存版 Reservations，
// 不连 MySQL 也能跑完整的「预约 -> 发 Kafka -> 落库」验收流程。
//
// The durable authority survives Redis loss. Only the Kafka delivery report is an accepted ACK.
// 译：MySQL 里的预约记录才是持久权威。Redis 上的序号计数器丢了可以从这里
// 重建（Reserve 首次遇到 last_seq=0 时会去扫 message/session 表取历史最大值），
// 但「消息已被接受」这件事只认 Kafka 的 delivery report，不认预约记录本身——
// 预约只保证序号不重复，不保证消息不会丢。
class MessageReservationRepository {
 public:
    virtual ~MessageReservationRepository() = default;
    // 为一条消息占下一个 msg_seq。proposed 是客户端提交的原样，
    // canonical 返回权威结果（msg_seq/msg_id 由服务端填），
    // fresh=true 表示新占的号，false 表示命中同 client_msg_id 的旧预约（幂等重放）。
    virtual bool Reserve(const Message& proposed, Message* canonical, bool* fresh, std::string* error) = 0;
    // 把待发的 Kafka payload 写进预约记录（stage 0 -> 1）。
    virtual bool PrepareOutbox(const Message&, const std::string& payload, std::string* error) = 0;
    // Kafka delivery report 回来后调用（stage -> 2）。
    virtual bool Published(const Message&, std::string* error) = 0;
};

// MySQL 实现，对应三张表：
//   message_sequence         每会话的持久序号计数器（Redis 丢失后的兜底权威）
//   message_reservation      outbox 本体，stage 字段是状态机
//   message_delivery_receipt 每个用户的送达回执，用于算离线补推游标
//
// stage 状态机（与 sql/schema.sql 里的注释一致，单向不回退）：
//   0 reserved  已占号，payload 还没写进去
//   1 prepared  payload 已落库，等待/正在发 Kafka
//   2 published Kafka 已确认收到
//   3 persisted Job 已把消息写进 message 表（终态）
//   4 sealed    占号后 10 分钟仍停在 0，判定「绝不可能到过 Kafka」，封存
//
// stage=4 是唯一一个「作废」态，它存在的意义是给 SafePrefix 一个明确答案：
// 没有它，一个卡死的 0 号预约会把安全同步水位永久压在它下面。
class MySqlMessageReservations final : public MessageReservationRepository {
 public:
    explicit MySqlMessageReservations(MySqlConnectionPool* pool) : pool_(pool) {}

    // 单事务内完成「占号 + 去重」，幂等键是 (session_id, sender_id, client_msg_id)。
    // 命中旧预约时会比对 msg_type/content：内容一致就当幂等重放返回 canonical，
    // 内容不一致直接报错——同一个 client_msg_id 换正文是不允许的。
    // client_msg_id 为空时会生成一个安全随机串顶上，等价于「每次都是新提交」，
    // 这是为了兼容不带 client_msg_id 的老 HTTP 接口。
    bool Reserve(const Message&, Message*, bool*, std::string*) override;

    // 启动时的 fail-fast 探针：三张表的关键列必须都在。
    // 用 SELECT ... LIMIT 0，只校验 schema 不取数据。
    bool CheckSchema(std::string* error);

    // 写 outbox payload 并把 stage 抬到 1。
    // WHERE 里的 (persist_payload IS NULL OR persist_payload=?) 是关键：
    // 已经用**不同** payload 预备过的记录不会被覆盖，随后的一致性回读会发现
    // rows[0][0] != payload 并返回冲突错误。防止同一条消息发出两个不同版本。
    bool PrepareOutbox(const Message&, const std::string& payload, std::string* error) override;

    // 用 GREATEST(stage,2) 而不是直接赋 2，因此迟到的 publication 报告
    // 无法把已经 persisted(3) 的记录降回去。
    bool Published(const Message&, std::string* error) override;

    // Job 落库成功后调用。这里是直接 stage=3（不走 GREATEST），
    // 因为 3 就是终态，任何更低的状态都应该被它覆盖。
    // 注意：不在虚接口里，只有 MySQL 实现和内部恢复逻辑需要它。
    bool Persisted(const Message&, std::string* error);

    // outbox 恢复扫描，由 logic/main.cpp 里 1s 一次的 PeriodicWorker 驱动。
    // 一次调用做三件事，顺序不能换：
    //   1. 把卡在 stage=0 超过 10 分钟的预约封存为 4
    //   2. 清理 stage in (3,4) 且超过 1 天的记录（每轮最多 1000 行，避免长事务）
    //   3. 按 created_at 升序取出 stage=1 的记录交给调用方重发
    // 返回 (Message, payload) 对：payload 就是当年 PrepareOutbox 存下的原始字节，
    // 重发时**不重新编码**，保证 Kafka 上出现的字节和第一次完全一致。
    bool Pending(std::vector<std::pair<Message, std::string>>*, int limit, std::string* error);

    // 安全同步前缀：所有 msg_seq <= 返回值 的消息都已确定持久化。
    // 取三者的 LEAST——
    //   message_sequence.last_seq        已分配到哪
    //   MAX(message.msg_seq)             已落库到哪
    //   MIN(未完成预约.msg_seq) - 1      最早的「空洞」在哪
    // 任何客户端只能被同步到这个水位为止。少算任何一项都可能让客户端
    // 认为某条消息已持久，而它其实还悬在 outbox 里。
    // static + 裸 MYSQL*：调用方（AdvanceDelivered）需要把它编进自己的事务。
    static bool SafePrefix(MYSQL*, const std::string& session, int64_t* seq, std::string* error);

 private:
    MySqlConnectionPool* pool_;
};
}  // namespace sparkpush
