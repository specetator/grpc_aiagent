#pragma once

#include <string>
#include <vector>

#include "model.h"
#include "message_reservation.h"
#include "mysql_pool.h"

namespace sparkpush {

// message 表的读写入口，同时持有 outbox（预约表）实例。
//
// 把 MySqlMessageReservations 做成成员而不是外部注入，是因为两者必须共用
// 同一个连接池、且语义上强绑定：序号分配（reservations）与消息落库
// （message 表）必须看到彼此的状态，SafePrefix 就同时查这两张表。
class MessageDao {
   public:
    explicit MessageDao(MySqlConnectionPool* pool) : pool_(pool), reservations_(pool) {}
    // 向上层（ConversationStore / Job）暴露 outbox 接口。
    MySqlMessageReservations& reservations() { return reservations_; }

    // 安全同步前缀，详见 MySqlMessageReservations::SafePrefix。
    // 这个包装自己管连接，适用于不需要事务的调用方。
    bool SafeSyncPrefix(const std::string& session, int64_t* seq, std::string* error);

    // 功能：插入一条消息记录
    // 参数：message 消息体；err_msg 错误信息
    // 返回：成功 true，失败 false
    //
    // 幂等契约（Job 重放同一条 Kafka 消息时全靠它）：
    //   用的是普通 INSERT，幂等靠唯一索引 uk_session_seq 撞 errno 1062，
    //   再调 IsSameStoredMessage 做逐字段内容比对：
    //     内容一致 -> 返回 true（幂等吸收，上层当成功处理）
    //     内容不一致 -> 返回 false 报「序号冲突」，消息进 DLQ
    //   这就是「同一个 msg_id 改正文会被拒」契约的实现点。
    bool InsertMessage(const Message& message, std::string* err_msg);

    // 功能：按会话分页查询历史消息（向上翻）
    // 参数：session_id 会话 id；anchor_seq 游标（小于该 seq）；limit 数量；
    //       messages 输出结果；err_msg 错误信息
    // 返回：成功 true，失败 false
    //
    // SQL 是 ORDER BY msg_seq DESC LIMIT ?，返回前会 std::reverse 成正序，
    // 因此调用方拿到的永远是升序。倒序取数是为了「从 anchor 往旧方向数 limit 条」，
    // 正序 SQL 做不到这个语义。
    // anchor_seq <= 0 时走不带 anchor 的 SQL，意为「取最新一页」。
    bool ListMessages(const std::string& session_id, int64_t anchor_seq,
                      int limit, std::vector<Message>* messages,
                      std::string* err_msg);

    // 按 msg_seq 正序读取 after_seq 之后的消息，用于游标补偿和离线补推。
    // 与 ListMessages 方向相反：这里是「从游标往新方向追」，不需要反转。
    // limit 会被钳到 [1,1000]，<=0 时默认 50；上限是防止一次拉太多
    // 把连接占死（mysql_stmt_store_result 会把结果集全量缓存到客户端内存）。
    bool ListMessagesAfter(const std::string& session_id, int64_t after_seq,
                           int limit, std::vector<Message>* messages,
                           std::string* err_msg);

    // 查询会话当前最大 msg_seq（无消息时 *max_seq=0）
    // 靠 SQL 里的 COALESCE(MAX(...),0)：空会话时 MAX 返回 NULL，
    // 不在 SQL 层兜住就会把 NULL 交给 std::stoll，直接抛异常。
    bool GetMaxMsgSeq(const std::string& session_id, int64_t* max_seq,
                      std::string* err_msg);

   private:
    // 1062 之后的内容比对。逐字段比（包括 BINARY 修饰的 msg_type/content/client_msg_id），
    // BINARY 是必需的：默认 collation 不区分大小写/尾部空格，
    // 不用它会把 "Hi" 与 "hi" 当成同一条消息而静默吸收掉一条真冲突。
    // 用 COUNT(*) 而不是 SELECT 整行：只需要一个布尔结果，
    // 避开把大段 MEDIUMTEXT 拉回客户端的开销。
    bool IsSameStoredMessage(MYSQL* conn, const Message& message, bool* same,
                             std::string* err_msg);

    MySqlConnectionPool* pool_{nullptr};
    MySqlMessageReservations reservations_;
};

}  // namespace sparkpush
