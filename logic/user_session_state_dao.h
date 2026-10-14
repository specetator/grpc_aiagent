#pragma once

#include <string>

#include "mysql_pool.h"

namespace sparkpush {

// 每个「用户 x 会话」的两个单调游标，存在 user_session_state 表。
//
// 两个游标语义完全不同，不要混用：
//   read_seq       用户**看到了**哪（业务语义，驱动未读数与已读回执）
//   delivered_seq  系统**确定送到了**哪（可靠性语义，驱动断线后的离线补推）
// delivered_seq 故意不等于「收到的最大 seq」，而是一个**无空洞水位线**：
// 只有当 1..N 全部有回执时它才会推到 N。若取最大值，中间漏掉的那条
// 就再也不会被补推了——这是静默丢消息，比报错难查得多。
class UserSessionStateDao {
   public:
    explicit UserSessionStateDao(MySqlConnectionPool* pool) : pool_(pool) {}

    // 功能：插入或更新用户会话已读序列
    // 参数：user_id 用户；session_id 会话；read_seq 已读；err_msg 错误
    // 返回：成功 true，失败 false
    // 用 GREATEST(read_seq, VALUES(read_seq))：并发的乱序已读上报不会把
    // 游标往回拉（否则用户刷一下历史消息，未读数就会反弹）。
    bool UpsertReadSeq(int64_t user_id, const std::string& session_id,
                       int64_t read_seq, std::string* err_msg);

    // 功能：查询用户会话已读序列
    // 参数：user_id 用户；session_id 会话；read_seq 输出；err_msg 错误
    // 返回：成功 true，失败 false
    // 无记录时返回 true 且 *read_seq=0：「从没读过」是正常状态，不是错误。
    bool GetReadSeq(int64_t user_id, const std::string& session_id,
                    int64_t* read_seq, std::string* err_msg);

    // 单调推进客户端已收到的 msg_seq，用于断线后的离线补推。
    // 名为 Upsert 但实际是「登记一条回执后重算水位」：传进去的 seq 不一定
    // 会反映到结果里（前面有空洞或 SQL 还没追上时就会被卡住）。
    bool UpsertDeliveredSeq(int64_t user_id, const std::string& session_id,
                            int64_t delivered_seq, std::string* err_msg);

    // 注意：这不是一个只读查询。它走的是同一个 AdvanceDelivered，
    // 会重算并回写 delivered_seq、还会删掉已被水位覆盖的回执行。
    // 这样设计是有意的：水位只能由「重算」得出，单独维护一个只读路径
    // 很容易和写入路径的算法不一致。
    bool GetDeliveredSeq(int64_t user_id, const std::string& session_id,
                         int64_t* delivered_seq, std::string* err_msg);

    // 兼容已经初始化旧 schema 的本地开发库。
    // 启动时调一次，用 errno 1060（Duplicate column）当幂等成功。
    bool EnsureDeliveredSeqColumn(std::string* err_msg);

   private:
    // Upsert 与 Get 的共同实现。receipt=0 就是「不登记新回执，只重算」。
    // 共用一份代码而不是写两个函数，是为了保证读写看到的算法永远一致。
    bool AdvanceDelivered(int64_t user, const std::string& session, int64_t receipt,
                          int64_t* sequence, std::string* error);
    MySqlConnectionPool* pool_{nullptr};
};

}  // namespace sparkpush
