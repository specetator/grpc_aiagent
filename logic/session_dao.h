#pragma once

#include <string>
#include <vector>

#include "model.h"
#include "mysql_pool.h"

namespace sparkpush {

// 负责读取/写入 `session` 表，提供会话的持久化能力。
//
// session_id 是**算出来的**而不是生成的，前缀区分类型：
//   s_<小 uid>_<大 uid>   单聊（两个 uid 先升序排列，保证 A->B 与 B->A 是同一个会话）
//   r_<room_id>           聊天室
// 这个设计的好处是任何地方都能从 (user1,user2) 或 room_id 直接重建出 id，
// 不需要先查库。并发创建的幂等不是靠 ON DUPLICATE，而是靠主键冲突：
// EnsureXxx 发的是普通 INSERT，但会把 errno 1062（Duplicate entry）当成功返回 true，
// 其他错误才返回 false——因为「行已存在」本身就意味着目的已达成。
class SessionDao {
   public:
    explicit SessionDao(MySqlConnectionPool* pool) : pool_(pool) {}

    // 按 user1/user2（升序）获取或创建单聊会话
    // 功能：获取或创建单聊会话（user1/user2 升序）
    // 参数：user1/user2 用户；session 输出；err_msg 错误
    // 返回：成功 true，失败 false
    //
    // 实现上是「先 Ensure 后 GetSessionById」两步，中间**释锁**。
    // Ensure 失败只记日志不返回失败：1062 已经在 Ensure 内部被吸收成成功，
    // 走到这里的只会是真错误（连接断、表丢失）。那种情况下行可能已经存在，
    // 后面的 GetSessionById 仍有机会读到，因此先试一次再定成败。
    bool GetOrCreateSingleSession(int64_t user1, int64_t user2,
                                  Session* session, std::string* err_msg);

    // 按 room_id 获取或创建聊天室会话
    // 功能：获取或创建聊天室会话
    // 聊天室会话是群聊消息序号与历史查询的载体，与 room 表分开：
    // room 表管成员关系与元信息，session 表只管消息序号轴。
    bool GetOrCreateRoomSession(int64_t room_id, Session* session,
                                std::string* err_msg);

    // 直接根据 session_id 查询
    // 功能：按 session_id 查询会话
    // 不存在时返回 false（而不是 true + 空 Session），调用方靠这个区分
    // 「会话不存在」与「查询出错」。
    bool GetSessionById(const std::string& session_id, Session* session,
                        std::string* err_msg);

    // 将 last_msg_seq +1，并返回新的 seq
    // 功能：为会话分配下一个消息序列号
    //
    // 警告：**当前全仓库没有调用方**，保留只是为了兼容旧接口。
    // 真正的序号分配走两条路：热路径用 Redis Lua（redis_store.cpp），
    // 持久兜底用 MySqlMessageReservations::Reserve。
    // 而且本实现是 UPDATE 后再发一条独立 SELECT，**两条语句不在同一事务里**，
    // 并发下两个调用方可能读到同一个值（重复发号）。
    // 若要重新启用它，必须先改成事务 + FOR UPDATE，或直接用
    // UPDATE ... ; SELECT LAST_INSERT_ID(last_msg_seq:=last_msg_seq+1) 这类原子写法。
    bool AllocateMessageSeq(const std::string& session_id, int64_t* next_seq,
                            std::string* err_msg);

    // 将 session.last_msg_seq 单调推进到至少 last_seq，乱序异步落盘不会回退。
    // GREATEST 是全部要点：Job 的四个 consumer group 并行落库，完成顺序
    // 与序号顺序无关。若直接赋值，一个迟到的 seq=5 会把已经推到 8 的
    // 游标拉回 5，客户端就会重复拉到 6/7/8。
    bool UpdateLastMessageSeqAtLeast(const std::string& session_id,
                                     int64_t last_seq,
                                     std::string* err_msg);

    // 列出某个用户参与的所有单聊会话
    // 功能：列出用户参与的单聊会话
    // SQL 是 type=0 AND (user1_id=? OR user2_id=?)，因此调用方不需要关心
    // 自己在会话里是 user1 还是 user2（建会话时已经升序归一了）。
    bool ListUserSingleSessions(int64_t user_id, std::vector<Session>* sessions,
                                std::string* err_msg);

   private:
    // 下面三个都收裸 MYSQL*：调用方已经拿着连接（可能还在事务里），
    // 不能在这里再从池里取一条，否则会把同一个逻辑操作拆到两条连接上。
    bool EnsureSingleSession(MYSQL* conn, const std::string& session_id,
                             int64_t user1, int64_t user2,
                             std::string* err_msg);
    bool EnsureRoomSession(MYSQL* conn, const std::string& session_id,
                           int64_t room_id, std::string* err_msg);
    // 把结果集的一行填进 Session。抽出来是因为四个查询方法返回的列完全相同，
    // 重复四份绑定代码很容易在改 schema 时漏改其中一处。
    bool FillSessionFromStmt(MYSQL_STMT* stmt, Session* session,
                             std::string* err_msg);

    MySqlConnectionPool* pool_{nullptr};
};

}  // namespace sparkpush
