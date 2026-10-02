#pragma once

#include <string>
#include <vector>

#include "mysql_pool.h"

namespace sparkpush {

class UserSessionStateDao {
   public:
    explicit UserSessionStateDao(MySqlConnectionPool* pool) : pool_(pool) {}

    // 功能：插入或更新用户会话已读序列
    // 参数：user_id 用户；session_id 会话；read_seq 已读；err_msg 错误
    // 返回：成功 true，失败 false
    bool UpsertReadSeq(int64_t user_id, const std::string& session_id,
                       int64_t read_seq, std::string* err_msg);

    // 功能：查询用户会话已读序列
    // 参数：user_id 用户；session_id 会话；read_seq 输出；err_msg 错误
    // 返回：成功 true，失败 false
    bool GetReadSeq(int64_t user_id, const std::string& session_id,
                    int64_t* read_seq, std::string* err_msg);

    // 单调推进客户端已收到的 msg_seq，用于断线后的离线补推。
    bool UpsertDeliveredSeq(int64_t user_id, const std::string& session_id,
                            int64_t delivered_seq, std::string* err_msg);

    bool GetDeliveredSeq(int64_t user_id, const std::string& session_id,
                         int64_t* delivered_seq, std::string* err_msg);

    // 兼容已经初始化旧 schema 的本地开发库。
    bool EnsureDeliveredSeqColumn(std::string* err_msg);
    bool EnsureReceivedSchema(std::string* err_msg);
    bool GetReceivedSeq(int64_t user_id, const std::string& device_id,
                        const std::string& session_id, int64_t* seq,
                        std::string* err_msg);
    bool MarkReceived(int64_t user_id, const std::string& device_id,
                      const std::string& session_id, int64_t prefix,
                      const std::vector<int64_t>& received, std::string* err_msg);

   private:
    MySqlConnectionPool* pool_{nullptr};
};

}  // namespace sparkpush
