#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "model.h"
#include "mysql_pool.h"

namespace sparkpush {

// 用户中心审计日志 DAO。日志只追加，不提供业务层 UPDATE/DELETE。
//
// 「不提供」是有意的而不是「还没来得及写」：审计日志的价值全在于
// 事后不可篡改。一旦 DAO 暴露出修改/删除接口，「谁改的」这个问题
// 本身就变得无法回答。真需要清理旧数据时应该走归档 + 整表轮换，
// 而不是在这里加一个 Delete 方法。
class AuditLogDao {
   public:
    explicit AuditLogDao(MySqlConnectionPool* pool) : pool_(pool) {}

    // 启动时建表（CREATE TABLE IF NOT EXISTS）。
    // 两个复合索引分别服务两种过滤：
    //   idx_audit_target_created (target_user_id, created_at)  按对象筛
    //   idx_audit_action_created (action, created_at)          按动作类型筛
    // 注意：List 实际用的是 ORDER BY id DESC，排序走的是主键而不是
    // 这两个索引里的 created_at。对只追加的表而言 id 递增与时间递增
    // 等价，因此结果一样；但若将来改成按 created_at 排序，才能真正用上它们。
    bool EnsureSchema(std::string* err_msg);

    // 追加一条审计记录。
    // 入参长度在碰 MySQL 之前先校验，且上限与 schema 列宽一致：
    //   action        非空且 <= 64（VARCHAR(64)）
    //   reason        <= 512（VARCHAR(512)）
    //   metadata_json <= 1 MiB（LONGTEXT）
    // 先在应用层拦下而不是让 MySQL 报 data too long，是为了给出可读的错误；
    // metadata 的 1 MiB 上限则是防止一个 bug 把整份消息正文写进审计表。
    //
    // target_user_id 允许为 0，那是「没有具体对象」的哨兵值
    // （例如 admin.login 只关心操作者自己）。
    //
    // 调用方对失败的处理是**不阻断主流程**：
    //   UserService  直接丢弃返回值（fire-and-forget）
    //   AdminService 记错误日志，并把 audit_recorded 放进响应 metadata，
    //                让调用方自己看到「动作成了但审计没写进去」
    // 这是一个有意识的权衡：审计写失败不应该让封禁/解禁这类管理动作失败。
    bool Append(int64_t actor_user_id, int64_t target_user_id,
                const std::string& action, const std::string& reason,
                const std::string& metadata_json, std::string* err_msg);

    // 分页查审计日志。limit 硬性钳在 [1,100]（管理后台不需要更多）。
    // target_user_id=0 表示不按对象过滤，查全部；此时走不带 WHERE 的 SQL。
    // 排序是 ORDER BY id DESC，即「最新的在前」。
    // total 是独立的 COUNT(*)，与数据查询不在同一快照，
    // 因此并发写入时总数可能与实际页数轻微不一致（可接受）。
    bool List(int offset, int limit, int64_t target_user_id,
              std::vector<AuditLogRecord>* records, int* total, std::string* err_msg);

   private:
    MySqlConnectionPool* pool_;
};

}  // namespace sparkpush
