#pragma once

#include "model.h"
#include "mysql_pool.h"

namespace sparkpush {

// 简单用户 DAO：目前只操作单库，后续可在这里加入分库分表路由。
//
// 本类只负责 SQL，不做任何业务判定：密码 hash 由调用方算好传进来，
// 账号重复、保留名之类的规则全在 UserService 里。
// 这样分层的好处是 DAO 可以被单测直接驱动，不需要 mock 业务规则。
class UserDao {
   public:
    explicit UserDao(MySqlConnectionPool* pool) : pool_(pool) {}

    // 启动时幂等补齐用户中心字段和索引，兼容已经存在的开发库。
    // 靠 errno 1060（Duplicate column）/ 1061（Duplicate key name）当幂等成功，
    // 因为 MySQL 8.0 的各小版本对 ALTER TABLE ... IF NOT EXISTS 支持不一致。
    // 任一条失败就返回 false，调用方（logic/main.cpp）会直接拒绝启动。
    bool EnsureUserCenterSchema(std::string* err_msg);

    // 启动时幂等创建 Hermes 机器人账号。机器人不通过普通登录接口获取
    // Token，只作为单聊会话的一端参与消息持久化和推送。
    // 与普通 CreateUser 的区别：这里显式指定 id（配置里的 bot_user_id），
    // 而且撞 1062 时会回读比对 account，一致就当成功——
    // 多个 Logic 节点同时启动时只有一个能插入成功，其余必须也认为成功。
    bool EnsureHermesBotUser(int64_t bot_user_id, const std::string& account,
                             const std::string& name, std::string* err_msg);

    // 功能：创建用户
    // 参数：account 账号；name 昵称；password 密码 hash；user_id 输出
    // id；err_msg 错误 返回：成功 true，失败 false
    //
    // password 参数名容易误导：它存的是 **PBKDF2 hash 字符串**而不是明文，
    // 本类从不接触明文密码。
    // 注意：这里**没有**对 1062（account 重复）做特殊处理，会直接返回 false
    // 并带上 MySQL 原始错误文本。账号唯一性靠 UserService::Register 先
    // FindByAccount 预检，那是一个 check-then-act，并发注册同名账号时
    // 输的那一方会拿到 500 而不是 409。
    bool CreateUser(const std::string& account, const std::string& name,
                    const std::string& password, int64_t* user_id,
                    std::string* err_msg);

    // 功能：按账号查询用户
    // 查不到时返回 false 且 err_msg = "user not found"。
    // 调用方靠这个特定文本区分「不存在」与「真错误」（见 UserService::Register），
    // 因此不要随意改动这个字符串。
    bool GetUserByAccount(const std::string& account, User* user, std::string* err_msg);

    // 功能：按 id 查询用户
    bool GetUserById(int64_t user_id, User* user, std::string* err_msg);

    // 登录时将旧密码摘要升级为当前 PBKDF2 编码。
    // 这是「透明升级」的写入端：hash 字符串里存了 iterations，
    // VerifyPassword 比对成功后会通过 needs_rehash 出参告知「强度已过时」，
    // UserService::Login 随即用同一个明文重算并调本方法回写。
    // 用户无感知，不需要改密码就能把全库逐步推到新的强度。
    // 回写失败只记错误日志、不阻断登录：升级是优化而不是正确性要求，
    // 下次登录会再试一次。
    bool UpdatePasswordHash(int64_t user_id, const std::string& password_hash,
                            std::string* err_msg);

    // 管理员分页查询用户；status=0 表示查询全部状态。
    // limit 被硬性钳在 [1,100]：这是管理后台接口，不需要也不应该允许一次拉很多。
    // total 是一次独立的 COUNT(*)，与数据查询不在同一快照里，
    // 因此分页过程中若有新增，总数可能与实际页数轻微不一致（可接受）。
    bool ListUsers(int offset, int limit, int status, std::vector<User>* users,
                   int* total, std::string* err_msg);

    // 修改用户状态：active / disabled / deleted。deleted 状态会写 deleted_at。
    // 这是**软删除**：行不会真的从表里消失，因此 account 唯一索引仍然占着，
    // 已删除用户的账号无法被重新注册。改成硬删除前必须先想清楚这个影响。
    bool UpdateStatus(int64_t user_id, int status, std::string* err_msg);

    // 管理员修改昵称。
    bool UpdateName(int64_t user_id, const std::string& name, std::string* err_msg);

   private:
    MySqlConnectionPool* pool_;
};

}  // namespace sparkpush
