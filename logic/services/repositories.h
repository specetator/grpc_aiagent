#pragma once

#include <string>
#include <vector>

#include "model.h"

namespace sparkpush {

// Narrow storage ports. Production adapters reuse existing DAOs/Redis/Store;
// in-memory test doubles exercise real business rules without MySQL or HTTP.
//
// 译：这是一组**窄接口**（存储端口）。生产环境用 repositories/dao_repositories.cpp
// 里的适配器包住现有 DAO/Redis/ConversationStore；测试则用内存假实现，
// 不启 MySQL 也不起 HTTP 就能跑完整的业务规则。
//
// 接口刻意做窄：只暴露 service 真正需要的那几个动作，而不是把 DAO 整个搬过来。
// 好处是业务层的依赖面一目了然，也让假实现只需写几十行。
//
// 每个类开头的 Available()/UsersAvailable() 不是冗余：它们让 service 能在
// 依赖未配置时给出干净的 500，而不是在空指针上崩。
class UserRepository {
   public:
    virtual ~UserRepository() = default;
    virtual bool UsersAvailable() const = 0;
    virtual bool TokensAvailable() const = 0;
    virtual bool FindById(int64_t user_id, User* user, std::string* error) = 0;
    // 查不到时约定返回 false 且 *error = "user not found"。
    // UserService::Register 靠比对这个字符串区分「可注册」与「查询出错」，
    // 因此假实现也必须遵守同样的约定，否则测试与生产行为会分叉。
    virtual bool FindByAccount(const std::string& account, User* user,
                               std::string* error) = 0;
    virtual bool Create(const std::string& account, const std::string& name,
                        const std::string& password_hash, int64_t* user_id,
                        std::string* error) = 0;
    virtual bool UpdatePassword(int64_t user_id, const std::string& hash,
                                std::string* error) = 0;
    virtual bool ResolveToken(const std::string& token, int64_t* user_id) = 0;
    virtual bool SaveToken(const std::string& token, int64_t user_id,
                           int ttl_seconds) = 0;
    // 返回 void 是有意的：审计写失败不应该也不能阻断登录/注册这类业务动作。
    // 实现方自己负责记日志：DaoUserRepository::Audit 失败时只记一条 LOG_WARN。
    // 需要把审计结果告知调用方的场景走另一条路：AdminService 直接用
    // AuditLogDao，并把 audit_recorded 放进响应 metadata。
    virtual void Audit(int64_t actor, int64_t target, const std::string& action,
                       const std::string& metadata) = 0;
};

class ConversationRepository {
   public:
    virtual ~ConversationRepository() = default;
    virtual bool Available() const = 0;
    virtual bool MembersAvailable() const = 0;
    virtual bool FindSession(const std::string& session_id, Session* session,
                             std::string* error) = 0;
    virtual bool EnsureSingle(int64_t user1, int64_t user2, Session* session,
                              std::string* error) = 0;
    // 对象级鉴权的依据。实现必须 fail-closed：查询出错时要把 *member 置 false，
    // 绝不能因为数据库抖动就把非成员当成成员放进来。
    virtual bool IsMember(int64_t room, int64_t user, bool* member,
                          std::string* error) = 0;
    virtual bool History(const std::string& session_id, int64_t anchor_seq, int limit,
                         std::vector<Message>* messages, std::string* error) = 0;
    virtual bool After(const std::string& session_id, int64_t after_seq, int limit,
                       std::vector<Message>* messages, std::string* error) = 0;
    // A conservative default cannot authorize skipping an unseen sequence.
    //
    // 译：保守的默认值不会授权客户端跳过任何未见过的序号。
    //
    // 这是本文件里最值得看的一行设计。SafeSyncPrefix 是**唯一一个非纯虚**的方法，
    // 默认实现返回 *seq = 0 + true。0 的含义是「没有任何序号是确定安全的」，
    // 于是客户端必须全量重拉而不能跳过任何消息。
    //
    // 为什么这样设计：若把它也做成纯虚，每个测试假实现都必须想清楚该返回什么，
    // 而想错的方向必然是「返回一个大值让测试好过」——那正好是会造成
    // 静默丢消息的方向。给一个永远安全的默认值，就把这个坑封死了。
    // 生产适配器 DaoConversationRepository 显式覆写它，转发到
    // MySqlMessageReservations::SafePrefix。
    virtual bool SafeSyncPrefix(const std::string&, int64_t* seq, std::string*) {
        *seq = 0;
        return true;
    }
    virtual bool Singles(int64_t user_id, std::vector<Session>* sessions,
                         std::string* error) = 0;
    virtual bool Rooms(int64_t user_id, std::vector<int64_t>* rooms,
                       std::string* error) = 0;
    // Delivered / MarkDelivered 操作的是**无空洞送达水位**，
    // 与 MarkRead 的已读游标是两回事（详见 UserSessionStateDao）。
    virtual bool Delivered(int64_t user_id, const std::string& session_id, int64_t* seq,
                           std::string* error) = 0;
    virtual bool MarkDelivered(int64_t user_id, const std::string& session_id,
                               int64_t seq, std::string* error) = 0;
    virtual bool MarkRead(int64_t user_id, const std::string& session_id, int64_t seq,
                          std::string* error) = 0;
    virtual bool Unread(int64_t user_id, const std::string& session_id, int64_t* count,
                        std::string* error) = 0;
};

class AttachmentRepository {
   public:
    virtual ~AttachmentRepository() = default;
    virtual bool Available() const = 0;
    virtual bool Create(int64_t owner, const std::string& session_id,
                        const std::string& name, const std::string& bytes,
                        StoredAttachment* attachment, std::string* error) = 0;
    virtual bool Find(const std::string& id, StoredAttachment* attachment,
                      std::string* error) = 0;
    virtual bool Bind(const std::string& id, const std::string& session_id,
                      std::string* error) = 0;
    // 注意端口里**没有**暴露任何路径或 storage_key：字节读取完全封在实现里。
    // 这样业务层就不可能拼出一个文件系统路径，路径穿越的防线
    // 也就只存在于 AttachmentDao 一个地方。
    virtual bool Read(const StoredAttachment& attachment, std::string* bytes,
                      std::string* error) = 0;
};

}  // namespace sparkpush
