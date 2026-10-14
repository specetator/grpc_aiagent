#pragma once

#include "services/repositories.h"

namespace sparkpush {
class UserDao;
class RedisStore;
class AuditLogDao;
class ConversationStore;
class GroupMemberDao;
class AttachmentDao;

// services/repositories.h 里那组抽象接口的**生产实现**：把接口调用
// 一对一转发给具体 DAO。
//
// 这一层存在的理由不是为了“好看的分层”，而是为了让 services 能被单测驱动：
// 依赖抽象接口的 service 可以用内存假实现跑完全部业务分支，不需要 MySQL/Redis。
//
// 但要诚实说清楚：这个抽象在本工程里是**局部的**。当前只有四个
// service 走仓储接口：UserService、ConversationService、AttachmentService、
// RoomAgentService/OperatorTaskService。AdminService、DanmakuService、
// MessageService、RoomService 仍然直接持有 DAO 指针（它们的 .cpp 里
// 能直接看到 #include "group_dao.h" 之类）。RoomAgentRepository 的生产
// 实现也不在本文件，而在 room_agent_repository.h/.cpp。
//
// 全文件的统一惯用法：`return ptr_ && ptr_->Method(...)`。
// 短路求值同时完成了空指针保护与“不可用 == 失败”的语义：
// 未配置 MySQL 的部署下所有仓储方法都返回 false，而不是崩在空指针上。
// 代价是错误信息为空（err 未被写），因此 services 层靠 *Available()
// 先区分“没配”与“配了但失败”，给出 503 而不是 500。

// 用户 + 登录 token + 审计，三个数据源合在一个仓储里。
// 分开注入而不是包成一个 UserDao 的理由：token 的权威在 Redis（唯一），
// 用户表在 MySQL，审计日志在 MySQL 但**允许丢**，三者可用性并不等价。
class DaoUserRepository final : public UserRepository {
   public:
    DaoUserRepository(UserDao* users, RedisStore* tokens, AuditLogDao* audit)
        : users_(users), tokens_(tokens), audit_(audit) {}
    bool UsersAvailable() const override;
    bool TokensAvailable() const override;
    bool FindById(int64_t, User*, std::string*) override;
    bool FindByAccount(const std::string&, User*, std::string*) override;
    bool Create(const std::string&, const std::string&, const std::string&, int64_t*,
                std::string*) override;
    bool UpdatePassword(int64_t, const std::string&, std::string*) override;
    bool ResolveToken(const std::string&, int64_t*) override;
    bool SaveToken(const std::string&, int64_t, int) override;
    // 唯一一个返回 void 的方法：审计写失败**不能**让业务操作跟着失败。
    // 实现里只记 LOG_WARN（见 .cpp），因此审计是“尽力而为”语义。
    void Audit(int64_t, int64_t, const std::string&, const std::string&) override;

   private:
    UserDao* users_;
    RedisStore* tokens_;
    AuditLogDao* audit_;
};

// 会话/消息/送达水位类操作的仓储。注意它持的是 **ConversationStore**
// 而不是 MessageDao/SessionDao：ConversationStore 已经把“序号安全前缀”、
// “历史分页方向”这些容易用错的细节封好了，仓储层不应该再自己拼。
class DaoConversationRepository final : public ConversationRepository {
   public:
    DaoConversationRepository(ConversationStore* store, GroupMemberDao* members)
        : store_(store), members_(members) {}
    bool Available() const override;
    bool MembersAvailable() const override;
    bool FindSession(const std::string&, Session*, std::string*) override;
    bool EnsureSingle(int64_t, int64_t, Session*, std::string*) override;
    bool IsMember(int64_t, int64_t, bool*, std::string*) override;
    bool History(const std::string&, int64_t, int, std::vector<Message>*,
                 std::string*) override;
    bool After(const std::string&, int64_t, int, std::vector<Message>*,
               std::string*) override;
    // 转发而不是直接写 store_->SafeSyncPrefix：多了 store_ 空指针保护。
    //
    // 这里有一个值得注意的行为差异：基类默认实现是 `*seq = 0; return true;`
    //（永远安全），而本 override 在 store_ 为空时返回 false 且**不写 *seq**。
    // 两者都安全，但语义不同：前者是“没数据，同步前缀为 0”，
    // 后者是“查询本身失败了”。因此调用方必须先判返回值再读 seq。
    // 当前唯一调用方 ConversationService::Sync 做对了（`int64_t safe = 0;`
    // 先初始化，再 `if (!repository_->SafeSyncPrefix(...)) return {500, error};`），
    // 但新增调用方时不要假定 *seq 已被写。
    bool SafeSyncPrefix(const std::string& id, int64_t* seq, std::string* error) override {
        return SafePrefix(id, seq, error);
    }
    bool Singles(int64_t, std::vector<Session>*, std::string*) override;
    bool Rooms(int64_t, std::vector<int64_t>*, std::string*) override;
    bool Delivered(int64_t, const std::string&, int64_t*, std::string*) override;
    bool MarkDelivered(int64_t, const std::string&, int64_t, std::string*) override;
    bool MarkRead(int64_t, const std::string&, int64_t, std::string*) override;
    bool Unread(int64_t, const std::string&, int64_t*, std::string*) override;

   private:
    ConversationStore* store_;
    bool SafePrefix(const std::string&, int64_t*, std::string*);
    GroupMemberDao* members_;
};

// 附件（当前只有图片）仓储。Create 对应 AttachmentDao::CreateImage，
// 名字上比实现宽：接口故意不绑定“图片”这个当前限制，以后加文件类型
// 只需换 DAO 实现，services/attachment_service.cpp 不用改。
class DaoAttachmentRepository final : public AttachmentRepository {
   public:
    explicit DaoAttachmentRepository(AttachmentDao* dao) : dao_(dao) {}
    bool Available() const override;
    bool Create(int64_t, const std::string&, const std::string&, const std::string&,
                StoredAttachment*, std::string*) override;
    bool Find(const std::string&, StoredAttachment*, std::string*) override;
    bool Bind(const std::string&, const std::string&, std::string*) override;
    bool Read(const StoredAttachment&, std::string*, std::string*) override;

   private:
    AttachmentDao* dao_;
};
}  // namespace sparkpush
