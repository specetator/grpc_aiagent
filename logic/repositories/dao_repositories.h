#pragma once

#include "services/repositories.h"

namespace sparkpush {
class UserDao;
class RedisStore;
class AuditLogDao;
class ConversationStore;
class GroupMemberDao;
class AttachmentDao;

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
    void Audit(int64_t, int64_t, const std::string&, const std::string&) override;

   private:
    UserDao* users_;
    RedisStore* tokens_;
    AuditLogDao* audit_;
};

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
    bool Singles(int64_t, std::vector<Session>*, std::string*) override;
    bool Rooms(int64_t, std::vector<int64_t>*, std::string*) override;
    bool Delivered(int64_t, const std::string&, int64_t*, std::string*) override;
    bool MarkDelivered(int64_t, const std::string&, int64_t, std::string*) override;
    bool MarkRead(int64_t, const std::string&, int64_t, std::string*) override;
    bool Unread(int64_t, const std::string&, int64_t*, std::string*) override;

   private:
    ConversationStore* store_;
    GroupMemberDao* members_;
};

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
