#include "repositories/dao_repositories.h"

#include "attachment_dao.h"
#include "audit_log_dao.h"
#include "conversation_store.h"
#include "group_dao.h"
#include "logging.h"
#include "redis_store.h"
#include "user_dao.h"

namespace sparkpush {
bool DaoUserRepository::UsersAvailable() const { return users_ != nullptr; }
bool DaoUserRepository::TokensAvailable() const { return tokens_ != nullptr; }
bool DaoUserRepository::FindById(int64_t id, User* out, std::string* err) {
    return users_ && users_->GetUserById(id, out, err);
}
bool DaoUserRepository::FindByAccount(const std::string& account, User* out,
                                      std::string* err) {
    return users_ && users_->GetUserByAccount(account, out, err);
}
bool DaoUserRepository::Create(const std::string& account, const std::string& name,
                               const std::string& hash, int64_t* id, std::string* err) {
    return users_ && users_->CreateUser(account, name, hash, id, err);
}
bool DaoUserRepository::UpdatePassword(int64_t id, const std::string& hash,
                                       std::string* err) {
    return users_ && users_->UpdatePasswordHash(id, hash, err);
}
bool DaoUserRepository::ResolveToken(const std::string& token, int64_t* id) {
    return tokens_ && tokens_->GetUserIdByToken(token, id);
}
bool DaoUserRepository::SaveToken(const std::string& token, int64_t id, int ttl) {
    return tokens_ && tokens_->SetToken(token, id, ttl);
}
void DaoUserRepository::Audit(int64_t actor, int64_t target, const std::string& action,
                              const std::string& metadata) {
    if (!audit_) return;
    std::string err;
    if (!audit_->Append(actor, target, action, "", metadata, &err))
        LOG_WARN << "Audit failed: " << err;
}

bool DaoConversationRepository::Available() const { return store_ != nullptr; }
bool DaoConversationRepository::MembersAvailable() const { return members_ != nullptr; }
bool DaoConversationRepository::FindSession(const std::string& id, Session* out,
                                            std::string* err) {
    return store_ && store_->GetSessionById(id, out, err);
}
bool DaoConversationRepository::EnsureSingle(int64_t a, int64_t b, Session* out,
                                             std::string* err) {
    return store_ && store_->GetOrCreateSingleSession(a, b, out, err);
}
bool DaoConversationRepository::IsMember(int64_t room, int64_t user, bool* out,
                                         std::string* err) {
    return members_ && members_->IsMember(room, user, out, err);
}
bool DaoConversationRepository::History(const std::string& id, int64_t anchor,
                                        int limit, std::vector<Message>* out,
                                        std::string* err) {
    return store_ && store_->GetHistory(id, anchor, limit, out, err);
}
bool DaoConversationRepository::After(const std::string& id, int64_t after, int limit,
                                      std::vector<Message>* out, std::string* err) {
    return store_ && store_->GetMessagesAfter(id, after, limit, out, err);
}
bool DaoConversationRepository::Singles(int64_t user, std::vector<Session>* out,
                                        std::string* err) {
    return store_ && store_->ListUserSingleSessions(user, out, err);
}
bool DaoConversationRepository::Rooms(int64_t user, std::vector<int64_t>* out,
                                      std::string* err) {
    return members_ && members_->ListUserChatrooms(user, out, err);
}
bool DaoConversationRepository::Delivered(int64_t user, const std::string& id,
                                          int64_t* seq, std::string* err) {
    return store_ && store_->GetDeliveredSeq(user, id, seq, err);
}
bool DaoConversationRepository::MarkDelivered(int64_t user, const std::string& id,
                                              int64_t seq, std::string* err) {
    return store_ && store_->MarkDelivered(user, id, seq, err);
}
bool DaoConversationRepository::MarkRead(int64_t user, const std::string& id,
                                         int64_t seq, std::string* err) {
    return store_ && store_->MarkRead(user, id, seq, err);
}
bool DaoConversationRepository::Unread(int64_t user, const std::string& id,
                                       int64_t* count, std::string* err) {
    return store_ && store_->GetUnread(user, id, count, err);
}

bool DaoAttachmentRepository::Available() const { return dao_ != nullptr; }
bool DaoAttachmentRepository::Create(int64_t user, const std::string& session,
                                     const std::string& name, const std::string& bytes,
                                     StoredAttachment* out, std::string* err) {
    return dao_ && dao_->CreateImage(user, session, name, bytes, out, err);
}
bool DaoAttachmentRepository::Find(const std::string& id, StoredAttachment* out,
                                   std::string* err) {
    return dao_ && dao_->GetById(id, out, err);
}
bool DaoAttachmentRepository::Bind(const std::string& id, const std::string& session,
                                   std::string* err) {
    return dao_ && dao_->BindSession(id, session, err);
}
bool DaoAttachmentRepository::Read(const StoredAttachment& item, std::string* bytes,
                                   std::string* err) {
    return dao_ && dao_->ReadBytes(item, bytes, err);
}
}  // namespace sparkpush
