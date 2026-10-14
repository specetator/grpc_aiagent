#include "repositories/dao_repositories.h"

#include "attachment_dao.h"
#include "audit_log_dao.h"
#include "conversation_store.h"
#include "group_dao.h"
#include "logging.h"
#include "redis_store.h"
#include "user_dao.h"

namespace sparkpush {
// 本文件几乎全是 `return ptr_ && ptr_->Xxx(...)` 形式的转发，不重复注释。
// 设计意图与统一惯用法见 dao_repositories.h 顶部的说明。
// 下面只给“不只是转发”的那几处加注释。

// 单独抽一个私有方法而不是写在 override 里：头文件里只声明接口，
// 具体转发放在 .cpp，以后要加日志/指标只改一处。
bool DaoConversationRepository::SafePrefix(const std::string& id, int64_t* seq, std::string* error) {
    return store_ && store_->SafeSyncPrefix(id, seq, error);
}
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
// 审计写入。三个细节：
//   1. 返回 void：审计失败**不得**让业务操作跟着失败。否则一个磁盘满
//      就能把整个登录/改密接口打死，而审计本身并不是业务正确性的一部分。
//   2. 只记 LOG_WARN 而不是 LOG_ERROR：这是有意降级。审计是合规辅助数据，
//      丢一条不应该触发告警风暴。（管理面的审计写入走的是另一条路径：
//      AdminService 直接用 AuditLogDao 写，并把结果放进响应 metadata，
//      那条路径上审计失败是会报给调用方的。）
//   3. 传给 Append 的第四个参数（reason）固定为空串：仓储接口上的 Audit
//      签名里没有“理由”这一维，而 reason 在 AuditLogDao 里本来就是可选的
//      （例如 admin.login 只关心操作者自己），因此宁可空着也不从
//      metadata 里反向解析出一个假理由。
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
    // 注意走的是 members_（GroupMemberDao）而不是 store_：房间订阅关系的
    // 权威在 group_member 表，ConversationStore 不管这块。
    // 这也是本仓储同时持两个依赖的原因：单聊/消息在 store_，群成员在 members_。
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
