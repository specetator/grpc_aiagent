#pragma once

#include <string>
#include <vector>

#include "model.h"

namespace sparkpush {

// Narrow storage ports. Production adapters reuse existing DAOs/Redis/Store;
// in-memory test doubles exercise real business rules without MySQL or HTTP.
class UserRepository {
   public:
    virtual ~UserRepository() = default;
    virtual bool UsersAvailable() const = 0;
    virtual bool TokensAvailable() const = 0;
    virtual bool FindById(int64_t user_id, User* user, std::string* error) = 0;
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
    virtual bool IsMember(int64_t room, int64_t user, bool* member,
                          std::string* error) = 0;
    virtual bool History(const std::string& session_id, int64_t anchor_seq, int limit,
                         std::vector<Message>* messages, std::string* error) = 0;
    virtual bool After(const std::string& session_id, int64_t after_seq, int limit,
                       std::vector<Message>* messages, std::string* error) = 0;
    virtual bool Singles(int64_t user_id, std::vector<Session>* sessions,
                         std::string* error) = 0;
    virtual bool Rooms(int64_t user_id, std::vector<int64_t>* rooms,
                       std::string* error) = 0;
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
    virtual bool Read(const StoredAttachment& attachment, std::string* bytes,
                      std::string* error) = 0;
};

}  // namespace sparkpush
