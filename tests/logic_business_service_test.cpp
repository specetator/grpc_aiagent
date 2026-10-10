#include <algorithm>
#include <iostream>
#include <map>
#include <stdexcept>
#include <tuple>

#include "security.h"
#include "services/admin_service.h"
#include "services/attachment_service.h"
#include "services/conversation_service.h"
#include "services/danmaku_service.h"
#include "services/knowledge_service.h"
#include "services/message_service.h"
#include "services/room_service.h"
#include "services/user_service.h"

namespace {
using namespace sparkpush;
int checks = 0;
void Require(bool value, const std::string& name) {
    if (!value) throw std::runtime_error(name);
    ++checks;
}

class MemoryUsers final : public UserRepository {
   public:
    bool users_available{true}, tokens_available{true}, save_ok{true},
        query_error{false};
    int ttl{0}, reads{0}, password_updates{0};
    std::map<int64_t, User> users;
    std::map<std::string, int64_t> tokens;
    std::vector<std::string> audits;
    bool UsersAvailable() const override { return users_available; }
    bool TokensAvailable() const override { return tokens_available; }
    bool FindById(int64_t id, User* out, std::string* error) override {
        ++reads;
        if (!users.count(id)) {
            *error = "user not found";
            return false;
        }
        *out = users.at(id);
        return true;
    }
    bool FindByAccount(const std::string& account, User* out,
                       std::string* error) override {
        if (query_error) {
            *error = "database unavailable";
            return false;
        }
        for (const auto& item : users)
            if (item.second.account == account) {
                *out = item.second;
                return true;
            }
        *error = "user not found";
        return false;
    }
    bool Create(const std::string& account, const std::string& name,
                const std::string& hash, int64_t* id, std::string*) override {
        *id = 10 + users.size();
        User user;
        user.id = *id;
        user.account = account;
        user.name = name;
        user.password_hash = hash;
        users[*id] = user;
        return true;
    }
    bool UpdatePassword(int64_t id, const std::string& hash, std::string*) override {
        users.at(id).password_hash = hash;
        ++password_updates;
        return true;
    }
    bool ResolveToken(const std::string& token, int64_t* id) override {
        if (!tokens.count(token)) return false;
        *id = tokens.at(token);
        return true;
    }
    bool SaveToken(const std::string& token, int64_t id, int expiry) override {
        if (!save_ok) return false;
        tokens[token] = id;
        ttl = expiry;
        return true;
    }
    void Audit(int64_t, int64_t, const std::string& action,
               const std::string&) override {
        audits.push_back(action);
    }
};

class MemoryConversations final : public ConversationRepository {
   public:
    bool available{true}, members_available{true}, membership_failure{false};
    int ensure_calls{0}, read_calls{0};
    std::map<std::string, Session> sessions;
    std::map<std::string, std::vector<Message>> messages;
    std::map<int64_t, std::vector<int64_t>> members;
    std::map<std::pair<int64_t, std::string>, int64_t> delivered, read;
    bool Available() const override { return available; }
    bool MembersAvailable() const override { return members_available; }
    bool FindSession(const std::string& id, Session* out, std::string* error) override {
        if (!sessions.count(id)) {
            *error = "session not found";
            return false;
        }
        *out = sessions.at(id);
        return true;
    }
    bool EnsureSingle(int64_t a, int64_t b, Session* out, std::string*) override {
        ++ensure_calls;
        out->id = "s_" + std::to_string(std::min(a, b)) + "_" +
                  std::to_string(std::max(a, b));
        out->user1_id = std::min(a, b);
        out->user2_id = std::max(a, b);
        sessions[out->id] = *out;
        return true;
    }
    bool IsMember(int64_t room, int64_t user, bool* out, std::string* error) override {
        if (membership_failure) {
            *error = "membership query failed";
            return false;
        }
        const auto& list = members[room];
        *out = std::find(list.begin(), list.end(), user) != list.end();
        return true;
    }
    bool History(const std::string& session, int64_t, int, std::vector<Message>* out,
                 std::string*) override {
        ++read_calls;
        *out = messages[session];
        return true;
    }
    bool After(const std::string& session, int64_t after, int limit,
               std::vector<Message>* out, std::string*) override {
        ++read_calls;
        out->clear();
        for (const auto& message : messages[session]) {
            if (message.msg_seq > after && static_cast<int>(out->size()) < limit)
                out->push_back(message);
        }
        return true;
    }
    bool Singles(int64_t user, std::vector<Session>* out, std::string*) override {
        for (const auto& item : sessions) {
            const auto& session = item.second;
            if (session.type == SessionType::kSingle &&
                (session.user1_id == user || session.user2_id == user))
                out->push_back(session);
        }
        return true;
    }
    bool Rooms(int64_t user, std::vector<int64_t>* out, std::string*) override {
        for (const auto& room : members)
            if (std::find(room.second.begin(), room.second.end(), user) !=
                room.second.end())
                out->push_back(room.first);
        return true;
    }
    bool Delivered(int64_t user, const std::string& id, int64_t* seq,
                   std::string*) override {
        *seq = delivered[{user, id}];
        return true;
    }
    bool MarkDelivered(int64_t user, const std::string& id, int64_t seq,
                       std::string*) override {
        auto& old = delivered[{user, id}];
        old = std::max(old, seq);
        return true;
    }
    bool MarkRead(int64_t user, const std::string& id, int64_t seq,
                  std::string*) override {
        read[{user, id}] = seq;
        return true;
    }
    bool Unread(int64_t user, const std::string& id, int64_t* count,
                std::string*) override {
        *count = std::max<int64_t>(0, sessions[id].last_msg_seq - read[{user, id}]);
        return true;
    }
};

class MemoryAttachments final : public AttachmentRepository {
   public:
    std::map<std::string, StoredAttachment> attachments;
    std::map<std::string, std::string> bytes;
    bool available{true}, bind_ok{true};
    int creates{0}, reads{0}, binds{0};
    bool Available() const override { return available; }
    bool Create(int64_t user, const std::string& session, const std::string& name,
                const std::string& data, StoredAttachment* out, std::string*) override {
        ++creates;
        out->id = "att_" + std::string(32, 'a') + std::to_string(creates);
        out->owner_user_id = user;
        out->session_id = session;
        out->display_name = name;
        out->bytes = data.size();
        out->status = "ready";
        attachments[out->id] = *out;
        bytes[out->id] = data;
        return true;
    }
    bool Find(const std::string& id, StoredAttachment* out,
              std::string* error) override {
        if (!attachments.count(id)) {
            *error = "attachment not found";
            return false;
        }
        *out = attachments.at(id);
        return true;
    }
    bool Bind(const std::string& id, const std::string& session,
              std::string* error) override {
        if (!bind_ok) {
            *error = "bind failed";
            return false;
        }
        ++binds;
        attachments.at(id).session_id = session;
        return true;
    }
    bool Read(const StoredAttachment& item, std::string* out, std::string*) override {
        ++reads;
        *out = bytes.at(item.id);
        return true;
    }
};

void TestUsers() {
    MemoryUsers storage;
    UserService service(&storage,
                        {"administrator", "test-admin-password", {kHermesBotUserId}});
    LoginResult login;
    Require(service.Register("", "123456", "", &login).code == 400,
            "registration validation");
    Require(service.Register("administrator", "123456", "", &login).code == 409,
            "reserved admin account");
    Require(service.Register(kHermesBotAccount, "123456", "", &login).code == 409,
            "reserved bot account");
    Require(service.Register("alice", "123456", "Alice \"quoted\"", &login).ok(),
            "registration");
    const auto id = login.user_id;
    const auto token = login.token;
    Require(storage.ttl == 86400 && token.size() == 64, "secure token and TTL");
    Require(storage.users.at(id).password_hash.rfind("pbkdf2_sha256$", 0) == 0,
            "no plaintext password storage");
    Require(storage.audits.back() == "user.register", "registration audit");
    Require(service.Register("alice", "123456", "", &login).code == 409,
            "duplicate registration");
    Require(service.Login("alice", "wrong-password", &login).code == 401,
            "invalid credentials");
    Require(service.Login("alice", "123456", &login).ok() && login.user_id == id,
            "login");
    Require(login.name == "Alice \"quoted\"", "domain DTO preserves name");
    Require(service.Login(kHermesBotAccount, "123456", &login).code == 403,
            "bot login denied");
    Require(service.Login("missing", "123456", &login).code == 404, "missing user");
    int64_t resolved = 0;
    Require(
        service.Authenticate(token, UserService::TokenAudience::kHttpUser, &resolved)
                .ok() &&
            resolved == id,
        "HTTP token audience");
    Require(
        service
            .Authenticate(token, UserService::TokenAudience::kRealtimeUser, &resolved)
            .ok(),
        "realtime token audience");
    User user;
    std::string error;
    Require(service.GetActiveUser(id, &user, &error), "hot user lookup");
    const auto reads = storage.reads;
    Require(service.GetActiveUser(id, &user, &error) && storage.reads == reads,
            "shared hot cache");
    storage.users.at(id).status = kUserStatusDisabled;
    Require(
        service.Authenticate(token, UserService::TokenAudience::kHttpUser, &resolved)
                    .code == 403 &&
            resolved == 0,
        "HTTP auth bypasses stale hot cache");
    Require(service.Authenticate(token, UserService::TokenAudience::kRealtimeUser,
                                 &resolved)
                    .code == 403,
            "realtime auth bypasses stale hot cache");
    Require(service.Login("alice", "123456", &login).reason ==
                ServiceReason::kInactiveAccount,
            "inactive login reason for adapter");
    Require(service.Login("administrator", "test-admin-password", &login).ok() &&
                login.administrator,
            "administrator login");
    Require(service.AuthenticateAdmin(login.token).ok(), "administrator credential");
    Require(service.Authenticate(login.token, UserService::TokenAudience::kRealtimeUser,
                                 &resolved)
                    .code == 403,
            "no admin WS token");
    storage.tokens["bot-credential"] = kHermesBotUserId;
    Require(service.Authenticate("bot-credential",
                                 UserService::TokenAudience::kRealtimeUser, &resolved)
                    .code == 403,
            "no bot WS token");
    Require(service.AuthenticateAdmin(token).code == 403, "user cannot administer");
    Require(service.Authenticate("missing", UserService::TokenAudience::kHttpUser,
                                 &resolved)
                    .code == 401,
            "expired token");
    storage.query_error = true;
    Require(service.Register("bob", "123456", "Bob", &login).code == 500,
            "database errors not treated as missing account");
    storage.query_error = false;
    User legacy;
    legacy.id = 77;
    legacy.account = "legacy";
    legacy.password_hash = "5f4dcc3b5aa765d61d8327deb882cf99";
    storage.users[legacy.id] = legacy;
    Require(service.Login("legacy", "password", &login).ok() &&
                storage.password_updates == 1,
            "legacy hash upgrade");
    storage.save_ok = false;
    Require(service.Login("administrator", "test-admin-password", &login).code == 500,
            "token write failure");
    storage.tokens_available = false;
    Require(
        service.Authenticate(token, UserService::TokenAudience::kHttpUser, &resolved)
                .code == 503,
        "HTTP dependency error");
    Require(service.Authenticate(token, UserService::TokenAudience::kRealtimeUser,
                                 &resolved)
                    .code == 500,
            "realtime dependency error compatibility");
}

void TestConversationsAndImages() {
    MemoryConversations storage;
    Session single;
    single.id = "s_1_2";
    single.user1_id = 1;
    single.user2_id = 2;
    storage.sessions[single.id] = single;
    Session room;
    room.id = "r_7";
    room.type = SessionType::kChatroom;
    room.group_id = 7;
    storage.sessions[room.id] = room;
    storage.members[7] = {1};
    for (int i = 1; i <= 1002; ++i) {
        Message message;
        message.msg_seq = i;
        message.session_id = single.id;
        storage.messages[single.id].push_back(message);
    }
    ConversationService service(&storage);
    Session session;
    Require(service.CheckAccess(1, single.id, &session).ok(), "single participant 1");
    Require(service.CheckAccess(2, single.id, &session).ok(), "single participant 2");
    Require(service.CheckAccess(3, single.id, &session).code == 403,
            "single outsider denied");
    Require(service.CheckAccess(1, room.id, &session).ok(), "room member");
    Require(service.CheckAccess(2, room.id, &session).code == 403,
            "room outsider denied");
    std::vector<Message> history;
    MessagePage page;
    Require(service.History(3, single.id, 0, 20, &history).code == 403 &&
                storage.read_calls == 0,
            "history authorization before read");
    Require(service.SyncAfter(3, single.id, 0, 20, &page).code == 403 &&
                storage.read_calls == 0,
            "cursor shares history access rule");
    Require(service.SyncAfter(1, single.id, 0, 5000, &page).ok() &&
                page.messages.size() == 1000 && page.next_seq == 1000 && page.has_more,
            "cursor page cap");
    Require(service.SyncAfter(1, single.id, 1000, 10, &page).ok() &&
                page.messages.size() == 2 && !page.has_more,
            "cursor remainder");
    Require(service.SyncAfter(1, single.id, 2000, 10, &page).ok() &&
                page.next_seq == 2000 && page.messages.empty(),
            "empty page preserves cursor");
    Require(service.SyncAfter(1, single.id, -1, 10, &page).code == 400,
            "negative cursor");
    Require(service.MarkDelivered(1, false, {{1, single.id, 5}}).ok(),
            "delivered cursor");
    Require(service.MarkDelivered(3, false, {{3, single.id, 6}}).code == 403 &&
                !storage.delivered.count({3, single.id}),
            "outsider cannot mark delivered");
    Require(service.MarkDelivered(0, true, {{2, single.id, 7}}).ok(),
            "per-user batched delivery");
    Require(service.MarkDelivered(0, false, {}).code == 400,
            "empty principal rejected");
    Require(service.MarkDelivered(1, false, {{0, "", -1}}).ok(),
            "invalid cursor entry legacy skip");
    storage.delivered[{1, single.id}] = 1000;
    Require(service.SyncOffline(1, 1, &page).ok() && page.messages.size() == 1 &&
                page.messages[0].msg_seq == 1001 && page.has_more,
            "offline uses delivered rather than read cursor");
    Require(service.MarkRead(1, 2, single.id, 5).code == 403, "read identity rule");
    Require(service.MarkRead(1, 1, single.id, 0).code == 400, "positive read cursor");
    Require(service.MarkRead(1, 1, single.id, 5).ok(), "read command");
    int64_t unread = 0;
    Require(service.Unread(1, 2, single.id, &unread).code == 403,
            "unread identity rule");
    std::vector<Session> sessions;
    Require(service.ListSingles(1, 2, &sessions).code == 403, "list identity rule");
    storage.membership_failure = true;
    Require(service.CheckAccess(1, room.id, &session).code == 500,
            "membership failure does not grant access");
    storage.membership_failure = false;

    MemoryAttachments images;
    AttachmentService attachments(&images, &service);
    StoredAttachment image;
    Require(attachments.Upload(1, single.id, "image", "YWJj", &image).ok() &&
                images.creates == 1,
            "authorized image upload");
    const auto image_id = image.id;
    Require(attachments.Upload(3, single.id, "image", "YWJj", &image).code == 403 &&
                images.creates == 1,
            "unauthorized upload has no file side effect");
    Require(
        attachments.Upload(1, single.id, "image", "not base64!", &image).code == 400 &&
            images.creates == 1,
        "invalid encoding no file write");
    AttachmentContents contents;
    Require(attachments.Read(2, image_id, &contents).ok() && contents.bytes == "abc",
            "conversation participant reads attachment");
    const auto image_reads = images.reads;
    Require(attachments.Read(3, image_id, &contents).code == 403 &&
                images.reads == image_reads,
            "attachment shares access policy");
    auto image_content = [&](const std::string& id) {
        return nlohmann::json{
            {"content",
             {{"attachments",
               nlohmann::json::array({{{"id", id}, {"mime", "image/png"}}})}}}}
            .dump();
    };
    Require(attachments.AuthorizeMessageImages(2, single.id, image_content(image_id))
                    .code == 400,
            "message sender must own image");
    Require(
        attachments.AuthorizeMessageImages(1, "s_1_4", image_content(image_id)).code ==
            400,
        "cross-session image reference denied");
    Require(
        attachments.AuthorizeMessageImages(1, single.id, image_content(image_id)).ok(),
        "valid message reference");
    images.attachments[image_id].status = "pending";
    Require(attachments.AuthorizeMessageImages(1, single.id, image_content(image_id))
                    .code == 400,
            "unready image denied");
    images.attachments[image_id].status = "ready";
    images.attachments[image_id].session_id.clear();
    Require(attachments.AuthorizeMessageImages(1, single.id, image_content(image_id))
                    .ok() &&
                images.binds == 1,
            "unbound image bound exactly once");
    Require(attachments.Upload(1, "s_1_9", "image", "YWJj", &image).ok() &&
                storage.ensure_calls == 1,
            "lazy single creation for upload");
    Require(attachments.Upload(3, "s_1_8", "image", "YWJj", &image).code == 404 &&
                storage.ensure_calls == 1,
            "outsider cannot create upload session");
    Require(attachments.Upload(1, "s_1_8junk", "image", "YWJj", &image).code == 404 &&
                storage.ensure_calls == 1,
            "noncanonical ID no create side effect");
    Require(
        attachments
            .AuthorizeMessageImages(1, single.id, "{\"content\":{\"text\":\"hello\"}}")
            .ok(),
        "text path needs no attachment lookup");
    images.available = false;
    Require(attachments.Read(1, image_id, &contents).code == 503,
            "attachment dependency failure");
}

void TestOtherServices() {
    RoomService rooms(nullptr, nullptr, nullptr, nullptr);
    Require(rooms.Join(1, 2, 7).code == 403, "room identity policy");
    Require(rooms.Leave(1, 1, 7).ok(), "HTTP leave preserves subscriptions");
    int64_t room = 0;
    Require(rooms.Create(1, "room", 1, &room).code == 403, "business room admin guard");
    Require(rooms.ReportRoomJoin(7, "comet", 1).code == 500,
            "online room dependency guard");
    AdminService admin(nullptr, nullptr, nullptr);
    nlohmann::json result;
    Require(admin.SetStatus(1, 2, kUserStatusDisabled, "", &result).code == 403,
            "business admin guard");
    DanmakuService danmaku(nullptr, nullptr, nullptr, nullptr, nullptr);
    Require(danmaku.Send("token", "video", 1, "text").status.code == 500,
            "danmaku missing storage");
    KnowledgeService knowledge("/nonexistent-knowledge-root");
    Require(knowledge.Document("../../etc/passwd", "", &result).code == 400,
            "knowledge ID guard");
    MessageService messages(nullptr, nullptr, nullptr, nullptr, nullptr, nullptr,
                            nullptr, nullptr, RateLimitConfig{}, 5000, nullptr, false,
                            kHermesBotUserId, {}, nullptr);
    Require(messages.Submit({}).status.code == 500, "message dependency guard");
    Require(messages.BroadcastAdmin(1, "all", 0, "text").status.code == 403,
            "business broadcast admin guard");
    Require(messages.SendLegacyText(0, 1, "text").status.code == 400,
            "legacy text validation");
    Require(
        messages.BroadcastAdmin(kAdminUserId, "unknown", 0, "text").status.code == 400,
        "broadcast scope rule");
    Require(
        messages.BroadcastAdmin(kAdminUserId, "chatroom", 0, "text").status.code == 400,
        "broadcast room rule");
}
}  // namespace

int main() {
    try {
        TestUsers();
        TestConversationsAndImages();
        TestOtherServices();
        std::cout << "logic_business_service_test: " << checks << " checks passed\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "logic_business_service_test failed: " << error.what() << '\n';
        return 1;
    }
}
