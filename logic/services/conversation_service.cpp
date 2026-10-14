#include "services/conversation_service.h"

#include <algorithm>

#include "logging.h"
#include "metrics.h"

namespace sparkpush {
namespace {
// Leave room for protobuf fields below the default 4 MiB gRPC receive limit.
//
// 把同步页的总字节数限制在 2 MiB。gRPC 默认接收上限是 4 MiB，
// 只给一半是为了给 protobuf 的字段头与同一个响应里的其他字段留出余量。
// 超了会在客户端报 RESOURCE_EXHAUSTED，那是一个很难归因的错误；
// 在服务端先截断并置 has_more 则只是多拉一页。
// 每条消息估算里那个 +256 是给 protobuf 开销留的常量余量，
// 不是精确值——这里只需要一个保守上界。
bool BoundPageBytes(MessagePage* page) {
    constexpr size_t capacity = 2 * 1024 * 1024;
    size_t used = 0;
    size_t retained = 0;
    for (const auto& message : page->messages) {
        const auto bytes = message.content_json.size() + message.msg_id.size() +
            message.session_id.size() + message.msg_type.size() + message.client_msg_id.size() + 256;
        // 用 bytes > capacity - used 而不是 used + bytes > capacity：
        // 后者在 used + bytes 溢出 size_t 时会得出错误结论。
        if (bytes > capacity - used) break;
        used += bytes;
        ++retained;
    }
    // 一条都没装下但页非空 -> 单条消息就超过了 2 MiB 预算。
    // 此时截断无意义（会永远返回空页 + has_more，客户端死循环），
    // 因此直接报 413 并提示改用 HTTP history 接口（那条路不走 gRPC 限制）。
    if (!page->messages.empty() && retained == 0) return false;
    if (retained < page->messages.size()) {
        // 截断后必须置 has_more：否则客户端会以为已经拉完，
        // 被丢掉的那些消息就再也不会被同步到。
        page->messages.resize(retained);
        page->has_more = true;
    }
    return true;
}
}  // namespace
namespace {
// 同步接口的 limit 钳制：[1,1000]，<=0 时默认 100。
// 上限 1000 与 BoundPageBytes 的 2 MiB 预算配合，共同保证一个同步响应
// 不会撞上 gRPC 的 4 MiB 上限。注意 History 路径不走本函数。
int SyncLimit(int limit) {
    return std::max(1, std::min(limit > 0 ? limit : 100, 1000));
}
// 从 "s_<a>_<b>" 里解出两个 uid。
bool ParseSingleParticipants(const std::string& id, int64_t* a, int64_t* b) {
    if (id.rfind("s_", 0) != 0) return false;
    const auto split = id.find('_', 2);
    if (split == std::string::npos) return false;
    try {
        *a = std::stoll(id.substr(2, split - 2));
        *b = std::stoll(id.substr(split + 1));
    } catch (...) {
        // stoll 对非数字与溢出都抛，这里一律当成「不是合法单聊 id」。
        return false;
    }
    // A noncanonical spelling must not create a side-effecting DB row.
    //
    // 译：非规范写法绝不得创建带副作用的数据库行。
    //
    // 最后那个往返比对是关键：把解出的 a/b 重新拼一次，必须与输入完全相等。
    // 它能挡掉 "s_007_8"、"s_7_08"、"s_+7_8" 这类写法。
    // 若不挡，EnsureSingle 会用规范形式建出一个 id 与请求不同的会话行，
    // 而调用方拿着旧 id 去查永远查不到——数据库里就多了一条脏行。
    // *a <= *b 则保证只接受升序形式（与 SessionDao 的归一规则一致）。
    return *a > 0 && *b > 0 && *a <= *b &&
           id == "s_" + std::to_string(*a) + "_" + std::to_string(*b);
}
}  // namespace

bool ConversationService::Available() const {
    return repository_ && repository_->Available();
}
bool ConversationService::MembersAvailable() const {
    return repository_ && repository_->MembersAvailable();
}

// 对象级鉴权。历史/同步/附件三类接口都走这里，因此它是「谁能看哪个会话」
// 的单一权威。任何新增的读接口都应该先调本方法而不是自己写鉴权。
ServiceStatus ConversationService::CheckAccess(int64_t user_id,
                                               const std::string& session_id,
                                               Session* session, bool create_single) {
    if (!Available()) return {500, "conversation store not initialized"};
    if (!session) return {500, "session output is null"};
    std::string error;
    if (!repository_->FindSession(session_id, session, &error)) {
        // 会话不存在。默认直接 404；只有 create_single=true（图片上传路径）
        // 才惰性创建。四个条件全部满足才允许建，少一个就回到 404：
        //   create_single            调用方显式声明允许创建
        //   ParseSingleParticipants  id 必须是规范单聊形式（否则会在库里造脏行）
        //   user_id == a || == b     只能建自己参与的会话
        //   session->id == session_id 建出来的 id 必须与请求一致（双重保险）
        // 最后一条尤其重要：若 EnsureSingle 因为归一化而返回了不同的 id，
        // 调用方拿着原 id 继续往下走就会读写到一个不存在于它认知里的会话。
        int64_t a = 0, b = 0;
        if (!create_single || !ParseSingleParticipants(session_id, &a, &b) ||
            (user_id != a && user_id != b) ||
            !repository_->EnsureSingle(a, b, session, &error) ||
            session->id != session_id)
            return {404, "session not found"};
    }
    bool authorized = false;
    if (session->type == SessionType::kSingle) {
        // 单聊：从已落库的行里读成员，不信任 session_id 里的字面 uid。
        authorized = session->user1_id == user_id || session->user2_id == user_id;
    } else if (session->group_id > 0 && MembersAvailable()) {
        // 房间/群聊：查成员表。查询失败时返回 500 + kMembershipLookupFailed，
        // 而不是降级成 403：前者让适配层能拼上详细错误文本，
        // 也避开把「数据库故障」误报成「你无权访问」误导排查方向。
        // 注意 authorized 已被 IsMember 置 false（fail-closed），
        // 因此即使调用方忽略返回值也不会误放行。
        if (!repository_->IsMember(session->group_id, user_id, &authorized, &error))
            return {500, error, ServiceReason::kMembershipLookupFailed};
    }
    // group_id <= 0 的非单聊会话（脏数据）会落到这里：authorized 保持 false -> 403。
    if (!authorized) return {403, "user is not a member of this session"};
    return {};
}

ServiceStatus ConversationService::History(int64_t user, const std::string& session,
                                           int64_t anchor, int limit,
                                           std::vector<Message>* messages) {
    Session value;
    auto status = CheckAccess(user, session, &value);
    if (!status.ok()) {
        if (status.reason == ServiceReason::kMembershipLookupFailed)
            status.message = "check session membership failed: " + status.message;
        return status;
    }
    std::string error;
    if (!messages || !repository_->History(session, anchor, limit, messages, &error))
        return {500, "query history failed: " + error};
    return {};
}

// 单会话增量同步。返回的 next_seq 与 safe_seq 含义不同，详见 MessagePage。
ServiceStatus ConversationService::SyncAfter(int64_t user, const std::string& session,
                                             int64_t after, int limit,
                                             MessagePage* page) {
    if (page) *page = {};
    if (!Available() || !MembersAvailable() || !page || user <= 0 || session.empty() ||
        after < 0)
        return {400, "user_id/session_id are required"};
    Session value;
    auto status = CheckAccess(user, session, &value);
    if (!status.ok()) return status;
    const int bounded_limit = SyncLimit(limit);
    std::string error;
    int64_t safe = 0;
    // 先取安全水位再拉消息：反过来的话，拉消息期间新落库的数据会抬高水位，
    // 导致返回一个「比本页内容更新」的 safe_seq，客户端据此丢掉本地缓存
    // 就会丢消息。先取水位只会偏保守（safe 偏小），方向是安全的。
    if (!repository_->SafeSyncPrefix(session, &safe, &error)) return {500, error};
    // 多拉一条探测 has_more，避开额外一次 COUNT 查询。
    if (!repository_->After(session, after, bounded_limit + 1, &page->messages, &error))
        return {500, error};
    page->has_more = static_cast<int>(page->messages.size()) > bounded_limit;
    if (page->has_more) page->messages.pop_back();
    // 字节预算检查放在 pop 之后：多拉的那一条不应该占用返回预算。
    if (!BoundPageBytes(page)) return {413, "stored message exceeds sync page budget; use HTTP history"};
    page->next_seq = page->messages.empty() ? after : page->messages.back().msg_seq;
    // 取两者的 min：既不能超过持久化水位（safe），也不能超过本页实际读到的位置。
    // 后者尤其关键：SQL 落库滞后时 safe 可能远大于 next_seq，
    // 若直接用 safe，客户端会以为 [next_seq, safe] 这段已安全而不再重拉——
    // 而它其实从未拿到过这些消息。集成测试里的
    // "real repository must not call next_seq a safe prefix during SQL lag" 盯的就是这个。
    page->safe_seq = std::min(safe, page->next_seq);
    // 空页时上面的 min 会把 safe_seq 锁在 after（= next_seq），使客户端无法推进。
    // 没有新消息不代表没有新的安全数据（之前的消息可能刚落库），
    // 因此这里直接用 safe，让客户端能把本地水位推上去。
    if (page->messages.empty()) page->safe_seq = safe;
    MetricsRegistry::Instance().Increment("spark_push_cursor_sync_total");
    return {};
}

// 跨会话离线补推。与 SyncAfter 的区别：它遍历用户所有可访问会话，
// 每个会话从自己的 delivered 水位开始拉，直到填满一页预算。
ServiceStatus ConversationService::SyncOffline(int64_t user, int limit, MessagePage* page,
                                               const std::string& after_session, int64_t after_seq) {
    if (page) *page = {};
    if (!Available() || !MembersAvailable() || !page || user <= 0 || after_seq < 0)
        return {400, "offline sync dependencies/user_id are missing"};
    std::vector<Session> sessions;
    std::string error;
    if (!repository_->Singles(user, &sessions, &error)) return {500, error};
    std::vector<int64_t> rooms;
    if (!repository_->Rooms(user, &rooms, &error)) return {500, error};
    for (int64_t room_id : rooms) {
        Session room;
        // 房间会话 id 由 room_id 算出（"r_<id>"），查不到就跳过：
        // 可能是已退订但成员表还有残留行，不应该因此让整个同步失败。
        if (repository_->FindSession("r_" + std::to_string(room_id), &room, &error))
            sessions.push_back(std::move(room));
    }
    // 排序是分页可重现的前提：不排序的话遍历顺序取决于两个查询的返回顺序，
    // 同一客户端两次续传可能拿到不同的会话顺序，造成重复推送或遗漏。
    std::sort(sessions.begin(), sessions.end(), [](const Session& a, const Session& b) { return a.id < b.id; });
    // 续传锚点必须在本用户可访问的会话集里。不验证的话，客户端可以
    // 拿一个任意的 session_id 当游标，靠 403/200 的差异探测它是否存在。
    if (!after_session.empty() && std::none_of(sessions.begin(), sessions.end(), [&](const Session& s) { return s.id == after_session; }))
        return {403, "offline continuation session is inaccessible"};
    const int bounded = SyncLimit(limit);
    // remaining 多留一条用于探测 has_more，与 SyncAfter 同一个手法。
    int remaining = bounded + 1;
    for (const auto& session : sessions) {
        if (remaining <= 0) break;
        // 按 id 字典序跳过已经拉过的会话。依赖上面的排序，
        // 否则 "session.id < after_session" 这个剪枝会错跳或漏跳。
        if (!after_session.empty() && session.id < after_session) continue;
        int64_t delivered = 0;
        // 起点是服务端的 delivered 水位（无空洞），不是客户端传的 after_seq。
        // 这使客户端无法靠谎报游标跳过它实际没收到的消息。
        if (!repository_->Delivered(user, session.id, &delivered, &error)) return {500, error};
        // 只有续传会话才参考 after_seq，而且取 max：同会话内不回退。
        if (session.id == after_session) delivered = std::max(delivered, after_seq);
        std::vector<Message> messages;
        if (!repository_->After(session.id, delivered, remaining, &messages, &error)) return {500, error};
        page->messages.insert(page->messages.end(), messages.begin(), messages.end());
        remaining -= static_cast<int>(messages.size());
    }
    page->has_more = static_cast<int>(page->messages.size()) > bounded;
    if (page->has_more) page->messages.pop_back();
    if (!BoundPageBytes(page)) return {413, "stored message exceeds offline page budget; use HTTP history"};
    if (!page->messages.empty()) {
        // 跨会话续传需要两个坐标（会话 + 序号），因此多返回一个 next_session_id。
        page->next_session_id = page->messages.back().session_id;
        page->next_seq = page->messages.back().msg_seq;
    }
    // 注意：本方法不设 safe_seq（保持 0）。跨会话页里的消息来自不同序号轴，
    // 单一的 safe_seq 无法表达「每个会话各自的安全水位」，
    // 客户端需要单会话精度时应改用 SyncAfter。
    MetricsRegistry::Instance().Increment("spark_push_offline_sync_total");
    return {};
}

ServiceStatus ConversationService::MarkDelivered(
    int64_t user, bool has_user_cursors, const std::vector<DeliveryCursor>& cursors) {
    if (!Available() || !MembersAvailable() || (user <= 0 && !has_user_cursors))
        return {400, "user_id or user_cursors is required"};
    for (const auto& cursor : cursors) {
        if (cursor.user_id <= 0 || cursor.session_id.empty() || cursor.msg_seq < 0)
            continue;
        Session session;
        auto status = CheckAccess(cursor.user_id, cursor.session_id, &session);
        if (!status.ok()) return status;
        std::string error;
        if (!repository_->MarkDelivered(cursor.user_id, cursor.session_id,
                                        cursor.msg_seq, &error))
            return {500, error};
    }
    MetricsRegistry::Instance().Increment("spark_push_delivered_cursor_total");
    return {};
}

ServiceStatus ConversationService::MarkRead(int64_t actor, int64_t user,
                                            const std::string& session,
                                            int64_t sequence) {
    if (actor != user) return {403, "user_id does not match bearer token"};
    if (user <= 0 || session.empty() || sequence <= 0)
        return {400, "user_id/session_id/read_seq required"};
    if (!Available()) return {500, "conversation store not initialized"};
    std::string error;
    if (!repository_->MarkRead(user, session, sequence, &error))
        return {500, "mark read failed: " + error};
    return {};
}
ServiceStatus ConversationService::Unread(int64_t actor, int64_t user,
                                          const std::string& session, int64_t* count) {
    if (actor != user) return {403, "user_id does not match bearer token"};
    if (user <= 0 || session.empty()) return {400, "user_id/session_id required"};
    if (!Available()) return {500, "conversation store not initialized"};
    std::string error;
    if (!count || !repository_->Unread(user, session, count, &error))
        return {500, "get unread failed: " + error};
    return {};
}
ServiceStatus ConversationService::ListSingles(int64_t actor, int64_t user,
                                               std::vector<Session>* sessions) {
    if (actor != user) return {403, "user_id does not match bearer token"};
    if (user <= 0) return {400, "user_id required"};
    if (!Available()) return {500, "conversation store not initialized"};
    std::string error;
    if (!sessions || !repository_->Singles(user, sessions, &error))
        return {500, "list sessions failed: " + error};
    return {};
}
}  // namespace sparkpush
