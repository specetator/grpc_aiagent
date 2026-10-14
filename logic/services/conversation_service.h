#pragma once

#include "services/repositories.h"
#include "services/service_status.h"

namespace sparkpush {
// 一次同步的返回页。next_seq 与 safe_seq 是两个不同用途的游标，
// 客户端必须分清，否则会在 SQL 落库滑后时把未持久化的消息当成已安全。
struct MessagePage {
    std::vector<Message> messages;
    bool has_more{false};
    // 下一页从哪里继续（= 本页最后一条的 msg_seq，空页时 = 请求的 after）。
    // 它只表示「读到哪了」，**不表示「这些都持久化了」**。
    int64_t next_seq{0};
    // 持久化安全水位 = min(SafeSyncPrefix, next_seq)。
    // 只有 <= safe_seq 的消息才保证已在 message 表里，客户端可以把它们
    // 当本地权威数据、丢掉重拉。把 next_seq 当 safe_seq 用是集成测试里
    // 专门盯着的一个回归（见 "real repository must not call next_seq a safe
    // prefix during SQL lag"）。
    int64_t safe_seq{0};
    // 离线同步跨会话时的续传锚点：下一页从「这个会话的这个 seq」接着拉。
    // 单会话同步时为空。
    std::string next_session_id;
};
// 客户端上报的送达回执。
struct DeliveryCursor {
    int64_t user_id{0};
    std::string session_id;
    int64_t msg_seq{0};
};

// 会话读取与游标维护的业务层：历史、增量同步、离线补推、已读/送达。
//
// 本类不发消息、不分配序号，只管「读」与「游标」。发消息在 MessageService。
class ConversationService {
   public:
    explicit ConversationService(ConversationRepository* repository)
        : repository_(repository) {}
    bool Available() const;
    // Shared object-level authorization for history, sync and attachments.
    // Only image upload may lazily create a canonical single-chat session.
    //
    // 译：历史、同步、附件三类接口共用的对象级鉴权。
    // 只有图片上传允许惰性创建规范单聊会话。
    //
    // 鉴权规则按 session_id 前缀分支：
    //   s_<a>_<b>  解出两个 uid，actor 必须是其中之一
    //   r_<room>   调 IsMember 查成员表（fail-closed）
    // create_single 默认 false：只有上传路径传 true，因为发图时
    // 单聊会话可能还没建。把这个能力限在一个参数上而不是全局打开，
    // 是为了避开「任意接口都能凭空造会话」。
    ServiceStatus CheckAccess(int64_t user_id, const std::string& session_id,
                              Session* session, bool create_single = false);
    // 向上翻历史。注意 limit **不**在本层钳制，直接透传给 repository
    //（与下面两个 Sync 接口不同，那两个走 SyncLimit 钳到 [1,1000]）。
    ServiceStatus History(int64_t user_id, const std::string& session_id,
                          int64_t anchor, int limit, std::vector<Message>* messages);
    // 单会话增量同步。内部多拉一条（bounded_limit + 1）来探测 has_more，
    // 再把多拉的那条 pop 掉：这比额外发一次 COUNT 便宜得多。
    ServiceStatus SyncAfter(int64_t user_id, const std::string& session_id,
                            int64_t after, int limit, MessagePage* page);
    // 跨会话离线补推。两个关键设计：
    //   1. 会话集按 id 排序后再遍历，让分页确定可重现（否则同一客户端
    //      两次续传可能拉到不同的会话顺序，造成重复或遗漏）
    //   2. after_session 必须在本用户可访问的会话集里，否则 403。
    //      不验证的话，客户端可以拿别人的 session_id 当游标来探测其存在性。
    // 每个会话的起点是 Delivered（无空洞送达水位）而不是客户端传的 seq，
    // 因此客户端无法靠谎报游标跳过未收到的消息。
    ServiceStatus SyncOffline(int64_t user_id, int limit, MessagePage* page,
                              const std::string& after_session = "", int64_t after_seq = 0);
    // 批量登记送达回执。has_user_cursors 为假时走旧契约（仅凭身份），
    // 为真时才用 cursors 里的逐会话游标。
    ServiceStatus MarkDelivered(int64_t user_id, bool has_user_cursors,
                                const std::vector<DeliveryCursor>& cursors);
    // Preserve the existing identity-only read/unread contract in this
    // structural refactor; do not silently change authorization semantics.
    //
    // 译：本次结构重构中保留现有的「仅校验身份」的已读/未读契约，
    // 不要默默改变鉴权语义。
    //
    // 「identity-only」的准确含义（很容易读错）：
    //   它**确实**校验 actor == user_id，不匹配就 403
    //   （"user_id does not match bearer token"），因此不能替别人标已读。
    //   它**不**校验的是会话成员关系——不走 CheckAccess。
    // 也就是说：一个已登录用户可以对一个自己并非成员的 session_id
    // 标已读或查未读数。影响有限（这两个操作只读写
    // user_session_state 里自己那一行，不暴露消息内容），
    // 但与 History/SyncAfter 的鉴权强度不一致。这是重构前就有的行为，
    // 被刻意保留以免改变线上契约。若要收紧，在这里补一次 CheckAccess 即可。
    ServiceStatus MarkRead(int64_t actor, int64_t user_id, const std::string& session,
                           int64_t sequence);
    ServiceStatus Unread(int64_t actor, int64_t user_id, const std::string& session,
                         int64_t* count);
    ServiceStatus ListSingles(int64_t actor, int64_t user_id,
                              std::vector<Session>* sessions);

   private:
    bool MembersAvailable() const;
    ConversationRepository* repository_;
};
}  // namespace sparkpush
