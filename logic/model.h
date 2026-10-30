#pragma once

#include <cstddef>
#include <cstdint>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace sparkpush {

// 全工程共用的纯数据类型与常量。本文件**不依赖任何存储、protobuf 或第三方库**，
// 只有标准库。这是有意的：它是所有层（DAO / services / 适配器）的共同词汇，
// 一旦它开始 include mysql.h 或 spark_push.pb.h，分层就会塌陷。

// 用户状态。与 user.status TINYINT 列一一对应。
inline constexpr int kUserStatusActive = 1;
inline constexpr int kUserStatusDisabled = 2;
inline constexpr int kUserStatusDeleted = 3;   // 软删除：行仍在表里
// 保留账号 id。选一个极大值是为了避开与 AUTO_INCREMENT 的真实用户 id 相撞：
// 普通 id 从 1 开始递增，要跑到 9e11 才会撞上这两个。
inline constexpr int64_t kAdminUserId = 900000000000LL;
inline constexpr int64_t kHermesBotUserId = 900000000001LL;
// bot 账号名是保留字：UserService::Register 会直接 409 拒掉，
// Login 也会 403。否则任何人都能注册一个叫 hermes_bot 的账号冒充 Agent。
inline constexpr const char* kHermesBotAccount = "hermes_bot";
inline constexpr const char* kHermesBotName = "Kernel Agent";

// 状态码 -> 展示文本。default 分支返回 "unknown" 而不是断言：
// 未知状态可能来自脏数据，抛异常会把一个展示问题变成服务崩溃。
// 注意这个字符串会直接出现在 API 响应里，前端可能在做字面比对，
// 改文本前要先确认没有客户端依赖它。
inline const char* UserStatusName(int status) {
    switch (status) {
        case kUserStatusActive:
            return "active";
        case kUserStatusDisabled:
            return "disabled";
        case kUserStatusDeleted:
            return "deleted";
        default:
            return "unknown";
    }
}

// 聊天室/群。当前只使用 group_type=1（聊天室），见 GroupDao。
struct ImGroup {
    int64_t id{0};
    std::string name;
    int64_t owner_id{0};
    int group_type{0};
};
// 一条弹幕。timeline_ms 是视频内偏移（回放定位用），
// timestamp_ms 是发送时刻（服务端自取，不接受客户端传）。
// 两者语义完全不同，不要混用。
// 注意本结构没有默认初始化（与其他 struct 不同），
// 因此使用前必须确保每个字段都被赋值过。
struct DanmakuItem {
    int64_t id;
    std::string video_id;
    int64_t timeline_ms;
    int64_t sender_id;
    std::string content_json;
    int64_t timestamp_ms;
};
struct AuditLogRecord {
    int64_t id{0};
    int64_t actor_user_id{0};
    int64_t target_user_id{0};
    std::string action;
    std::string reason;
    std::string metadata_json;
    std::string created_at;
};

struct User {
    int64_t id{0};
    std::string account;
    std::string name;
    // PBKDF2 摘要字符串，格式 pbkdf2_sha256$iterations$salt_hex$digest_hex。
    // 把 iterations 存进字符串而不是写死在代码里，是为了支持透明升级：
    // 提高迭代次数后，旧摘要仍能用它自己记录的次数验证，
    // 登录成功后再用新参数重算并回写（见 UserDao::UpdatePasswordHash）。
    std::string password_hash;
    // 1=active, 2=disabled, 3=deleted（软删除）。
    int status{1};
    std::string deleted_at;
    std::string created_at;
};

// 会话类型。数值与 session.type TINYINT 列直接对应，不要重排：
// 库里已有数据按这些数字存，改了枚举值就会把存量会话读成错的类型。
enum class SessionType {
    kSingle = 0,
    kGroup = 1,
    kChatroom = 2,
};

// 一个会话。id 是**算出来的**而不是生成的：
//   s_<小 uid>_<大 uid>   单聊
//   r_<room_id>           聊天室
// user1_id/user2_id 只对单聊有意义，聊天室下均为 0；
// group_id 则相反，只对聊天室有意义（存的是 room_id）。
// last_msg_seq 是已落库的单调水位，由 UpdateLastMessageSeqAtLeast 用
// GREATEST 推进，因此乱序异步落盘不会把它拉回去。
struct Session {
    std::string id;  // session_id
    SessionType type{SessionType::kSingle};
    int64_t user1_id{0};
    int64_t user2_id{0};
    int64_t group_id{0};
    int64_t last_msg_seq{0};
};

// 全系统的核心数据结构：一条消息。
//
// msg_id 与 msg_seq 的关系必须记清：msg_id = session_id + "-" + msg_seq，
// 是**算出来的**而不是独立生成的。因此任何地方重新拼都会得到同一个值，
// 不需要额外存储或查询，也让幂等比对变得简单。
//
// msg_seq 由服务端分配（Redis Lua 热路径或 MySQL 预约兜底），
// 客户端传的序号一律被忽略。这是会话内有序与幂等的基础。
//
// client_msg_id 是客户端生成的幂等键，可以为空（为空时服务端会生成
// 一个随机串顶上，等价于「每次都是新提交」）。
struct Message {
    std::string msg_id;
    std::string session_id;
    int64_t msg_seq{0};
    int64_t sender_id{0};
    int64_t timestamp_ms{0};
    std::string msg_type;
    // 消息正文的原始 JSON 字符串。服务端**不解析也不重写**它，
    // 只校验它是合法 JSON object 且不超 64 KiB（见 MessageService::Submit）。
    // 不解析是为了让业务层不需要知道每种 msg_type 的内部结构，
    // 新增消息类型时服务端无需改动。
    std::string content_json;
    std::string client_msg_id;
};

// Storage-neutral attachment metadata shared by repositories and services.
// 译：与存储无关的附件元信息，由 repository 与 service 共用。
//
// 注意本结构里**没有文件路径**，只有 storage_key（= id）。
// 把路径拼接封在 AttachmentDao::FilePath 里，业务层就拿不到路径，
// 也就不可能绕开 IsValidAttachmentId 那道路径穿越防线。
struct StoredAttachment {
    std::string id;
    int64_t owner_user_id{0};
    std::string session_id;
    std::string display_name;
    // 由服务端按魔数探测得出，**不是**客户端声明的 Content-Type。
    std::string mime;
    std::size_t bytes{0};
    // 内容哈希，相同图片得到相同值，可用于去重与篡改检测。
    std::string sha256;
    std::string storage_key;
    // "ready" 才允许被消息引用（见 AttachmentService::AuthorizeMessageImages）。
    std::string status;
};

struct UserSessionState {
    int64_t read_seq{0};
};

// 简单内存模型，后续可以替换为 MySQL/Redis 实现。
//
// 现状（阅读前先知道）：**本类当前全仓库没有任何使用者**。
// 生产路径早已换成了 SessionDao / MessageDao / ConversationStore，
// 单测也改用 services/repositories.h 里的内存假实现（那套能直接驱动
// 真实业务规则）。本类只是早期原型的遗留，保留不影响运行。
// 若要删掉它，先确认 model.cpp 里对应的实现也一并清理。
//
// 它也不具备生产实现的关键性质：没有幂等去重、没有序号 floor 校准、
// 没有 outbox、重启全丢。**不要拿它当参考实现**。
class InMemoryModel {
   public:
    InMemoryModel() = default;

    // 用户相关
    // 功能：注册或返回已存在用户（内存模型）
    // 参数：account 账号；password 明文/Hash
    // 返回：用户结构
    User RegisterOrGetUser(const std::string& account, const std::string& password);
    // 功能：根据用户 id 获取用户
    User GetUserById(int64_t user_id);

    // 会话与消息
    // 功能：获取或创建单聊会话
    Session GetOrCreateSingleSession(int64_t user1, int64_t user2);
    // 功能：获取或创建聊天室会话
    Session GetOrCreateRoomSession(int64_t room_id);
    // 功能：向会话追加一条消息并分配 seq
    Message AppendMessage(const Session& session, int64_t sender_id,
                          const std::string& msg_type, const std::string& content_json,
                          int64_t now_ms, const std::string& client_msg_id);

    // 功能：按时间倒序分页获取历史消息
    std::vector<Message> GetHistory(const std::string& session_id, int64_t anchor_seq,
                                    int limit);

    // 会话列表（给 Web / App 拉取最近会话列表使用，简单内存实现）
    // 一对一会话列表：当前用户参与的所有单聊会话
    // 功能：列出用户的单聊会话
    std::vector<Session> ListUserSingleSessions(int64_t user_id);
    // 聊天室会话列表：当前用户加入过的所有聊天室会话
    std::vector<Session> ListUserRoomSessions(int64_t user_id);

    // 聊天室成员管理（最小实现）
    // 功能：将用户加入房间
    void JoinRoom(int64_t room_id, int64_t user_id);
    // 功能：将用户从房间移除
    void LeaveRoom(int64_t room_id, int64_t user_id);
    // 功能：获取房间成员列表
    std::vector<int64_t> GetRoomMembers(int64_t room_id);

    // 未读与已读
    // 功能：标记已读
    void MarkRead(int64_t user_id, const std::string& session_id, int64_t read_seq);
    // 功能：获取未读数
    int64_t GetUnread(int64_t user_id, const std::string& session_id);

   private:
    std::string makeUserSessionKey(int64_t user_id,
                                   const std::string& session_id) const;

    // 一把大锁保护下面所有表。对原型而言够用，但这也正是它
    // 不能用于生产的原因之一：所有会话的所有操作全部串行。
    // 生产实现用的是 KeyedMutex（同会话串行、跨会话并行）。
    std::mutex mutex_;
    std::unordered_map<int64_t, User> users_;
    std::unordered_map<std::string, Session> sessions_;
    std::unordered_map<std::string, std::vector<Message>>
        messages_;  // session_id -> msgs
    std::unordered_map<std::string, UserSessionState> user_session_state_;
    std::unordered_map<int64_t, std::vector<int64_t>>
        room_members_;  // room_id -> user_ids
};

}  // namespace sparkpush
