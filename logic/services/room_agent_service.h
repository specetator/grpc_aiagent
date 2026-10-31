#pragma once
#include <map>
#include <functional>
#include <mutex>
#include <nlohmann/json.hpp>
#include "model.h"
#include "services/service_status.h"

namespace sparkpush {
struct AgentDefinition {
    std::string agent_id, display_name, runtime;
    int64_t bot_user_id{0};
    bool enabled{false};
};
struct ProjectDefinition { std::string project_id, display_name; };
std::vector<ProjectDefinition> ParseRoomProjectCatalog(const nlohmann::json&);
// Only administrator-owned registry files feed this catalog. No HTTP write API.
std::vector<AgentDefinition> ParseRoomAgentCatalog(const nlohmann::json& registry,
                                                 bool hermes_test = false);
// A repository owns persistence, not permissions or use-case rules.
//
// 译：repository 只负责持久化，不负责权限与用例规则。
//
// 这条分工必须守住：所有「谁能改」「能改哪些字段」的判定都在
// RoomAgentService / OperatorTaskService 里，repository 只做读写。
// 否则权限规则会散到两个实现（MySQL 版与测试假实现）里，很容易分叉。
class RoomAgentRepository {
 public:
    virtual ~RoomAgentRepository() = default;
    // 一次查询同时回答两件事：房间拥有者是谁、actor 是不是成员。
    // MySQL 实现用同一条查询读取 owner 与当前正式成员角色。
    virtual bool Access(int64_t room, int64_t user, int64_t* owner, bool* member,
                        std::string* error) = 0;
    virtual bool Role(int64_t room, int64_t user, int* role, std::string* error) {
        int64_t owner=0; bool member=false;
        if(!Access(room,user,&owner,&member,error)) return false;
        *role=member ? (owner==user ? 2 : 0) : -1;
        return true;
    }
    // Missing rows return true + null; database failures return false.
    // 译：行不存在时返回 true 且 task 为 null（非 object）；数据库故障才返回 false。
    // 这个区分很关键：「还没配置任务」是正常状态（对应 404/409），
    // 「查不出来」是故障（对应 500）。合并成一种返回值会把两者混为一谈。
    virtual bool Load(int64_t room, nlohmann::json* task, std::string* error) = 0;
    // expected_revision 是乐观锁：实现方必须比对当前 revision，不匹配就失败。
    // 调用方把失败统一映射成 409（并发修改冲突）。
    virtual bool Save(int64_t room, int64_t expected_revision,
                      const nlohmann::json& task, std::string* error) = 0;
    virtual bool Request(const std::string& id, nlohmann::json* request,
                         std::string* error) = 0;
    virtual bool PutRequest(const nlohmann::json& request, std::string* error) = 0;
    virtual bool Finish(const std::string& id, const nlohmann::json& result,
                        std::string* error) = 0;
    virtual bool Requests(int64_t room, nlohmann::json* requests,
                          std::string* error) = 0;
    virtual bool Tasks(int64_t,nlohmann::json*,std::string*) { return false; }
    virtual bool Task(int64_t,const std::string&,nlohmann::json*,std::string*) { return false; }
    virtual bool SaveTask(int64_t,int64_t,const nlohmann::json&,std::string*) { return false; }
    virtual bool Bindings(int64_t, nlohmann::json*, std::string*) { return false; }
    virtual bool SetBinding(int64_t, int64_t, const std::string&, bool, std::string*) { return false; }
    virtual bool Workspace(int64_t, nlohmann::json* out, std::string*) {
        *out={{"project_id",""},{"generation",0}}; return true;
    }
    virtual bool SetWorkspace(int64_t,int64_t,int64_t,const std::string&,std::string*) { return false; }
    // Persist the first publication decision while holding the room, actor and
    // request locks. The callback owns policy and must not perform database I/O.
    using PublicationDecision=std::function<nlohmann::json(const nlohmann::json&)>;
    virtual bool Publication(const nlohmann::json&,const std::string&,
                             const PublicationDecision&,nlohmann::json*,std::string*) { return false; }
    virtual bool Pending(nlohmann::json*, std::string*) { return false; }
    virtual bool Dispatched(const std::string&, std::string*) { return false; }
    virtual bool MarkAttempt(const std::string&, std::string*) { return false; }
    virtual bool RecoverDispatch(const std::string&, std::string*) { return false; }
    virtual bool BlockDispatch(const std::string&, const std::string&, std::string*) { return false; }
};

// Durable operator progress is separate from the Agent transcript.
//
// 译：持久的算子任务进度与 Agent 对话记录是分开的。
//
// 分开存的理由：任务进度（summary/next_steps/status）是**多人共享的权威状态**，
// 需要版本控制与权限门禁；而 Agent 对话是一次性的请求/响应。
// 把两者混在一张表里会让「修改任务描述」与「发一次提问」共用同一把锁。
//
// 与 RoomAgentService 的权限区别（很容易看错）：
//   Configure   owner/admin 可管理开发成员授权（不重置任务）
//   Checkpoint  拥有者**或** developer_user_ids 里的人能改
// 后者宽是为了让多个开发者能交接进度，但可改字段被严格限定。
class OperatorTaskService {
 public:
    explicit OperatorTaskService(RoomAgentRepository* repo) : repo_(repo) {}
    // 推进任务进度。三重限制：
    //   1. 字段白名单：只能改 summary / next_steps / status，其他一律 400。
    //      尤其不能改 owner_user_id / bot_user_id / task_id / developer_user_ids：
    //      否则一个开发者就能把任务转给自己或把 Agent 换成别的 bot。
    //   2. status 枚举白名单（planning/developing/blocked/review/done）。
    //   3. revision 乐观锁：不匹配就 409，避开两人同时编辑互相覆盖。
    ServiceStatus Checkpoint(int64_t actor, int64_t room, int64_t revision,
                            const nlohmann::json& patch, nlohmann::json* result);
 private:
    RoomAgentRepository* repo_;
};

// 房间共享 Agent（@CANN）的触发、请求构造与回复校验。
//
// 本类是**对抗提示注入（prompt injection）的主要防线**。
// 房间是多人共享的，任何人都能发言，因此房间内容必须被当成**不可信输入**：
// 既不能让它提权，也不能让它改变 Agent 的行为边界。
// 具体手段见 .cpp 里的 Trigger 与 BuildRequest。
class RoomAgentService {
 public:
    // public_cann_bot 是共享 Agent 的 bot 账号 id；enabled 为假时所有功能回 503。
    RoomAgentService(RoomAgentRepository* repo, int64_t public_cann_bot,
                     bool enabled, std::vector<AgentDefinition> catalog = {})
        : repo_(repo), bot_(public_cann_bot), enabled_(enabled), catalog_(std::move(catalog)) {}
    using ControlTransport=std::function<nlohmann::json(const nlohmann::json&)>;
    void SetControlTransport(ControlTransport transport) { control_=std::move(transport); }
    void SetProjectCatalog(std::vector<ProjectDefinition>, std::string authorization_key);
    ServiceStatus Workspace(int64_t actor,int64_t room,nlohmann::json*);
    ServiceStatus SetWorkspace(int64_t actor,int64_t room,const nlohmann::json&,nlohmann::json*);
    ServiceStatus AuthorizeWorkspace(const std::string& key,const nlohmann::json&,nlohmann::json*);
    ServiceStatus Control(int64_t,int64_t,const nlohmann::json&,nlohmann::json*);
    ServiceStatus Tasks(int64_t,int64_t,nlohmann::json*);
    ServiceStatus CreateTask(int64_t,int64_t,const nlohmann::json&,nlohmann::json*);
    ServiceStatus UpdateTask(int64_t,int64_t,const nlohmann::json&,nlohmann::json*);
    ServiceStatus Agents(int64_t actor, int64_t room, nlohmann::json* result);
    ServiceStatus Install(int64_t actor, int64_t room, const nlohmann::json& input, bool installed);
    ServiceStatus CheckDispatch(const nlohmann::json& request);
    bool PublishReply(const nlohmann::json& request,const nlohmann::json& reply,
                      const std::string& canonical_content,nlohmann::json*,std::string*);
    bool BuildRequests(const Message&, const std::vector<Message>&, nlohmann::json*, std::string*);
    bool MarkAttempt(const std::string& id,std::string* error) { return repo_->MarkAttempt(id,error); }
    bool Pending(nlohmann::json* requests, std::string* error) { return repo_->Pending(requests,error); }
    bool Dispatched(const std::string& id, std::string* error) { return repo_->Dispatched(id,error); }
    bool RejectPending(const nlohmann::json&, const std::string&, std::string*);
    // 创建/更新房间任务。owner/admin 可调。
    // 已存在的任务**绝不会被重置或重新绑定**（见 .cpp 里的注释），
    // 只能改 developer_user_ids。revision 不匹配回 409。
    ServiceStatus Configure(int64_t actor, int64_t room, const nlohmann::json& input,
                            nlohmann::json* result);
    // 查看任务与请求列表。任何房间成员都能看，
    // 并额外返回 can_develop 告知前端是否展示编辑入口。
    ServiceStatus View(int64_t actor, int64_t room, nlohmann::json* result);
    // 只校验成员关系，不做其他事。供外部（如 message_service）复用。
    ServiceStatus CheckMember(int64_t actor, int64_t room);
    // 判定一条房间消息是否应该触发 Agent。trigger 为输出参数：
    // 返回 ok() 但 *trigger == false 意味着「合法消息，但不是 @CANN」，
    // 这是正常路径而不是错误。区分这两种情况很关键：
    // 若把「不是 @CANN」当错误，普通聊天就会全部发送失败。
    ServiceStatus Trigger(int64_t actor, int64_t room, const std::string& content,
                          bool* trigger);
    // 构造发给 Agent 的请求并落库。
    // 幂等：request_id 直接用 current.msg_id，已存在则原样返回旧请求，
    // 因此同一条消息被重放时不会跟 Agent 发起两次调用。
    bool BuildRequest(const Message& current, const std::vector<Message>& history,
                      nlohmann::json* request, std::string* error);
    // 校验 Agent 回复的归属。reply 必须原样带回存储请求里的
    // session_id / user_id / bot_user_id / collaboration 四个字段，
    // 任一不匹配就拒收。这是防止一个被攻破或出错的 hermes_bridge
    // 把回复归到别的用户/房间头上的关键校验。
    bool ValidateReply(const nlohmann::json& reply, nlohmann::json* request,
                       std::string* error);
    // 标记请求完成并存下结果（ok/text/completed_at_ms，可选 workspace_checkpoint）。
    bool Complete(const nlohmann::json& reply, std::string* error);
    // Single Logic instance orders room append -> persistence -> AI enqueue.
    // 译：单个 Logic 实例内保证「房间消息追加 -> 持久化 -> AI 入队」的顺序。
    // 这是一个**部署约束**而不是代码约束：一旦跑多个 Logic 实例，
    // 这个顺序就不再成立，需要额外的分布式协调。
 private:
    RoomAgentRepository* repo_;
    int64_t bot_;
    bool enabled_;
    std::vector<AgentDefinition> catalog_;
    ControlTransport control_;
    std::vector<ProjectDefinition> projects_;
    std::string workspace_key_;
    ServiceStatus CheckAuthorization(const nlohmann::json& scope,const nlohmann::json& state) const;
    static nlohmann::json ResultFromReply(const nlohmann::json& request,const nlohmann::json& reply);
    ServiceStatus Mentions(int64_t, int64_t, const nlohmann::json&, std::vector<std::string>*);
    const AgentDefinition* Definition(const std::string&) const;
};
}  // namespace sparkpush
