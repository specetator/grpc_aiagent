#include "repositories/room_agent_repository.h"
namespace sparkpush {
namespace {
using Json = nlohmann::json;
// 手工转义 + 加单引号。本工程绝大多数 SQL 用占位符绑定，这里是少数
// 例外：因为要绑的是整个 JSON 文档（task.dump()），而 mysql C API 的
// 占位符绑定需要 prepared statement，与本文件的 mysql_query 直调风格不兼容。
//
// 缓冲区开 value.size()*2+1 是 mysql_real_escape_string 文档里的最坏上界
//（每个字节最多变成两个，再加一个结尾 '\0'）。与 danmaku_dao.cpp 里那个
// 写死 256 字节的栈缓冲区不同，这里长度跟着输入走，因此没有溢出风险。
//
// 必须传入活的 MYSQL* 而不是用 mysql_escape_string：转义规则依赖于
// 连接当前字符集，没有连接就不知道该怎么转。
std::string Quote(MYSQL* conn, const std::string& value) {
    // 先按上界分配再用 resize 截到真实长度：&escaped[0] 在 C++11 后
    // 保证连续且可写，因此可以直接当 char* 给 C API 填。
    std::string escaped(value.size() * 2 + 1, '\0');
    escaped.resize(mysql_real_escape_string(conn, &escaped[0], value.data(), value.size()));
    return "'" + escaped + "'";
}
bool Exec(MYSQL* conn, const std::string& sql, std::string* error) {
    if (!conn || mysql_query(conn, sql.c_str())) {
        // Do not put SQL, prompts, or credentials into public errors.
        // 译：不要把 SQL、提示词或凭据放进对外错误信息里。
        //
        // 这条约束在本文件里比在普通 DAO 里重要得多：这里执行的 SQL 里
        // 嵌着 task.dump()，而 task 包含房间任务描述与 developer_user_ids；
        // request payload 更包含拼好的 prompt（里面有历史消息正文）。
        // 而 error 会经 RoomAgentService 原样变成 HTTP 500 的响应体。
        // 用 mysql_error(conn) 就会把这些内容回传给任意一个房间成员。
        // 真实错因只应该进服务端日志，不应该进响应。
        if (error) *error = "room Agent database operation failed";
        return false;
    }
    return true;
}
// 执行一条 SELECT 并取第一行第一列当 JSON 读。
//
// 本函数实现了接口契约里那句“Missing rows return true + null”：
//   行不存在 -> return true，*out 保持为 JSON null
//   数据库故障 -> return false
// 两者必须分开：房间还没配置任务是**正常状态**（上层映射成 404/409），
// 而查不出来是故障（上层映射成 500）。如果行不存在也返回 false，
// 每个新房间的第一次访问都会变成 500。
bool One(MYSQL* conn, const std::string& sql, Json* out, std::string* error) {
    // 先置 null 再查：保证“无行”与“查询前”两种情况下 *out 都是 null，
    // 调用方拿到的一定是个定义良好的值。
    *out = nullptr;
    if (!Exec(conn, sql, error)) return false;
    // SELECT 即使零行也会返回一个非空的结果集，因此这里为 NULL 只意味着真故障。
    MYSQL_RES* result = mysql_store_result(conn);
    if (!result) return false;
    MYSQL_ROW row = mysql_fetch_row(result);
    // parse 的第三参 false = 不抛异常，失败时返回 discarded 值。
    // 库里的 JSON 列可能被人工改坏，这里必须假定它可能不是合法 JSON。
    if (row && row[0]) *out = Json::parse(row[0], nullptr, false);
    mysql_free_result(result);
    // 注意 is_discarded() 对 null 是 false，所以“无行”不会误判为“解析失败”。
    if (out->is_discarded()) { *error = "invalid persisted room Agent state"; return false; }
    return true;
}
}
// 启动时建三张表，由 main.cpp 在组装 service 之前调一次，失败直接退出进程。
//
// 字段设计上几个值得看的点：
//   room_agent_task.room_id 直接当主键：一房一任务，不存在“同一房间多个任务”，
//                    因此不需要自增 id，也顺便让 Save 的 CAS 能直接锁到行。
//   room_agent_checkpoint 用 (room_id, revision) 做复合主键：它既定义了
//                    “每个版本最多一行”，又让 Save 里的普通 INSERT（没 IGNORE）
//                    能靠主键冲突充当第二道乐观锁。
//   room_agent_request.request_id 是 VARCHAR(121) CHARACTER SET ascii COLLATE
//                    ascii_bin。request_id 实际就是 msg_id（"r_<room_id>-<seq>"，
//                    最长 ~41 字符），121 是宽裕余量。用 ascii 是因为内容只含
//                    [a-z0-9_-]，主键索引能按字节存（比 utf8mb4 省四倍）；
//                    binary collation 让比较走纯字节序，既精确又免掉权重计算。
//   result 列可为 NULL：NULL 就是“已提交但未完成”，Requests() 靠它区分
//                    submitted / completed / failed 三态。
//   INDEX room_requests(room_id, message_seq) 专门为 Requests() 的
//                    WHERE room_id=? ORDER BY message_seq DESC LIMIT 20 而建，
//                    能直接倒序扫索引而不需 filesort。
//
// 三张表都显式写 ENGINE=InnoDB：需要事务与行锁，MyISAM 上 Save 的
// START TRANSACTION/ROLLBACK 会静默失效（建表语句看起来成功了，但回滚不生效）。
bool MySqlRoomAgentRepository::EnsureSchema(std::string* error) {
    // guard 必须活到整个循环结束：三条 DDL 共用同一个连接。
    auto guard = pool_->Acquire();
    for (const auto* sql : {
        "CREATE TABLE IF NOT EXISTS room_agent_task (room_id BIGINT PRIMARY KEY, revision BIGINT NOT NULL, payload JSON NOT NULL) ENGINE=InnoDB",
        "CREATE TABLE IF NOT EXISTS room_agent_checkpoint (room_id BIGINT NOT NULL, revision BIGINT NOT NULL, payload JSON NOT NULL, PRIMARY KEY(room_id,revision)) ENGINE=InnoDB",
        "CREATE TABLE IF NOT EXISTS room_agent_request (request_id VARCHAR(121) CHARACTER SET ascii COLLATE ascii_bin PRIMARY KEY, room_id BIGINT NOT NULL, message_seq BIGINT NOT NULL, payload JSON NOT NULL, result JSON NULL, INDEX room_requests(room_id,message_seq)) ENGINE=InnoDB"})
        if (!Exec(guard.get(), sql, error)) return false;
    return true;
}
// 一次查询同时回答“拥有者是谁”与“actor 是不是成员”。
//
// group_type != 1 直接拒：schema 里 0=normal_group、1=chatroom、2=danmaku_room。
// 共享 Agent 只对聊天室开放：弹幕房间是按视频时间轴组织的，没有
// “房间成员”这层持久订阅语义；普通群则没暴露在房间类 API 上。
// 不在这里拦住，一个弹幕房间 id 就能被当成聊天室去创建共享任务。
//
// 注意失败时不写 *owner（GetGroup 失败就直接 return false）。
// 调用方 room_agent_service.cpp 的 Access() 把 owner 初始化为 0 并先判
// 返回值，因此不会读到未初始化值；新增调用方时要保持这个习惯。
// *owner 在 IsMember 之前就已写入，所以“不是成员”这条路径上 owner 仍有效
//（Configure 靠它给出“只有拥有者能配置”的 403）。
bool MySqlRoomAgentRepository::Access(int64_t room, int64_t user, int64_t* owner,
                                     bool* member, std::string* error) {
    ImGroup group;
    if (!groups_->GetGroup(room, &group, error) || group.group_type != 1) return false;
    *owner = group.owner_id;
    return members_->IsMember(room, user, member, error);
}
// 读当前任务。room 是已经过鉴权路径校验的 int64，to_string 后拼进 SQL
// 不存在注入风险（整型没有引号可逃逸）；这也是本文件只对 JSON 负载
// 调 Quote、对数字直接拼的原因。
// 行不存在时返回 true + null（见 One 的注释），上层用 !task.is_object()
// 区分“没配置过”（409）与“查失败”（500）。
bool MySqlRoomAgentRepository::Load(int64_t room, Json* task, std::string* error) {
    auto guard = pool_->Acquire();
    return One(guard.get(), "SELECT payload FROM room_agent_task WHERE room_id=" + std::to_string(room), task, error);
}
// 写入任务，带**双重乐观锁** + 事务。本文件里最关键的一个方法。
//
// revision 参数的含义是“调用方看到的版本”：
//   revision == 0  表示创建（库里还没行），走 INSERT IGNORE，写进去的版本是 1
//   revision >  0  表示更新，走 UPDATE ... WHERE revision=<预期值>，写进去的是 +1
// 两个分支都把 checkpoint 的版本写成 revision+1，因此两边对得上。
//
// 两道锁各自的职责：
//   第一道：mysql_affected_rows(conn) == 1
//           拦“别人已经改过了”。INSERT IGNORE 撞主键时 affected_rows 为 0，
//           UPDATE 的 WHERE revision=? 不匹配时也为 0。
//   第二道：INSERT INTO room_agent_checkpoint（**没有 IGNORE**）
//           撞 (room_id, revision) 主键就报错 -> 回滚。它拦的是“同一个
//           版本号被写了两次”：即使第一道因为某种原因没拦住，
//           checkpoint 也不会出现两个内容不同的同版本行。
//
// affected_rows 的一个陷（修改本方法前必读）：本项目建连时 client_flag 传 0
//（见 common/mysql_pool.cpp 的 mysql_real_connect），因此**没有**开
// CLIENT_FOUND_ROWS，UPDATE 的 affected_rows 是“实际发生变化的行数”
// 而不是“匹配到的行数”。当前写法安全，因为 revision 一定会 +1；
// 但若把 UPDATE 改成只写 payload，一个内容完全相同的重试就会得到
// affected_rows=0 而被误报为版本冲突。
bool MySqlRoomAgentRepository::Save(int64_t room, int64_t revision, const Json& task,
                                   std::string* error) {
    auto guard = pool_->Acquire();
    auto* conn = guard.get();
    // guard 必须持有到 COMMIT/ROLLBACK 之后才能释：事务是**连接级**状态，
    // 中途归还连接会把一个未提交的事务交给下一个使用者，
    // 对方的语句会被隐式包进本事务，然后被本函数的 ROLLBACK 一起吐掉。
    if (!Exec(conn, "START TRANSACTION", error)) return false;
    // 只转义一次，两个语句复用同一份：task.dump() 可能有几十 KB
    //（spec/summary 各限 6000 字节），转义两遍是白花钱。
    const auto payload = Quote(conn, task.dump());
    std::string sql = revision == 0
        ? "INSERT IGNORE INTO room_agent_task VALUES(" + std::to_string(room) + ",1," + payload + ")"
        : "UPDATE room_agent_task SET revision=revision+1,payload=" + payload +
            " WHERE room_id=" + std::to_string(room) + " AND revision=" + std::to_string(revision);
    const bool saved = Exec(conn, sql, error) && mysql_affected_rows(conn) == 1 &&
        Exec(conn, "INSERT INTO room_agent_checkpoint VALUES(" + std::to_string(room) + "," +
             std::to_string(revision + 1) + "," + payload + ")", error);
    // 短路求值很关键：Exec 失败时不能再拿 affected_rows 当真。
    // 下面这一行里 `*error = ...` 必须写在 Exec(ROLLBACK) 之后：
    // ROLLBACK 本身也可能失败并覆盖 *error（Exec 会写固定文案），
    // 不重新赋值的话调用方看到的会是“rollback 失败”而不是真正的原因。
    if (!saved) { Exec(conn, "ROLLBACK", error); *error = "task revision conflict or storage failure"; return false; }
    return Exec(conn, "COMMIT", error);
}
// 按 request_id 读回一个已存的请求。ValidateReply 靠它做归属校验：
// Agent 回一个 request_id，服务端从库里取出当初存的请求，逐字段比对
// 确认这个回复真的属于那个房间/那个用户。
// id 来自外部（hermes_bridge 的回复负载），**必须**过 Quote。
bool MySqlRoomAgentRepository::Request(const std::string& id, Json* request, std::string* error) {
    auto guard = pool_->Acquire();
    if (!guard.get()) return false;
    return One(guard.get(), "SELECT payload FROM room_agent_request WHERE request_id=" + Quote(guard.get(), id), request, error);
}
// 写入一次 Agent 调用请求。幂等语义：“INSERT IGNORE + 读回比对”，
// 与 MessageDao::InsertMessage 是同一个模式（参见那里的注释）。
//
// 为什么不能只看 INSERT 是否成功：request_id 就是 msg_id，而 msg_id 是
// session_id+msg_seq 的确定性拼接，同一条房间消息重试时 request_id 完全相同。
// INSERT IGNORE 对重复键静默返回成功，但它**区分不了**“同一个请求重发”
// 与“不同内容撞了同一个 id”。后者意味着上游算错了 msg_id，必须报错。
// 因此额外读回一次做 `stored == request` 的结构比对：
//   内容一致 -> true（幂等吸收，BuildRequest 可以安全重试）
//   内容不一致 -> false
// nlohmann 的 operator== 对 object 是**按 key 比较**而不是按字节比较
//（底层是有序 map），因此字段书写顺序不同不会误判为不一致。
bool MySqlRoomAgentRepository::PutRequest(const Json& request, std::string* error) {
    auto guard = pool_->Acquire();
    auto* conn = guard.get();
    if (!conn) return false;
    // at() 而不是 value()：这几个字段缺失就是调用方的编程错误，
    // 应该当场抛出来而不是静默写一个残缺的行。
    // 代价：这里抛的 nlohmann::out_of_range 没人接（调用链上没 try/catch），
    // 会直接终止进程。BuildRequest 里的 request 是本函数上一行刚拼的，
    // 字段一定齐，因此生产路径上不会触发。
    const auto id = Quote(conn, request.at("request_id"));
    // room_id / message_seq 不从顶层取，而是从 collaboration.room_id 取：
    // 它们是鉴权时算好的 scope（见 BuildRequest），而这两个列只是
    // 为了建 room_requests 索引而冗余出来的，必须与 scope 保持一致。
    if (!Exec(conn, "INSERT IGNORE INTO room_agent_request(request_id,room_id,message_seq,payload) VALUES(" +
        id + "," + std::to_string(request.at("collaboration").at("room_id").get<int64_t>()) + "," +
        std::to_string(request.at("message_seq").get<int64_t>()) + "," + Quote(conn, request.dump()) + ")", error)) return false;
    Json stored;
    return One(conn, "SELECT payload FROM room_agent_request WHERE request_id=" + id, &stored, error) && stored == request;
}
// 写入一次调用的结果。
//
// `AND result IS NULL` 实现了**首次写入获胜**：同一个 request_id 被
// 完成两次时，只有第一次的 UPDATE 会生效，第二次影响 0 行。
//
// 注意本函数**故意不查 affected_rows**（与 Save 相反）：影响 0 行在这里
// 是幂等重试的正常情况，不能当失败。真正的判定靠后面的读回比对：
//   已存结果 == 本次结果 -> true（重试成功，Kafka 重放走的就是这条）
//   已存结果 != 本次结果 -> false（两次算出了不同结果，必须暴露）
// 若改成看 affected_rows，Kafka 至少一次语义下的正常重放就会被当成错误。
bool MySqlRoomAgentRepository::Finish(const std::string& id, const Json& result, std::string* error) {
    auto guard = pool_->Acquire();
    auto* conn = guard.get();
    if (!conn) return false;
    const auto quoted = Quote(conn, id);
    if (!Exec(conn, "UPDATE room_agent_request SET result=" + Quote(conn, result.dump()) +
        " WHERE request_id=" + quoted + " AND result IS NULL", error)) return false;
    Json stored;
    return One(conn, "SELECT result FROM room_agent_request WHERE request_id=" + quoted, &stored, error) && stored == result;
}
// 列出一个房间最近 20 次调用，给前端任务面板展示。
//
// 返回的是**投影后的字段**而不是原始 payload，这一点很重要：
// payload 里的 messages[0].content 是拼好的完整 prompt（包含任务规格、
// 交接摘要、以及最近 20 条房间历史正文），而本接口的调用方只是
// “房间任意成员”（View 只要求 CheckMember）。直接回 payload 等于把
// 整份 prompt 泄露给所有成员，也会把响应体撑到 24KB 上限。
//
// LIMIT 20 写死而不是参数化：这是展示用的固定窗口，开成参数就给了
// 客户端一个放大查询的杠杆。排序走 room_requests 索引倒序扫，无 filesort。
bool MySqlRoomAgentRepository::Requests(int64_t room, Json* requests, std::string* error) {
    auto guard = pool_->Acquire();
    if (!Exec(guard.get(), "SELECT payload,result FROM room_agent_request WHERE room_id=" +
              std::to_string(room) + " ORDER BY message_seq DESC LIMIT 20", error)) return false;
    MYSQL_RES* result = mysql_store_result(guard.get());
    if (!result) return false;
    *requests = Json::array();
    MYSQL_ROW row;
    while ((row = mysql_fetch_row(result))) {
        // payload 是 NOT NULL 列，必须是对象；result 可为 NULL（未完成），
        // 因此两个列的校验方式故意不对称：前者查 is_object，后者只查
        // is_discarded（null 是合法的，但非法 JSON 不是）。
        const auto request = Json::parse(row[0], nullptr, false);
        const auto completed = row[1] ? Json::parse(row[1], nullptr, false) : Json(nullptr);
        // 任何一行坏了就整个失败，而不是跳过那一行：静默跳过会让前端
        // 看到一份“看起来正常但少了条目”的列表，排查时无从下手。
        // 注意 return 前必须先 mysql_free_result，否则泄漏结果集。
        if (!request.is_object() || completed.is_discarded()) { mysql_free_result(result); return false; }
        // 三态映射：result 为 NULL -> submitted；result.ok 为真 -> completed；
        // 否则 failed。靠 completed.value("ok", false) 而不是 contains，
        // 这样“存了个结果但没 ok 字段”也会被归为 failed（fail-closed）。
        requests->push_back({{"request_id", request.at("request_id")},
                             {"actor_user_id", request.at("user_id")},
                             {"message_seq", request.at("message_seq")},
                             {"status", completed.is_null() ? "submitted" : completed.value("ok", false) ? "completed" : "failed"},
                             {"result", completed}});
    }
    mysql_free_result(result);
    return true;
}
}  // namespace sparkpush
