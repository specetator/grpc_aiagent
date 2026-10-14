#include "session_dao.h"

#include <mysql/mysql.h>

#include <cstring>

#include "logging.h"

namespace sparkpush {

namespace {

SessionType ParseSessionType(int type) {
    switch (type) {
        case 0:
            return SessionType::kSingle;
        case 1:
            return SessionType::kGroup;
        case 2:
            return SessionType::kChatroom;
        default:
            return SessionType::kSingle;
    }
}

}  // namespace

// 绑定 stmt 结果到 Session 结构。
//
// 四个查询方法（GetSessionById / ListUserSingleSessions 等）共用它，
// 前提是它们的 SELECT 列顺序必须与下面的 result[0..5] 完全一致。
// 改 schema 时这是最容易遗漏的隐含约束——列顺序变了不会报错，
// 只会静地把 user2_id 读成 group_id。
bool SessionDao::FillSessionFromStmt(MYSQL_STMT* stmt, Session* session,
                                     std::string* err_msg) {
    if (!session) return false;

    MYSQL_BIND result[6];
    memset(result, 0, sizeof(result));

    // 256 字节对 VARCHAR(128) 的 session_id 是安全的，不会触发截断分支。
    char session_id_buf[256] = {0};
    unsigned long session_id_len = 0;
    int type_buf = 0;
    long long user1_buf = 0;
    long long user2_buf = 0;
    long long group_id_buf = 0;
    long long last_seq_buf = 0;

    result[0].buffer_type = MYSQL_TYPE_STRING;
    result[0].buffer = session_id_buf;
    result[0].buffer_length = sizeof(session_id_buf);
    result[0].length = &session_id_len;

    result[1].buffer_type = MYSQL_TYPE_LONG;
    result[1].buffer = &type_buf;

    result[2].buffer_type = MYSQL_TYPE_LONGLONG;
    result[2].buffer = &user1_buf;

    result[3].buffer_type = MYSQL_TYPE_LONGLONG;
    result[3].buffer = &user2_buf;

    result[4].buffer_type = MYSQL_TYPE_LONGLONG;
    result[4].buffer = &group_id_buf;

    result[5].buffer_type = MYSQL_TYPE_LONGLONG;
    result[5].buffer = &last_seq_buf;

    if (mysql_stmt_bind_result(stmt, result) != 0) {
        if (err_msg) *err_msg = mysql_stmt_error(stmt);
        return false;
    }

    int fetch_ret = mysql_stmt_fetch(stmt);
    // 注意 MYSQL_NO_DATA（值为 100）也会走到这里：它不等于 0 也不等于
    // MYSQL_DATA_TRUNCATED，因此「查不到行」被当成失败返回。
    // 副作用：此时 mysql_stmt_error 返回空字符串，调用方拿到的 err_msg 是空的。
    // 排查「会话不存在」类问题时不要指望这里有错误文本。
    if (fetch_ret != 0 && fetch_ret != MYSQL_DATA_TRUNCATED) {
        if (err_msg) *err_msg = mysql_stmt_error(stmt);
        return false;
    }

    // assign(buf, len) 而不是用 buf 构造 string：session_id 是 ascii_bin，
    // 虽然实际上不会出现 '\0'，但用长度构造能避开任何截断风险。
    session->id.assign(session_id_buf, session_id_len);
    session->type = ParseSessionType(type_buf);
    session->user1_id = user1_buf;
    session->user2_id = user2_buf;
    session->group_id = group_id_buf;
    session->last_msg_seq = last_seq_buf;
    return true;
}

// 确保单聊会话存在（若不存在则插入）
bool SessionDao::EnsureSingleSession(MYSQL* conn, const std::string& session_id,
                                     int64_t user1, int64_t user2,
                                     std::string* err_msg) {
    const char* sql =
        "INSERT INTO `session`(session_id, type, user1_id, user2_id, group_id, "
        "last_msg_seq) "
        "VALUES(?, 0, ?, ?, 0, 0)";
    MYSQL_STMT* stmt = mysql_stmt_init(conn);
    if (!stmt) {
        if (err_msg) *err_msg = "mysql_stmt_init failed";
        return false;
    }
    if (mysql_stmt_prepare(stmt, sql, strlen(sql)) != 0) {
        if (err_msg) *err_msg = mysql_stmt_error(stmt);
        mysql_stmt_close(stmt);
        return false;
    }
    MYSQL_BIND bind[3];
    memset(bind, 0, sizeof(bind));
    unsigned long sid_len = static_cast<unsigned long>(session_id.size());
    bind[0].buffer_type = MYSQL_TYPE_STRING;
    bind[0].buffer = const_cast<char*>(session_id.data());
    bind[0].buffer_length = sid_len;
    bind[0].length = &sid_len;

    long long user1_buf = user1;
    bind[1].buffer_type = MYSQL_TYPE_LONGLONG;
    bind[1].buffer = &user1_buf;

    long long user2_buf = user2;
    bind[2].buffer_type = MYSQL_TYPE_LONGLONG;
    bind[2].buffer = &user2_buf;

    bool ok = true;
    // 绑定参数
    if (mysql_stmt_bind_param(stmt, bind) != 0) {
        if (err_msg) *err_msg = mysql_stmt_error(stmt);
        mysql_stmt_close(stmt);
        return false;
    }
    if (mysql_stmt_execute(stmt) != 0) {
        unsigned int err = mysql_stmt_errno(stmt);
        // 1062 = Duplicate entry，意味着会话已存在，这正是本函数想要的结果，
        // 因此把它当成功（ok 保持 true）。并发创建时后到的那一侧全靠这个分支，
        // 否则两个用户同时发起会话就会有一方报错。
        // 注意这里故意**不**用 INSERT IGNORE：IGNORE 会连数据截断、
        // 约束失败一起吞掉，而显式判 1062 只吸收真正预期的那一种情况。
        if (err != 1062) {  // duplicate key
            if (err_msg) *err_msg = mysql_stmt_error(stmt);
            ok = false;
        }
    }
    mysql_stmt_close(stmt);
    return ok;
}

// 确保聊天室会话存在。与 EnsureSingleSession 的区别只在固定值：
// type=2（kChatroom），user1_id/user2_id 都给 0（聊天室没有「两个端点」），
// room_id 存在 group_id 列里。last_msg_seq 从 0 开始，与序号从 1 开始对应。
bool SessionDao::EnsureRoomSession(MYSQL* conn, const std::string& session_id,
                                   int64_t room_id, std::string* err_msg) {
    const char* sql =
        "INSERT INTO `session`(session_id, type, user1_id, user2_id, group_id, "
        "last_msg_seq) "
        "VALUES(?, 2, 0, 0, ?, 0)";
    MYSQL_STMT* stmt = mysql_stmt_init(conn);
    if (!stmt) {
        if (err_msg) *err_msg = "mysql_stmt_init failed";
        return false;
    }
    if (mysql_stmt_prepare(stmt, sql, strlen(sql)) != 0) {
        if (err_msg) *err_msg = mysql_stmt_error(stmt);
        mysql_stmt_close(stmt);
        return false;
    }
    MYSQL_BIND bind[2];
    memset(bind, 0, sizeof(bind));
    unsigned long sid_len = static_cast<unsigned long>(session_id.size());
    bind[0].buffer_type = MYSQL_TYPE_STRING;
    bind[0].buffer = const_cast<char*>(session_id.data());
    bind[0].buffer_length = sid_len;
    bind[0].length = &sid_len;

    long long room_buf = room_id;
    bind[1].buffer_type = MYSQL_TYPE_LONGLONG;
    bind[1].buffer = &room_buf;

    bool ok = true;
    // 绑定参数
    if (mysql_stmt_bind_param(stmt, bind) != 0) {
        if (err_msg) *err_msg = mysql_stmt_error(stmt);
        mysql_stmt_close(stmt);
        return false;
    }
    if (mysql_stmt_execute(stmt) != 0) {
        unsigned int err = mysql_stmt_errno(stmt);
        if (err != 1062) {
            if (err_msg) *err_msg = mysql_stmt_error(stmt);
            ok = false;
        }
    }
    mysql_stmt_close(stmt);
    return ok;
}

// 按 session_id 查询会话
bool SessionDao::GetSessionById(const std::string& session_id, Session* session,
                                std::string* err_msg) {
    if (!pool_ || !session) {
        if (err_msg) *err_msg = "invalid arguments";
        return false;
    }
    auto guard = pool_->Acquire();
    MYSQL* conn = guard.get();
    if (!conn) {
        if (err_msg) *err_msg = "no mysql connection";
        return false;
    }
    const char* sql =
        "SELECT session_id, type, user1_id, user2_id, group_id, last_msg_seq "
        "FROM `session` WHERE session_id=? LIMIT 1";
    MYSQL_STMT* stmt = mysql_stmt_init(conn);
    if (!stmt) {
        if (err_msg) *err_msg = "mysql_stmt_init failed";
        return false;
    }
    if (mysql_stmt_prepare(stmt, sql, strlen(sql)) != 0) {
        if (err_msg) *err_msg = mysql_stmt_error(stmt);
        mysql_stmt_close(stmt);
        return false;
    }
    MYSQL_BIND bind[1];
    memset(bind, 0, sizeof(bind));
    unsigned long sid_len = static_cast<unsigned long>(session_id.size());
    bind[0].buffer_type = MYSQL_TYPE_STRING;
    bind[0].buffer = const_cast<char*>(session_id.data());
    bind[0].buffer_length = sid_len;
    bind[0].length = &sid_len;
    if (mysql_stmt_bind_param(stmt, bind) != 0) {
        if (err_msg) *err_msg = mysql_stmt_error(stmt);
        mysql_stmt_close(stmt);
        return false;
    }
    if (mysql_stmt_execute(stmt) != 0) {
        if (err_msg) *err_msg = mysql_stmt_error(stmt);
        mysql_stmt_close(stmt);
        return false;
    }
    if (!FillSessionFromStmt(stmt, session, err_msg)) {
        mysql_stmt_close(stmt);
        return false;
    }
    mysql_stmt_close(stmt);
    return true;
}

// 获取或创建单聊会话。
//
// 两步分开而不是一条 INSERT ... ON DUPLICATE + 回读，原因是需要返回完整行：
// INSERT 只能告知「写没写」，而调用方要的是 last_msg_seq 等字段。
// 中间的大括号是一个作用域块，目的是让 guard 在 GetSessionById 之前析构、
// 把连接还给池：否则一个请求会同时占两条连接，高并发下很容易把池抽干。
bool SessionDao::GetOrCreateSingleSession(int64_t user1, int64_t user2,
                                          Session* session,
                                          std::string* err_msg) {
    if (!session || !pool_) {
        if (err_msg) *err_msg = "invalid arguments";
        return false;
    }
    // 升序归一是单聊会话幂等的全部：不排序就会为 A->B 与 B->A
    // 建出两个不同会话，双方看到的历史互相不可见。
    if (user1 > user2) std::swap(user1, user2);
    std::string session_id =
        "s_" + std::to_string(user1) + "_" + std::to_string(user2);

    {
        auto guard = pool_->Acquire();
        MYSQL* conn = guard.get();
        if (!conn) {
            if (err_msg) *err_msg = "no mysql connection";
            return false;
        }
        std::string err;
        // 失败只记日志：1062（已存在）已在 EnsureSingleSession 内部被吸收成成功，
        // 能走到这里的只剩真错误（连接断、表丢失）。那种情况下行也可能已经存在，
        // 下面的 GetSessionById 仍有机会读到，因此先试一次再定成败。
        if (!EnsureSingleSession(conn, session_id, user1, user2, &err)) {
            LOG_ERROR << "EnsureSingleSession failed: " << err;
        }
    }

    return GetSessionById(session_id, session, err_msg);
}

// 获取或创建聊天室会话
bool SessionDao::GetOrCreateRoomSession(int64_t room_id, Session* session,
                                        std::string* err_msg) {
    if (!session || !pool_) {
        if (err_msg) *err_msg = "invalid arguments";
        return false;
    }
    std::string session_id = "r_" + std::to_string(room_id);

    {
        auto guard = pool_->Acquire();
        MYSQL* conn = guard.get();
        if (!conn) {
            if (err_msg) *err_msg = "no mysql connection";
            return false;
        }
        std::string err;
        if (!EnsureRoomSession(conn, session_id, room_id, &err)) {
            LOG_ERROR << "EnsureRoomSession failed: " << err;
        }
    }
    return GetSessionById(session_id, session, err_msg);
}

// 分配消息序列号：last_msg_seq 自增并返回。
//
// 警告：本函数当前**没有任何调用方**，而且实现上不是原子的：
// 上面的 UPDATE +1 与下面的 SELECT 是两条独立语句，中间没有事务也没有锁，
// 并发下两个调用方可能读到同一个 last_msg_seq（重复发号）。
// 真正的序号分配走 Redis Lua（热路径）或 MySqlMessageReservations::Reserve（持久兜底）。
// 保留本函数只是为了兼容旧接口；重新启用前必须先修成原子操作。
bool SessionDao::AllocateMessageSeq(const std::string& session_id,
                                    int64_t* next_seq, std::string* err_msg) {
    if (!pool_ || !next_seq) {
        if (err_msg) *err_msg = "invalid arguments";
        return false;
    }
    auto guard = pool_->Acquire();
    MYSQL* conn = guard.get();
    if (!conn) {
        if (err_msg) *err_msg = "no mysql connection";
        return false;
    }

    const char* inc_sql =
        "UPDATE `session` SET last_msg_seq = last_msg_seq + 1 WHERE "
        "session_id=?";
    MYSQL_STMT* inc_stmt = mysql_stmt_init(conn);
    if (!inc_stmt) {
        if (err_msg) *err_msg = "mysql_stmt_init failed";
        return false;
    }
    if (mysql_stmt_prepare(inc_stmt, inc_sql, strlen(inc_sql)) != 0) {
        if (err_msg) *err_msg = mysql_stmt_error(inc_stmt);
        mysql_stmt_close(inc_stmt);
        return false;
    }
    MYSQL_BIND bind[1];
    memset(bind, 0, sizeof(bind));
    unsigned long sid_len = static_cast<unsigned long>(session_id.size());
    bind[0].buffer_type = MYSQL_TYPE_STRING;
    bind[0].buffer = const_cast<char*>(session_id.data());
    bind[0].buffer_length = sid_len;
    bind[0].length = &sid_len;
    if (mysql_stmt_bind_param(inc_stmt, bind) != 0) {
        if (err_msg) *err_msg = mysql_stmt_error(inc_stmt);
        mysql_stmt_close(inc_stmt);
        return false;
    }
    if (mysql_stmt_execute(inc_stmt) != 0) {
        if (err_msg) *err_msg = mysql_stmt_error(inc_stmt);
        mysql_stmt_close(inc_stmt);
        return false;
    }
    my_ulonglong affected = mysql_stmt_affected_rows(inc_stmt);
    mysql_stmt_close(inc_stmt);
    // affected==0 意味着会话行不存在。不能当成「自增到 1」继续跑，
    // 否则会在一个根本不存在的会话上发出消息。
    if (affected == 0) {
        if (err_msg) *err_msg = "session not found";
        return false;
    }

    const char* sel_sql =
        "SELECT last_msg_seq FROM `session` WHERE session_id=? LIMIT 1";
    MYSQL_STMT* sel_stmt = mysql_stmt_init(conn);
    if (!sel_stmt) {
        if (err_msg) *err_msg = "mysql_stmt_init failed";
        return false;
    }
    if (mysql_stmt_prepare(sel_stmt, sel_sql, strlen(sel_sql)) != 0) {
        if (err_msg) *err_msg = mysql_stmt_error(sel_stmt);
        mysql_stmt_close(sel_stmt);
        return false;
    }
    memset(bind, 0, sizeof(bind));
    bind[0].buffer_type = MYSQL_TYPE_STRING;
    bind[0].buffer = const_cast<char*>(session_id.data());
    bind[0].buffer_length = sid_len;
    bind[0].length = &sid_len;
    if (mysql_stmt_bind_param(sel_stmt, bind) != 0) {
        if (err_msg) *err_msg = mysql_stmt_error(sel_stmt);
        mysql_stmt_close(sel_stmt);
        return false;
    }
    if (mysql_stmt_execute(sel_stmt) != 0) {
        if (err_msg) *err_msg = mysql_stmt_error(sel_stmt);
        mysql_stmt_close(sel_stmt);
        return false;
    }
    MYSQL_BIND result[1];
    memset(result, 0, sizeof(result));
    long long last_seq = 0;
    result[0].buffer_type = MYSQL_TYPE_LONGLONG;
    result[0].buffer = &last_seq;
    if (mysql_stmt_bind_result(sel_stmt, result) != 0) {
        if (err_msg) *err_msg = mysql_stmt_error(sel_stmt);
        mysql_stmt_close(sel_stmt);
        return false;
    }
    if (mysql_stmt_fetch(sel_stmt) != 0) {
        if (err_msg) *err_msg = mysql_stmt_error(sel_stmt);
        mysql_stmt_close(sel_stmt);
        return false;
    }
    mysql_stmt_close(sel_stmt);
    *next_seq = last_seq;
    return true;
}

// 把 last_msg_seq 单调推进到至少 last_seq。
//
// GREATEST 是本函数存在的全部理由：Job 的多个 consumer group 并行落库，
// 完成顺序与序号顺序无关。若写成直接赋值，一个迟到的 seq=5 会把已推到 8
// 的游标拉回 5，客户端下次同步就会重复拉到 6/7/8。
// 同时它也是单语句原子的，不需要事务或额外锁。
bool SessionDao::UpdateLastMessageSeqAtLeast(const std::string& session_id,
                                             int64_t last_seq,
                                             std::string* err_msg) {
    // last_seq <= 0 直接拒：序号从 1 开始，0 只可能是调用方的默认值泄露进来，
    // 把它写进去虽然被 GREATEST 挡住不会造成损害，但会掩盖上游的 bug。
    if (!pool_ || session_id.empty() || last_seq <= 0) {
        if (err_msg) *err_msg = "invalid last_msg_seq update arguments";
        return false;
    }
    auto guard = pool_->Acquire();
    MYSQL* conn = guard.get();
    if (!conn) {
        if (err_msg) *err_msg = "no mysql connection";
        return false;
    }
    const char* sql =
        "UPDATE `session` SET last_msg_seq=GREATEST(last_msg_seq, ?) "
        "WHERE session_id=?";
    MYSQL_STMT* stmt = mysql_stmt_init(conn);
    if (!stmt) {
        if (err_msg) *err_msg = "mysql_stmt_init failed";
        return false;
    }
    if (mysql_stmt_prepare(stmt, sql, strlen(sql)) != 0) {
        if (err_msg) *err_msg = mysql_stmt_error(stmt);
        mysql_stmt_close(stmt);
        return false;
    }
    MYSQL_BIND bind[2];
    memset(bind, 0, sizeof(bind));
    long long seq_buf = last_seq;
    bind[0].buffer_type = MYSQL_TYPE_LONGLONG;
    bind[0].buffer = &seq_buf;
    unsigned long sid_len = static_cast<unsigned long>(session_id.size());
    bind[1].buffer_type = MYSQL_TYPE_STRING;
    bind[1].buffer = const_cast<char*>(session_id.data());
    bind[1].buffer_length = sid_len;
    bind[1].length = &sid_len;
    if (mysql_stmt_bind_param(stmt, bind) != 0 ||
        mysql_stmt_execute(stmt) != 0) {
        if (err_msg) *err_msg = mysql_stmt_error(stmt);
        mysql_stmt_close(stmt);
        return false;
    }
    mysql_stmt_close(stmt);
    return true;
}

// 列出用户参与的单聊会话
bool SessionDao::ListUserSingleSessions(int64_t user_id,
                                        std::vector<Session>* sessions,
                                        std::string* err_msg) {
    if (!sessions || !pool_) {
        if (err_msg) *err_msg = "invalid arguments";
        return false;
    }
    sessions->clear();
    auto guard = pool_->Acquire();
    MYSQL* conn = guard.get();
    if (!conn) {
        if (err_msg) *err_msg = "no mysql connection";
        return false;
    }
    const char* sql =
        "SELECT session_id, type, user1_id, user2_id, group_id, last_msg_seq "
        "FROM `session` WHERE type=0 AND (user1_id=? OR user2_id=?)";
    MYSQL_STMT* stmt = mysql_stmt_init(conn);
    if (!stmt) {
        if (err_msg) *err_msg = "mysql_stmt_init failed";
        return false;
    }
    if (mysql_stmt_prepare(stmt, sql, strlen(sql)) != 0) {
        if (err_msg) *err_msg = mysql_stmt_error(stmt);
        mysql_stmt_close(stmt);
        return false;
    }
    MYSQL_BIND bind[2];
    memset(bind, 0, sizeof(bind));
    long long uid_buf = user_id;
    bind[0].buffer_type = MYSQL_TYPE_LONGLONG;
    bind[0].buffer = &uid_buf;
    bind[1].buffer_type = MYSQL_TYPE_LONGLONG;
    bind[1].buffer = &uid_buf;
    if (mysql_stmt_bind_param(stmt, bind) != 0) {
        if (err_msg) *err_msg = mysql_stmt_error(stmt);
        mysql_stmt_close(stmt);
        return false;
    }
    if (mysql_stmt_execute(stmt) != 0) {
        if (err_msg) *err_msg = mysql_stmt_error(stmt);
        mysql_stmt_close(stmt);
        return false;
    }
    if (mysql_stmt_store_result(stmt) != 0) {
        if (err_msg) *err_msg = mysql_stmt_error(stmt);
        mysql_stmt_close(stmt);
        return false;
    }
    MYSQL_BIND result[6];
    memset(result, 0, sizeof(result));
    char session_id_buf[256];
    unsigned long session_id_len = 0;
    int type_buf = 0;
    long long user1_buf = 0;
    long long user2_buf = 0;
    long long group_id_buf = 0;
    long long last_seq_buf = 0;

    result[0].buffer_type = MYSQL_TYPE_STRING;
    result[0].buffer = session_id_buf;
    result[0].buffer_length = sizeof(session_id_buf);
    result[0].length = &session_id_len;

    result[1].buffer_type = MYSQL_TYPE_LONG;
    result[1].buffer = &type_buf;

    result[2].buffer_type = MYSQL_TYPE_LONGLONG;
    result[2].buffer = &user1_buf;

    result[3].buffer_type = MYSQL_TYPE_LONGLONG;
    result[3].buffer = &user2_buf;

    result[4].buffer_type = MYSQL_TYPE_LONGLONG;
    result[4].buffer = &group_id_buf;

    result[5].buffer_type = MYSQL_TYPE_LONGLONG;
    result[5].buffer = &last_seq_buf;

    if (mysql_stmt_bind_result(stmt, result) != 0) {
        if (err_msg) *err_msg = mysql_stmt_error(stmt);
        mysql_stmt_close(stmt);
        return false;
    }

    while (true) {
        int fetch_ret = mysql_stmt_fetch(stmt);
        if (fetch_ret == MYSQL_NO_DATA) {
            break;
        }
        if (fetch_ret != 0 && fetch_ret != MYSQL_DATA_TRUNCATED) {
            if (err_msg) *err_msg = mysql_stmt_error(stmt);
            mysql_stmt_close(stmt);
            return false;
        }
        Session s;
        s.id.assign(session_id_buf, session_id_len);
        s.type = ParseSessionType(type_buf);
        s.user1_id = user1_buf;
        s.user2_id = user2_buf;
        s.group_id = group_id_buf;
        s.last_msg_seq = last_seq_buf;
        sessions->push_back(std::move(s));
    }
    mysql_stmt_close(stmt);
    return true;
}

}  // namespace sparkpush
