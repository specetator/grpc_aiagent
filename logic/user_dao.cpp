#include "user_dao.h"

#include <mysql/mysql.h>

#include <cstring>

#include "logging.h"

namespace sparkpush {
namespace {

struct UserResultBuffers {
    long long id{0};
    char account[128]{};
    char name[128]{};
    char password_hash[256]{};
    int status{0};
    char deleted_at[64]{};
    char created_at[64]{};
    unsigned long account_len{0};
    unsigned long name_len{0};
    unsigned long password_hash_len{0};
    unsigned long deleted_at_len{0};
    unsigned long created_at_len{0};
};

void BindUserResult(MYSQL_BIND* result, UserResultBuffers* buffers) {
    std::memset(result, 0, sizeof(MYSQL_BIND) * 7);

    result[0].buffer_type = MYSQL_TYPE_LONGLONG;
    result[0].buffer = &buffers->id;
    result[0].is_unsigned = 0;

    result[1].buffer_type = MYSQL_TYPE_STRING;
    result[1].buffer = buffers->account;
    result[1].buffer_length = sizeof(buffers->account);
    result[1].length = &buffers->account_len;

    result[2].buffer_type = MYSQL_TYPE_STRING;
    result[2].buffer = buffers->name;
    result[2].buffer_length = sizeof(buffers->name);
    result[2].length = &buffers->name_len;

    result[3].buffer_type = MYSQL_TYPE_STRING;
    result[3].buffer = buffers->password_hash;
    result[3].buffer_length = sizeof(buffers->password_hash);
    result[3].length = &buffers->password_hash_len;

    result[4].buffer_type = MYSQL_TYPE_LONG;
    result[4].buffer = &buffers->status;

    result[5].buffer_type = MYSQL_TYPE_STRING;
    result[5].buffer = buffers->deleted_at;
    result[5].buffer_length = sizeof(buffers->deleted_at);
    result[5].length = &buffers->deleted_at_len;

    result[6].buffer_type = MYSQL_TYPE_STRING;
    result[6].buffer = buffers->created_at;
    result[6].buffer_length = sizeof(buffers->created_at);
    result[6].length = &buffers->created_at_len;
}

void AssignUser(const UserResultBuffers& buffers, User* user) {
    if (!user) return;
    user->id = buffers.id;
    user->account.assign(buffers.account, buffers.account_len);
    user->name.assign(buffers.name, buffers.name_len);
    user->password_hash.assign(buffers.password_hash,
                                buffers.password_hash_len);
    user->status = buffers.status;
    user->deleted_at.assign(buffers.deleted_at, buffers.deleted_at_len);
    user->created_at.assign(buffers.created_at, buffers.created_at_len);
}

bool FetchOneUser(MYSQL_STMT* stmt, User* user, std::string* err_msg) {
    if (mysql_stmt_store_result(stmt) != 0) {
        if (err_msg) *err_msg = mysql_stmt_error(stmt);
        return false;
    }
    if (mysql_stmt_num_rows(stmt) == 0) {
        if (err_msg) *err_msg = "user not found";
        return false;
    }

    UserResultBuffers buffers;
    MYSQL_BIND result[7];
    BindUserResult(result, &buffers);
    if (mysql_stmt_bind_result(stmt, result) != 0) {
        if (err_msg) *err_msg = mysql_stmt_error(stmt);
        return false;
    }
    const int fetch_ret = mysql_stmt_fetch(stmt);
    if (fetch_ret != 0 && fetch_ret != MYSQL_DATA_TRUNCATED) {
        if (err_msg) *err_msg = mysql_stmt_error(stmt);
        return false;
    }
    AssignUser(buffers, user);
    return true;
}

bool ExecSchemaStatement(MYSQL* conn, const char* sql,
                         std::string* err_msg) {
    if (mysql_query(conn, sql) == 0) return true;
    // MySQL 8.0 does not have identical IF NOT EXISTS support across all
    // ALTER TABLE variants. Duplicate column/index means the migration has
    // already been applied and is therefore a successful idempotent result.
    const unsigned int code = mysql_errno(conn);
    if (code == 1060 || code == 1061) return true;
    if (err_msg) *err_msg = mysql_error(conn);
    return false;
}

std::string UserSelectSql(const char* suffix) {
    std::string sql =
        "SELECT id, account, name, password_hash, status, "
        "COALESCE(DATE_FORMAT(deleted_at, '%Y-%m-%dT%H:%i:%sZ'), ''), "
        "COALESCE(DATE_FORMAT(created_at, '%Y-%m-%dT%H:%i:%sZ'), '') "
        "FROM `user` ";
    sql += suffix;
    return sql;
}

bool BindUserIdParam(MYSQL_BIND* bind, long long* user_id) {
    if (!bind || !user_id) return false;
    std::memset(bind, 0, sizeof(MYSQL_BIND));
    bind->buffer_type = MYSQL_TYPE_LONGLONG;
    bind->buffer = user_id;
    bind->is_unsigned = 0;
    return true;
}

}  // namespace

bool UserDao::EnsureUserCenterSchema(std::string* err_msg) {
    if (!pool_) {
        if (err_msg) *err_msg = "mysql pool not initialized";
        return false;
    }
    auto guard = pool_->Acquire();
    MYSQL* conn = guard.get();
    if (!conn) {
        if (err_msg) *err_msg = "no mysql connection";
        return false;
    }

    if (!ExecSchemaStatement(
            conn,
            "ALTER TABLE `user` ADD COLUMN `status` TINYINT NOT NULL "
            "DEFAULT 1 AFTER `password_hash`",
            err_msg)) {
        return false;
    }
    if (!ExecSchemaStatement(
            conn,
            "ALTER TABLE `user` ADD COLUMN `deleted_at` DATETIME(3) NULL "
            "DEFAULT NULL AFTER `status`",
            err_msg)) {
        return false;
    }
    if (!ExecSchemaStatement(
            conn,
            "ALTER TABLE `user` ADD COLUMN `updated_at` DATETIME(3) NOT NULL "
            "DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) "
            "AFTER `created_at`",
            err_msg)) {
        return false;
    }
    return ExecSchemaStatement(
        conn,
        "CREATE INDEX `idx_user_status` ON `user` (`status`, `deleted_at`)",
        err_msg);
}

bool UserDao::EnsureHermesBotUser(int64_t bot_user_id,
                                  const std::string& account,
                                  const std::string& name,
                                  std::string* err_msg) {
    if (!pool_ || bot_user_id <= 0 || account.empty()) {
        if (err_msg) *err_msg = "invalid Hermes bot arguments";
        return false;
    }

    User existing;
    std::string lookup_error;
    if (GetUserById(bot_user_id, &existing, &lookup_error)) {
        if (existing.account != account) {
            if (err_msg) {
                *err_msg = "Hermes bot user id is already owned by account " +
                           existing.account;
            }
            return false;
        }
        if (existing.name != name) {
            LOG_INFO << "Hermes bot exists with name=" << existing.name
                     << ", keep the database value";
        }
        return true;
    }
    if (!lookup_error.empty() && lookup_error != "user not found") {
        if (err_msg) *err_msg = "lookup Hermes bot failed: " + lookup_error;
        return false;
    }

    User account_owner;
    std::string account_error;
    if (GetUserByAccount(account, &account_owner, &account_error)) {
        if (err_msg) {
            *err_msg = "Hermes bot account is already owned by user_id=" +
                       std::to_string(account_owner.id);
        }
        return false;
    }
    if (!account_error.empty() && account_error != "user not found") {
        if (err_msg) *err_msg = "lookup Hermes bot account failed: " + account_error;
        return false;
    }

    auto guard = pool_->Acquire();
    MYSQL* conn = guard.get();
    if (!conn) {
        if (err_msg) *err_msg = "no mysql connection";
        return false;
    }

    // 机器人没有普通登录密码；该值只用于满足现有 user 表的非空约束。
    const std::string reserved_hash = "!hermes_bot_reserved!";
    const char* sql =
        "INSERT INTO `user`(id, account, name, password_hash, status) "
        "VALUES(?, ?, ?, ?, 1)";
    MYSQL_STMT* stmt = mysql_stmt_init(conn);
    if (!stmt) {
        if (err_msg) *err_msg = "mysql_stmt_init failed";
        return false;
    }
    if (mysql_stmt_prepare(stmt, sql, std::strlen(sql)) != 0) {
        if (err_msg) *err_msg = mysql_stmt_error(stmt);
        mysql_stmt_close(stmt);
        return false;
    }

    MYSQL_BIND bind[4];
    std::memset(bind, 0, sizeof(bind));
    long long id_value = bot_user_id;
    unsigned long account_len = static_cast<unsigned long>(account.size());
    unsigned long name_len = static_cast<unsigned long>(name.size());
    unsigned long hash_len =
        static_cast<unsigned long>(reserved_hash.size());
    bind[0].buffer_type = MYSQL_TYPE_LONGLONG;
    bind[0].buffer = &id_value;
    bind[0].is_unsigned = 0;
    bind[1].buffer_type = MYSQL_TYPE_STRING;
    bind[1].buffer = const_cast<char*>(account.data());
    bind[1].buffer_length = account_len;
    bind[1].length = &account_len;
    bind[2].buffer_type = MYSQL_TYPE_STRING;
    bind[2].buffer = const_cast<char*>(name.data());
    bind[2].buffer_length = name_len;
    bind[2].length = &name_len;
    bind[3].buffer_type = MYSQL_TYPE_STRING;
    bind[3].buffer = const_cast<char*>(reserved_hash.data());
    bind[3].buffer_length = hash_len;
    bind[3].length = &hash_len;

    if (mysql_stmt_bind_param(stmt, bind) != 0 ||
        mysql_stmt_execute(stmt) != 0) {
        const unsigned int mysql_error_code = mysql_stmt_errno(stmt);
        const std::string mysql_error = mysql_stmt_error(stmt);
        mysql_stmt_close(stmt);
        if (mysql_error_code == 1062) {
            // 多个 Logic 节点同时启动时允许其中一个完成创建，其他节点
            // 重新读取并确认 id/account 一致后也视为成功。
            User by_id;
            std::string by_id_error;
            if (GetUserById(bot_user_id, &by_id, &by_id_error) &&
                by_id.account == account) {
                return true;
            }
            if (err_msg) {
                *err_msg = "Hermes bot insert conflicted with another account: " +
                           mysql_error;
            }
            return false;
        }
        if (err_msg) *err_msg = mysql_error;
        return false;
    }
    mysql_stmt_close(stmt);
    LOG_INFO << "Created Hermes bot user_id=" << std::to_string(bot_user_id)
             << ", account=" << account;
    return true;
}

bool UserDao::CreateUser(const std::string& account, const std::string& name,
                         const std::string& password, int64_t* user_id,
                         std::string* err_msg) {
    if (!pool_) {
        if (err_msg) *err_msg = "mysql pool not initialized";
        return false;
    }
    auto guard = pool_->Acquire();
    MYSQL* conn = guard.get();
    if (!conn) {
        if (err_msg) *err_msg = "no mysql connection";
        return false;
    }

    const char* sql =
        "INSERT INTO `user`(account, name, password_hash, status) "
        "VALUES(?, ?, ?, 1)";
    MYSQL_STMT* stmt = mysql_stmt_init(conn);
    if (!stmt) {
        if (err_msg) *err_msg = "mysql_stmt_init failed";
        return false;
    }
    if (mysql_stmt_prepare(stmt, sql, std::strlen(sql)) != 0) {
        if (err_msg) *err_msg = mysql_stmt_error(stmt);
        mysql_stmt_close(stmt);
        return false;
    }

    MYSQL_BIND bind[3];
    std::memset(bind, 0, sizeof(bind));
    unsigned long account_len = static_cast<unsigned long>(account.size());
    unsigned long name_len = static_cast<unsigned long>(name.size());
    unsigned long password_len = static_cast<unsigned long>(password.size());
    bind[0].buffer_type = MYSQL_TYPE_STRING;
    bind[0].buffer = const_cast<char*>(account.data());
    bind[0].buffer_length = account_len;
    bind[0].length = &account_len;
    bind[1].buffer_type = MYSQL_TYPE_STRING;
    bind[1].buffer = const_cast<char*>(name.data());
    bind[1].buffer_length = name_len;
    bind[1].length = &name_len;
    bind[2].buffer_type = MYSQL_TYPE_STRING;
    bind[2].buffer = const_cast<char*>(password.data());
    bind[2].buffer_length = password_len;
    bind[2].length = &password_len;

    if (mysql_stmt_bind_param(stmt, bind) != 0 ||
        mysql_stmt_execute(stmt) != 0) {
        if (err_msg) *err_msg = mysql_stmt_error(stmt);
        mysql_stmt_close(stmt);
        return false;
    }
    if (user_id) *user_id = static_cast<int64_t>(mysql_insert_id(conn));
    mysql_stmt_close(stmt);
    return true;
}

bool UserDao::GetUserByAccount(const std::string& account, User* user,
                               std::string* err_msg) {
    if (!pool_ || !user) {
        if (err_msg) *err_msg = "invalid user lookup arguments";
        return false;
    }
    auto guard = pool_->Acquire();
    MYSQL* conn = guard.get();
    if (!conn) {
        if (err_msg) *err_msg = "no mysql connection";
        return false;
    }
    const std::string sql = UserSelectSql("WHERE account = ? LIMIT 1");
    MYSQL_STMT* stmt = mysql_stmt_init(conn);
    if (!stmt) {
        if (err_msg) *err_msg = "mysql_stmt_init failed";
        return false;
    }
    if (mysql_stmt_prepare(stmt, sql.c_str(), sql.size()) != 0) {
        if (err_msg) *err_msg = mysql_stmt_error(stmt);
        mysql_stmt_close(stmt);
        return false;
    }
    MYSQL_BIND bind[1];
    std::memset(bind, 0, sizeof(bind));
    unsigned long account_len = static_cast<unsigned long>(account.size());
    bind[0].buffer_type = MYSQL_TYPE_STRING;
    bind[0].buffer = const_cast<char*>(account.data());
    bind[0].buffer_length = account_len;
    bind[0].length = &account_len;
    if (mysql_stmt_bind_param(stmt, bind) != 0 ||
        mysql_stmt_execute(stmt) != 0 || !FetchOneUser(stmt, user, err_msg)) {
        if (err_msg && err_msg->empty()) *err_msg = mysql_stmt_error(stmt);
        mysql_stmt_close(stmt);
        return false;
    }
    mysql_stmt_close(stmt);
    return true;
}

bool UserDao::GetUserById(int64_t user_id, User* user, std::string* err_msg) {
    if (!pool_ || !user || user_id <= 0) {
        if (err_msg) *err_msg = "invalid user lookup arguments";
        return false;
    }
    auto guard = pool_->Acquire();
    MYSQL* conn = guard.get();
    if (!conn) {
        if (err_msg) *err_msg = "no mysql connection";
        return false;
    }
    const std::string sql = UserSelectSql("WHERE id = ? LIMIT 1");
    MYSQL_STMT* stmt = mysql_stmt_init(conn);
    if (!stmt) {
        if (err_msg) *err_msg = "mysql_stmt_init failed";
        return false;
    }
    if (mysql_stmt_prepare(stmt, sql.c_str(), sql.size()) != 0) {
        if (err_msg) *err_msg = mysql_stmt_error(stmt);
        mysql_stmt_close(stmt);
        return false;
    }
    MYSQL_BIND bind[1];
    long long id_param = user_id;
    BindUserIdParam(bind, &id_param);
    if (mysql_stmt_bind_param(stmt, bind) != 0 ||
        mysql_stmt_execute(stmt) != 0 || !FetchOneUser(stmt, user, err_msg)) {
        if (err_msg && err_msg->empty()) *err_msg = mysql_stmt_error(stmt);
        mysql_stmt_close(stmt);
        return false;
    }
    mysql_stmt_close(stmt);
    return true;
}

bool UserDao::UpdatePasswordHash(int64_t user_id,
                                 const std::string& password_hash,
                                 std::string* err_msg) {
    if (!pool_ || user_id <= 0 || password_hash.empty()) {
        if (err_msg) *err_msg = "invalid password hash update arguments";
        return false;
    }
    auto guard = pool_->Acquire();
    MYSQL* conn = guard.get();
    if (!conn) {
        if (err_msg) *err_msg = "no mysql connection";
        return false;
    }
    const char* sql = "UPDATE `user` SET password_hash=? WHERE id=?";
    MYSQL_STMT* stmt = mysql_stmt_init(conn);
    if (!stmt) {
        if (err_msg) *err_msg = "mysql_stmt_init failed";
        return false;
    }
    if (mysql_stmt_prepare(stmt, sql, std::strlen(sql)) != 0) {
        if (err_msg) *err_msg = mysql_stmt_error(stmt);
        mysql_stmt_close(stmt);
        return false;
    }
    MYSQL_BIND bind[2];
    std::memset(bind, 0, sizeof(bind));
    unsigned long hash_len = static_cast<unsigned long>(password_hash.size());
    bind[0].buffer_type = MYSQL_TYPE_STRING;
    bind[0].buffer = const_cast<char*>(password_hash.data());
    bind[0].buffer_length = hash_len;
    bind[0].length = &hash_len;
    long long id_param = user_id;
    BindUserIdParam(&bind[1], &id_param);
    if (mysql_stmt_bind_param(stmt, bind) != 0 ||
        mysql_stmt_execute(stmt) != 0) {
        if (err_msg) *err_msg = mysql_stmt_error(stmt);
        mysql_stmt_close(stmt);
        return false;
    }
    mysql_stmt_close(stmt);
    return true;
}

bool UserDao::ListUsers(int offset, int limit, int status,
                        std::vector<User>* users, int* total,
                        std::string* err_msg) {
    if (!pool_ || !users || !total || offset < 0 || limit <= 0 || limit > 100 ||
        status < 0 || status > kUserStatusDeleted) {
        if (err_msg) *err_msg = "invalid user list arguments";
        return false;
    }
    users->clear();
    *total = 0;
    auto guard = pool_->Acquire();
    MYSQL* conn = guard.get();
    if (!conn) {
        if (err_msg) *err_msg = "no mysql connection";
        return false;
    }

    const char* count_sql = status > 0
                                ? "SELECT COUNT(*) FROM `user` WHERE status=?"
                                : "SELECT COUNT(*) FROM `user`";
    MYSQL_STMT* count_stmt = mysql_stmt_init(conn);
    if (!count_stmt || mysql_stmt_prepare(count_stmt, count_sql,
                                          std::strlen(count_sql)) != 0) {
        if (err_msg) *err_msg = count_stmt ? mysql_stmt_error(count_stmt)
                                            : "mysql_stmt_init failed";
        if (count_stmt) mysql_stmt_close(count_stmt);
        return false;
    }
    int status_param = status;
    if (status > 0) {
        MYSQL_BIND param[1];
        std::memset(param, 0, sizeof(param));
        param[0].buffer_type = MYSQL_TYPE_LONG;
        param[0].buffer = &status_param;
        if (mysql_stmt_bind_param(count_stmt, param) != 0) {
            if (err_msg) *err_msg = mysql_stmt_error(count_stmt);
            mysql_stmt_close(count_stmt);
            return false;
        }
    }
    if (mysql_stmt_execute(count_stmt) != 0 ||
        mysql_stmt_store_result(count_stmt) != 0) {
        if (err_msg) *err_msg = mysql_stmt_error(count_stmt);
        mysql_stmt_close(count_stmt);
        return false;
    }
    long long total_buf = 0;
    MYSQL_BIND count_result[1];
    std::memset(count_result, 0, sizeof(count_result));
    count_result[0].buffer_type = MYSQL_TYPE_LONGLONG;
    count_result[0].buffer = &total_buf;
    if (mysql_stmt_bind_result(count_stmt, count_result) != 0 ||
        mysql_stmt_fetch(count_stmt) != 0) {
        if (err_msg) *err_msg = mysql_stmt_error(count_stmt);
        mysql_stmt_close(count_stmt);
        return false;
    }
    *total = static_cast<int>(total_buf);
    mysql_stmt_close(count_stmt);

    std::string data_sql = UserSelectSql(status > 0
                                             ? "WHERE status=? "
                                               "ORDER BY id DESC LIMIT ? OFFSET ?"
                                             : "ORDER BY id DESC LIMIT ? OFFSET ?");
    MYSQL_STMT* data_stmt = mysql_stmt_init(conn);
    if (!data_stmt || mysql_stmt_prepare(data_stmt, data_sql.c_str(),
                                         data_sql.size()) != 0) {
        if (err_msg) *err_msg = data_stmt ? mysql_stmt_error(data_stmt)
                                           : "mysql_stmt_init failed";
        if (data_stmt) mysql_stmt_close(data_stmt);
        return false;
    }
    int limit_param = limit;
    int offset_param = offset;
    MYSQL_BIND params[3];
    std::memset(params, 0, sizeof(params));
    int param_count = 0;
    if (status > 0) {
        params[param_count].buffer_type = MYSQL_TYPE_LONG;
        params[param_count].buffer = &status_param;
        ++param_count;
    }
    params[param_count].buffer_type = MYSQL_TYPE_LONG;
    params[param_count].buffer = &limit_param;
    ++param_count;
    params[param_count].buffer_type = MYSQL_TYPE_LONG;
    params[param_count].buffer = &offset_param;
    ++param_count;
    if (mysql_stmt_bind_param(data_stmt, params) != 0 ||
        mysql_stmt_execute(data_stmt) != 0 ||
        mysql_stmt_store_result(data_stmt) != 0) {
        if (err_msg) *err_msg = mysql_stmt_error(data_stmt);
        mysql_stmt_close(data_stmt);
        return false;
    }
    MYSQL_BIND result[7];
    UserResultBuffers buffers;
    BindUserResult(result, &buffers);
    if (mysql_stmt_bind_result(data_stmt, result) != 0) {
        if (err_msg) *err_msg = mysql_stmt_error(data_stmt);
        mysql_stmt_close(data_stmt);
        return false;
    }
    while (true) {
        const int fetch_ret = mysql_stmt_fetch(data_stmt);
        if (fetch_ret == MYSQL_NO_DATA) break;
        if (fetch_ret != 0 && fetch_ret != MYSQL_DATA_TRUNCATED) {
            if (err_msg) *err_msg = mysql_stmt_error(data_stmt);
            mysql_stmt_close(data_stmt);
            return false;
        }
        User user;
        AssignUser(buffers, &user);
        users->push_back(std::move(user));
    }
    mysql_stmt_close(data_stmt);
    return true;
}

bool UserDao::UpdateStatus(int64_t user_id, int status,
                           std::string* err_msg) {
    if (!pool_ || user_id <= 0 || status < kUserStatusActive ||
        status > kUserStatusDeleted) {
        if (err_msg) *err_msg = "invalid user status arguments";
        return false;
    }
    auto guard = pool_->Acquire();
    MYSQL* conn = guard.get();
    if (!conn) {
        if (err_msg) *err_msg = "no mysql connection";
        return false;
    }
    const char* sql = status == kUserStatusDeleted
                          ? "UPDATE `user` SET status=?, deleted_at="
                            "CURRENT_TIMESTAMP(3) WHERE id=?"
                          : "UPDATE `user` SET status=?, deleted_at=NULL "
                            "WHERE id=?";
    MYSQL_STMT* stmt = mysql_stmt_init(conn);
    if (!stmt) {
        if (err_msg) *err_msg = "mysql_stmt_init failed";
        return false;
    }
    if (mysql_stmt_prepare(stmt, sql, std::strlen(sql)) != 0) {
        if (err_msg) *err_msg = mysql_stmt_error(stmt);
        mysql_stmt_close(stmt);
        return false;
    }
    MYSQL_BIND bind[2];
    std::memset(bind, 0, sizeof(bind));
    bind[0].buffer_type = MYSQL_TYPE_LONG;
    bind[0].buffer = &status;
    long long id_param = user_id;
    BindUserIdParam(&bind[1], &id_param);
    if (mysql_stmt_bind_param(stmt, bind) != 0 ||
        mysql_stmt_execute(stmt) != 0) {
        if (err_msg) *err_msg = mysql_stmt_error(stmt);
        mysql_stmt_close(stmt);
        return false;
    }
    mysql_stmt_close(stmt);
    return true;
}

bool UserDao::UpdateName(int64_t user_id, const std::string& name,
                         std::string* err_msg) {
    if (!pool_ || user_id <= 0 || name.empty() || name.size() > 64) {
        if (err_msg) *err_msg = "invalid user name arguments";
        return false;
    }
    auto guard = pool_->Acquire();
    MYSQL* conn = guard.get();
    if (!conn) {
        if (err_msg) *err_msg = "no mysql connection";
        return false;
    }
    const char* sql = "UPDATE `user` SET name=? WHERE id=?";
    MYSQL_STMT* stmt = mysql_stmt_init(conn);
    if (!stmt) {
        if (err_msg) *err_msg = "mysql_stmt_init failed";
        return false;
    }
    if (mysql_stmt_prepare(stmt, sql, std::strlen(sql)) != 0) {
        if (err_msg) *err_msg = mysql_stmt_error(stmt);
        mysql_stmt_close(stmt);
        return false;
    }
    MYSQL_BIND bind[2];
    std::memset(bind, 0, sizeof(bind));
    unsigned long name_len = static_cast<unsigned long>(name.size());
    bind[0].buffer_type = MYSQL_TYPE_STRING;
    bind[0].buffer = const_cast<char*>(name.data());
    bind[0].buffer_length = name_len;
    bind[0].length = &name_len;
    long long id_param = user_id;
    BindUserIdParam(&bind[1], &id_param);
    if (mysql_stmt_bind_param(stmt, bind) != 0 ||
        mysql_stmt_execute(stmt) != 0) {
        if (err_msg) *err_msg = mysql_stmt_error(stmt);
        mysql_stmt_close(stmt);
        return false;
    }
    mysql_stmt_close(stmt);
    return true;
}

}  // namespace sparkpush
