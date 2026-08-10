#include "audit_log_dao.h"

#include <mysql/mysql.h>

#include <cstring>

namespace sparkpush {
namespace {

bool Prepare(MYSQL* conn, const char* sql, MYSQL_STMT** stmt,
             std::string* err_msg) {
    *stmt = mysql_stmt_init(conn);
    if (!*stmt) {
        if (err_msg) *err_msg = "mysql_stmt_init failed";
        return false;
    }
    if (mysql_stmt_prepare(*stmt, sql, std::strlen(sql)) != 0) {
        if (err_msg) *err_msg = mysql_stmt_error(*stmt);
        mysql_stmt_close(*stmt);
        *stmt = nullptr;
        return false;
    }
    return true;
}

void BindLongLong(MYSQL_BIND* bind, long long* value) {
    std::memset(bind, 0, sizeof(MYSQL_BIND));
    bind->buffer_type = MYSQL_TYPE_LONGLONG;
    bind->buffer = value;
    bind->is_unsigned = 0;
}

}  // namespace

bool AuditLogDao::EnsureSchema(std::string* err_msg) {
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
        "CREATE TABLE IF NOT EXISTS `audit_log` ("
        "`id` BIGINT NOT NULL AUTO_INCREMENT,"
        "`actor_user_id` BIGINT NOT NULL DEFAULT 0,"
        "`target_user_id` BIGINT NOT NULL DEFAULT 0,"
        "`action` VARCHAR(64) NOT NULL,"
        "`reason` VARCHAR(512) NOT NULL DEFAULT '',"
        "`metadata_json` LONGTEXT NOT NULL,"
        "`created_at` DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),"
        "PRIMARY KEY (`id`),"
        "KEY `idx_audit_target_created` (`target_user_id`, `created_at`),"
        "KEY `idx_audit_action_created` (`action`, `created_at`)"
        ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4";
    if (mysql_query(conn, sql) != 0) {
        if (err_msg) *err_msg = mysql_error(conn);
        return false;
    }
    return true;
}

bool AuditLogDao::Append(int64_t actor_user_id, int64_t target_user_id,
                         const std::string& action,
                         const std::string& reason,
                         const std::string& metadata_json,
                         std::string* err_msg) {
    if (!pool_ || action.empty() || action.size() > 64 || reason.size() > 512 ||
        metadata_json.size() > 1024 * 1024) {
        if (err_msg) *err_msg = "invalid audit log arguments";
        return false;
    }
    auto guard = pool_->Acquire();
    MYSQL* conn = guard.get();
    if (!conn) {
        if (err_msg) *err_msg = "no mysql connection";
        return false;
    }
    const char* sql =
        "INSERT INTO `audit_log`(actor_user_id, target_user_id, action, "
        "reason, metadata_json) VALUES(?, ?, ?, ?, ?)";
    MYSQL_STMT* stmt = nullptr;
    if (!Prepare(conn, sql, &stmt, err_msg)) return false;

    MYSQL_BIND bind[5];
    std::memset(bind, 0, sizeof(bind));
    long long actor = actor_user_id;
    long long target = target_user_id;
    BindLongLong(&bind[0], &actor);
    BindLongLong(&bind[1], &target);

    const std::string* values[] = {&action, &reason, &metadata_json};
    unsigned long lengths[3] = {
        static_cast<unsigned long>(action.size()),
        static_cast<unsigned long>(reason.size()),
        static_cast<unsigned long>(metadata_json.size()),
    };
    for (int i = 0; i < 3; ++i) {
        const int index = i + 2;
        bind[index].buffer_type = MYSQL_TYPE_STRING;
        bind[index].buffer = const_cast<char*>(values[i]->data());
        bind[index].buffer_length = lengths[i];
        bind[index].length = &lengths[i];
    }
    const bool ok = mysql_stmt_bind_param(stmt, bind) == 0 &&
                    mysql_stmt_execute(stmt) == 0;
    if (!ok && err_msg) *err_msg = mysql_stmt_error(stmt);
    mysql_stmt_close(stmt);
    return ok;
}

bool AuditLogDao::List(int offset, int limit, int64_t target_user_id,
                       std::vector<AuditLogRecord>* records, int* total,
                       std::string* err_msg) {
    if (!pool_ || !records || !total || offset < 0 || limit <= 0 || limit > 100 ||
        target_user_id < 0) {
        if (err_msg) *err_msg = "invalid audit list arguments";
        return false;
    }
    records->clear();
    *total = 0;
    auto guard = pool_->Acquire();
    MYSQL* conn = guard.get();
    if (!conn) {
        if (err_msg) *err_msg = "no mysql connection";
        return false;
    }

    const bool filtered = target_user_id > 0;
    const char* count_sql = filtered
                                ? "SELECT COUNT(*) FROM `audit_log` "
                                  "WHERE target_user_id=?"
                                : "SELECT COUNT(*) FROM `audit_log`";
    MYSQL_STMT* count_stmt = nullptr;
    if (!Prepare(conn, count_sql, &count_stmt, err_msg)) return false;
    long long target = target_user_id;
    if (filtered) {
        MYSQL_BIND param[1];
        BindLongLong(&param[0], &target);
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
    long long count = 0;
    MYSQL_BIND count_result[1];
    BindLongLong(&count_result[0], &count);
    if (mysql_stmt_bind_result(count_stmt, count_result) != 0 ||
        mysql_stmt_fetch(count_stmt) != 0) {
        if (err_msg) *err_msg = mysql_stmt_error(count_stmt);
        mysql_stmt_close(count_stmt);
        return false;
    }
    *total = static_cast<int>(count);
    mysql_stmt_close(count_stmt);

    const char* data_sql =
        "SELECT id, actor_user_id, target_user_id, action, reason, "
        "metadata_json, DATE_FORMAT(created_at, '%Y-%m-%dT%H:%i:%sZ') "
        "FROM `audit_log` "
        "WHERE target_user_id=? ORDER BY id DESC LIMIT ? OFFSET ?";
    const char* all_data_sql =
        "SELECT id, actor_user_id, target_user_id, action, reason, "
        "metadata_json, DATE_FORMAT(created_at, '%Y-%m-%dT%H:%i:%sZ') "
        "FROM `audit_log` ORDER BY id DESC LIMIT ? OFFSET ?";
    MYSQL_STMT* data_stmt = nullptr;
    if (!Prepare(conn, filtered ? data_sql : all_data_sql, &data_stmt,
                 err_msg)) {
        return false;
    }

    int limit_param = limit;
    int offset_param = offset;
    MYSQL_BIND params[3];
    std::memset(params, 0, sizeof(params));
    int param_count = 0;
    if (filtered) {
        BindLongLong(&params[param_count++], &target);
    }
    params[param_count].buffer_type = MYSQL_TYPE_LONG;
    params[param_count++].buffer = &limit_param;
    params[param_count].buffer_type = MYSQL_TYPE_LONG;
    params[param_count++].buffer = &offset_param;
    if (mysql_stmt_bind_param(data_stmt, params) != 0 ||
        mysql_stmt_execute(data_stmt) != 0 ||
        mysql_stmt_store_result(data_stmt) != 0) {
        if (err_msg) *err_msg = mysql_stmt_error(data_stmt);
        mysql_stmt_close(data_stmt);
        return false;
    }

    long long id = 0;
    long long actor = 0;
    long long target_result = 0;
    char action[96]{};
    char reason[768]{};
    char metadata[1024 * 1024]{};
    char created_at[64]{};
    unsigned long action_out_len = 0;
    unsigned long reason_out_len = 0;
    unsigned long metadata_out_len = 0;
    unsigned long created_at_out_len = 0;
    MYSQL_BIND result[7];
    std::memset(result, 0, sizeof(result));
    result[0].buffer_type = MYSQL_TYPE_LONGLONG;
    result[0].buffer = &id;
    result[1].buffer_type = MYSQL_TYPE_LONGLONG;
    result[1].buffer = &actor;
    result[2].buffer_type = MYSQL_TYPE_LONGLONG;
    result[2].buffer = &target_result;
    result[3].buffer_type = MYSQL_TYPE_STRING;
    result[3].buffer = action;
    result[3].buffer_length = sizeof(action);
    result[3].length = &action_out_len;
    result[4].buffer_type = MYSQL_TYPE_STRING;
    result[4].buffer = reason;
    result[4].buffer_length = sizeof(reason);
    result[4].length = &reason_out_len;
    result[5].buffer_type = MYSQL_TYPE_STRING;
    result[5].buffer = metadata;
    result[5].buffer_length = sizeof(metadata);
    result[5].length = &metadata_out_len;
    result[6].buffer_type = MYSQL_TYPE_STRING;
    result[6].buffer = created_at;
    result[6].buffer_length = sizeof(created_at);
    result[6].length = &created_at_out_len;
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
        AuditLogRecord record;
        record.id = id;
        record.actor_user_id = actor;
        record.target_user_id = target_result;
        record.action.assign(action, action_out_len);
        record.reason.assign(reason, reason_out_len);
        record.metadata_json.assign(metadata, metadata_out_len);
        record.created_at.assign(created_at, created_at_out_len);
        records->push_back(std::move(record));
    }
    mysql_stmt_close(data_stmt);
    return true;
}

}  // namespace sparkpush
