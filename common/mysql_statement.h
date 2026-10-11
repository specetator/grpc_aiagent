#pragma once
#include <mysql/mysql.h>
#include <cstring>
#include <stdexcept>
#include <string>
#include <vector>
#include <memory>
#include <type_traits>
namespace sparkpush {
// Prepared parameters also cover arbitrary binary outbox payloads and NO_BACKSLASH_ESCAPES.
inline bool SqlExecute(MYSQL* connection, const char* sql,
                       const std::vector<std::string>& parameters,
                       std::vector<std::vector<std::string>>* rows, std::string* error) {
    MYSQL_STMT* stmt = mysql_stmt_init(connection);
    if (!stmt) { if (error) *error = "mysql_stmt_init failed"; return false; }
    struct Close { MYSQL_STMT* stmt; ~Close() { mysql_stmt_close(stmt); } } close{stmt};
    auto failed = [&] { if (error) *error = mysql_stmt_error(stmt); return false; };
    if (mysql_stmt_prepare(stmt, sql, std::strlen(sql))) return failed();
    std::vector<MYSQL_BIND> binds(parameters.size());
    std::vector<unsigned long> lengths(parameters.size());
    for (size_t i = 0; i < parameters.size(); ++i) {
        binds[i] = {};
        lengths[i] = parameters[i].size();
        binds[i].buffer_type = MYSQL_TYPE_BLOB;
        binds[i].buffer = const_cast<char*>(parameters[i].data());
        binds[i].buffer_length = lengths[i];
        binds[i].length = &lengths[i];
    }
    if (!binds.empty() && mysql_stmt_bind_param(stmt, binds.data())) return failed();
    if (mysql_stmt_execute(stmt)) return failed();
    if (!rows) return true;
    rows->clear();
    using MysqlBool = std::remove_pointer_t<decltype(MYSQL_BIND{}.is_null)>;
    MysqlBool update_lengths = 1;
    mysql_stmt_attr_set(stmt, STMT_ATTR_UPDATE_MAX_LENGTH, &update_lengths);
    if (mysql_stmt_store_result(stmt)) return failed();
    MYSQL_RES* metadata = mysql_stmt_result_metadata(stmt);
    if (!metadata) return failed();
    struct Free { MYSQL_RES* result; ~Free() { mysql_free_result(result); } } free{metadata};
    const unsigned count = mysql_num_fields(metadata);
    MYSQL_FIELD* fields = mysql_fetch_fields(metadata);
    std::vector<MYSQL_BIND> results(count);
    std::vector<std::vector<char>> buffers(count);
    lengths.resize(count);
    auto nulls = std::make_unique<MysqlBool[]>(count);
    auto errors = std::make_unique<MysqlBool[]>(count);
    for (unsigned i = 0; i < count; ++i) {
        if (fields[i].max_length > 16 * 1024 * 1024) {
            if (error) *error = "SQL result exceeds 16 MiB";
            return false;
        }
        buffers[i].resize(fields[i].max_length + 1);
        results[i] = {};
        results[i].buffer_type = MYSQL_TYPE_BLOB;
        results[i].buffer = buffers[i].data();
        results[i].buffer_length = buffers[i].size();
        results[i].length = &lengths[i];
        results[i].is_null = &nulls[i];
        results[i].error = &errors[i];
    }
    if (mysql_stmt_bind_result(stmt, results.data())) return failed();
    int status;
    while ((status = mysql_stmt_fetch(stmt)) == 0) {
        std::vector<std::string> row;
        for (unsigned i = 0; i < count; ++i)
            row.emplace_back(nulls[i] ? "" : std::string(buffers[i].data(), lengths[i]));
        rows->push_back(std::move(row));
    }
    return status == MYSQL_NO_DATA ? true : failed();
}
class SqlTransaction {
 public:
    explicit SqlTransaction(MYSQL* connection) : connection_(connection) {
        active_ = mysql_query(connection_, "START TRANSACTION") == 0;
    }
    ~SqlTransaction() { if (active_) mysql_query(connection_, "ROLLBACK"); }
    bool active() const { return active_; }
    bool Commit(std::string* error) {
        if (!active_ || mysql_query(connection_, "COMMIT")) {
            if (error) *error = mysql_error(connection_);
            return false;
        }
        active_ = false;
        return true;
    }
 private:
    MYSQL* connection_;
    bool active_{false};
};
}  // namespace sparkpush
