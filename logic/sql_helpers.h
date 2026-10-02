#pragma once
#include <mysql/mysql.h>
#include <string>
#include <vector>

namespace sparkpush {
inline std::string SqlQuote(MYSQL* conn, const std::string& value) {
    std::string escaped(value.size() * 2 + 1, '\0');
    const auto len = mysql_real_escape_string(conn, escaped.data(), value.data(), value.size());
    escaped.resize(len);
    return "'" + escaped + "'";
}
inline bool SqlExec(MYSQL* conn, const std::string& sql, std::string* error) {
    if (!conn || mysql_real_query(conn, sql.data(), sql.size()) != 0) {
        if (error) *error = conn ? mysql_error(conn) : "no MySQL connection";
        return false;
    }
    return true;
}
inline bool SqlRows(MYSQL* conn, const std::string& sql,
                    std::vector<std::vector<std::string>>* rows, std::string* error) {
    rows->clear();
    if (!SqlExec(conn, sql, error)) return false;
    MYSQL_RES* result = mysql_store_result(conn);
    if (!result) { if (error) *error = mysql_error(conn); return false; }
    while (MYSQL_ROW row = mysql_fetch_row(result)) {
        const auto lengths = mysql_fetch_lengths(result);
        std::vector<std::string> values;
        for (unsigned i = 0; i < mysql_num_fields(result); ++i)
            values.emplace_back(row[i] ? std::string(row[i], lengths[i]) : std::string{});
        rows->push_back(std::move(values));
    }
    const bool ok = mysql_errno(conn) == 0;
    if (!ok && error) *error = mysql_error(conn);
    mysql_free_result(result);
    return ok;
}
inline bool ValidDeviceId(const std::string& id) {
    if (id.empty() || id.size() > 64) return false;
    for (unsigned char c : id)
        if (!((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
              (c >= '0' && c <= '9') || c == '-' || c == '_' || c == '.')) return false;
    return true;
}
} // namespace sparkpush
