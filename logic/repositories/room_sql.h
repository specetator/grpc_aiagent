#pragma once
#include <mysql/mysql.h>
#include <memory>
#include <cstdint>
#include <stdexcept>
#include <string>
#include <vector>

namespace sparkpush::room_sql {
using Rows = std::vector<std::vector<std::string>>;
// All values (including numeric IDs) are bound, never interpolated. Result
// lengths come from MySQL, so quotes, NULs and utf8mb4 survive a round trip.
inline Rows Query(MYSQL* conn, const char* sql, const std::vector<std::string>& values = {}, uint64_t* affected = nullptr) {
    if (!conn) throw std::runtime_error("room database unavailable");
    std::unique_ptr<MYSQL_STMT, decltype(&mysql_stmt_close)> stmt(mysql_stmt_init(conn), mysql_stmt_close);
    auto fail = [&] { throw std::runtime_error("room database operation failed ("+std::to_string(stmt ? mysql_stmt_errno(stmt.get()) : mysql_errno(conn))+")"); };
    if (!stmt || mysql_stmt_prepare(stmt.get(), sql, std::char_traits<char>::length(sql))) fail();
    std::vector<MYSQL_BIND> params(values.size());
    std::vector<unsigned long> lengths(values.size());
    for (size_t i = 0; i < values.size(); ++i) {
        lengths[i] = values[i].size();
        params[i].buffer_type = MYSQL_TYPE_STRING;
        params[i].buffer = const_cast<char*>(values[i].data());
        params[i].buffer_length = lengths[i];
        params[i].length = &lengths[i];
    }
    if ((!params.empty() && mysql_stmt_bind_param(stmt.get(), params.data())) || mysql_stmt_execute(stmt.get())) fail();
    if (affected) *affected=mysql_stmt_affected_rows(stmt.get());
    Rows rows;
    std::unique_ptr<MYSQL_RES, decltype(&mysql_free_result)> meta(mysql_stmt_result_metadata(stmt.get()), mysql_free_result);
    if (!meta) { if (mysql_stmt_field_count(stmt.get())) fail(); return rows; }
    if (mysql_stmt_store_result(stmt.get())) fail();
    const size_t count = mysql_num_fields(meta.get());
    std::vector<MYSQL_BIND> fields(count);
    std::vector<unsigned long> sizes(count);
    for (size_t i = 0; i < count; ++i) {
        fields[i].buffer_type = MYSQL_TYPE_STRING;
        fields[i].length = &sizes[i];
    }
    if (mysql_stmt_bind_result(stmt.get(), fields.data())) fail();
    for (;;) {
        const int status = mysql_stmt_fetch(stmt.get());
        if (status == MYSQL_NO_DATA) break;
        if (status != 0 && status != MYSQL_DATA_TRUNCATED) fail();
        std::vector<std::string> row(count);
        for (size_t i = 0; i < count; ++i) {
            if (sizes[i] > 16 * 1024 * 1024) fail();
            row[i].resize(sizes[i]);
            MYSQL_BIND field{};
            field.buffer_type = MYSQL_TYPE_STRING;
            field.buffer = row[i].data();
            field.buffer_length = sizes[i];
            if (mysql_stmt_fetch_column(stmt.get(), &field, i, 0)) fail();
        }
        rows.push_back(std::move(row));
    }
    return rows;
}
class Transaction {
 public:
    explicit Transaction(MYSQL* conn) : conn_(conn) {
        if(!conn_ || mysql_query(conn_, "START TRANSACTION")) throw std::runtime_error("room transaction unavailable");
    }
    ~Transaction() { if (!committed_) mysql_rollback(conn_); }
    void Commit() { if (mysql_commit(conn_)) throw std::runtime_error("room commit failed"); committed_ = true; }
 private:
    MYSQL* conn_;
    bool committed_{false};
};
} // namespace sparkpush::room_sql
