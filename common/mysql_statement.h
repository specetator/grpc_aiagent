#pragma once
#include <mysql/mysql.h>
#include <cstring>
#include <stdexcept>
#include <string>
#include <vector>
#include <memory>
#include <type_traits>
namespace sparkpush {

// =====================================================================
// MySQL prepared statement 的统一执行封装。
//
// Prepared parameters also cover arbitrary binary outbox payloads and NO_BACKSLASH_ESCAPES.
// 这句话是本文件存在的核心理由：所有 SQL 都必须走参数绑定，绝不拼接。
//
// 两个具体收益：
// 1) 防注入：参数以二进制协议单独传输，不参与 SQL 解析，引号/分号
//    都不会被当成语法。而字符串拼接遇上 NO_BACKSLASH_ESCAPES 模式时，
//    常见的反斜杠转义手法会直接失效。
// 2) 二进制安全：outbox 存的是 protobuf 序列化后的字节串，里面
//    什么值都可能出现（包括 \0）。只有 BLOB 绑定能完整传递它。
//
// 返回约定：rows == nullptr 表示只要执行不要结果（INSERT/UPDATE）；
// 非空则回填全部行，每列都以 string 返回（调用方自己转数值）。
inline bool SqlExecute(MYSQL* connection, const char* sql,
                       const std::vector<std::string>& parameters,
                       std::vector<std::vector<std::string>>* rows, std::string* error) {
    MYSQL_STMT* stmt = mysql_stmt_init(connection);
    if (!stmt) { if (error) *error = "mysql_stmt_init failed"; return false; }
    // RAII 关闭句柄。本函数有十几处提前 return，靠手写 close 必漏。
    // 用局部 struct 而不是 unique_ptr：避开自定义 deleter 的样板代码。
    struct Close { MYSQL_STMT* stmt; ~Close() { mysql_stmt_close(stmt); } } close{stmt};
    // 把「取错误信息 + 返回 false」合成一个 lambda，减少重复且保证
    // 每个失败分支都回传了具体原因（排障时必需）。
    auto failed = [&] { if (error) *error = mysql_stmt_error(stmt); return false; };
    if (mysql_stmt_prepare(stmt, sql, std::strlen(sql))) return failed();
    // 参数绑定。lengths 必须独立于 binds 存在且生命周期覆盖 execute：
    // MYSQL_BIND::length 存的是**指针**，若把局部变量地址交给它，
    // execute 时读到的是已销毁的栈内存。
    std::vector<MYSQL_BIND> binds(parameters.size());
    std::vector<unsigned long> lengths(parameters.size());
    for (size_t i = 0; i < parameters.size(); ++i) {
        binds[i] = {};
        lengths[i] = parameters[i].size();
        // 统一用 BLOB 类型：不预设字符集，因此能完整传递任意字节串。
        // 用 MYSQL_TYPE_STRING 会在含 \0 的 protobuf 负载上被截断。
        binds[i].buffer_type = MYSQL_TYPE_BLOB;
        binds[i].buffer = const_cast<char*>(parameters[i].data());
        binds[i].buffer_length = lengths[i];
        binds[i].length = &lengths[i];
    }
    if (!binds.empty() && mysql_stmt_bind_param(stmt, binds.data())) return failed();
    if (mysql_stmt_execute(stmt)) return failed();
    if (!rows) return true;   // 只要执行不要结果
    rows->clear();
    // MYSQL_BIND::is_null 的真实类型在不同 MySQL/MariaDB 头文件下不一致
    // （my_bool / bool / char）。用 decltype 从结构体成员反推，避开写死后
    // 换驱动就编不过的问题。
    using MysqlBool = std::remove_pointer_t<decltype(MYSQL_BIND{}.is_null)>;
    MysqlBool update_lengths = 1;
    // 让 store_result 回填每列的实际最大长度，下面才能按需分配缓冲区。
    mysql_stmt_attr_set(stmt, STMT_ATTR_UPDATE_MAX_LENGTH, &update_lengths);
    // store_result 把全部结果集拉到客户端内存：后续 fetch 不再走网络，
    // 也允许在遍历中重用同一连接。代价是大结果集占内存，因此下面有 16 MiB 上限。
    if (mysql_stmt_store_result(stmt)) return failed();
    MYSQL_RES* metadata = mysql_stmt_result_metadata(stmt);
    if (!metadata) return failed();
    struct Free { MYSQL_RES* result; ~Free() { mysql_free_result(result); } } free{metadata};
    const unsigned count = mysql_num_fields(metadata);
    MYSQL_FIELD* fields = mysql_fetch_fields(metadata);
    std::vector<MYSQL_BIND> results(count);
    std::vector<std::vector<char>> buffers(count);
    lengths.resize(count);   // 复用上面的 lengths 存结果列长度
    auto nulls = std::make_unique<MysqlBool[]>(count);
    auto errors = std::make_unique<MysqlBool[]>(count);
    for (unsigned i = 0; i < count; ++i) {
        // 16 MiB 上限：防止一个损坏/恶意的超大列把进程内存吃光。
        // content_json 已是 MEDIUMTEXT（上限也是 16 MiB），因此这个
        // 阈值刚好能容纳合法最大值。
        if (fields[i].max_length > 16 * 1024 * 1024) {
            if (error) *error = "SQL result exceeds 16 MiB";
            return false;
        }
        // +1 是为了给可能的 C 字符串终止符留位（虽然下面按 length 取，
        // 并不依赖它，但避开某些驱动对 buffer_length 的边界假设）。
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
        // 按 lengths[i] 而不是 strlen 取数据：BLOB 列可能含 \0，
        // strlen 会提前截断。NULL 列统一映射为空串，让调用方
        // 不必处理 optional（代价是区分不了 NULL 与 ""）。
        for (unsigned i = 0; i < count; ++i)
            row.emplace_back(nulls[i] ? "" : std::string(buffers[i].data(), lengths[i]));
        rows->push_back(std::move(row));
    }
    // MYSQL_NO_DATA 是正常结束；其他非 0 值（如 DATA_TRUNCATED）才是错误。
    return status == MYSQL_NO_DATA ? true : failed();
}

// RAII 事务守卫：构造开事务，析构时若未 Commit 则自动 ROLLBACK。
//
// 价值在于异常安全：多步写库中间任何一步提前 return 或抛异常，
// 都不会留下「改了一半」的脏数据。手写 BEGIN/COMMIT 在多个 return
// 分支下极容易漏掉回滚。
//
// 使用前必须检查 active()：START TRANSACTION 失败时（连接已断）
// active_ 为 false，此时继续写会落到自动提交模式，每句立即生效，
// 原子性就没了。
class SqlTransaction {
 public:
    explicit SqlTransaction(MYSQL* connection) : connection_(connection) {
        active_ = mysql_query(connection_, "START TRANSACTION") == 0;
    }
    // 只在仍 active 时回滚：已 Commit 的事务再发 ROLLBACK 会报错，
    // 并可能意外撤销后续无关语句。
    ~SqlTransaction() { if (active_) mysql_query(connection_, "ROLLBACK"); }
    bool active() const { return active_; }
    // 提交成功后立即把 active_ 置 false，保证析构不会重复提交/回滚。
    bool Commit(std::string* error) {
        if (!active_ || mysql_query(connection_, "COMMIT")) {
            if (error) *error = mysql_error(connection_);
            return false;
        }
        active_ = false;
        return true;
    }
 private:
    MYSQL* connection_;   // 不拥有连接，生命周期由 MySqlConnectionPool 管
    bool active_{false};
};
}  // namespace sparkpush
