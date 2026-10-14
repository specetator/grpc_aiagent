#include "attachment_dao.h"

#include <mysql/mysql.h>

#include <cstring>
#include <fstream>
#include <system_error>

#include <filesystem>

#include "logging.h"

namespace sparkpush {
namespace fs = std::filesystem;

namespace {

constexpr const char* kCreateSql =
    "CREATE TABLE IF NOT EXISTS `attachment` ("
    "  `id` VARCHAR(72) NOT NULL,"
    "  `owner_user_id` BIGINT NOT NULL,"
    "  `session_id` VARCHAR(128) NOT NULL DEFAULT '',"
    "  `display_name` VARCHAR(256) NOT NULL DEFAULT '',"
    "  `mime` VARCHAR(64) NOT NULL,"
    "  `bytes` INT NOT NULL,"
    "  `sha256` CHAR(64) NOT NULL,"
    "  `storage_key` VARCHAR(128) NOT NULL,"
    "  `status` VARCHAR(16) NOT NULL DEFAULT 'ready',"
    "  `created_at` DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),"
    "  PRIMARY KEY (`id`),"
    "  KEY `idx_attachment_owner` (`owner_user_id`),"
    "  KEY `idx_attachment_session` (`session_id`)"
    ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4";

bool BindString(MYSQL_BIND* bind, const std::string& value,
                unsigned long* length) {
    *length = static_cast<unsigned long>(value.size());
    bind->buffer_type = MYSQL_TYPE_STRING;
    bind->buffer = const_cast<char*>(value.data());
    bind->buffer_length = *length;
    bind->length = length;
    return true;
}

}  // namespace

bool AttachmentDao::EnsureSchema(std::string* err_msg) {
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
    if (mysql_query(conn, kCreateSql) != 0) {
        if (err_msg) *err_msg = mysql_error(conn);
        return false;
    }
    std::error_code ec;
    fs::create_directories(storage_dir_, ec);
    if (ec) {
        if (err_msg) *err_msg = "cannot create attachment dir: " + ec.message();
        return false;
    }
    return true;
}

// 全工程唯一把附件 id 拼成文件系统路径的地方。
// fs::path 的 / 运算符对 "../x"、"/etc/passwd" 这类输入**不做任何防护**
// （绝对路径甚至会直接取代左边），因此所有调用方必须先过
// IsValidAttachmentId。把拼接收拢到这一个函数，就是为了让这道校验
// 无处可逃——审查时只需要确认每个 FilePath 调用点前面都有校验。
std::string AttachmentDao::FilePath(const std::string& id) const {
    return (fs::path(storage_dir_) / id).string();
}

// 写临时文件 + 原子 rename。不直接写正式路径的原因：
// 同目录下的 rename 在 POSIX 上是原子的，并发读者要么看到旧文件、
// 要么看到完整的新文件，永远不会读到写一半的字节。
bool AttachmentDao::WriteFile(const std::string& id, const std::string& bytes,
                              std::string* err_msg) const {
    std::error_code ec;
    // 用 error_code 重载而不是抛异常版：目录已存在时 create_directories
    // 在某些实现上会抛，而这里已经存在是正常情况。
    fs::create_directories(storage_dir_, ec);
    if (ec) {
        if (err_msg) *err_msg = "cannot create attachment dir: " + ec.message();
        return false;
    }
    const std::string path = FilePath(id);
    const std::string tmp = path + ".tmp";
    {
        // 大括号作用域让 ofstream 在 rename 之前析构，确保数据已落盘。
        std::ofstream out(tmp, std::ios::binary | std::ios::trunc);
        if (!out) {
            if (err_msg) *err_msg = "cannot open attachment temp file";
            return false;
        }
        out.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
        // write 失败（磁盘满、超配额）不会抛异常，只能靠检查流状态发现。
        // 不检查就会 rename 一个零字节文件上去，并入库一条假记录。
        if (!out) {
            if (err_msg) *err_msg = "failed to write attachment bytes";
            return false;
        }
    }
    fs::rename(tmp, path, ec);
    if (ec) {
        // 失败时清掉 tmp：否则重试会不断积累 .tmp 垃圾文件。
        fs::remove(tmp, ec);
        if (err_msg) *err_msg = "cannot publish attachment file";
        return false;
    }
    return true;
}

// 存一张图片。四道门禁的顺序是有讲究的，便宜的检查先做：
// 尺寸 -> 魔数探测 -> 白名单 -> 生成 id 并落盘。
// 把尺寸放最前是因为它是 O(1)，而探测需要扫字节；
// 先拒掉 4 MiB 以上的上传能避开无谓的内存与 CPU 开销。
bool AttachmentDao::CreateImage(int64_t owner_user_id,
                                const std::string& session_id,
                                const std::string& display_name,
                                const std::string& bytes,
                                StoredAttachment* out, std::string* err_msg) {
    if (!out) return false;
    if (owner_user_id <= 0) {
        if (err_msg) *err_msg = "invalid owner";
        return false;
    }
    if (bytes.empty() || bytes.size() > kMaxImageBytes) {
        if (err_msg) *err_msg = "image must be 1 byte to 4 MiB";
        return false;
    }
    // 一律用魔数探测结果，**从不采信客户端声明的 Content-Type**。
    // 否则一个声明为 image/png 的 HTML/JS 文件会被存下来，
    // 再通过附件下载接口以 image/png 返回，在部分浏览器上就成了存储型 XSS 载体。
    const std::string mime = DetectImageMime(bytes);
    if (!IsAllowedImageMime(mime)) {
        if (err_msg) *err_msg = "only jpeg/png/gif/webp images are allowed";
        return false;
    }
    std::string id;
    if (!GenerateAttachmentId(&id, err_msg)) return false;
    // 先磁盘后数据库：若反过来，INSERT 成功后写盘失败就会留下一行
    // 指向不存在文件的记录，读取时报 404 很难定位；
    // 而当前顺序下失败只会留下一个没人引用的孤儿文件，仅浪费磁盘。
    if (!WriteFile(id, bytes, err_msg)) return false;

    StoredAttachment row;
    row.id = id;
    row.owner_user_id = owner_user_id;
    row.session_id = session_id;
    // display_name 为空时退化为 "<id>.img"，保证列非空且前端有东西可展示。
    row.display_name = display_name.empty() ? (id + ".img") : display_name;
    // 截到 256 而不是报错：display_name 是纯展示字段，超长不应该让整次上传失败。
    if (row.display_name.size() > 256) row.display_name.resize(256);
    row.mime = mime;
    row.bytes = bytes.size();
    // sha256 存的是内容的哈希而不是 id 的：相同图片会得到相同哈希，
    // 将来做内容级去重或校验文件是否被篡改时直接可用。
    row.sha256 = Sha256Hex(bytes);
    row.storage_key = id;
    row.status = "ready";

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
        "INSERT INTO attachment(id, owner_user_id, session_id, display_name, "
        "mime, bytes, sha256, storage_key, status) "
        "VALUES(?,?,?,?,?,?,?,?,?)";
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
    MYSQL_BIND bind[9];
    std::memset(bind, 0, sizeof(bind));
    unsigned long id_len = 0, session_len = 0, name_len = 0, mime_len = 0,
                  sha_len = 0, key_len = 0, status_len = 0;
    BindString(&bind[0], row.id, &id_len);
    long long owner = row.owner_user_id;
    bind[1].buffer_type = MYSQL_TYPE_LONGLONG;
    bind[1].buffer = &owner;
    BindString(&bind[2], row.session_id, &session_len);
    BindString(&bind[3], row.display_name, &name_len);
    BindString(&bind[4], row.mime, &mime_len);
    int bytes_buf = static_cast<int>(row.bytes);
    bind[5].buffer_type = MYSQL_TYPE_LONG;
    bind[5].buffer = &bytes_buf;
    BindString(&bind[6], row.sha256, &sha_len);
    BindString(&bind[7], row.storage_key, &key_len);
    BindString(&bind[8], row.status, &status_len);
    if (mysql_stmt_bind_param(stmt, bind) != 0 || mysql_stmt_execute(stmt) != 0) {
        if (err_msg) *err_msg = mysql_stmt_error(stmt);
        mysql_stmt_close(stmt);
        std::error_code ec;
        fs::remove(FilePath(id), ec);
        return false;
    }
    mysql_stmt_close(stmt);
    *out = std::move(row);
    return true;
}

bool AttachmentDao::GetById(const std::string& id, StoredAttachment* out,
                            std::string* err_msg) {
    if (!out || !IsValidAttachmentId(id)) {
        if (err_msg) *err_msg = "invalid attachment id";
        return false;
    }
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
        "SELECT id, owner_user_id, session_id, display_name, mime, bytes, "
        "sha256, storage_key, status FROM attachment WHERE id=? LIMIT 1";
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
    MYSQL_BIND in{};
    unsigned long id_len = static_cast<unsigned long>(id.size());
    in.buffer_type = MYSQL_TYPE_STRING;
    in.buffer = const_cast<char*>(id.data());
    in.buffer_length = id_len;
    in.length = &id_len;
    if (mysql_stmt_bind_param(stmt, &in) != 0) {
        if (err_msg) *err_msg = mysql_stmt_error(stmt);
        mysql_stmt_close(stmt);
        return false;
    }
    char id_buf[80]{};
    char session_buf[160]{};
    char name_buf[280]{};
    char mime_buf[80]{};
    char sha_buf[72]{};
    char key_buf[160]{};
    char status_buf[24]{};
    long long owner = 0;
    int bytes_buf = 0;
    unsigned long id_out = 0, session_out = 0, name_out = 0, mime_out = 0,
                  sha_out = 0, key_out = 0, status_out = 0;
    MYSQL_BIND result[9];
    std::memset(result, 0, sizeof(result));
    result[0].buffer_type = MYSQL_TYPE_STRING;
    result[0].buffer = id_buf;
    result[0].buffer_length = sizeof(id_buf);
    result[0].length = &id_out;
    result[1].buffer_type = MYSQL_TYPE_LONGLONG;
    result[1].buffer = &owner;
    result[2].buffer_type = MYSQL_TYPE_STRING;
    result[2].buffer = session_buf;
    result[2].buffer_length = sizeof(session_buf);
    result[2].length = &session_out;
    result[3].buffer_type = MYSQL_TYPE_STRING;
    result[3].buffer = name_buf;
    result[3].buffer_length = sizeof(name_buf);
    result[3].length = &name_out;
    result[4].buffer_type = MYSQL_TYPE_STRING;
    result[4].buffer = mime_buf;
    result[4].buffer_length = sizeof(mime_buf);
    result[4].length = &mime_out;
    result[5].buffer_type = MYSQL_TYPE_LONG;
    result[5].buffer = &bytes_buf;
    result[6].buffer_type = MYSQL_TYPE_STRING;
    result[6].buffer = sha_buf;
    result[6].buffer_length = sizeof(sha_buf);
    result[6].length = &sha_out;
    result[7].buffer_type = MYSQL_TYPE_STRING;
    result[7].buffer = key_buf;
    result[7].buffer_length = sizeof(key_buf);
    result[7].length = &key_out;
    result[8].buffer_type = MYSQL_TYPE_STRING;
    result[8].buffer = status_buf;
    result[8].buffer_length = sizeof(status_buf);
    result[8].length = &status_out;
    if (mysql_stmt_bind_result(stmt, result) != 0 ||
        mysql_stmt_execute(stmt) != 0 || mysql_stmt_store_result(stmt) != 0) {
        if (err_msg) *err_msg = mysql_stmt_error(stmt);
        mysql_stmt_close(stmt);
        return false;
    }
    const int fetch = mysql_stmt_fetch(stmt);
    mysql_stmt_close(stmt);
    if (fetch == MYSQL_NO_DATA) {
        if (err_msg) *err_msg = "attachment not found";
        return false;
    }
    if (fetch != 0) {
        if (err_msg) *err_msg = "failed to read attachment";
        return false;
    }
    out->id.assign(id_buf, id_out);
    out->owner_user_id = owner;
    out->session_id.assign(session_buf, session_out);
    out->display_name.assign(name_buf, name_out);
    out->mime.assign(mime_buf, mime_out);
    out->bytes = bytes_buf > 0 ? static_cast<std::size_t>(bytes_buf) : 0;
    out->sha256.assign(sha_buf, sha_out);
    out->storage_key.assign(key_buf, key_out);
    out->status.assign(status_buf, status_out);
    return true;
}

bool AttachmentDao::BindSession(const std::string& id,
                                const std::string& session_id,
                                std::string* err_msg) {
    if (!pool_ || !IsValidAttachmentId(id) || session_id.empty()) {
        if (err_msg) *err_msg = "invalid attachment bind";
        return false;
    }
    auto guard = pool_->Acquire();
    MYSQL* conn = guard.get();
    if (!conn) {
        if (err_msg) *err_msg = "no mysql connection";
        return false;
    }
    const char* sql =
        "UPDATE attachment SET session_id=? WHERE id=? AND session_id=''";
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
    unsigned long session_len = 0, id_len = 0;
    BindString(&bind[0], session_id, &session_len);
    BindString(&bind[1], id, &id_len);
    if (mysql_stmt_bind_param(stmt, bind) != 0 || mysql_stmt_execute(stmt) != 0) {
        if (err_msg) *err_msg = mysql_stmt_error(stmt);
        mysql_stmt_close(stmt);
        return false;
    }
    mysql_stmt_close(stmt);
    return true;
}

// 从磁盘读回附件字节。
bool AttachmentDao::ReadBytes(const StoredAttachment& attachment,
                              std::string* bytes, std::string* err_msg) const {
    if (!bytes) return false;
    // storage_key 为空时回退到 id：早期数据只有 id，两个字段值本来就相同。
    const std::string key = attachment.storage_key.empty() ? attachment.id
                                                           : attachment.storage_key;
    // 重复校验（调用方已经校过 id）是有意的纵深防御：本方法接收的是
    // 一个完整的 StoredAttachment，无法假定它一定是从 GetById 出来的。
    if (!IsValidAttachmentId(key)) {
        if (err_msg) *err_msg = "invalid attachment storage key";
        return false;
    }
    std::ifstream in(FilePath(key), std::ios::binary);
    if (!in) {
        if (err_msg) *err_msg = "attachment file missing";
        return false;
    }
    in.seekg(0, std::ios::end);
    const auto size = in.tellg();
    // 不信任磁盘上的实际大小：文件可能在写入后被外部替换成巨文件，
    // 而下面的 assign 会一次性把整个文件读进内存。
    // size < 0 是 tellg 失败的返回值（例如目录而非文件），也要挡掉。
    if (size < 0 || static_cast<std::size_t>(size) > kMaxImageBytes) {
        if (err_msg) *err_msg = "attachment file is invalid";
        return false;
    }
    bytes->assign(static_cast<std::size_t>(size), '\0');
    in.seekg(0);
    in.read(bytes->data(), size);
    // 转成 bool 检查流状态：读不满（文件在 seekg 与 read 之间被截断）会置 failbit。
    return static_cast<bool>(in);
}

// 拼出 Pi Agent 需要的 images 负载，实际读文件与 base64 编码在
// common/image_attachment.cpp 的 LoadPiImagesFromDir 里。
// 放在 common/ 而不是这里，是为了让 hermes_bridge 与单测能直接复用
// 同一套探测/编码逻辑，避开两边行为不一致。
bool AttachmentDao::LoadPiImages(const std::vector<ImageRef>& refs,
                                 nlohmann::json* images,
                                 std::string* err_msg) const {
    nlohmann::json payload = nlohmann::json::array();
    for (const auto& ref : refs) {
        payload.push_back({{"attachment_id", ref.id}, {"mime", ref.mime}});
    }
    return LoadPiImagesFromDir(storage_dir_, payload, images, err_msg);
}

}  // namespace sparkpush
