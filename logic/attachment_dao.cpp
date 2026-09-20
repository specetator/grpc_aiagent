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

std::string AttachmentDao::FilePath(const std::string& id) const {
    return (fs::path(storage_dir_) / id).string();
}

bool AttachmentDao::WriteFile(const std::string& id, const std::string& bytes,
                              std::string* err_msg) const {
    std::error_code ec;
    fs::create_directories(storage_dir_, ec);
    if (ec) {
        if (err_msg) *err_msg = "cannot create attachment dir: " + ec.message();
        return false;
    }
    const std::string path = FilePath(id);
    const std::string tmp = path + ".tmp";
    {
        std::ofstream out(tmp, std::ios::binary | std::ios::trunc);
        if (!out) {
            if (err_msg) *err_msg = "cannot open attachment temp file";
            return false;
        }
        out.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
        if (!out) {
            if (err_msg) *err_msg = "failed to write attachment bytes";
            return false;
        }
    }
    fs::rename(tmp, path, ec);
    if (ec) {
        fs::remove(tmp, ec);
        if (err_msg) *err_msg = "cannot publish attachment file";
        return false;
    }
    return true;
}

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
    const std::string mime = DetectImageMime(bytes);
    if (!IsAllowedImageMime(mime)) {
        if (err_msg) *err_msg = "only jpeg/png/gif/webp images are allowed";
        return false;
    }
    std::string id;
    if (!GenerateAttachmentId(&id, err_msg)) return false;
    if (!WriteFile(id, bytes, err_msg)) return false;

    StoredAttachment row;
    row.id = id;
    row.owner_user_id = owner_user_id;
    row.session_id = session_id;
    row.display_name = display_name.empty() ? (id + ".img") : display_name;
    if (row.display_name.size() > 256) row.display_name.resize(256);
    row.mime = mime;
    row.bytes = bytes.size();
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

bool AttachmentDao::ReadBytes(const StoredAttachment& attachment,
                              std::string* bytes, std::string* err_msg) const {
    if (!bytes) return false;
    const std::string key = attachment.storage_key.empty() ? attachment.id
                                                           : attachment.storage_key;
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
    if (size < 0 || static_cast<std::size_t>(size) > kMaxImageBytes) {
        if (err_msg) *err_msg = "attachment file is invalid";
        return false;
    }
    bytes->assign(static_cast<std::size_t>(size), '\0');
    in.seekg(0);
    in.read(bytes->data(), size);
    return static_cast<bool>(in);
}

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
