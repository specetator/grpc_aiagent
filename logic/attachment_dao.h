#pragma once

#include <string>
#include <vector>

#include "image_attachment.h"
#include "mysql_pool.h"

namespace sparkpush {

struct StoredAttachment {
    std::string id;
    int64_t owner_user_id{0};
    std::string session_id;
    std::string display_name;
    std::string mime;
    std::size_t bytes{0};
    std::string sha256;
    std::string storage_key;
    std::string status;
};

class AttachmentDao {
   public:
    AttachmentDao(MySqlConnectionPool* pool, std::string storage_dir)
        : pool_(pool), storage_dir_(std::move(storage_dir)) {}

    bool EnsureSchema(std::string* err_msg);
    bool CreateImage(int64_t owner_user_id, const std::string& session_id,
                     const std::string& display_name, const std::string& bytes,
                     StoredAttachment* out, std::string* err_msg);
    bool GetById(const std::string& id, StoredAttachment* out,
                 std::string* err_msg);
    bool BindSession(const std::string& id, const std::string& session_id,
                     std::string* err_msg);
    bool ReadBytes(const StoredAttachment& attachment, std::string* bytes,
                   std::string* err_msg) const;
    bool LoadPiImages(const std::vector<ImageRef>& refs,
                      nlohmann::json* images, std::string* err_msg) const;

    const std::string& storage_dir() const { return storage_dir_; }

   private:
    bool WriteFile(const std::string& id, const std::string& bytes,
                   std::string* err_msg) const;
    std::string FilePath(const std::string& id) const;

    MySqlConnectionPool* pool_{nullptr};
    std::string storage_dir_;
};

}  // namespace sparkpush
