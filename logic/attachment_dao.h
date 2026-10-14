#pragma once

#include <string>
#include <vector>

#include "image_attachment.h"
#include "model.h"
#include "mysql_pool.h"

namespace sparkpush {

// 图片附件的存储与元信息 DAO。
//
// 与其他 DAO 不同的是它同时管两个存储：MySQL（元信息）与本地磁盘（字节）。
// 两者不在一个事务里，因此必须想清楚失败时哪一边可能遗留脏数据。
//
// 安全边界：本类是**全工程唯一**把附件 id 拼成文件系统路径的地方
// （见私有方法 FilePath），因此 IsValidAttachmentId 的正则校验就是
// 路径穿越（../../etc/passwd）的唯一防线。新增任何读文件的方法时
// 必须记得先过这道校验。
class AttachmentDao {
   public:
    // storage_dir 由配置传入，构造后不再变。不在这里做校验，
    // 因为目录允许启动时不存在（EnsureSchema 会建）。
    AttachmentDao(MySqlConnectionPool* pool, std::string storage_dir)
        : pool_(pool), storage_dir_(std::move(storage_dir)) {}

    // 启动时建 attachment 表并创建 storage_dir。
    // 失败就拒绝启动：附件目录不可写时继续跑，只会在第一次上传时
    // 报一个与真实原因无关的错误。
    bool EnsureSchema(std::string* err_msg);

    // 存一张图片，四道门禁按此顺序，不能调换：
    //   1. 尺寸：1 字节 ~ 4 MiB（kMaxImageBytes）
    //   2. MIME：用魔数探测（DetectImageMime）而不是信客户端声明，
    //      再过白名单（jpeg/png/gif/webp）
    //   3. 先写磁盘，再写 MySQL：反过来的话，建表失败会留下一行
    //      指向不存在文件的记录，而孤儿文件只是浪费磁盘
    //   4. sha256 入库，供后续去重与完整性校验
    bool CreateImage(int64_t owner_user_id, const std::string& session_id,
                     const std::string& display_name, const std::string& bytes,
                     StoredAttachment* out, std::string* err_msg);

    // 按 id 取元信息。id 不过校验就直接返回 false，不碰数据库。
    bool GetById(const std::string& id, StoredAttachment* out, std::string* err_msg);

    // 把已上传的附件绑定到会话。
    // 单独存在是因为上传与会话可能不同步：客户端可以先传图拿到 id，
    // 等会话建好（或 Agent 回复时）再绑定。
    bool BindSession(const std::string& id, const std::string& session_id,
                     std::string* err_msg);

    // 从磁盘读回字节。优先用 storage_key，为空时回退到 id。
    // 两个值都来自数据库，但仍然重新跑一次 IsValidAttachmentId：
    // 数据库内容不应该被当成可信输入，万一有人手工改了表里的
    // storage_key，这里就是最后一道拦路。
    // 同时会校验实际文件大小不超过 kMaxImageBytes，避开被替换成巨文件后
    // 一次 assign 把内存吃光。
    bool ReadBytes(const StoredAttachment& attachment, std::string* bytes,
                   std::string* err_msg) const;

    // 把附件转成 Pi Agent 能消费的 images 负载。
    // 真正的文件读取与 base64 编码在 common/image_attachment.cpp 的
    // LoadPiImagesFromDir 里，本方法只负责拼 {attachment_id, mime} 列表。
    // 注意传的是 ref.mime（客户端声明），而下游会自己重新探测并以探测结果为准。
    bool LoadPiImages(const std::vector<ImageRef>& refs, nlohmann::json* images,
                      std::string* err_msg) const;

    const std::string& storage_dir() const { return storage_dir_; }

   private:
    // 先写 <path>.tmp，再 fs::rename 到正式路径。这是有意为之的：
    // 同一目录下 rename 是原子的，因此并发读者永远看不到半个文件。
    // 直接写正式路径的话，一个正在上传的附件可能被另一边读到不完整字节。
    // rename 失败时会顺手删掉 tmp，不留垃圾。
    bool WriteFile(const std::string& id, const std::string& bytes,
                   std::string* err_msg) const;
    // 唯一的路径拼接点。调用前必须先过 IsValidAttachmentId，
    // 因为 fs::path / 运算符对 "../x" 这类输入不会做任何防护。
    std::string FilePath(const std::string& id) const;

    MySqlConnectionPool* pool_{nullptr};
    std::string storage_dir_;
};

}  // namespace sparkpush
