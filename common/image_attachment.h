#pragma once

#include <cstddef>
#include <string>
#include <vector>

#include <nlohmann/json.hpp>

namespace sparkpush {

// =====================================================================
// 图片附件的编解码、校验与引用提取。
//
// 核心契约（贯穿整个附件链路）：**图片二进制不进 WebSocket、
// 不进 Kafka、不进 content_json**。客户端先走 HTTP 上传拿到 att_* id，
// 聊天帧里只带引用；发给 Agent 时由 Bridge 再按 id 读本地文件。
// 这样消息负载始终很小，Kafka 与 MySQL 不会被图片撑爆。

// 单张上限 4 MiB。与 Comet 的 WebSocket 帧上限（64 KiB）差很多，
// 这是合理的：图片走 HTTP 上传而不是 WS，不受帧长限制。
constexpr std::size_t kMaxImageBytes = 4 * 1024 * 1024;
// 每条消息最多 4 张。限制数量而不只限单张大小，是因为总量才是
// 真正影响 Agent prompt 长度与存储的因素。
constexpr std::size_t kMaxImagesPerMessage = 4;

// 聊天帧 content.attachments[] 里的一项。只有元数据，没有字节。
struct ImageRef {
    std::string id;      // att_ 前缀的附件 id，也是本地文件名
    std::string name;    // 原始文件名，仅供展示
    std::string mime;    // 由服务端按魔数探测得出，不信任客户端声明
    std::size_t bytes{0};
};

// Base64 编解码。HTTP 上传接口用 base64 传图片，解码失败返回 false。
bool DecodeBase64(const std::string& input, std::string* output);
bool EncodeBase64(const std::string& input, std::string* output);
// 内容指纹。写入 attachment 表用于完整性校验与去重参考。
std::string Sha256Hex(const std::string& input);
// 生成 att_ 前缀的随机 id。用 CSPRNG，不可预测——否则能枚举出
// 别人的附件 id 并尝试读取。
bool GenerateAttachmentId(std::string* id, std::string* error = nullptr);
// 校验 id 格式。在拼文件路径前必须先过这一道：否则 "../../etc/passwd"
// 这类 id 会造成路径穿越，读到存储目录之外的任意文件。
bool IsValidAttachmentId(const std::string& id);
// 按文件头魔数探测真实 MIME，而不是相信上传时声明的 Content-Type。
std::string DetectImageMime(const std::string& bytes);
// 白名单校验：jpeg/png/gif/webp。挡住 SVG（可内嵌脚本，渲染时
// 会造成 XSS）与可执行文件改后缀的情况。
bool IsAllowedImageMime(const std::string& mime);

// Read attachments from the IM content object (content.attachments or
// the nested content.content.attachments used after Logic wraps a WS frame).
//
// 为何要兼容两种嵌套层级：客户端发的原始 WS 帧是
//   {"type":"single_chat","content":{"text":...,"attachments":[...]}}
// 而 Logic 入库时会把这个帧整体当作 content_json 存下来，于是从历史
// 读出来就多了一层。ExtractImageRefs 同时看两个位置，避开调用方
// 需要自己判断「这是新消息还是历史消息」。
std::vector<ImageRef> ExtractImageRefs(const nlohmann::json& value);
std::vector<ImageRef> ExtractImageRefsFromContentJson(const std::string& content_json);

// Load local files named by attachment id. Used by Logic and the Agent bridge.
//
// 这是全链路里**唯一**把图片转成 base64 的地方：仅在调 Pi RPC 前
// 把本轮图片读出来填进 prompt.images。历史轮次的图片不会再次入内存，
// 避免多轮对话后 prompt 无限膨胀。
// 任一文件缺失就整体失败（而不是跳过）：缺图的回复会误导用户。
bool LoadPiImagesFromDir(const std::string& storage_dir,
                         const nlohmann::json& refs, nlohmann::json* images,
                         std::string* err_msg);

}  // namespace sparkpush
