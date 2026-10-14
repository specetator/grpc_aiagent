#include "image_attachment.h"

#include <openssl/rand.h>
#include <openssl/sha.h>

#include <cctype>
#include <fstream>
#include <regex>

namespace sparkpush {
namespace {

constexpr char kHex[] = "0123456789abcdef";

int Base64Value(unsigned char c) {
    if (c >= 'A' && c <= 'Z') return c - 'A';
    if (c >= 'a' && c <= 'z') return c - 'a' + 26;
    if (c >= '0' && c <= '9') return c - '0' + 52;
    if (c == '+') return 62;
    if (c == '/') return 63;
    return -1;
}

// 在两个嵌套层级里找到真正装 attachments 的对象。
//
// 三种情况：
//   1) 顶层就有 attachments（客户端原始 WS 帧的 content 对象）
//   2) 嵌在 content 里（Logic 入库后多包了一层的完整帧）
//   3) 都没有 -> 仍返回顶层，让上层因找不到 attachments 而返回空列表
// 注意情况 3 不返回 nullptr：区分「结构不对」与「就是没附件」没意义，
// 两者对调用方而言结果相同。
const nlohmann::json* ContentObject(const nlohmann::json& value) {
    if (!value.is_object()) return nullptr;
    if (value.contains("attachments") && value["attachments"].is_array()) {
        return &value;
    }
    if (value.contains("content") && value["content"].is_object()) {
        return &value["content"];
    }
    return &value;
}

}  // namespace

// Base64 解码。用「位累加器」而不是查表四分一组：
//   val 累移入 6 bit，valb 记录已积累但未满 8 bit 的位数（初始 -8）。
// 这种写法天然容忍输入长度不是 4 的倍数，也不需要处理 padding 分支。
//
// 严格模式：遇到非 base64 字符立即清空输出并返回 false。
// 不能宽容地跳过脏字符：那会让一段损坏的上传静默产出一个
// 错误但「看上去正常」的图片，后续魔数校验会拒，但错误原因就不清了。
// 空白字符（\r\n）例外：HTTP 传输中常见的折行不应被当成错误。
bool DecodeBase64(const std::string& input, std::string* output) {
    if (!output) return false;
    output->clear();
    int val = 0;
    int valb = -8;
    for (unsigned char c : input) {
        if (std::isspace(c)) continue;
        if (c == '=') break;
        const int decoded = Base64Value(c);
        if (decoded < 0) {
            output->clear();
            return false;
        }
        val = (val << 6) + decoded;
        valb += 6;
        if (valb >= 0) {
            output->push_back(static_cast<char>((val >> valb) & 0xff));
            valb -= 8;
        }
    }
    return true;
}

bool EncodeBase64(const std::string& input, std::string* output) {
    if (!output) return false;
    static constexpr char kTable[] =
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    output->clear();
    output->reserve(((input.size() + 2) / 3) * 4);
    std::size_t i = 0;
    while (i + 2 < input.size()) {
        const unsigned int n = (static_cast<unsigned char>(input[i]) << 16) |
                               (static_cast<unsigned char>(input[i + 1]) << 8) |
                               static_cast<unsigned char>(input[i + 2]);
        output->push_back(kTable[(n >> 18) & 63]);
        output->push_back(kTable[(n >> 12) & 63]);
        output->push_back(kTable[(n >> 6) & 63]);
        output->push_back(kTable[n & 63]);
        i += 3;
    }
    if (i < input.size()) {
        unsigned int n = static_cast<unsigned char>(input[i]) << 16;
        if (i + 1 < input.size()) n |= static_cast<unsigned char>(input[i + 1]) << 8;
        output->push_back(kTable[(n >> 18) & 63]);
        output->push_back(kTable[(n >> 12) & 63]);
        output->push_back(i + 1 < input.size() ? kTable[(n >> 6) & 63] : '=');
        output->push_back('=');
    }
    return true;
}

std::string Sha256Hex(const std::string& input) {
    unsigned char digest[SHA256_DIGEST_LENGTH];
    SHA256(reinterpret_cast<const unsigned char*>(input.data()), input.size(),
           digest);
    std::string out(SHA256_DIGEST_LENGTH * 2, '0');
    for (int i = 0; i < SHA256_DIGEST_LENGTH; ++i) {
        out[i * 2] = kHex[(digest[i] >> 4) & 0x0f];
        out[i * 2 + 1] = kHex[digest[i] & 0x0f];
    }
    return out;
}

// 生成附件 id：att_ + 12 字节（96 bit）CSPRNG 的 hex 编码。
//
// 96 bit 足以让 id 不可枚举：即使攻击者知道某张图存在，
// 也无法通过遍历 id 找到它。这是「拿到 id 就能读文件」这个设计
// 能成立的前提（读取接口另外还有 Bearer + 会话成员校验）。
//
// 12 字节而不是 32：id 会出现在 content_json、MySQL 行、URL 里，
// 过长会无谓地放大存储与传输；96 bit 已远超安全需求。
//
// 生成的 id 恰好匹配 IsValidAttachmentId 的正则 ^att_[0-9a-f]{16,64}$
// （24 个 hex 字符）。两处必须保持一致，否则自己生成的 id 会被自己拒。
bool GenerateAttachmentId(std::string* id, std::string* error) {
    if (!id) return false;
    unsigned char bytes[12];
    if (RAND_bytes(bytes, sizeof(bytes)) != 1) {
        if (error) *error = "failed to generate attachment id";
        return false;
    }
    *id = "att_";
    id->reserve(4 + sizeof(bytes) * 2);
    for (unsigned char byte : bytes) {
        id->push_back(kHex[(byte >> 4) & 0x0f]);
        id->push_back(kHex[byte & 0x0f]);
    }
    return true;
}

// id 格式校验：att_ 前缀 + 16~64 位小写 hex。
//
// 这是**路径穿越的唯一防线**：下面 LoadPiImagesFromDir 直接拼
// storage_dir + "/" + id 去开文件。正则里没有 '/'、'\\'、'.'，
// 因此 "../../etc/passwd" 这类 id 在拼路径前就被拒了。
//
// 用 static const 正则：std::regex 构造开销很大，而本函数在
// ExtractImageRefs 里对每个附件都会调一次。C++11 保证局部 static
// 初始化线程安全。
bool IsValidAttachmentId(const std::string& id) {
    static const std::regex kId("^att_[0-9a-f]{16,64}$");
    return std::regex_match(id, kId);
}

// 按文件头魔数探测真实类型，不信任任何声明值。
//
// 返回空串表示「不是已知的图片格式」，调用方据此拒绝。
// 注意这里故意不做深度校验（比如解析 PNG 的 IHDR）：目的是挡住
// 「可执行文件改后缀」与 SVG 这类风险，而不是做图片完整性验证。
std::string DetectImageMime(const std::string& bytes) {
    const auto size = bytes.size();
    const auto at = [&](std::size_t i) {
        return static_cast<unsigned char>(bytes[i]);
    };
    if (size >= 3 && at(0) == 0xff && at(1) == 0xd8 && at(2) == 0xff) {
        return "image/jpeg";
    }
    if (size >= 8 && at(0) == 0x89 && at(1) == 0x50 && at(2) == 0x4e &&
        at(3) == 0x47 && at(4) == 0x0d && at(5) == 0x0a && at(6) == 0x1a &&
        at(7) == 0x0a) {
        return "image/png";
    }
    if (size >= 6 && bytes.compare(0, 6, "GIF87a") == 0) return "image/gif";
    if (size >= 6 && bytes.compare(0, 6, "GIF89a") == 0) return "image/gif";
    if (size >= 12 && bytes.compare(0, 4, "RIFF") == 0 &&
        bytes.compare(8, 4, "WEBP") == 0) {
        return "image/webp";
    }
    return {};
}

bool IsAllowedImageMime(const std::string& mime) {
    return mime == "image/jpeg" || mime == "image/png" || mime == "image/gif" ||
           mime == "image/webp";
}

std::vector<ImageRef> ExtractImageRefs(const nlohmann::json& value) {
    std::vector<ImageRef> refs;
    const auto* content = ContentObject(value);
    if (!content || !content->contains("attachments") ||
        !(*content)["attachments"].is_array()) {
        return refs;
    }
    for (const auto& item : (*content)["attachments"]) {
        if (!item.is_object()) continue;
        ImageRef ref;
        ref.id = item.value("id", "");
        ref.name = item.value("name", "");
        ref.mime = item.value("mime", "");
        if (item.contains("bytes") && item["bytes"].is_number_unsigned()) {
            ref.bytes = item["bytes"].get<std::size_t>();
        }
        if (!IsValidAttachmentId(ref.id)) continue;
        refs.push_back(std::move(ref));
        if (refs.size() >= kMaxImagesPerMessage) break;
    }
    return refs;
}

// 按 id 从本地目录读出图片，转成 Pi RPC 要求的 base64 图片块。
//
// 这是全链路里唯一做 base64 编码的地方（参见头文件的核心契约）。
//
// fail-fast 策略：任一图缺失/超限/类型不对就整体返回 false，
// 而不是跳过它继续。理由：缺图的模型回答会误导用户（比如说
// 「我没看到图片」），不如让本轮失败并由 Kafka 重试。
bool LoadPiImagesFromDir(const std::string& storage_dir,
                         const nlohmann::json& refs, nlohmann::json* images,
                         std::string* err_msg) {
    if (!images) return false;
    *images = nlohmann::json::array();
    if (!refs.is_array()) {
        if (err_msg) *err_msg = "images must be an array";
        return false;
    }
    if (refs.size() > kMaxImagesPerMessage) {
        if (err_msg) *err_msg = "at most 4 images are allowed";
        return false;
    }
    for (const auto& item : refs) {
        std::string id;
        std::string mime;
        if (item.is_string()) {
            id = item.get<std::string>();
        } else if (item.is_object()) {
            id = item.value("attachment_id", item.value("id", ""));
            mime = item.value("mime", item.value("mimeType", ""));
        } else {
            if (err_msg) *err_msg = "invalid image reference";
            return false;
        }
        if (!IsValidAttachmentId(id)) {
            if (err_msg) *err_msg = "invalid attachment id";
            return false;
        }
        // id 已经过正则校验（不含 '/' 与 '.'），因此这里的拼接是安全的。
        std::ifstream in(storage_dir + "/" + id, std::ios::binary);
        if (!in) {
            if (err_msg) *err_msg = "attachment file missing";
            return false;
        }
        // 先探大小再分配：避开先读入内存才发现超限。
        // size <= 0 也拒：空文件不可能是合法图片。
        in.seekg(0, std::ios::end);
        const auto size = in.tellg();
        if (size <= 0 || static_cast<std::size_t>(size) > kMaxImageBytes) {
            if (err_msg) *err_msg = "attachment file is invalid";
            return false;
        }
        std::string bytes(static_cast<std::size_t>(size), '\0');
        in.seekg(0);
        in.read(bytes.data(), size);
        if (!in) {   // 读取中途出错（文件被截断/磁盘错误）
            if (err_msg) *err_msg = "failed to read attachment";
            return false;
        }
        // 关键安全点：用**实际字节的魔数**判定类型，而不是相信
        // 客户端传来的 mime。下面 (void)mime 就是故意丢弃声明值。
        // 若不这样，一个声明为 image/png 的 SVG 或 HTML 会被当成图片
        // 传给模型/渲染到页面，形成注入面。
        const std::string detected = DetectImageMime(bytes);
        if (!IsAllowedImageMime(detected)) {
            if (err_msg) *err_msg = "attachment is not an allowed image";
            return false;
        }
        (void)mime;   // 故意忽略客户端声明的 mime，一律用探测结果
        std::string encoded;
        if (!EncodeBase64(bytes, &encoded)) return false;
        // Pi RPC 的图片块格式：type/mimeType/data。mimeType 用 detected，
        // 保证与实际字节一致。
        images->push_back({{"type", "image"},
                           {"mimeType", detected},
                           {"data", encoded}});
    }
    return true;
}

// 从 content_json 字符串提取图片引用。在热路径上被调（每条上行消息），
// 因此必须不抛异常：catch(...) 后返回空列表。
//
// 返回空而不是报错是有意的：绝大多数消息本来就没图片，
// 而「正文不是合法 JSON」已经在上游 Submit 里拦过了，
// 这里不应因为解析问题把一条正常文本消息误判为失败。
std::vector<ImageRef> ExtractImageRefsFromContentJson(
    const std::string& content_json) {
    if (content_json.empty()) return {};
    try {
        return ExtractImageRefs(nlohmann::json::parse(content_json));
    } catch (...) {
        return {};
    }
}

}  // namespace sparkpush
