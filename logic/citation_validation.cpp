#include "citation_validation.h"

#include <algorithm>
#include <cctype>
#include <unordered_set>

namespace sparkpush {
namespace {

// 取一个字符串字段并做三重检查：存在且是字符串、长度在 [min,max]、不含控制字符。
// 控制字符检查（iscntrl）是关键：它挡掉 \n、\t、\0 以及 ANSI 转义序列。
// 不挡的话，一个带终端转义码的 title 写进日志就能伪造日志行，
// 展到前端也可能触发意外换行或不可见字符。
bool BoundedString(const nlohmann::json& object, const char* key,
                   size_t min_size, size_t max_size, std::string* out) {
    const auto it = object.find(key);
    if (it == object.end() || !it->is_string()) return false;
    const std::string value = it->get<std::string>();
    if (value.size() < min_size || value.size() > max_size) return false;
    if (std::any_of(value.begin(), value.end(), [](unsigned char c) {
            return std::iscntrl(c) != 0;
        })) {
        return false;
    }
    if (out) *out = value;
    return true;
}

bool IsHex(const std::string& value) {
    return std::all_of(value.begin(), value.end(), [](unsigned char c) {
        return std::isdigit(c) || (c >= 'a' && c <= 'f');
    });
}

// 固定形式：前缀 + 恰好 24 个小写十六进制字符。
// 与 knowledge_catalog.cpp 里的 HexId 规则完全一致——这不是巧合：
// 引用里的 doc_id/chunk_id 最终会被客户端拿去调 KnowledgeService，
// 两边的校验规则必须同步，否则这里放过的 id 会在那里被 400 掉。
// 只允许 [0-9a-f] 同时也就排除了 '/'、'\\'、'.'，天然防路径穿越。
bool StableId(const std::string& value, const char* prefix) {
    const std::string expected(prefix);
    return value.size() == expected.size() + 24 &&
           value.rfind(expected, 0) == 0 &&
           IsHex(value.substr(expected.size()));
}

bool CitationId(const std::string& value) {
    return value.size() == 20 && value.rfind("ref_", 0) == 0 &&
           IsHex(value.substr(4));
}

// 单条引用的完整校验。
bool ValidCitation(const nlohmann::json& item) {
    if (!item.is_object()) return false;
    std::string schema, citation_id, doc_id, chunk_id, title, uri;
    std::string authority, revision, sha;
    // 九个字段全部必需，且每个都有独立的长度上限。
    // 任一项不过就整条丢弃，不做部分接受：一个缺字段的引用
    // 对客户端而言无法安全渲染。
    if (!BoundedString(item, "schema_version", 1, 64, &schema) ||
        // schema_version 必须精确匹配：将来升版时，旧服务端会直接拒掉
        // 新格式的引用而不是试图部分解析它。fail-closed 比宽容更安全。
        schema != "cannkb.citation.v1" ||
        !BoundedString(item, "citation_id", 1, 80, &citation_id) ||
        !BoundedString(item, "doc_id", 1, 64, &doc_id) ||
        !BoundedString(item, "chunk_id", 1, 64, &chunk_id) ||
        !BoundedString(item, "title", 1, 512, &title) ||
        !BoundedString(item, "uri", 1, 1024, &uri) ||
        !BoundedString(item, "authority", 1, 64, &authority) ||
        !BoundedString(item, "source_revision", 1, 128, &revision) ||
        // sha 必须恰好 64 位：SHA-256 的十六进制表示长度是固定的。
        !BoundedString(item, "content_sha256", 64, 64, &sha)) {
        return false;
    }
    // 格式级校验：长度 + 前缀 + 十六进制字符集。
    if (!CitationId(citation_id) || !StableId(doc_id, "doc_") ||
        !StableId(chunk_id, "chk_") || !IsHex(sha)) {
        return false;
    }
    const auto locator_it = item.find("locator");
    if (locator_it == item.end() || !locator_it->is_object()) return false;
    std::string locator_type, locator_value;
    if (!BoundedString(*locator_it, "type", 1, 32, &locator_type) ||
        !BoundedString(*locator_it, "value", 1, 512, &locator_value)) {
        return false;
    }
    // 定位方式白名单：前端靠 type 选择渲染方式，
    // 未知 type 会落到 default 分支，不如直接拒掉。
    if (locator_type != "heading" && locator_type != "page" &&
        locator_type != "lines") {
        return false;
    }
    // 最关键的一条校验：uri 必须与由 doc_id/chunk_id 算出的规范形式完全相等。
    //
    // 这把 uri 从「模型可以自由填的字段」变成了「由 id 推导出的值」。
    // 没有这一步，模型（或被提示注入误导的模型）可以填一个
    // 任意外部链接，而客户端会把它当成可信知识库文档展示并允许点击。
    // 只允许 cannkb:// 这个自建 scheme，也就排除了 http(s):// 与 javascript:。
    const std::string expected_uri =
        "cannkb://document/" + doc_id + "?chunk=" + chunk_id;
    return uri == expected_uri;
}

}  // namespace

nlohmann::json ValidateHermesCitations(const nlohmann::json& input,
                                       std::string* warning) {
    nlohmann::json valid = nlohmann::json::array();
    if (warning) warning->clear();
    // 不是数组就返回空数组而不是报错：模型有时会返回 null 或对象，
    // 这两种情况都应该被当成「没有引用」，而不是让整条回复失败。
    if (!input.is_array()) {
        if (warning) *warning = "citations is not an array";
        return valid;
    }
    // 20 条上限：引用会随回复一起落库并推给所有接收者，
    // 不封顶就能用一条回复把消息体积（以及下游存储）撑得很大。
    constexpr size_t kMaxCitations = 20;
    std::unordered_set<std::string> seen;
    size_t rejected = 0;
    for (const auto& item : input) {
        // 超限的也计入 rejected，让 warning 能反映「太多了」而不只是「不合法」。
        if (valid.size() >= kMaxCitations) {
            ++rejected;
            continue;
        }
        if (!ValidCitation(item)) {
            ++rejected;
            continue;
        }
        const std::string id = item.at("citation_id").get<std::string>();
        // 按 citation_id 去重。重复项不计入 rejected：
        // 它们是合法的，只是多余，不应该让 warning 看起来像出了错。
        if (!seen.insert(id).second) continue;
        // 注意：这里 push 的是**原始 item**，不是按白名单重建的新对象。
        // 因此模型多填的未知字段会原样保留并传给客户端。
        // 与其他地方（如 RoomAgentService::Complete）的「重建而非过滤」不同：
        // 那些地方只取固定几个字段。若将来客户端开始信任引用里的额外字段，
        // 应改成只拷贝上面校验过的九个字段的白名单重建。
        valid.push_back(item);
    }
    if (warning && rejected) {
        *warning = "rejected " + std::to_string(rejected) +
                   " invalid or excessive citations";
    }
    return valid;
}

}  // namespace sparkpush
