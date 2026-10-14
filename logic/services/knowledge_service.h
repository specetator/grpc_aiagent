#pragma once
#include <nlohmann/json.hpp>

#include "services/service_status.h"

namespace sparkpush {
// 知识库文档的只读查询入口（CANN 算子知识库）。
//
// 本类很薄，只做两件事：校验 id 格式、转给 knowledge_catalog 读文件。
// 把校验放在这一层而不是只在 catalog 里，是为了让非法 id
// 在碰文件系统之前就被 400 挡下。
//
// id 格式是硬约束（见 HexId）：固定前缀 + 恰好 24 个小写十六进制字符，
// 即 doc_<24 hex> 或 chk_<24 hex>，长度不对也直接拒。
// 字符集里不包含 '/'、'\\'、'.'，因此这也是路径穿越的防线：
// 一个形如 "../../etc/passwd" 的 id 在拼路径前就被拒了。
// 与附件模块（IsValidAttachmentId）是同一个设计思路。
class KnowledgeService {
   public:
    // root 是知识库根目录，由配置传入。为空时 LoadKnowledgeDocument 会失败，
    // 因此未在配置里开启知识库的部署会得到 404 而不是崩溃。
    explicit KnowledgeService(std::string root) : root_(std::move(root)) {}

    // 取一份文档（或文档里的一个 chunk）。
    // chunk 为空时返回整份文档，非空时只返回那一段。
    // 找不到返回 404（而不是 400）：id 格式合法但内容不存在是正常情况。
    ServiceStatus Document(const std::string& id, const std::string& chunk,
                           nlohmann::json* result);

   private:
    std::string root_;
};
}  // namespace sparkpush
