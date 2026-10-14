#include "services/knowledge_service.h"

#include "knowledge_catalog.h"

namespace sparkpush {
ServiceStatus KnowledgeService::Document(const std::string& id,
                                         const std::string& chunk,
                                         nlohmann::json* result) {
    // 先校验再读文件。id 字符集不含 '/'、'\\'、'.'，
    // 非法 id 在拼路径之前就被 400 挡下，这是路径穿越的防线。
    // chunk 为空表示整份文档，不做 chunk 校验。
    if (!IsValidKnowledgeDocumentId(id) ||
        (!chunk.empty() && !IsValidKnowledgeChunkId(chunk)))
        return {400, "invalid knowledge document or chunk id"};
    std::string error;
    if (!LoadKnowledgeDocument(root_, id, chunk, result, &error)) return {404, error};
    return {};
}
}  // namespace sparkpush
