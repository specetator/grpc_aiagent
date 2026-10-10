#include "services/knowledge_service.h"

#include "knowledge_catalog.h"

namespace sparkpush {
ServiceStatus KnowledgeService::Document(const std::string& id,
                                         const std::string& chunk,
                                         nlohmann::json* result) {
    if (!IsValidKnowledgeDocumentId(id) ||
        (!chunk.empty() && !IsValidKnowledgeChunkId(chunk)))
        return {400, "invalid knowledge document or chunk id"};
    std::string error;
    if (!LoadKnowledgeDocument(root_, id, chunk, result, &error)) return {404, error};
    return {};
}
}  // namespace sparkpush
