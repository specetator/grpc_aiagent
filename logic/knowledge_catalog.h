#pragma once

#include <string>

#include <nlohmann/json.hpp>

namespace sparkpush {

bool IsValidKnowledgeDocumentId(const std::string& value);
bool IsValidKnowledgeChunkId(const std::string& value);

// 从当前 generation 的按文档视图读取内容。调用方只能传稳定 ID，不能传路径。
bool LoadKnowledgeDocument(const std::string& knowledge_root,
                           const std::string& doc_id,
                           const std::string& chunk_id,
                           nlohmann::json* output, std::string* error);

}  // namespace sparkpush
