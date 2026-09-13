#include "knowledge_catalog.h"

#include <algorithm>
#include <cctype>
#include <filesystem>
#include <fstream>

namespace sparkpush {
namespace {

bool HexId(const std::string& value, const char* prefix) {
    const std::string expected(prefix);
    if (value.size() != expected.size() + 24 ||
        value.rfind(expected, 0) != 0) {
        return false;
    }
    return std::all_of(value.begin() + expected.size(), value.end(),
                       [](unsigned char c) {
                           return std::isdigit(c) || (c >= 'a' && c <= 'f');
                       });
}

bool ReadJsonBounded(const std::filesystem::path& path, size_t max_bytes,
                     nlohmann::json* output, std::string* error) {
    std::error_code ec;
    const auto size = std::filesystem::file_size(path, ec);
    if (ec) {
        if (error) *error = "knowledge document not found";
        return false;
    }
    if (size > max_bytes) {
        if (error) *error = "knowledge document exceeds size limit";
        return false;
    }
    std::ifstream input(path, std::ios::binary);
    if (!input) {
        if (error) *error = "knowledge document cannot be opened";
        return false;
    }
    try {
        input >> *output;
    } catch (const nlohmann::json::exception&) {
        if (error) *error = "knowledge catalog JSON is invalid";
        return false;
    }
    return true;
}

bool ValidGeneration(const std::string& value) {
    if (value.size() < 16 || value.size() > 80 ||
        value.find("..") != std::string::npos) {
        return false;
    }
    return std::all_of(value.begin(), value.end(), [](unsigned char c) {
        return std::isalnum(c) || c == '-';
    });
}

}  // namespace

bool IsValidKnowledgeDocumentId(const std::string& value) {
    return HexId(value, "doc_");
}

bool IsValidKnowledgeChunkId(const std::string& value) {
    return HexId(value, "chk_");
}

bool LoadKnowledgeDocument(const std::string& knowledge_root,
                           const std::string& doc_id,
                           const std::string& chunk_id,
                           nlohmann::json* output, std::string* error) {
    if (!output || knowledge_root.empty() ||
        !IsValidKnowledgeDocumentId(doc_id) ||
        (!chunk_id.empty() && !IsValidKnowledgeChunkId(chunk_id))) {
        if (error) *error = "invalid knowledge document or chunk id";
        return false;
    }
    nlohmann::json current;
    const std::filesystem::path root(knowledge_root);
    if (!ReadJsonBounded(root / "data" / "current.json", 4096, &current,
                         error)) {
        return false;
    }
    const std::string generation = current.value("generation", "");
    if (!ValidGeneration(generation)) {
        if (error) *error = "invalid knowledge generation";
        return false;
    }
    nlohmann::json stored;
    const auto path = root / "data" / "generations" / generation /
                      "documents" / (doc_id + ".json");
    if (!ReadJsonBounded(path, 8 * 1024 * 1024, &stored, error)) return false;
    if (!stored.is_object() || stored.value("generation", "") != generation ||
        !stored.contains("document") || !stored["document"].is_object() ||
        stored["document"].value("doc_id", "") != doc_id ||
        !stored.contains("chunks") || !stored["chunks"].is_array()) {
        if (error) *error = "knowledge document failed catalog validation";
        return false;
    }

    nlohmann::json result = {
        {"generation", generation},
        {"document", stored["document"]},
    };
    if (!chunk_id.empty()) {
        auto found = std::find_if(
            stored["chunks"].begin(), stored["chunks"].end(),
            [&](const nlohmann::json& chunk) {
                return chunk.is_object() &&
                       chunk.value("chunk_id", "") == chunk_id &&
                       chunk.value("doc_id", "") == doc_id;
            });
        if (found == stored["chunks"].end()) {
            if (error) *error = "knowledge chunk not found in document";
            return false;
        }
        result["chunk"] = *found;
    }
    *output = std::move(result);
    return true;
}

}  // namespace sparkpush
