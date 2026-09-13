#include "citation_validation.h"

#include <algorithm>
#include <cctype>
#include <unordered_set>

namespace sparkpush {
namespace {

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

bool ValidCitation(const nlohmann::json& item) {
    if (!item.is_object()) return false;
    std::string schema, citation_id, doc_id, chunk_id, title, uri;
    std::string authority, revision, sha;
    if (!BoundedString(item, "schema_version", 1, 64, &schema) ||
        schema != "cannkb.citation.v1" ||
        !BoundedString(item, "citation_id", 1, 80, &citation_id) ||
        !BoundedString(item, "doc_id", 1, 64, &doc_id) ||
        !BoundedString(item, "chunk_id", 1, 64, &chunk_id) ||
        !BoundedString(item, "title", 1, 512, &title) ||
        !BoundedString(item, "uri", 1, 1024, &uri) ||
        !BoundedString(item, "authority", 1, 64, &authority) ||
        !BoundedString(item, "source_revision", 1, 128, &revision) ||
        !BoundedString(item, "content_sha256", 64, 64, &sha)) {
        return false;
    }
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
    if (locator_type != "heading" && locator_type != "page" &&
        locator_type != "lines") {
        return false;
    }
    const std::string expected_uri =
        "cannkb://document/" + doc_id + "?chunk=" + chunk_id;
    return uri == expected_uri;
}

}  // namespace

nlohmann::json ValidateHermesCitations(const nlohmann::json& input,
                                       std::string* warning) {
    nlohmann::json valid = nlohmann::json::array();
    if (warning) warning->clear();
    if (!input.is_array()) {
        if (warning) *warning = "citations is not an array";
        return valid;
    }
    constexpr size_t kMaxCitations = 20;
    std::unordered_set<std::string> seen;
    size_t rejected = 0;
    for (const auto& item : input) {
        if (valid.size() >= kMaxCitations) {
            ++rejected;
            continue;
        }
        if (!ValidCitation(item)) {
            ++rejected;
            continue;
        }
        const std::string id = item.at("citation_id").get<std::string>();
        if (!seen.insert(id).second) continue;
        valid.push_back(item);
    }
    if (warning && rejected) {
        *warning = "rejected " + std::to_string(rejected) +
                   " invalid or excessive citations";
    }
    return valid;
}

}  // namespace sparkpush
