#include "citation_validation.h"

#include <iostream>
#include <string>

namespace {
nlohmann::json ValidCitation() {
    return {
        {"schema_version", "cannkb.citation.v1"},
        {"citation_id", "ref_0123456789abcdef"},
        {"doc_id", "doc_0123456789abcdef01234567"},
        {"chunk_id", "chk_89abcdef0123456789abcdef"},
        {"title", "DataCopyPad"},
        {"locator", {{"type", "heading"}, {"value", "参数"}}},
        {"uri", "cannkb://document/doc_0123456789abcdef01234567?chunk=chk_89abcdef0123456789abcdef"},
        {"authority", "official-cann"},
        {"source_revision", "abc123"},
        {"content_sha256", std::string(64, 'a')},
    };
}
}

int main() {
    std::string warning;
    auto valid = sparkpush::ValidateHermesCitations(
        nlohmann::json::array({ValidCitation()}), &warning);
    if (valid.size() != 1 || !warning.empty()) return 1;

    auto bad = ValidCitation();
    bad["uri"] = "javascript:alert(1)";
    valid = sparkpush::ValidateHermesCitations(
        nlohmann::json::array({bad}), &warning);
    if (!valid.empty() || warning.empty()) return 1;

    bad = ValidCitation();
    bad["doc_id"] = "../../etc/passwd";
    valid = sparkpush::ValidateHermesCitations(
        nlohmann::json::array({bad}), &warning);
    if (!valid.empty()) return 1;

    bad = ValidCitation();
    bad["citation_id"] = "ref_not-a-valid-token";
    valid = sparkpush::ValidateHermesCitations(
        nlohmann::json::array({bad}), &warning);
    if (!valid.empty()) return 1;
    std::cout << "citation validation tests passed\n";
    return 0;
}
