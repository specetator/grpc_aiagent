#pragma once
#include <nlohmann/json.hpp>
#include <string>
namespace sparkpush {
constexpr uint64_t kMediaParsedFragments=4096;
bool ValidateMediaFragment(const nlohmann::json&);
bool ValidateMediaCoverage(const std::string& mime, const nlohmann::json& coverage,
                          const nlohmann::json& fragments, const nlohmann::json& artifacts);
std::string MediaManifestDigest(const nlohmann::json& source, const std::string& pipeline,
    const std::string& parser, const nlohmann::json& coverage,
    const nlohmann::json& fragment_hashes, const nlohmann::json& artifact_hashes);
}  // namespace sparkpush
