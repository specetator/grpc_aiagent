#pragma once

#include <cstddef>
#include <string>
#include <vector>

#include <nlohmann/json.hpp>

namespace sparkpush {

constexpr std::size_t kMaxImageBytes = 4 * 1024 * 1024;
constexpr std::size_t kMaxImagesPerMessage = 4;

struct ImageRef {
    std::string id;
    std::string name;
    std::string mime;
    std::size_t bytes{0};
};

bool DecodeBase64(const std::string& input, std::string* output);
bool EncodeBase64(const std::string& input, std::string* output);
std::string Sha256Hex(const std::string& input);
bool GenerateAttachmentId(std::string* id, std::string* error = nullptr);
bool IsValidAttachmentId(const std::string& id);
std::string DetectImageMime(const std::string& bytes);
bool IsAllowedImageMime(const std::string& mime);

// Read attachments from the IM content object (content.attachments or
// the nested content.content.attachments used after Logic wraps a WS frame).
std::vector<ImageRef> ExtractImageRefs(const nlohmann::json& value);
std::vector<ImageRef> ExtractImageRefsFromContentJson(const std::string& content_json);

// Load local files named by attachment id. Used by Logic and the Agent bridge.
bool LoadPiImagesFromDir(const std::string& storage_dir,
                         const nlohmann::json& refs, nlohmann::json* images,
                         std::string* err_msg);

}  // namespace sparkpush
