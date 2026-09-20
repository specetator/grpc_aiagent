#include "image_attachment.h"

#include <iostream>
#include <string>

int main() {
    const std::string png = {
        '\x89', 'P', 'N', 'G', '\r', '\n', '\x1a', '\n',
        '\x00', '\x00', '\x00', '\x0d'
    };
    if (sparkpush::DetectImageMime(png) != "image/png") {
        std::cerr << "png magic failed\n";
        return 1;
    }
    std::string encoded;
    std::string decoded;
    if (!sparkpush::EncodeBase64(png, &encoded) ||
        !sparkpush::DecodeBase64(encoded, &decoded) || decoded != png) {
        std::cerr << "base64 roundtrip failed\n";
        return 1;
    }
    if (!sparkpush::IsValidAttachmentId("att_0123456789abcdef") ||
        sparkpush::IsValidAttachmentId("../etc/passwd")) {
        std::cerr << "attachment id validation failed\n";
        return 1;
    }
    nlohmann::json payload = {
        {"content",
         {{"text", "see this"},
          {"attachments",
           nlohmann::json::array(
               {{{"id", "att_0123456789abcdef"},
                 {"name", "a.png"},
                 {"mime", "image/png"}}})}}},
    };
    const auto refs = sparkpush::ExtractImageRefs(payload);
    if (refs.size() != 1 || refs[0].id != "att_0123456789abcdef") {
        std::cerr << "extract refs failed\n";
        return 1;
    }
    return 0;
}
