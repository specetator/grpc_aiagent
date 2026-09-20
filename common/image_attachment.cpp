#include "image_attachment.h"

#include <openssl/rand.h>
#include <openssl/sha.h>

#include <cctype>
#include <fstream>
#include <regex>

namespace sparkpush {
namespace {

constexpr char kHex[] = "0123456789abcdef";

int Base64Value(unsigned char c) {
    if (c >= 'A' && c <= 'Z') return c - 'A';
    if (c >= 'a' && c <= 'z') return c - 'a' + 26;
    if (c >= '0' && c <= '9') return c - '0' + 52;
    if (c == '+') return 62;
    if (c == '/') return 63;
    return -1;
}

const nlohmann::json* ContentObject(const nlohmann::json& value) {
    if (!value.is_object()) return nullptr;
    if (value.contains("attachments") && value["attachments"].is_array()) {
        return &value;
    }
    if (value.contains("content") && value["content"].is_object()) {
        return &value["content"];
    }
    return &value;
}

}  // namespace

bool DecodeBase64(const std::string& input, std::string* output) {
    if (!output) return false;
    output->clear();
    int val = 0;
    int valb = -8;
    for (unsigned char c : input) {
        if (std::isspace(c)) continue;
        if (c == '=') break;
        const int decoded = Base64Value(c);
        if (decoded < 0) {
            output->clear();
            return false;
        }
        val = (val << 6) + decoded;
        valb += 6;
        if (valb >= 0) {
            output->push_back(static_cast<char>((val >> valb) & 0xff));
            valb -= 8;
        }
    }
    return true;
}

bool EncodeBase64(const std::string& input, std::string* output) {
    if (!output) return false;
    static constexpr char kTable[] =
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    output->clear();
    output->reserve(((input.size() + 2) / 3) * 4);
    std::size_t i = 0;
    while (i + 2 < input.size()) {
        const unsigned int n = (static_cast<unsigned char>(input[i]) << 16) |
                               (static_cast<unsigned char>(input[i + 1]) << 8) |
                               static_cast<unsigned char>(input[i + 2]);
        output->push_back(kTable[(n >> 18) & 63]);
        output->push_back(kTable[(n >> 12) & 63]);
        output->push_back(kTable[(n >> 6) & 63]);
        output->push_back(kTable[n & 63]);
        i += 3;
    }
    if (i < input.size()) {
        unsigned int n = static_cast<unsigned char>(input[i]) << 16;
        if (i + 1 < input.size()) n |= static_cast<unsigned char>(input[i + 1]) << 8;
        output->push_back(kTable[(n >> 18) & 63]);
        output->push_back(kTable[(n >> 12) & 63]);
        output->push_back(i + 1 < input.size() ? kTable[(n >> 6) & 63] : '=');
        output->push_back('=');
    }
    return true;
}

std::string Sha256Hex(const std::string& input) {
    unsigned char digest[SHA256_DIGEST_LENGTH];
    SHA256(reinterpret_cast<const unsigned char*>(input.data()), input.size(),
           digest);
    std::string out(SHA256_DIGEST_LENGTH * 2, '0');
    for (int i = 0; i < SHA256_DIGEST_LENGTH; ++i) {
        out[i * 2] = kHex[(digest[i] >> 4) & 0x0f];
        out[i * 2 + 1] = kHex[digest[i] & 0x0f];
    }
    return out;
}

bool GenerateAttachmentId(std::string* id, std::string* error) {
    if (!id) return false;
    unsigned char bytes[12];
    if (RAND_bytes(bytes, sizeof(bytes)) != 1) {
        if (error) *error = "failed to generate attachment id";
        return false;
    }
    *id = "att_";
    id->reserve(4 + sizeof(bytes) * 2);
    for (unsigned char byte : bytes) {
        id->push_back(kHex[(byte >> 4) & 0x0f]);
        id->push_back(kHex[byte & 0x0f]);
    }
    return true;
}

bool IsValidAttachmentId(const std::string& id) {
    static const std::regex kId("^att_[0-9a-f]{16,64}$");
    return std::regex_match(id, kId);
}

std::string DetectImageMime(const std::string& bytes) {
    const auto size = bytes.size();
    const auto at = [&](std::size_t i) {
        return static_cast<unsigned char>(bytes[i]);
    };
    if (size >= 3 && at(0) == 0xff && at(1) == 0xd8 && at(2) == 0xff) {
        return "image/jpeg";
    }
    if (size >= 8 && at(0) == 0x89 && at(1) == 0x50 && at(2) == 0x4e &&
        at(3) == 0x47 && at(4) == 0x0d && at(5) == 0x0a && at(6) == 0x1a &&
        at(7) == 0x0a) {
        return "image/png";
    }
    if (size >= 6 && bytes.compare(0, 6, "GIF87a") == 0) return "image/gif";
    if (size >= 6 && bytes.compare(0, 6, "GIF89a") == 0) return "image/gif";
    if (size >= 12 && bytes.compare(0, 4, "RIFF") == 0 &&
        bytes.compare(8, 4, "WEBP") == 0) {
        return "image/webp";
    }
    return {};
}

bool IsAllowedImageMime(const std::string& mime) {
    return mime == "image/jpeg" || mime == "image/png" || mime == "image/gif" ||
           mime == "image/webp";
}

std::vector<ImageRef> ExtractImageRefs(const nlohmann::json& value) {
    std::vector<ImageRef> refs;
    const auto* content = ContentObject(value);
    if (!content || !content->contains("attachments") ||
        !(*content)["attachments"].is_array()) {
        return refs;
    }
    for (const auto& item : (*content)["attachments"]) {
        if (!item.is_object()) continue;
        ImageRef ref;
        ref.id = item.value("id", "");
        ref.name = item.value("name", "");
        ref.mime = item.value("mime", "");
        if (item.contains("bytes") && item["bytes"].is_number_unsigned()) {
            ref.bytes = item["bytes"].get<std::size_t>();
        }
        if (!IsValidAttachmentId(ref.id)) continue;
        refs.push_back(std::move(ref));
        if (refs.size() >= kMaxImagesPerMessage) break;
    }
    return refs;
}

bool LoadPiImagesFromDir(const std::string& storage_dir,
                         const nlohmann::json& refs, nlohmann::json* images,
                         std::string* err_msg) {
    if (!images) return false;
    *images = nlohmann::json::array();
    if (!refs.is_array()) {
        if (err_msg) *err_msg = "images must be an array";
        return false;
    }
    if (refs.size() > kMaxImagesPerMessage) {
        if (err_msg) *err_msg = "at most 4 images are allowed";
        return false;
    }
    for (const auto& item : refs) {
        std::string id;
        std::string mime;
        if (item.is_string()) {
            id = item.get<std::string>();
        } else if (item.is_object()) {
            id = item.value("attachment_id", item.value("id", ""));
            mime = item.value("mime", item.value("mimeType", ""));
        } else {
            if (err_msg) *err_msg = "invalid image reference";
            return false;
        }
        if (!IsValidAttachmentId(id)) {
            if (err_msg) *err_msg = "invalid attachment id";
            return false;
        }
        std::ifstream in(storage_dir + "/" + id, std::ios::binary);
        if (!in) {
            if (err_msg) *err_msg = "attachment file missing";
            return false;
        }
        in.seekg(0, std::ios::end);
        const auto size = in.tellg();
        if (size <= 0 || static_cast<std::size_t>(size) > kMaxImageBytes) {
            if (err_msg) *err_msg = "attachment file is invalid";
            return false;
        }
        std::string bytes(static_cast<std::size_t>(size), '\0');
        in.seekg(0);
        in.read(bytes.data(), size);
        if (!in) {
            if (err_msg) *err_msg = "failed to read attachment";
            return false;
        }
        const std::string detected = DetectImageMime(bytes);
        if (!IsAllowedImageMime(detected)) {
            if (err_msg) *err_msg = "attachment is not an allowed image";
            return false;
        }
        (void)mime;
        std::string encoded;
        if (!EncodeBase64(bytes, &encoded)) return false;
        images->push_back({{"type", "image"},
                           {"mimeType", detected},
                           {"data", encoded}});
    }
    return true;
}

std::vector<ImageRef> ExtractImageRefsFromContentJson(
    const std::string& content_json) {
    if (content_json.empty()) return {};
    try {
        return ExtractImageRefs(nlohmann::json::parse(content_json));
    } catch (...) {
        return {};
    }
}

}  // namespace sparkpush
