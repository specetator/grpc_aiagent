#include "security.h"

#include <openssl/crypto.h>
#include <openssl/evp.h>
#include <openssl/rand.h>

#include <array>
#include <cctype>
#include <cstdlib>
#include <sstream>
#include <vector>

namespace sparkpush {
namespace {

constexpr int kPbkdf2Iterations = 120000;
constexpr size_t kSaltBytes = 16;
constexpr size_t kDigestBytes = 32;
constexpr const char* kPrefix = "pbkdf2_sha256";

std::string HexEncode(const unsigned char* data, size_t size) {
    static constexpr char kHex[] = "0123456789abcdef";
    std::string out(size * 2, '0');
    for (size_t i = 0; i < size; ++i) {
        out[i * 2] = kHex[(data[i] >> 4) & 0x0f];
        out[i * 2 + 1] = kHex[data[i] & 0x0f];
    }
    return out;
}

bool HexDecode(const std::string& input, std::vector<unsigned char>* output) {
    if (!output || input.size() % 2 != 0) return false;
    output->clear();
    output->reserve(input.size() / 2);
    auto nibble = [](char c) -> int {
        if (c >= '0' && c <= '9') return c - '0';
        if (c >= 'a' && c <= 'f') return c - 'a' + 10;
        if (c >= 'A' && c <= 'F') return c - 'A' + 10;
        return -1;
    };
    for (size_t i = 0; i < input.size(); i += 2) {
        int high = nibble(input[i]);
        int low = nibble(input[i + 1]);
        if (high < 0 || low < 0) return false;
        output->push_back(static_cast<unsigned char>((high << 4) | low));
    }
    return true;
}

bool Derive(const std::string& password, const unsigned char* salt,
            size_t salt_size, int iterations,
            std::array<unsigned char, kDigestBytes>* digest) {
    return digest && iterations > 0 &&
           PKCS5_PBKDF2_HMAC(password.data(),
                             static_cast<int>(password.size()), salt,
                             static_cast<int>(salt_size), iterations,
                             EVP_sha256(), static_cast<int>(digest->size()),
                             digest->data()) == 1;
}

bool IsLegacyMd5(const std::string& encoded) {
    if (encoded.size() != 32) return false;
    for (unsigned char c : encoded) {
        if (!std::isxdigit(c)) return false;
    }
    return true;
}

bool Md5Hex(const std::string& value, std::string* output) {
    if (!output) return false;
    EVP_MD_CTX* ctx = EVP_MD_CTX_new();
    if (!ctx) return false;
    unsigned char digest[EVP_MAX_MD_SIZE];
    unsigned int digest_size = 0;
    const bool ok = EVP_DigestInit_ex(ctx, EVP_md5(), nullptr) == 1 &&
                    EVP_DigestUpdate(ctx, value.data(), value.size()) == 1 &&
                    EVP_DigestFinal_ex(ctx, digest, &digest_size) == 1;
    EVP_MD_CTX_free(ctx);
    if (!ok) return false;
    *output = HexEncode(digest, digest_size);
    return true;
}

}  // namespace

bool ConstantTimeEquals(const std::string& lhs, const std::string& rhs) {
    if (lhs.size() != rhs.size()) return false;
    if (lhs.empty()) return true;
    return CRYPTO_memcmp(lhs.data(), rhs.data(), lhs.size()) == 0;
}

bool HashPassword(const std::string& password, std::string* encoded,
                  std::string* error) {
    if (!encoded || password.empty()) {
        if (error) *error = "password is empty";
        return false;
    }
    std::array<unsigned char, kSaltBytes> salt{};
    if (RAND_bytes(salt.data(), static_cast<int>(salt.size())) != 1) {
        if (error) *error = "RAND_bytes failed";
        return false;
    }
    std::array<unsigned char, kDigestBytes> digest{};
    if (!Derive(password, salt.data(), salt.size(), kPbkdf2Iterations,
                &digest)) {
        if (error) *error = "PBKDF2 failed";
        return false;
    }
    *encoded = std::string(kPrefix) + "$" +
               std::to_string(kPbkdf2Iterations) + "$" +
               HexEncode(salt.data(), salt.size()) + "$" +
               HexEncode(digest.data(), digest.size());
    return true;
}

bool VerifyPassword(const std::string& password, const std::string& encoded,
                    bool* needs_rehash, std::string* error) {
    if (needs_rehash) *needs_rehash = false;
    if (password.empty() || encoded.empty()) return false;

    if (IsLegacyMd5(encoded)) {
        std::string legacy;
        if (!Md5Hex(password, &legacy)) {
            if (error) *error = "legacy MD5 verification failed";
            return false;
        }
        const bool ok = ConstantTimeEquals(legacy, encoded);
        if (ok && needs_rehash) *needs_rehash = true;
        return ok;
    }

    std::vector<std::string> parts;
    std::stringstream ss(encoded);
    std::string part;
    while (std::getline(ss, part, '$')) parts.push_back(part);
    if (parts.size() != 4 || parts[0] != kPrefix) return false;

    char* end = nullptr;
    long parsed_iterations = std::strtol(parts[1].c_str(), &end, 10);
    if (!end || *end != '\0' || parsed_iterations <= 0 ||
        parsed_iterations > 5000000) {
        return false;
    }
    std::vector<unsigned char> salt;
    std::vector<unsigned char> expected;
    if (!HexDecode(parts[2], &salt) || !HexDecode(parts[3], &expected) ||
        expected.size() != kDigestBytes) {
        return false;
    }
    std::array<unsigned char, kDigestBytes> actual{};
    if (!Derive(password, salt.data(), salt.size(),
                static_cast<int>(parsed_iterations), &actual)) {
        if (error) *error = "PBKDF2 failed";
        return false;
    }
    const bool ok =
        CRYPTO_memcmp(actual.data(), expected.data(), expected.size()) == 0;
    if (ok && needs_rehash && parsed_iterations < kPbkdf2Iterations) {
        *needs_rehash = true;
    }
    return ok;
}

bool GenerateSecureToken(std::string* token, std::string* error) {
    if (!token) return false;
    std::array<unsigned char, 32> bytes{};
    if (RAND_bytes(bytes.data(), static_cast<int>(bytes.size())) != 1) {
        if (error) *error = "RAND_bytes failed";
        return false;
    }
    *token = HexEncode(bytes.data(), bytes.size());
    return true;
}

}  // namespace sparkpush
