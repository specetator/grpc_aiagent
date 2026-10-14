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

// 12 万次迭代：对齐 OWASP 对 PBKDF2-HMAC-SHA256 的推荐下限。
// 目的是把单次校验的耗时抬到几十毫秒量级，使离线暴力破解不可行；
// 同时不至于慢到影响登录体验。提高它会触发存量口令的 needs_rehash。
constexpr int kPbkdf2Iterations = 120000;
constexpr size_t kSaltBytes = 16;    // 128 bit salt，足以避开碰撞与预计算
constexpr size_t kDigestBytes = 32;  // SHA-256 原生输出长度
constexpr const char* kPrefix = "pbkdf2_sha256";   // 格式识别标志

std::string HexEncode(const unsigned char* data, size_t size) {
    static constexpr char kHex[] = "0123456789abcdef";
    std::string out(size * 2, '0');
    for (size_t i = 0; i < size; ++i) {
        out[i * 2] = kHex[(data[i] >> 4) & 0x0f];
        out[i * 2 + 1] = kHex[data[i] & 0x0f];
    }
    return out;
}

// 十六进制解码。严格校验：长度为奇数或含非 hex 字符均返回 false。
//
// 这里必须严格而不能宽容：encoded 来自数据库，格式错乱意味着数据
// 被损坏或被注入。静默忽略脏字符会让校验在一个错误的 salt 上运行，
// 结果不可预测。
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

// PBKDF2 派生的薄封装。
//
// iterations > 0 的校验不能省：这个值在 VerifyPassword 里是从数据库
// 字符串解析出来的，传 0 或负数给 PKCS5_PBKDF2_HMAC 是未定义行为，
// 某些 OpenSSL 版本下会直接返回成功而产出全零摘要。
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

// 识别遗留的裸 MD5 哈希：恰好 32 个十六进制字符。
//
// 判定很宽松（只看长度与字符集），但因为 PBKDF2 格式带 "pbkdf2_sha256$"
// 前缀且长度远超 32，两者不会误判。保留它是为了兼容旧教学版本的
// 存量账号，让它们能登录并被自动升级，而不是直接失效。
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

// 定长比较。长度不同时直接返回 false：长度本身不算机密，
// 而 CRYPTO_memcmp 要求两边等长。
//
// 这个函数是内部 RPC 鉴权（internal_rpc.h）与口令校验的共同底座，
// 两处都依赖它的恒定时间特性。
bool ConstantTimeEquals(const std::string& lhs, const std::string& rhs) {
    if (lhs.size() != rhs.size()) return false;
    if (lhs.empty()) return true;
    return CRYPTO_memcmp(lhs.data(), rhs.data(), lhs.size()) == 0;
}

// 生成口令哈希：随机 salt -> PBKDF2 -> 拼成自描述字符串。
//
// RAND_bytes 失败时直接返回 false，不退回任何弱随机源。
// 空口令也拒绝：空口令哈希仍能算出来，但那意味着允许无密码账号。
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

// 校验口令，兼容遗留 MD5 与 PBKDF2 两种存储格式。
//
// 开头先把 needs_rehash 置 false：调用方可能不看返回值就读这个指针，
// 未初始化会造成「随机触发重哈希」的难查 bug。
bool VerifyPassword(const std::string& password, const std::string& encoded,
                    bool* needs_rehash, std::string* error) {
    if (needs_rehash) *needs_rehash = false;
    if (password.empty() || encoded.empty()) return false;

    // 分支一：遗留 MD5。校验通过后置 needs_rehash，让调用方把它
    // 升级成 PBKDF2。这是存量弱哈希的迁移通道。
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

    // 分支二：PBKDF2。按 '$' 拆四段，前缀不符则拒。
    std::vector<std::string> parts;
    std::stringstream ss(encoded);
    std::string part;
    while (std::getline(ss, part, '$')) parts.push_back(part);
    if (parts.size() != 4 || parts[0] != kPrefix) return false;

    // iterations 从存储值解析，因此必须做上下界校验：
    //   <= 0        会让 PKCS5_PBKDF2_HMAC 进入未定义行为
    //   > 5000000   是 DoS 防护：encoded 来自数据库，若被注入一个
    //               巨大迭代数，每次登录都会把 CPU 占满几十分钟。
    // strtol 后检查 *end != '\0' 是为了拒绝 "100abc" 这类部分合法的输入。
    char* end = nullptr;
    long parsed_iterations = std::strtol(parts[1].c_str(), &end, 10);
    if (!end || *end != '\0' || parsed_iterations <= 0 ||
        parsed_iterations > 5000000) {
        return false;
    }
    std::vector<unsigned char> salt;
    std::vector<unsigned char> expected;
    // expected.size() 必须等于 kDigestBytes：不等说明存储被损坏，
    // 拿它去 CRYPTO_memcmp 会读越界。
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
    // 校验通过但存储的迭代数低于当前默认值 -> 标记升级。
    // 这就是「提高 kPbkdf2Iterations 后存量用户仍能登录并逐步变强」
    // 的实现点。注意只在校验成功时置位，否则会给错口令白做一次重哈希。
    if (ok && needs_rehash && parsed_iterations < kPbkdf2Iterations) {
        *needs_rehash = true;
    }
    return ok;
}

// 生成 256 bit 会话 Token（输出 64 位 hex）。
// 用 std::array 而不是 vector：栈上分配，避开堆，且尺寸编译期固定。
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
