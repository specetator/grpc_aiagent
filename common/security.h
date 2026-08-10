#pragma once

#include <string>

namespace sparkpush {

// PBKDF2-SHA256 编码格式：pbkdf2_sha256$iterations$salt_hex$digest_hex。
bool HashPassword(const std::string& password, std::string* encoded,
                  std::string* error = nullptr);

// 支持 PBKDF2；若数据库仍是旧 32 位 MD5，则校验明文的 MD5 并标记升级。
bool VerifyPassword(const std::string& password, const std::string& encoded,
                    bool* needs_rehash = nullptr,
                    std::string* error = nullptr);

// 生成 256 bit 随机 Token，以 64 位十六进制字符串返回。
bool GenerateSecureToken(std::string* token, std::string* error = nullptr);

bool ConstantTimeEquals(const std::string& lhs, const std::string& rhs);

}  // namespace sparkpush
