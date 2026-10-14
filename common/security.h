#pragma once

#include <string>

namespace sparkpush {

// =====================================================================
// 口令哈希与会话 Token 生成，基于 OpenSSL。
//
// 设计原则：所有对外接口都返回 bool + 输出参数，失败时通过 error
// 回传原因，**不抛异常**。调用方（登录/注册路径）需要把失败转成
// HTTP 状态码，异常会穿透到 muduo 回调里导致进程终止。

// PBKDF2-SHA256 编码格式：pbkdf2_sha256$iterations$salt_hex$digest_hex。
//
// 为什么把 iterations 存进字符串而不是写死在代码里：这样将来提高
// 迭代次数时，旧口令仍能按它自己的参数校验，并在下次登录时
// 透明升级（参见 VerifyPassword 的 needs_rehash）。否则一改常量
// 就会让全部存量用户无法登录。
//
// salt 每用户独立随机生成，因此相同口令产生不同哈希，抗彩虹表。
bool HashPassword(const std::string& password, std::string* encoded,
                  std::string* error = nullptr);

// 支持 PBKDF2；若数据库仍是旧 32 位 MD5，则校验明文的 MD5 并标记升级。
//
// needs_rehash 输出参数的用途：登录成功后调用方据此决定是否用
// HashPassword 重算并回写数据库。这让存量弱哈希能在用户下次登录时
// 自动迁移，而不需要强制全员改密码。
//
// 两种情况会置位：遗留 MD5 校验通过；或 PBKDF2 迭代数低于当前默认值。
bool VerifyPassword(const std::string& password, const std::string& encoded,
                    bool* needs_rehash = nullptr,
                    std::string* error = nullptr);

// 生成 256 bit 随机 Token，以 64 位十六进制字符串返回。
//
// 用 RAND_bytes（CSPRNG）而不是 rand()/mt19937：后者的状态可从少量
// 输出推导出来，用于会话 Token 会被劫持。RAND_bytes 失败时返回 false
// 而不是退化到弱随机源——宁可不发 Token 也不能发可预测的 Token。
bool GenerateSecureToken(std::string* token, std::string* error = nullptr);

// 定长比较，用于密钥/哈希比对。
//
// 为什么不能用 ==：std::string 的比较在首个不同字节处提前返回，
// 耗时与前缀匹配长度相关。攻击者反复提交候选值并测量响应时间，
// 就能逐字节猜出密钥（timing attack）。
// 本函数底层用 OpenSSL 的 CRYPTO_memcmp，无论内容如何都扫完全部字节。
//
// 注意：长度不同时仍会立即返回 false（长度本身不是机密），
// 因此调用方不能拿它比较长度可变的秘密。
bool ConstantTimeEquals(const std::string& lhs, const std::string& rhs);

}  // namespace sparkpush
