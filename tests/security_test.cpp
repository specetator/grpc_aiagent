#include "security.h"

#include <iostream>
#include <string>

namespace {

bool Check(bool condition, const char* message) {
    if (!condition) std::cerr << message << '\n';
    return condition;
}

}  // namespace

int main() {
    const std::string password = "correct horse battery staple";
    std::string first;
    std::string second;
    std::string error;
    if (!Check(sparkpush::HashPassword(password, &first, &error),
               "HashPassword(first) failed") ||
        !Check(sparkpush::HashPassword(password, &second, &error),
               "HashPassword(second) failed")) {
        return 1;
    }
    if (!Check(first != password, "password was stored verbatim") ||
        !Check(first != second, "independent hashes reused the same salt") ||
        !Check(first.rfind("pbkdf2_sha256$", 0) == 0,
               "unexpected password encoding") ||
        !Check(sparkpush::VerifyPassword(password, first),
               "correct password rejected") ||
        !Check(!sparkpush::VerifyPassword("wrong", first),
               "wrong password accepted")) {
        return 1;
    }

    bool needs_rehash = false;
    if (!Check(sparkpush::VerifyPassword(
                   "legacy-password", "12121b2b7fdedd5ec5777926650d7119",
                   &needs_rehash),
               "legacy MD5 password rejected") ||
        !Check(needs_rehash, "legacy hash was not marked for upgrade")) {
        return 1;
    }

    std::string token_a;
    std::string token_b;
    if (!Check(sparkpush::GenerateSecureToken(&token_a, &error),
               "GenerateSecureToken(first) failed") ||
        !Check(sparkpush::GenerateSecureToken(&token_b, &error),
               "GenerateSecureToken(second) failed") ||
        !Check(token_a.size() == 64, "unexpected token length") ||
        !Check(token_a != token_b, "tokens were unexpectedly identical")) {
        return 1;
    }
    return 0;
}
