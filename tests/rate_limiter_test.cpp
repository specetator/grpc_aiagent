#include "rate_limiter.h"

#include <iostream>

int main() {
    sparkpush::RateLimitConfig config;
    config.single_rate_per_sec = 1000.0;
    config.single_burst = 2.0;
    config.group_rate_per_sec = 1000.0;
    config.group_burst = 1.0;
    config.broadcast_rate_per_sec = 1000.0;
    config.broadcast_burst = 1.0;

    sparkpush::SceneRateLimiter limiter(config);
    if (!limiter.Allow("single", "sender-1") ||
        !limiter.Allow("single", "sender-1") ||
        limiter.Allow("single", "sender-1")) {
        std::cerr << "single token bucket burst failed\n";
        return 1;
    }
    // 不同 scene / key 不能互相消耗令牌。
    if (!limiter.Allow("group", "room-1") ||
        !limiter.Allow("broadcast", "all")) {
        std::cerr << "scene buckets are not isolated\n";
        return 1;
    }
    return 0;
}
