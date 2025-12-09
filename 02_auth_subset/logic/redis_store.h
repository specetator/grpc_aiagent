#pragma once

#include <string>

#include "redis_pool.h"

namespace sparkpush {

// 封装 Redis 访问：token 存储、用户路由信息
class RedisStore {
   public:
    explicit RedisStore(RedisConnectionPool* pool) : pool_(pool) {}

    // 功能：写入 token 映射
    // 参数：token 字符串；user_id 用户；ttl_seconds 过期秒
    // 返回：成功 true
    bool SetToken(const std::string& token, int64_t user_id, int ttl_seconds);
    
    // 功能：通过 token 获取 user_id
    bool GetUserIdByToken(const std::string& token, int64_t* user_id);

    // 功能：添加/移除用户路由 comet
    bool AddRoute(int64_t user_id, const std::string& comet_id);
    bool RemoveRoute(int64_t user_id, const std::string& comet_id);

   private:
    RedisConnectionPool* pool_;
};

}  // namespace sparkpush
