// ============================================================================
// Redis 存储封装
// 
// 提供业务层面的 Redis 访问接口，包括：
// 1. Token 管理：存储用户 token 到 user_id 的映射，支持自动过期
// 2. 路由管理：记录用户当前连接的 comet 节点，用于消息推送时查找用户
// 
// Redis 数据结构设计：
// - token:<token_string> -> user_id (String 类型，带 TTL)
// - route:user:<user_id> -> Set(comet_id1, comet_id2, ...) (Set 类型)
// 
// 说明：
// - 一个用户可能同时在多个设备/浏览器登录，因此路由使用 Set 存储
// - token 使用 SETEX 命令设置过期时间，过期后自动删除
// ============================================================================
#pragma once

#include <string>

#include "redis_pool.h"

namespace sparkpush {

// Redis 存储访问类：封装 token 和路由相关的 Redis 操作
class RedisStore {
   public:
    // 构造函数：注入 Redis 连接池
    explicit RedisStore(RedisConnectionPool* pool) : pool_(pool) {}

    // 存储 token 到 user_id 的映射
    // @param token: 登录时生成的 token 字符串
    // @param user_id: 用户 ID
    // @param ttl_seconds: 过期时间（秒），通常为 24 小时
    // @return: 成功返回 true
    bool SetToken(const std::string& token, int64_t user_id, int ttl_seconds);
    
    // 通过 token 查询对应的 user_id
    // @param token: token 字符串
    // @param user_id: 输出参数，返回用户 ID
    // @return: 找到返回 true，未找到或已过期返回 false
    bool GetUserIdByToken(const std::string& token, int64_t* user_id);

    // 添加用户路由：记录用户在哪个 comet 节点上建立了连接
    // @param user_id: 用户 ID
    // @param comet_id: comet 节点标识
    // @return: 成功返回 true
    bool AddRoute(int64_t user_id, const std::string& comet_id);
    
    // 移除用户路由：用户断开连接时调用
    // @param user_id: 用户 ID
    // @param comet_id: comet 节点标识
    // @return: 成功返回 true
    bool RemoveRoute(int64_t user_id, const std::string& comet_id);

   private:
    RedisConnectionPool* pool_;  // Redis 连接池
};

}  // namespace sparkpush
