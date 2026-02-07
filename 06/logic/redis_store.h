#pragma once

#include "redis_pool.h"

#include <string>
#include <vector>
#include <unordered_map>
#include <chrono>
#include <shared_mutex>

namespace sparkpush {

// 封装 Redis 访问：token 存储、用户路由信息
class RedisStore {
 public:
  explicit RedisStore(RedisConnectionPool* pool, int route_cache_ttl_ms = 1000) 
      : pool_(pool), route_cache_ttl_ms_(route_cache_ttl_ms) {}

  bool SetToken(const std::string& token, int64_t user_id, int ttl_seconds);
  bool GetUserIdByToken(const std::string& token, int64_t* user_id);

  // conn_id 粒度的路由管理：一个 user 可以有多条连接（多设备/多标签页）
  // Redis 存储结构: HASH route:user:{uid} { conn_id => comet_id }
  bool AddRoute(int64_t user_id, const std::string& comet_id, const std::string& conn_id);
  bool RemoveRoute(int64_t user_id, const std::string& conn_id);
  // 返回去重后的 comet_id 列表（用于推送路由）
  bool GetUserRoutes(int64_t user_id, std::vector<std::string>* comets);

  // 未读相关：会话最新 seq 与用户已读 seq
  bool SetSessionLastSeq(const std::string& session_id, int64_t last_seq);
  bool GetSessionLastSeq(const std::string& session_id, int64_t* last_seq);
  bool SetUserReadSeq(int64_t user_id,
                      const std::string& session_id,
                      int64_t read_seq);
  bool GetUserReadSeq(int64_t user_id,
                      const std::string& session_id,
                      int64_t* read_seq);

  // 聊天室房间路由：room_id -> set<comet_id>
  bool AddRoomComet(int64_t room_id, const std::string& comet_id);
  bool RemoveRoomComet(int64_t room_id, const std::string& comet_id);
  bool GetRoomComets(int64_t room_id, std::vector<std::string>* comets);

  // 聊天室在线人数：room_id -> online_count
  bool SetRoomOnlineCount(int64_t room_id, int64_t count);
  bool GetRoomOnlineCount(int64_t room_id, int64_t* count);

  // 使用 INCRBY/DECRBY 维护在线人数计数（delta 可为正/负）
  bool IncrRoomOnlineCount(int64_t room_id, int64_t delta, int64_t* new_value = nullptr);

  // 按 room_id + comet_id 维度维护计数，用于精确维护 room:comets:{room_id}
  bool IncrRoomCometCount(int64_t room_id,
                          const std::string& comet_id,
                          int64_t delta,
                          int64_t* new_value = nullptr);

  // 获取session的下一个msg_seq（原子自增）
  bool IncrSessionMsgSeq(const std::string& session_id, int64_t* new_seq);

  // 路由变更时主动失效缓存
  void InvalidateRouteCache(int64_t user_id);

  // 滑动窗口限流：判断指定 key 在 window_ms 内是否超过 max_count
  // 返回 true 表示允许通过，false 表示被限流
  bool CheckRateLimit(const std::string& key, int window_ms, int max_count);

 private:
  // 从Redis查询路由（内部方法）
  bool GetUserRoutesFromRedis(int64_t user_id, std::vector<std::string>* comets);

  RedisConnectionPool* pool_;
  
  // 本地路由缓存
  struct RouteCacheEntry {
    std::vector<std::string> comets;
    std::chrono::steady_clock::time_point expire_time;
  };
  std::unordered_map<int64_t, RouteCacheEntry> route_cache_;
  mutable std::shared_mutex route_cache_mutex_;  // 读写锁：读不互斥
  int route_cache_ttl_ms_;
};

}  // namespace sparkpush


