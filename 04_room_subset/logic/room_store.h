#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "redis_store.h"
#include "room_dao.h"

namespace sparkpush {

// RoomStore：组合 MySQL(RoomDao) + Redis(成员列表缓存)
// - MySQL：最终一致的持久化来源
// - Redis：成员列表 read-through 缓存，join/leave 写穿
class RoomStore {
   public:
    // 构造函数：注入 RoomDao + RedisStore，并配置缓存 TTL 与列表读取策略
    // @param members_cache_ttl_seconds: 成员列表缓存 TTL（秒）
    // @param meta_cache_ttl_seconds: 房间元信息缓存 TTL（秒）
    // @param room_list_prefer_redis: 是否优先从 Redis 拉取房间列表（默认
    // false，优先 MySQL）
    RoomStore(RoomDao* dao, RedisStore* redis_store,
              int members_cache_ttl_seconds = 300,
              int meta_cache_ttl_seconds = 600,
              bool room_list_prefer_redis = false)
        : dao_(dao),
          redis_store_(redis_store),
          members_cache_ttl_seconds_(members_cache_ttl_seconds),
          meta_cache_ttl_seconds_(meta_cache_ttl_seconds),
          room_list_prefer_redis_(room_list_prefer_redis) {}

    // 创建房间（写 MySQL）：本版本不做任何“自动加入房间”。
    // 说明：auto_join_all 参数为历史兼容保留，后端会忽略该参数。
    bool CreateRoom(const std::string& name, int64_t owner_id,
                    bool auto_join_all, int64_t* room_id, std::string* err_msg);
    // 拉取房间列表（按配置可能走 MySQL 或 Redis 加速）
    bool ListRooms(std::vector<RoomInfo>* rooms, std::string* err_msg);

    // 用户加入房间：写 MySQL + 刷新/写穿 Redis 成员缓存
    bool JoinRoom(int64_t user_id, int64_t room_id, std::string* err_msg);
    // 用户退出房间：写 MySQL + 刷新/写穿 Redis 成员缓存
    bool LeaveRoom(int64_t user_id, int64_t room_id, std::string* err_msg);

    // 判断用户是否在房间内（优先 Redis/必要时回源 MySQL）
    bool IsUserInRoom(int64_t user_id, int64_t room_id, bool* is_in,
                      std::string* err_msg);
    // 列出房间成员（read-through：缓存 miss 时回源 MySQL 并回填 Redis）
    bool ListRoomMembers(int64_t room_id, std::vector<int64_t>* user_ids,
                         std::string* err_msg);
    // 获取房间成员数（优先 Redis/必要时回源 MySQL）
    bool GetRoomMemberCount(int64_t room_id, int64_t* count,
                            std::string* err_msg);

    // 登录/注册后：自动加入所有已存在房间（MySQL 落库）
    bool AutoJoinAllRoomsForUser(int64_t user_id, std::string* err_msg);

   private:
    // 判断成员缓存是否存在（用于区分“缓存不存在”和“缓存为空集合”）
    bool RoomMembersCacheExists(int64_t room_id, bool* exists);
    // 从 DB 拉取成员列表并回填缓存（read-through 回源路径）
    bool WarmRoomMembersCacheFromDb(int64_t room_id,
                                    std::vector<int64_t>* members_out,
                                    std::string* err_msg);
    // 写入/刷新房间元信息缓存（name/owner_id/created_at_ms/group_type 等）
    void WarmRoomMetaCache(const RoomInfo& room);

    // MySQL DAO：房间元信息与成员关系持久化
    RoomDao* dao_{nullptr};
    // Redis 存储：成员列表与房间元信息缓存
    RedisStore* redis_store_{nullptr};
    // 成员列表缓存 TTL（秒）
    int members_cache_ttl_seconds_{300};
    // 元信息缓存 TTL（秒）
    int meta_cache_ttl_seconds_{600};
    // 是否优先从 Redis 获取房间列表（性能更好但可能与 DB 有短暂不一致）
    bool room_list_prefer_redis_{false};
};

}  // namespace sparkpush
