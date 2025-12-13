// ============================================================================
// RoomStore：MySQL 持久化 + Redis 成员列表缓存
// ============================================================================
#include "room_store.h"

#include <chrono>

#include "logging.h"

namespace sparkpush {

void RoomStore::WarmRoomMetaCache(const RoomInfo& room) {
    if (!redis_store_) return;
    if (room.id <= 0) return;
    redis_store_->SetRoomMetaCache(room.id, room.name, room.owner_id,
                                   room.created_at_ms, room.group_type,
                                   meta_cache_ttl_seconds_);
}

// 说明：这里用 RedisStore 现有的 room:members:<rid> set 作为“缓存容器”。
// 由于 SISMEMBER/SMEMBERS 无法区分 key 不存在与空集合，这里额外使用 EXISTS
// 判断缓存是否命中；若 miss，则回源 MySQL 并回填 Redis。
bool RoomStore::RoomMembersCacheExists(int64_t room_id, bool* exists) {
    if (!exists) return false;
    *exists = false;
    if (!redis_store_ || room_id <= 0) return false;

    // 复用 redis_store_ 的连接池能力：这里直接新增一个小接口在 RedisStore
    // 实现 EXISTS room:members:<rid>。
    return redis_store_->RoomMembersCacheExists(room_id, exists);
}

bool RoomStore::WarmRoomMembersCacheFromDb(int64_t room_id,
                                           std::vector<int64_t>* members_out,
                                           std::string* err_msg) {
    if (!dao_) {
        if (err_msg) *err_msg = "room dao not initialized";
        return false;
    }
    std::vector<int64_t> members;
    if (!dao_->ListMembers(room_id, &members, err_msg)) {
        return false;
    }

    // 回填 Redis（允许失败：回填失败不影响正确性，只是失去缓存）
    if (redis_store_) {
        redis_store_->ReplaceRoomMembersCache(room_id, members,
                                              members_cache_ttl_seconds_);
    }

    if (members_out) *members_out = std::move(members);
    return true;
}

bool RoomStore::CreateRoom(const std::string& name, int64_t owner_id,
                           bool auto_join_all, int64_t* room_id,
                           std::string* err_msg) {
    // 需求：房间创建仅负责创建；不自动让任何用户加入（包括“全员自动加入”）。
    (void)auto_join_all;
    if (!dao_) {
        if (err_msg) *err_msg = "room dao not initialized";
        return false;
    }
    int64_t rid = 0;
    // group_type: 1=chatroom
    if (!dao_->CreateRoom(name, owner_id, 1, &rid, err_msg)) {
        return false;
    }

    // owner 默认作为 owner 角色加入
    dao_->AddMember(rid, owner_id, 2, nullptr);

    // 回读一次，拿到 created_at_ms 并写入 meta cache（允许失败）
    RoomInfo info;
    if (dao_->GetRoomById(rid, &info, nullptr)) {
        WarmRoomMetaCache(info);
    }

    // 只把 owner 写入缓存（写穿）
    if (redis_store_) {
        redis_store_->JoinRoom(owner_id, rid);
        redis_store_->ExpireRoomMembersCache(rid, members_cache_ttl_seconds_);
    }

    if (room_id) *room_id = rid;
    return true;
}

bool RoomStore::ListRooms(std::vector<RoomInfo>* rooms, std::string* err_msg) {
    if (!dao_) {
        if (err_msg) *err_msg = "room dao not initialized";
        return false;
    }
    if (!rooms) return false;
    rooms->clear();

    // 默认：直接走 MySQL list（最简单且一致性最强）
    if (!room_list_prefer_redis_) {
        if (!dao_->ListRooms(rooms, err_msg)) return false;
        if (redis_store_) {
            for (const auto& r : *rooms) {
                WarmRoomMetaCache(r);
            }
        }
        return true;
    }

    // 开关开启：MySQL 仅取 id 列表，然后尽量从 Redis meta 组装，缺失回源 MySQL
    std::vector<int64_t> ids;
    if (!dao_->ListRoomIds(&ids, err_msg)) return false;

    rooms->reserve(ids.size());
    for (int64_t rid : ids) {
        if (rid <= 0) continue;
        RoomInfo r;
        r.id = rid;

        bool cache_exists = false;
        if (redis_store_ &&
            redis_store_->RoomMetaCacheExists(rid, &cache_exists) &&
            cache_exists) {
            std::string name;
            int64_t owner_id = 0;
            int64_t created_at_ms = 0;
            int group_type = 1;
            if (redis_store_->GetRoomMetaCache(rid, &name, &owner_id,
                                               &created_at_ms, &group_type)) {
                r.name = std::move(name);
                r.owner_id = owner_id;
                r.created_at_ms = created_at_ms;
                r.group_type = group_type;
                rooms->push_back(std::move(r));
                continue;
            }
        }

        // Redis miss/异常：回源 MySQL 并回填
        RoomInfo from_db;
        if (!dao_->GetRoomById(rid, &from_db, err_msg)) {
            // 单个房间失败不阻断整体列表（避免因脏数据/瞬态错误导致整体不可用）
            // 这里选择跳过，并继续下一个。
            continue;
        }
        WarmRoomMetaCache(from_db);
        rooms->push_back(std::move(from_db));
    }

    return true;
}

bool RoomStore::JoinRoom(int64_t user_id, int64_t room_id,
                         std::string* err_msg) {
    if (!dao_) {
        if (err_msg) *err_msg = "room dao not initialized";
        return false;
    }
    if (user_id <= 0 || room_id <= 0) {
        if (err_msg) *err_msg = "invalid user_id/room_id";
        return false;
    }
    if (!dao_->AddMember(room_id, user_id, 0, err_msg)) {
        return false;
    }
    // 写穿缓存（无论 key 是否存在，SADD 都是安全的）
    if (redis_store_) {
        redis_store_->JoinRoom(user_id, room_id);
        redis_store_->ExpireRoomMembersCache(room_id,
                                             members_cache_ttl_seconds_);
    }
    return true;
}

bool RoomStore::LeaveRoom(int64_t user_id, int64_t room_id,
                          std::string* err_msg) {
    if (!dao_) {
        if (err_msg) *err_msg = "room dao not initialized";
        return false;
    }
    if (user_id <= 0 || room_id <= 0) {
        if (err_msg) *err_msg = "invalid user_id/room_id";
        return false;
    }
    if (!dao_->RemoveMember(room_id, user_id, err_msg)) {
        return false;
    }
    if (redis_store_) {
        redis_store_->LeaveRoom(user_id, room_id);
        redis_store_->ExpireRoomMembersCache(room_id,
                                             members_cache_ttl_seconds_);
    }
    return true;
}

bool RoomStore::IsUserInRoom(int64_t user_id, int64_t room_id, bool* is_in,
                             std::string* err_msg) {
    if (!is_in) return false;
    *is_in = false;
    if (!dao_) {
        if (err_msg) *err_msg = "room dao not initialized";
        return false;
    }
    if (user_id <= 0 || room_id <= 0) return true;

    bool cache_exists = false;
    if (RoomMembersCacheExists(room_id, &cache_exists) && cache_exists &&
        redis_store_) {
        return redis_store_->IsUserInRoom(user_id, room_id, is_in);
    }

    // cache miss：回源 MySQL（并顺带回填缓存）
    bool db_in = false;
    if (!dao_->IsMember(room_id, user_id, &db_in, err_msg)) {
        return false;
    }
    *is_in = db_in;

    // 这里选择回填整个成员列表（减少后续 fanout 的 DB 压力）
    WarmRoomMembersCacheFromDb(room_id, nullptr, nullptr);
    return true;
}

bool RoomStore::ListRoomMembers(int64_t room_id, std::vector<int64_t>* user_ids,
                                std::string* err_msg) {
    if (!user_ids) return false;
    user_ids->clear();
    if (!dao_) {
        if (err_msg) *err_msg = "room dao not initialized";
        return false;
    }
    if (room_id <= 0) return true;

    bool cache_exists = false;
    if (RoomMembersCacheExists(room_id, &cache_exists) && cache_exists &&
        redis_store_) {
        if (redis_store_->ListRoomMembers(room_id, user_ids)) {
            return true;
        }
        // Redis 异常时回源
        user_ids->clear();
    }

    return WarmRoomMembersCacheFromDb(room_id, user_ids, err_msg);
}

bool RoomStore::GetRoomMemberCount(int64_t room_id, int64_t* count,
                                   std::string* err_msg) {
    if (!count) return false;
    *count = 0;
    if (!dao_) {
        if (err_msg) *err_msg = "room dao not initialized";
        return false;
    }
    if (room_id <= 0) return true;

    bool cache_exists = false;
    if (RoomMembersCacheExists(room_id, &cache_exists) && cache_exists &&
        redis_store_) {
        if (redis_store_->GetRoomMemberCount(room_id, count)) {
            return true;
        }
    }

    return dao_->CountMembers(room_id, count, err_msg);
}

bool RoomStore::AutoJoinAllRoomsForUser(int64_t user_id, std::string* err_msg) {
    if (!dao_) {
        if (err_msg) *err_msg = "room dao not initialized";
        return false;
    }
    // 落库即可；缓存靠读时回填/写时写穿
    return dao_->AutoJoinAllRoomsForUser(user_id, err_msg);
}

}  // namespace sparkpush
