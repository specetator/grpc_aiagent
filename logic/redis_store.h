#pragma once

#include <chrono>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "redis_pool.h"

namespace sparkpush {

// 封装 Redis 访问：token 存储、用户路由信息。
//
// 读这个类之前先分清「什么是 Redis 权威、什么不是」，否则会误判丢数据的影响：
//
//   数据               Redis 丢了会怎样                     权威在哪
//   登录 token          所有用户被踢下线，需重新登录        Redis（唯一）
//   用户路由 user->comet 在线用户暂时收不到推送，重连后恢复  Redis（唯一）
//   房间路由 room->comet 广播退化到成员表精确扇出（慢）    Redis（唯一）
//   msg_seq 计数器      从 MySQL 历史最大值重建（floor 校准） MySQL
//   在线人数            归零后随上下线逐步恢复              Redis（可丢）
//
// 关键结论：**消息本体与序号的权威在 MySQL，不在 Redis**。
// Redis 上的计数器只是热路径加速，AllocateSessionMsgSeq 的 floor_seq
// 参数就是为了在计数器丢失后从 MySQL 重建而存在的。
class RedisStore {
   public:
    // route_cache_ttl_ms 默认 1000：路由查询在发消息热路径上每条消息都要跑，
    // 1 秒的本地缓存能显著减少 Redis 往返。代价是用户刚重连到另一个 Comet 时，
    // 最多 1 秒内旧路由仍会被使用（消息可能推到一个已空的 Comet）。
    explicit RedisStore(RedisConnectionPool* pool, int route_cache_ttl_ms = 1000)
        : pool_(pool), route_cache_ttl_ms_(route_cache_ttl_ms) {}

    // 功能：写入 token 映射
    // 参数：token 字符串；user_id 用户；ttl_seconds 过期秒
    // 返回：成功 true
    // 同时维护两个键：token:<token> -> user_id（正向查）与
    // user:tokens:<uid> 集合（反向索引，供吊销时一次拿全）。
    // 反向索引是后加的，因此 RevokeUserTokens 必须保留 SCAN 兜底。
    bool SetToken(const std::string& token, int64_t user_id, int ttl_seconds);
    // 功能：通过 token 获取 user_id
    bool GetUserIdByToken(const std::string& token, int64_t* user_id);

    // 撤销用户的全部登录凭证，同时清理在线路由和本地路由缓存。
    // 返回 revoked_count，兼容没有反向索引的旧 token（会用 SCAN 兜底）。
    //
    // 三件事必须一起做，只做前两件会留下一个难查的窗口：
    // 若不清本地路由缓存，已缓存的路由仍会把消息推给一个 token 已失效的用户。
    // SCAN 兜底代价不低（遍历整个 keyspace），但只在升级期的旧 token 上生效。
    bool RevokeUserTokens(int64_t user_id, int* revoked_count = nullptr);

    // 功能：添加/移除/获取用户路由 comet 集合
    // 一个用户可以同时在多个 Comet 上（多设备），因此路由是**集合**而不是单值。
    bool AddRoute(int64_t user_id, const std::string& comet_id);
    // 带代数的路由登记。一个 Lua 脚本里原子完成三件事：
    //   INCR 代数计数器 -> SET 路由令牌为 "<generation>:<token>" -> SADD 路由集
    // 返回的 generation 与 token 必须由调用方保存，下线时原样带回。
    bool AddRouteGeneration(int64_t user, const std::string& comet, int64_t* generation, std::string* token);
    // 带代数校验的摘路由。**只有 generation 与 token 都对得上才会真摘**。
    // 这是防「迟到的下线上报误删新连接路由」的全部机制：
    // 用户快速重连时，旧连接的下线回调可能在新连接建立之后才到达，
    // 不校验代数就会把新路由清掉，造成「明明在线却收不到消息」。
    bool RemoveRouteGeneration(int64_t user, const std::string& comet, int64_t generation, const std::string& token);
    // 不带代数校验的版本。只应该用在确定不存在竞争的场景（如强制踢人），
    // 普通下线路径请用 RemoveRouteGeneration。
    bool RemoveRoute(int64_t user_id, const std::string& comet_id);
    // 带本地缓存（shared_mutex + TTL），对齐 06 优化。
    // 读多写少，因此用 shared_mutex 让多个发送线程能并发读同一张缓存表。
    bool GetUserRoutes(int64_t user_id, std::vector<std::string>* comets);
    // 主动作废某用户的路由缓存。在路由变更（上/下线、吊销）后必须调，
    // 否则变更要等 TTL 过期才生效。
    void InvalidateRouteCache(int64_t user_id);

    // 未读相关：会话最新 seq 与用户已读 seq。
    // 注意这一组是**热路径缓存**，不是权威：真正的已读游标在
    // user_session_state 表（见 UserSessionStateDao）。Redis 丢了只会
    // 让未读数短暂不准，不会丢消息。
    bool SetSessionLastSeq(const std::string& session_id, int64_t last_seq);
    bool GetSessionLastSeq(const std::string& session_id, int64_t* last_seq);
    // 热路径：会话内消息序号原子递增，并以 max 语义更新 last_seq。
    // 不做去重也不做 floor 校准，因此只适用于不需要幂等重试的场景。
    bool IncrSessionMsgSeq(const std::string& session_id, int64_t* new_seq);
    // 原子完成：计数器至少校准到 floor_seq、client_msg_id 去重、分配新 seq、
    // 以 max 语义更新 last_seq。is_new=false 表示命中幂等键，不应再次推送/落库。
    //
    // 这是**发消息热路径的序号分配入口**，五步全在一个 Lua 脚本里
    //（详见 redis_store.cpp 里 kAllocateSeqScript 的逐步注释）。
    // 必须用 Lua 而不是多条命令：拆开后两个并发请求可能读到同一个
    // current 值，各自算出相同的 next_seq，直接撞消息表的唯一索引。
    // floor_seq 由调用方从 MySQL 的 max(msg_seq) 算出，是 Redis 丢数据后的
    // 重建手段：只抬高不降低，因此正常跑时这个分支不生效。
    bool AllocateSessionMsgSeq(const std::string& session_id,
                               int64_t sender_id,
                               const std::string& client_msg_id,
                               int64_t floor_seq,
                               int dedup_ttl_seconds,
                               int64_t* msg_seq,
                               bool* is_new);
    bool SetUserReadSeq(int64_t user_id, const std::string& session_id,
                        int64_t read_seq);
    bool GetUserReadSeq(int64_t user_id, const std::string& session_id,
                        int64_t* read_seq);

    // 聊天室房间路由：room_id -> set<comet_id>
    // 这个集合是房间广播扇出的关键：有了它，Logic 只需往「有人的 Comet」
    // 各发一条，由 Comet 自己按房间成员二次分发；没有它就退化到
    // 「查成员表 + 逐个查用户路由」，慢一个量级。
    // 由 RoomService::ReportRoomJoin/Leave 按引用计数维护。
    bool AddRoomComet(int64_t room_id, const std::string& comet_id);
    bool RemoveRoomComet(int64_t room_id, const std::string& comet_id);
    bool GetRoomComets(int64_t room_id, std::vector<std::string>* comets);

    // 聊天室在线人数：room_id -> online_count
    bool SetRoomOnlineCount(int64_t room_id, int64_t count);
    bool GetRoomOnlineCount(int64_t room_id, int64_t* count);

    // 使用 INCRBY/DECRBY 维护在线人数计数（delta 可为正/负）。
    // 用 INCRBY 而不是 GET+SET：后者在并发上下线时会丢失更新，
    // 人数会慢慢漂移到不真实的值且永远不自我修正。
    bool IncrRoomOnlineCount(int64_t room_id, int64_t delta,
                             int64_t* new_value = nullptr);

    // 按 room_id + comet_id 维度维护计数，用于精确维护 room:comets:{room_id}。
    // new_value 是调用方判断跳变的依据：0->1 时 AddRoomComet，<=0 时 RemoveRoomComet。
    // 这一层计数存在的理由：同一个 Comet 上可能有多个用户在同一房间，
    // 只有最后一个离开时才能把该 Comet 从路由集里摘掉。
    bool IncrRoomCometCount(int64_t room_id, const std::string& comet_id,
                            int64_t delta, int64_t* new_value = nullptr);

   private:
    bool GetUserRoutesFromRedis(int64_t user_id,
                                std::vector<std::string>* comets);

    RedisConnectionPool* pool_;

    // 路由的进程内缓存。只缓存「查到过」的结果，TTL 默认 1 秒。
    struct RouteCacheEntry {
        std::vector<std::string> comets;
        // steady_clock：过期判定只看时间差，用墙上时钟会被 NTP 调整弄坏。
        std::chrono::steady_clock::time_point expire_time;
    };
    std::unordered_map<int64_t, RouteCacheEntry> route_cache_;
    // shared_mutex：路由查询在发消息热路径上极频繁且绝大多数是读，
    // 用读写锁让多个发送线程能并发命中缓存。声明为 mutable 是因为
    // GetUserRoutes 语义上是只读的，但需要写缓存。
    mutable std::shared_mutex route_cache_mutex_;
    int route_cache_ttl_ms_{1000};
};

}  // namespace sparkpush
