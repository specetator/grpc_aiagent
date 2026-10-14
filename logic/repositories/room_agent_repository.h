#pragma once
#include "mysql_pool.h"
#include "group_dao.h"
#include "services/room_agent_service.h"
namespace sparkpush {
// RoomAgentRepository 的 MySQL 实现（房间共享 Agent 的持久层）。
//
// 三张表，均在启动时由 EnsureSchema 用 CREATE TABLE IF NOT EXISTS 建：
//   room_agent_task       (room_id PK, revision, payload)
//                         当前任务状态，一房一行，revision 做乐观锁
//   room_agent_checkpoint (room_id, revision, payload; PK(room_id,revision))
//                         每个版本留一行，只增不改，用于审计与回溯
//   room_agent_request    (request_id PK, room_id, message_seq, payload, result)
//                         一次 Agent 调用的请求与结果，靠 request_id 幂等
//
// 与其余 DAO 的一个区别：本类**自己管 DDL**（EnsureSchema），而
// message_reservation 那套用的是 CheckSchema（表不在就报错退出，要求
// 显式跑 sql/migrations/）。两种做法共存是有意的：房间 Agent 是可选特性，
// 自带建表让它能自包含地开关；而 outbox 表一旦建错就会造成消息丢失，
// 必须走人工审阅的迁移脚本。两者均在 main.cpp 启动序列里，失败直接退出。
//
// 本文件排版比工程其余部分紧凑得多（一行多个语句），是有意为之：
// 所有方法都是“拼 SQL -> Exec -> 读回校验”三步，摊开写会把真正的
// 关键信息（乐观锁、幂等比对）淡在模板代码里。
class MySqlRoomAgentRepository : public RoomAgentRepository {
 public:
    // groups_ / members_ 不是用来读写的，而是用来做 Access 判定：
    // 房间拥有者取自 im_group.owner_id，成员关系取自 group_member。
    // 两个依赖都是借用（不拥有），生命周期由 main.cpp 保证长于本对象。
    MySqlRoomAgentRepository(MySqlConnectionPool* pool, GroupDao* groups,
                            GroupMemberDao* members) : pool_(pool), groups_(groups), members_(members) {}
    // 建三张表。幂等（IF NOT EXISTS），因此重启安全。
    // 注意它不是 RoomAgentRepository 接口的一部分（没有 override），
    // 因为只有生产实现需要建表，测试假实现不需要。
    bool EnsureSchema(std::string* error);
    bool Access(int64_t, int64_t, int64_t*, bool*, std::string*) override;
    bool Load(int64_t, nlohmann::json*, std::string*) override;
    bool Save(int64_t, int64_t, const nlohmann::json&, std::string*) override;
    bool Request(const std::string&, nlohmann::json*, std::string*) override;
    bool PutRequest(const nlohmann::json&, std::string*) override;
    bool Finish(const std::string&, const nlohmann::json&, std::string*) override;
    bool Requests(int64_t, nlohmann::json*, std::string*) override;
 private:
    MySqlConnectionPool* pool_;
    GroupDao* groups_;
    GroupMemberDao* members_;
};
}  // namespace sparkpush
