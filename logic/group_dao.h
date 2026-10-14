#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "model.h"
#include "mysql_pool.h"

namespace sparkpush {

// 对应 sql/schema.sql 中的 im_group / group_member 表，当前仅使用 group_type=1
// 作为聊天室。
//
// 安全注意：本文件是工程里仅有的两处**用字符串拼接而不是占位符绑定**
// 拼 SQL 的地方之一（另一处是 danmaku_dao.cpp）。大多数拼接的是 int64，
// 不存在注入面；但 CreateChatroom 把房间名直接拼进单引号里且**完全没转义**。
// 目前靠上游限制风险：那个接口是 /api/admin/chatroom/create，
// 有 requireAdmin 门禁，因此需要管理员权限才能触发。
// 把本 DAO 暴露给普通用户之前，必须先改成占位符绑定。
class GroupDao {
   public:
    explicit GroupDao(MySqlConnectionPool* pool) : pool_(pool) {}

    // 功能：创建聊天室（group_type=1）
    // 参数：name 房间名；owner_id 创建者；group_id 输出新房间 id；err_msg
    // 错误信息 返回：成功 true，失败 false
    // group_id 靠 mysql_insert_id 取回（表是 AUTO_INCREMENT）。
    bool CreateChatroom(const std::string& name, int64_t owner_id, int64_t* group_id,
                        std::string* err_msg);

    // 功能：按 id 查询聊天室信息
    // 参数：group_id 房间 id；g 输出房间结构；err_msg 错误信息
    // 返回：成功 true，失败 false
    bool GetGroup(int64_t group_id, ImGroup* g, std::string* err_msg);

    // 功能：分页列出聊天室列表
    // 参数：offset 偏移；limit 数量；groups 输出列表；err_msg 错误信息
    // 返回：成功 true，失败 false
    bool ListChatrooms(int offset, int limit, std::vector<ImGroup>* groups,
                       std::string* err_msg);

   private:
    MySqlConnectionPool* pool_;
};

// 聊天室成员关系。这张表同时承担两个职责，读代码时要分清：
//   订阅关系  用户进/出房间（ListUserChatrooms 用它算该给谁推广播）
//   授权依据  IsMember 是历史/离线同步的对象级鉴权
// 后者尤其关键：房间消息的可见范围就是本表，不能只靠「知道 room_id」就放行。
class GroupMemberDao {
   public:
    explicit GroupMemberDao(MySqlConnectionPool* pool) : pool_(pool) {}

    // 功能：添加或更新聊天室成员角色
    // 参数：group_id 房间；user_id 用户；role 角色；err_msg 错误信息
    // 返回：成功 true，失败 false
    //
    // 名实不符（阅读实现前先看这里）：函数名与实现里的注释都说
    // 「INSERT IGNORE，再按需更新 role」，但代码**只有 INSERT IGNORE，
    // 没有任何 UPDATE**。因此成员已存在时 role 会被静默忽略，
    // 传一个新 role 进来既不会生效也不会报错。
    // 需要真改角色时请先补上 UPDATE 分支，或改名为 AddMemberIfAbsent。
    bool AddOrUpdateMember(int64_t group_id, int64_t user_id, int role,
                           std::string* err_msg);

    // 功能：从成员表移除用户（取消订阅）
    // 参数：group_id 房间；user_id 用户；err_msg 错误信息
    // 返回：成功 true，失败 false
    // 幂等：行不存在时 DELETE 影响 0 行，仍然返回 true。
    bool RemoveMember(int64_t group_id, int64_t user_id, std::string* err_msg);

    // 功能：查询用户订阅的聊天室 ID（仅 group_type=1）
    // 参数：user_id 用户；group_ids 输出房间 id；err_msg 错误信息
    // 返回：成功 true，失败 false
    // JOIN im_group 而不是直接读 group_member：成员行可能是历史遗留的
    // 普通群（group_type!=1），不过滤就会把非聊天室的 id 也当订阅返回。
    bool ListUserChatrooms(int64_t user_id, std::vector<int64_t>* group_ids,
                           std::string* err_msg);

    // 校验用户是否仍订阅指定聊天室，用于历史/离线同步的对象级授权。
    // 注意返回值的两层含义：函数返回 false 是「查询本身失败」，
    // 此时 *is_member 已被置 false（fail-closed）。调用方必须把这两种情况
    // 都当成「无权访问」，绝不能因为查询失败就放行。
    bool IsMember(int64_t group_id, int64_t user_id, bool* is_member,
                  std::string* err_msg);

    // 功能：列出聊天室成员列表
    // 参数：group_id 房间；user_ids 输出成员；err_msg 错误信息
    // 返回：成功 true，失败 false
    // 这个列表就是房间广播的扇出目标集，直接决定了谁能收到消息。
    bool ListRoomMembers(int64_t group_id, std::vector<int64_t>* user_ids,
                         std::string* err_msg);

   private:
    MySqlConnectionPool* pool_;
};

}  // namespace sparkpush
