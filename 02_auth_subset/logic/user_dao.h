// ============================================================================
// 用户数据访问对象（DAO）
// 
// 封装用户表的 CRUD 操作，当前实现为单库单表模式
// 
// 数据库表结构：
//   CREATE TABLE user (
//     id BIGINT PRIMARY KEY AUTO_INCREMENT,
//     account VARCHAR(64) UNIQUE NOT NULL,
//     name VARCHAR(64) NOT NULL,
//     password_hash VARCHAR(128) NOT NULL,
//     created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
//   );
// 
// 设计考虑：
// - 使用 MySQL 预编译语句（Prepared Statement）防止 SQL 注入
// - 密码存储为 hash 值，不存储明文
// - 预留分库分表扩展能力（可在此层添加路由逻辑）
// ============================================================================
#pragma once

#include <string>
#include "mysql_pool.h"

namespace sparkpush {

// 用户数据模型：对应数据库 user 表的一行记录
struct User {
    int64_t id{0};               // 用户 ID（主键）
    std::string account;         // 账号（唯一）
    std::string name;            // 昵称
    std::string password_hash;   // 密码 hash（通常为 MD5 或 bcrypt）
};

// 用户 DAO：负责用户数据的持久化操作
class UserDao {
   public:
    // 构造函数：注入 MySQL 连接池
    explicit UserDao(MySqlConnectionPool* pool) : pool_(pool) {}

    // 创建新用户
    // @param account: 账号，必须唯一
    // @param name: 昵称
    // @param password: 密码 hash（客户端已做 hash）
    // @param user_id: 输出参数，返回新创建的用户 ID
    // @param err_msg: 输出参数，错误消息
    // @return: 成功返回 true，失败返回 false
    bool CreateUser(const std::string& account, const std::string& name,
                    const std::string& password, int64_t* user_id,
                    std::string* err_msg);

    // 按账号查询用户
    // @param account: 要查询的账号
    // @param user: 输出参数，返回用户信息
    // @param err_msg: 输出参数，错误消息
    // @return: 找到用户返回 true，未找到或出错返回 false
    bool GetUserByAccount(const std::string& account, User* user,
                          std::string* err_msg);

    // 按 ID 查询用户
    // @param user_id: 用户 ID
    // @param user: 输出参数，返回用户信息
    // @param err_msg: 输出参数，错误消息
    // @return: 找到用户返回 true，未找到或出错返回 false
    bool GetUserById(int64_t user_id, User* user, std::string* err_msg);

   private:
    MySqlConnectionPool* pool_;  // MySQL 连接池
};

}  // namespace sparkpush
