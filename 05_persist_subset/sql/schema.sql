-- 自动创建数据库并初始化用户表
-- 支持重复执行（幂等）
DROP DATABASE IF EXISTS spark_push;
CREATE DATABASE IF NOT EXISTS `spark_push`
  DEFAULT CHARACTER SET utf8mb4
  COLLATE utf8mb4_unicode_ci;

USE `spark_push`;

-- 用户表：账户与密码（演示版使用 MD5，生产环境应使用 bcrypt + 盐值）
CREATE TABLE IF NOT EXISTS `user` (
  `id` BIGINT NOT NULL AUTO_INCREMENT,
  `account` VARCHAR(64) NOT NULL UNIQUE,
  `name` VARCHAR(64) NOT NULL DEFAULT '',
  `password_hash` VARCHAR(128) NOT NULL,
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_account` (`account`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 消息表：持久化聊天消息
CREATE TABLE IF NOT EXISTS `message` (
  `msg_id` VARCHAR(128) NOT NULL,
  `session_id` VARCHAR(64) NOT NULL,
  `msg_seq` BIGINT NOT NULL,
  `sender_id` BIGINT NOT NULL,
  `msg_type` VARCHAR(32) NOT NULL DEFAULT 'text',
  `content_json` LONGTEXT NOT NULL,
  `timestamp_ms` BIGINT NOT NULL,
  `client_msg_id` VARCHAR(128) NOT NULL DEFAULT '',
  PRIMARY KEY (`session_id`, `msg_seq`),
  UNIQUE KEY `uk_msg_id` (`msg_id`),
  UNIQUE KEY `uk_client_msg` (`session_id`, `client_msg_id`),
  KEY `idx_session_time` (`session_id`, `timestamp_ms`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='会话消息表';

   
-- ============================================================================
-- 话题（聊天室房间）表设计 
--
-- 重要说明：
-- - 本 04_room_subset 版本“房间/话题”不落 MySQL，全部落 Redis（见 logic/redis_store.*）。
-- - 这里仅提供将来迁移到 MySQL 的表结构参考，便于后续版本演进。
-- - 若未来启用：建议对 name 做唯一约束（或做软删除 + 唯一约束变体）。
-- ============================================================================
CREATE TABLE IF NOT EXISTS `im_group` (
  `id` BIGINT NOT NULL AUTO_INCREMENT,
  `name` VARCHAR(128) NOT NULL COMMENT '群组/房间名称',
  `owner_id` BIGINT NOT NULL COMMENT '创建者（管理员）user_id',
  `group_type` TINYINT NOT NULL DEFAULT 1 COMMENT '0=normal_group,1=chatroom,2=danmaku_room',
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  KEY `idx_owner` (`owner_id`),
  UNIQUE KEY `uk_group_name` (`name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='群组/聊天室房间表（设计稿）';

-- 话题成员表（设计稿）：记录用户加入/退出关系（便于统计、黑名单等扩展）。
CREATE TABLE IF NOT EXISTS `group_member` (
  `id` BIGINT NOT NULL AUTO_INCREMENT,
  `group_id` BIGINT NOT NULL,
  `user_id` BIGINT NOT NULL,
  `role` TINYINT NOT NULL DEFAULT 0 COMMENT '0=member,1=admin,2=owner',
  `join_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_group_user` (`group_id`, `user_id`),
  KEY `idx_user` (`user_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
