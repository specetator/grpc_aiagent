-- 创建数据库并初始化所有表；不会删除已有业务数据。
-- 如需清空本地开发库，请显式执行 reset_dev.sql 后再执行本文件。
CREATE DATABASE IF NOT EXISTS `spark_push`
  DEFAULT CHARACTER SET utf8mb4
  COLLATE utf8mb4_unicode_ci;

USE `spark_push`;

-- 用户表：账户与密码（后续可扩展 profile 字段、盐值和密码哈希）
CREATE TABLE IF NOT EXISTS `user` (
  `id` BIGINT NOT NULL AUTO_INCREMENT,
  `account` VARCHAR(64) NOT NULL UNIQUE,
  `name` VARCHAR(64) NOT NULL DEFAULT '',
  `password_hash` VARCHAR(128) NOT NULL,
  `status` TINYINT NOT NULL DEFAULT 1,       -- 1=active,2=disabled,3=deleted
  `deleted_at` DATETIME(3) NULL DEFAULT NULL,
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
    ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_account` (`account`),
  KEY `idx_user_status` (`status`, `deleted_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 用户中心审计日志：管理员操作、注册和登录等生命周期事件只追加不更新。
CREATE TABLE IF NOT EXISTS `audit_log` (
  `id` BIGINT NOT NULL AUTO_INCREMENT,
  `actor_user_id` BIGINT NOT NULL DEFAULT 0,
  `target_user_id` BIGINT NOT NULL DEFAULT 0,
  `action` VARCHAR(64) NOT NULL,
  `reason` VARCHAR(512) NOT NULL DEFAULT '',
  `metadata_json` LONGTEXT NOT NULL,
  `created_at` DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  PRIMARY KEY (`id`),
  KEY `idx_audit_target_created` (`target_user_id`, `created_at`),
  KEY `idx_audit_action_created` (`action`, `created_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 会话表：单聊/群聊/聊天室等（当前代码主要使用单聊）
CREATE TABLE IF NOT EXISTS `session` (
  `id` BIGINT NOT NULL AUTO_INCREMENT,
  `session_id` VARCHAR(128) NOT NULL,      -- 业务层的 session_id，如 s_<uid1>_<uid2>
  `type` TINYINT NOT NULL DEFAULT 0,       -- 0=single,1=group,2=chatroom
  `user1_id` BIGINT DEFAULT 0,             -- 单聊一端
  `user2_id` BIGINT DEFAULT 0,             -- 单聊另一端
  `group_id` BIGINT DEFAULT 0,             -- 群聊/聊天室 id
  `last_msg_seq` BIGINT NOT NULL DEFAULT 0,
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_session_id` (`session_id`),
  KEY `idx_user1` (`user1_id`),
  KEY `idx_user2` (`user2_id`),
  KEY `idx_group` (`group_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 消息表：单聊/群聊/聊天室消息
CREATE TABLE IF NOT EXISTS `message` (
  `id` BIGINT NOT NULL AUTO_INCREMENT,
  `session_id` VARCHAR(128) NOT NULL,
  `msg_seq` BIGINT NOT NULL,             -- 会话内自增序号
  `sender_id` BIGINT NOT NULL,
  `msg_type` VARCHAR(32) NOT NULL DEFAULT 'text',
  `content_json` MEDIUMTEXT NOT NULL,
  `timestamp_ms` BIGINT NOT NULL,        -- 发送时间（毫秒）
  `client_msg_id` VARCHAR(128) NOT NULL DEFAULT '',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_session_seq` (`session_id`, `msg_seq`),
  KEY `idx_session` (`session_id`),
  KEY `idx_sender` (`sender_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 用户会话状态表：记录每个用户在每个会话的已读游标
CREATE TABLE IF NOT EXISTS `user_session_state` (
  `id` BIGINT NOT NULL AUTO_INCREMENT,
  `user_id` BIGINT NOT NULL,
  `session_id` VARCHAR(128) NOT NULL,
  `read_seq` BIGINT NOT NULL DEFAULT 0,     -- 已读到的最大 msg_seq
  `delivered_seq` BIGINT NOT NULL DEFAULT 0, -- 已交给客户端连接的最大 msg_seq
  `last_visit_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_user_session` (`user_id`, `session_id`),
  KEY `idx_session` (`session_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 群组表（后续 V2/V3 可用：normal_group / chatroom / danmaku_room）
CREATE TABLE IF NOT EXISTS `im_group` (
  `id` BIGINT NOT NULL AUTO_INCREMENT,
  `name` VARCHAR(128) NOT NULL,
  `owner_id` BIGINT NOT NULL,
  `group_type` TINYINT NOT NULL DEFAULT 0,   -- 0=normal_group,1=chatroom,2=danmaku_room
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  KEY `idx_owner` (`owner_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 群成员表
CREATE TABLE IF NOT EXISTS `group_member` (
  `id` BIGINT NOT NULL AUTO_INCREMENT,
  `group_id` BIGINT NOT NULL,
  `user_id` BIGINT NOT NULL,
  `role` TINYINT NOT NULL DEFAULT 0,         -- 0=member,1=admin,2=owner
  `join_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_group_user` (`group_id`, `user_id`),
  KEY `idx_user` (`user_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 弹幕表（V3：按视频时间轴拉取）
CREATE TABLE IF NOT EXISTS `video_danmaku` (
  `id` BIGINT NOT NULL AUTO_INCREMENT,
  `video_id` VARCHAR(128) NOT NULL,
  `timeline_ms` BIGINT NOT NULL,
  `sender_id` BIGINT NOT NULL,
  `content_json` TEXT NOT NULL,
  `timestamp_ms` BIGINT NOT NULL,
  PRIMARY KEY (`id`),
  KEY `idx_video_timeline` (`video_id`, `timeline_ms`),
  KEY `idx_sender` (`sender_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 广播任务表（可用于记录系统广播）
CREATE TABLE IF NOT EXISTS `broadcast` (
  `id` BIGINT NOT NULL AUTO_INCREMENT,
  `task_id` VARCHAR(128) NOT NULL,
  `title` VARCHAR(256) DEFAULT '',
  `scope` VARCHAR(64) DEFAULT 'all',
  `group_id` BIGINT DEFAULT 0,
  `content_json` TEXT NOT NULL,
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_task_id` (`task_id`),
  KEY `idx_group` (`group_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
