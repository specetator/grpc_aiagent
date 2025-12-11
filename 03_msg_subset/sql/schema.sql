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

-- 会话表：管理单聊和群聊会话
CREATE TABLE IF NOT EXISTS `session` (
  `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  `session_id` VARCHAR(64) NOT NULL COMMENT '会话ID（单聊：s_1_2，群聊：g_groupid）',
  `type` TINYINT NOT NULL DEFAULT 0 COMMENT '会话类型（0=单聊，1=群聊）',
  `user1_id` BIGINT NOT NULL DEFAULT 0 COMMENT '单聊用户1（user1 < user2）',
  `user2_id` BIGINT NOT NULL DEFAULT 0 COMMENT '单聊用户2',
  `group_id` BIGINT NOT NULL DEFAULT 0 COMMENT '群聊ID',
  `last_msg_seq` BIGINT NOT NULL DEFAULT 0 COMMENT '最后消息序号（用于未读计数）',
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_session_id` (`session_id`),
  KEY `idx_user1` (`user1_id`),
  KEY `idx_user2` (`user2_id`),
  KEY `idx_group` (`group_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='会话表';

-- 消息表：存储所有聊天消息
CREATE TABLE IF NOT EXISTS `message` (
  `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  `msg_id` VARCHAR(128) NOT NULL COMMENT '消息ID（格式：msgid:1:2-123）',
  `session_id` VARCHAR(64) NOT NULL COMMENT '会话ID',
  `msg_seq` BIGINT NOT NULL DEFAULT 0 COMMENT '消息序号（从msg_id提取的严格递增序号）',
  `sender_id` BIGINT NOT NULL COMMENT '发送方用户ID',
  `msg_type` VARCHAR(32) NOT NULL DEFAULT 'text' COMMENT '消息类型（text/image/video/system）',
  `content_json` TEXT NOT NULL COMMENT '消息内容（JSON格式）',
  `timestamp_ms` BIGINT NOT NULL COMMENT '消息时间戳（毫秒）',
  `client_msg_id` VARCHAR(128) DEFAULT NULL COMMENT '客户端消息ID（用于ACK匹配）',
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_msg_id` (`msg_id`),
  KEY `idx_session_seq` (`session_id`, `msg_seq`),
  KEY `idx_session_time` (`session_id`, `timestamp_ms`),
  KEY `idx_sender` (`sender_id`, `timestamp_ms`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='消息内容表';

-- 用户会话状态表：记录用户已读位置（用于未读计数）
CREATE TABLE IF NOT EXISTS `user_session_state` (
  `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  `user_id` BIGINT NOT NULL COMMENT '用户ID',
  `session_id` VARCHAR(64) NOT NULL COMMENT '会话ID',
  `read_seq` BIGINT NOT NULL DEFAULT 0 COMMENT '用户已读位置（消息序号）',
  `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_user_session` (`user_id`, `session_id`),
  KEY `idx_session` (`session_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='用户已读位置表（用于未读计数）';
