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

-- 房间表（群组表）：存储房间基本信息
CREATE TABLE IF NOT EXISTS `im_group` (
  `id` BIGINT NOT NULL AUTO_INCREMENT COMMENT '房间 ID',
  `name` VARCHAR(128) NOT NULL DEFAULT '' COMMENT '房间名称',
  `owner_id` BIGINT NOT NULL COMMENT '创建者/房主用户 ID',
  `group_type` TINYINT NOT NULL DEFAULT 1 COMMENT '房间类型：1=聊天室，2=弹幕房间（预留）',
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  PRIMARY KEY (`id`),
  KEY `idx_owner` (`owner_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='房间/群组表';

-- 房间成员表：存储房间成员关系（join/leave 的持久化）
CREATE TABLE IF NOT EXISTS `group_member` (
  `id` BIGINT NOT NULL AUTO_INCREMENT,
  `group_id` BIGINT NOT NULL COMMENT '房间 ID',
  `user_id` BIGINT NOT NULL COMMENT '成员用户 ID',
  `role` VARCHAR(16) NOT NULL DEFAULT 'member' COMMENT '角色：owner/admin/member',
  `join_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '加入时间',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_group_user` (`group_id`, `user_id`),
  KEY `idx_user` (`user_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='房间成员表';
