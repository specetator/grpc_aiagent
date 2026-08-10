-- 已存在的开发库执行一次即可；新库由 sql/schema.sql 直接创建该字段。
ALTER TABLE `user_session_state`
  ADD COLUMN IF NOT EXISTS `delivered_seq` BIGINT NOT NULL DEFAULT 0
  AFTER `read_seq`;
