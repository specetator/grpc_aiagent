-- 用户中心增量迁移。该文件用于生产迁移工具执行一次；不要在生产环境盲目重复执行。
-- Logic 启动时会做同等的幂等检查，方便本 Demo 使用已有 Docker 数据卷直接升级。

ALTER TABLE `user`
  ADD COLUMN `status` TINYINT NOT NULL DEFAULT 1 AFTER `password_hash`;

ALTER TABLE `user`
  ADD COLUMN `deleted_at` DATETIME(3) NULL DEFAULT NULL AFTER `status`;

ALTER TABLE `user`
  ADD COLUMN `updated_at` DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
    ON UPDATE CURRENT_TIMESTAMP(3) AFTER `created_at`;

CREATE INDEX `idx_user_status` ON `user` (`status`, `deleted_at`);

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
