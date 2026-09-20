-- Image attachments for IM and Agent screenshot Q&A.
-- Logic also creates this table on startup if it is missing.
USE `spark_push`;

CREATE TABLE IF NOT EXISTS `attachment` (
  `id` VARCHAR(72) NOT NULL,
  `owner_user_id` BIGINT NOT NULL,
  `session_id` VARCHAR(128) NOT NULL DEFAULT '',
  `display_name` VARCHAR(256) NOT NULL DEFAULT '',
  `mime` VARCHAR(64) NOT NULL,
  `bytes` INT NOT NULL,
  `sha256` CHAR(64) NOT NULL,
  `storage_key` VARCHAR(128) NOT NULL,
  `status` VARCHAR(16) NOT NULL DEFAULT 'ready',
  `created_at` DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  PRIMARY KEY (`id`),
  KEY `idx_attachment_owner` (`owner_user_id`),
  KEY `idx_attachment_session` (`session_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
