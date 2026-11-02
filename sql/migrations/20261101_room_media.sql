-- Additive room media upload/processing metadata. Original attachment IDs,
-- image APIs, old messages and native Agent sessions retain their meaning.
ALTER TABLE attachment MODIFY bytes BIGINT UNSIGNED NOT NULL;
ALTER TABLE attachment MODIFY mime VARCHAR(128) NOT NULL;
CREATE TABLE IF NOT EXISTS room_media_asset (
  asset_id VARCHAR(72) NOT NULL PRIMARY KEY,
  ordinal BIGINT UNSIGNED NOT NULL AUTO_INCREMENT UNIQUE,
  upload_key CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL UNIQUE,
  kind VARCHAR(16) NOT NULL,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE TABLE IF NOT EXISTS room_media_chunk (
  asset_id VARCHAR(72) NOT NULL,
  chunk_index INT UNSIGNED NOT NULL,
  bytes INT UNSIGNED NOT NULL,
  sha256 CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  PRIMARY KEY(asset_id,chunk_index)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE TABLE IF NOT EXISTS room_media_job (
  job_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  asset_id VARCHAR(72) NOT NULL UNIQUE,
  pipeline VARCHAR(64) NOT NULL,
  state VARCHAR(16) NOT NULL,
  attempt INT UNSIGNED NOT NULL DEFAULT 0,
  lease_token CHAR(64) NOT NULL DEFAULT '',
  lease_until DATETIME(3) NULL,
  error_code VARCHAR(64) NOT NULL DEFAULT '',
  error_message VARCHAR(256) NOT NULL DEFAULT '',
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  INDEX pending_media(state,lease_until,job_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
