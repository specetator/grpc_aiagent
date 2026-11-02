-- Apply after 20261101_room_media.sql. Immutable assignment/revision history
-- fences lost responses and expired workers. No old data is removed.
CREATE TABLE IF NOT EXISTS room_media_claim (
  claim_key CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL PRIMARY KEY,
  revision_id CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT '',
  pipeline_sha CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
) ENGINE=InnoDB;
CREATE TABLE IF NOT EXISTS room_media_revision (
  revision_id CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL PRIMARY KEY,
  asset_id VARCHAR(72) NOT NULL,
  attempt INT UNSIGNED NOT NULL,
  pipeline_sha CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  lease_token CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  state VARCHAR(16) NOT NULL,
  manifest_sha CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT '',
  fragment_count INT UNSIGNED NOT NULL DEFAULT 0,
  text_bytes BIGINT UNSIGNED NOT NULL DEFAULT 0,
  line_count BIGINT UNSIGNED NOT NULL DEFAULT 0,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  UNIQUE(asset_id,attempt)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE TABLE IF NOT EXISTS room_media_fragment (
  revision_id CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  fragment_index INT UNSIGNED NOT NULL,
  sha256 CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  payload JSON NOT NULL,
  text MEDIUMTEXT NOT NULL,
  text_bytes INT UNSIGNED NOT NULL,
  PRIMARY KEY(revision_id,fragment_index)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE TABLE IF NOT EXISTS room_media_publication (
  asset_id VARCHAR(72) NOT NULL PRIMARY KEY,
  revision_id CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL UNIQUE,
  published_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
