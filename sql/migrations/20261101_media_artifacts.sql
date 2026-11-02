-- Apply after 20261101_media_processing.sql. No original or session is changed.
-- Reservations survive lost SQL responses and file writes. GC releases bytes
-- only after confirmed durable file cleanup. A failed attempt keeps its charge.
CREATE TABLE IF NOT EXISTS room_media_revision_storage (
  revision_id CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL PRIMARY KEY,
  asset_id VARCHAR(72) NOT NULL,
  room_id BIGINT NOT NULL,
  charged_bytes BIGINT UNSIGNED NOT NULL,
  state VARCHAR(16) NOT NULL,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  reclaimed_at DATETIME(3) NULL,
  INDEX room_storage(room_id,state),
  INDEX asset_storage(asset_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE TABLE IF NOT EXISTS room_media_artifact (
  revision_id CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  artifact_index INT UNSIGNED NOT NULL,
  artifact_id VARCHAR(72) CHARACTER SET ascii COLLATE ascii_bin NOT NULL UNIQUE,
  sha256 CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  bytes INT UNSIGNED NOT NULL,
  metadata JSON NOT NULL,
  installed BOOLEAN NOT NULL DEFAULT FALSE,
  PRIMARY KEY(revision_id,artifact_index)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
