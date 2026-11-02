-- Media profiles are additive. Revisions without a profile retain text-v1's
-- exact source reconstruction and original completion digest contract.
CREATE TABLE IF NOT EXISTS room_media_revision_profile (
  revision_id CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL PRIMARY KEY,
  pipeline_kind VARCHAR(32) NOT NULL,
  parser_sha CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  coverage JSON NULL,
  artifact_count INT UNSIGNED NOT NULL DEFAULT 0,
  artifact_bytes BIGINT UNSIGNED NOT NULL DEFAULT 0
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
