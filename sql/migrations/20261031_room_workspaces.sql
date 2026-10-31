-- Additive workspace authorization. Empty project_id is a revocation tombstone.
CREATE TABLE IF NOT EXISTS room_workspace (
 room_id BIGINT NOT NULL PRIMARY KEY,
 project_id VARCHAR(48) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT '',
 generation BIGINT NOT NULL,
 updated_by BIGINT NOT NULL,
 updated_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)
) ENGINE=InnoDB;

-- First publication decision is immutable across Kafka retries and revocation.
CREATE TABLE IF NOT EXISTS room_agent_publication (
 request_id VARCHAR(121) CHARACTER SET ascii COLLATE ascii_bin NOT NULL PRIMARY KEY,
 input_digest CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
 reply JSON NOT NULL,
 created_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
) ENGINE=InnoDB;
