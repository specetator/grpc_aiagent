-- CANN-only deployments also require the existing immutable publication fence.
-- Existing requests, results, history, checkpoints and workspaces are retained.
CREATE TABLE IF NOT EXISTS room_agent_publication (
 request_id VARCHAR(121) CHARACTER SET ascii COLLATE ascii_bin NOT NULL PRIMARY KEY,
 input_digest CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
 reply JSON NOT NULL,
 created_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
) ENGINE=InnoDB;
