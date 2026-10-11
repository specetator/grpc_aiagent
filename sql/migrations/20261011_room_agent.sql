-- Additive room Agent schema. No DROP/DELETE or migration of private sessions.
CREATE TABLE IF NOT EXISTS room_agent_task (
  room_id BIGINT PRIMARY KEY,
  revision BIGINT NOT NULL,
  payload JSON NOT NULL
) ENGINE=InnoDB;
CREATE TABLE IF NOT EXISTS room_agent_checkpoint (
  room_id BIGINT NOT NULL,
  revision BIGINT NOT NULL,
  payload JSON NOT NULL,
  PRIMARY KEY(room_id, revision)
) ENGINE=InnoDB;
CREATE TABLE IF NOT EXISTS room_agent_request (
  request_id VARCHAR(121) CHARACTER SET ascii COLLATE ascii_bin PRIMARY KEY,
  room_id BIGINT NOT NULL,
  message_seq BIGINT NOT NULL,
  payload JSON NOT NULL,
  result JSON NULL,
  INDEX room_requests(room_id, message_seq)
) ENGINE=InnoDB;
