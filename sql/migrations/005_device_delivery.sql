-- Non-destructive reliability upgrade. Logic/Job also ensure these tables at startup.
CREATE TABLE IF NOT EXISTS device_session_state (
  user_id BIGINT NOT NULL,
  device_id VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  session_id VARCHAR(128) NOT NULL,
  received_seq BIGINT NOT NULL DEFAULT 0,
  updated_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY(user_id,device_id,session_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE TABLE IF NOT EXISTS device_receipt (
  user_id BIGINT NOT NULL,
  device_id VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  session_id VARCHAR(128) NOT NULL,
  msg_seq BIGINT NOT NULL,
  PRIMARY KEY(user_id,device_id,session_id,msg_seq)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE TABLE IF NOT EXISTS delivery_outbox (
  msg_id VARCHAR(160) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  session_id VARCHAR(128) NOT NULL, msg_seq BIGINT NOT NULL,
  payload MEDIUMBLOB NOT NULL,
  attempts INT NOT NULL DEFAULT 0,
  next_attempt_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  lease_token VARCHAR(96) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT '',
  lease_until DATETIME(3) NULL, completed_at DATETIME(3) NULL,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  PRIMARY KEY(msg_id), KEY idx_delivery_ready(completed_at,next_attempt_at),
  KEY idx_delivery_session(session_id,completed_at,msg_seq)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
-- Existing databases: apply once, or let Logic's index existence check add it.
-- ALTER TABLE message ADD INDEX idx_message_client(session_id,sender_id,client_msg_id);
