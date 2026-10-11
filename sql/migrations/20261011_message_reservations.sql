-- Drain old Kafka persistence events before this migration; all new writers use this allocator.
CREATE TABLE IF NOT EXISTS message_sequence (
  session_id VARCHAR(128) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  last_seq BIGINT NOT NULL,
  PRIMARY KEY(session_id)
) ENGINE=InnoDB;
CREATE TABLE IF NOT EXISTS message_reservation (
  session_id VARCHAR(128) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  msg_seq BIGINT NOT NULL,
  sender_id BIGINT NOT NULL,
  client_msg_id VARBINARY(128) NOT NULL,
  original_message MEDIUMBLOB NOT NULL,
  persist_payload MEDIUMBLOB NULL,
  -- 0 reserved, 1 prepared, 2 Kafka published, 3 MySQL persisted, 4 sealed unused reservation
  stage TINYINT NOT NULL DEFAULT 0,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  PRIMARY KEY(session_id,msg_seq),
  UNIQUE KEY dedup(session_id,sender_id,client_msg_id),
  KEY recovery(stage,created_at)
) ENGINE=InnoDB;
CREATE TABLE IF NOT EXISTS message_delivery_receipt (
  user_id BIGINT NOT NULL,
  session_id VARCHAR(128) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  msg_seq BIGINT NOT NULL,
  PRIMARY KEY(user_id,session_id,msg_seq)
) ENGINE=InnoDB;
-- A historical MAX cursor has no proof that lower messages were delivered. Rescan; clients deduplicate.
UPDATE user_session_state SET delivered_seq=0;
