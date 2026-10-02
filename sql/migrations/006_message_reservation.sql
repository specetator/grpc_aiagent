-- Immutable allocation ledger also protects accepted-but-not-yet-persisted messages.
CREATE TABLE IF NOT EXISTS session_sequence_reservation (
  session_id VARBINARY(128) NOT NULL,
  highest_seq BIGINT NOT NULL,
  PRIMARY KEY(session_id)
) ENGINE=InnoDB;
CREATE TABLE IF NOT EXISTS message_reservation (
  session_id VARBINARY(128) NOT NULL,
  sender_id BIGINT NOT NULL,
  client_msg_id VARBINARY(128) NOT NULL,
  msg_seq BIGINT NOT NULL,
  payload MEDIUMBLOB NOT NULL,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  PRIMARY KEY(session_id,sender_id,client_msg_id),
  UNIQUE KEY uk_reserved_seq(session_id,msg_seq)
) ENGINE=InnoDB;
