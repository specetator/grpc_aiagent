-- Additive; existing CANN tasks, requests and runtime sessions are preserved.
CREATE TABLE IF NOT EXISTS room_agent_binding (
 room_id BIGINT NOT NULL,
 agent_id VARCHAR(48) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
 installed BOOLEAN NOT NULL DEFAULT TRUE,
 generation BIGINT NOT NULL DEFAULT 1,
 installed_by BIGINT NOT NULL,
 PRIMARY KEY(room_id,agent_id)
) ENGINE=InnoDB;
CREATE TABLE IF NOT EXISTS room_agent_dispatch (
 request_id VARCHAR(121) CHARACTER SET ascii COLLATE ascii_bin PRIMARY KEY,
 source_msg_id VARCHAR(160) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
 session_id VARCHAR(128) NOT NULL,
 message_seq BIGINT NOT NULL,
 published TINYINT NOT NULL DEFAULT 0,
 attempted BOOLEAN NOT NULL DEFAULT FALSE,
 error VARCHAR(240) NOT NULL DEFAULT '',
 created_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
 INDEX pending_dispatch(published,created_at)
) ENGINE=InnoDB;
CREATE TABLE IF NOT EXISTS room_collaboration_task (
 task_id VARCHAR(32) CHARACTER SET ascii COLLATE ascii_bin PRIMARY KEY,
 room_id BIGINT NOT NULL,
 revision BIGINT NOT NULL,
 payload JSON NOT NULL,
 created_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
 INDEX room_tasks(room_id,created_at)
) ENGINE=InnoDB;
CREATE TABLE IF NOT EXISTS room_collaboration_task_history (
 task_id VARCHAR(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
 revision BIGINT NOT NULL,
 payload JSON NOT NULL,
 PRIMARY KEY(task_id,revision)
) ENGINE=InnoDB;
