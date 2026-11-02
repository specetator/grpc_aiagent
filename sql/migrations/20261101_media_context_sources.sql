-- Additive source receipts; preserves requests, messages and native sessions.
-- A receipt records a server-observed range, not model correctness or client ACK.
CREATE TABLE IF NOT EXISTS room_context_source (
 request_id VARCHAR(121) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
 source_id VARCHAR(68) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
 room_id BIGINT NOT NULL,
 context_generation BIGINT NOT NULL,
 reference JSON NOT NULL,
 created_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
 PRIMARY KEY(request_id,source_id),
 INDEX room_context_sources(room_id,context_generation),
 CONSTRAINT room_context_source_generation CHECK(context_generation>=0 AND context_generation<=9007199254740991)
) ENGINE=InnoDB;
