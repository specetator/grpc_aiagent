-- Additive authorization epoch; no existing request, task or session is rewritten.
-- Missing room row means original epoch 0; missing table must fail closed.
-- Future revocation writers must lock im_group before room_context. Do not expose
-- revocation until native tools, history filtering and final publication agree.
CREATE TABLE IF NOT EXISTS room_context (
 room_id BIGINT NOT NULL PRIMARY KEY,
 generation BIGINT NOT NULL DEFAULT 0,
 updated_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
 CONSTRAINT room_context_generation_range CHECK (generation >= 0 AND generation <= 9007199254740991)
) ENGINE=InnoDB;
