-- Additive, rerunnable migration; run against the selected application database.
-- Deploy Logic/Comet together after this migration. No message/Agent state is reset.
SET @has_description = (SELECT COUNT(*) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='im_group' AND COLUMN_NAME='description');
SET @ddl = IF(@has_description=0, 'ALTER TABLE im_group ADD COLUMN description VARCHAR(2048) NOT NULL DEFAULT \'\'', 'SELECT 1');
PREPARE space_migration FROM @ddl; EXECUTE space_migration; DEALLOCATE PREPARE space_migration;
SET @has_visibility = (SELECT COUNT(*) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='im_group' AND COLUMN_NAME='visibility');
-- Existing rows must stay public. Once added, change the default for future rows.
SET @ddl = IF(@has_visibility=0, 'ALTER TABLE im_group ADD COLUMN visibility VARCHAR(8) NOT NULL DEFAULT \'public\'', 'SELECT 1');
PREPARE space_migration FROM @ddl; EXECUTE space_migration; DEALLOCATE PREPARE space_migration;
ALTER TABLE im_group ALTER COLUMN visibility SET DEFAULT 'private';
CREATE TABLE IF NOT EXISTS room_invitation (
  room_id BIGINT NOT NULL,
  user_id BIGINT NOT NULL,
  invited_by BIGINT NOT NULL,
  status VARCHAR(16) NOT NULL,
  updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  PRIMARY KEY(room_id,user_id),
  INDEX user_invitations(user_id,status,updated_at)
) ENGINE=InnoDB;
-- Repair historical owner roles without touching other members or Agent payloads.
INSERT INTO group_member(group_id,user_id,role)
SELECT id,owner_id,2 FROM im_group WHERE group_type=1 AND owner_id>0
ON DUPLICATE KEY UPDATE role=2;
UPDATE group_member gm JOIN im_group g ON g.id=gm.group_id
SET gm.role=0 WHERE g.group_type=1 AND gm.user_id<>g.owner_id AND gm.role=2;
INSERT INTO session(session_id,type,group_id)
SELECT CONCAT('r_',id),2,id FROM im_group WHERE group_type=1
ON DUPLICATE KEY UPDATE session_id=VALUES(session_id);
