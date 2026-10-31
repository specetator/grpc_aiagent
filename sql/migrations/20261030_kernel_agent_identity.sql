-- Rename only the shipped reserved robot labels. Keep IDs, account names,
-- custom labels, passwords, session foreign keys and message history intact.
UPDATE `user`
SET name = 'Kernel Agent'
WHERE id = 900000000001 AND account = 'hermes_bot'
  AND password_hash = '!hermes_bot_reserved!' AND BINARY name = 'Pi Agent';

UPDATE `user`
SET name = 'Kernel Agent · Android'
WHERE id = 900000000201 AND account = 'agent_bot_900000000201'
  AND password_hash = '!hermes_bot_reserved!'
  AND (BINARY name = 'Pi Agent · Android' OR BINARY name = 'Pi · Android');
