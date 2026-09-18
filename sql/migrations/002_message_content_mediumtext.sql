-- 保留现有消息内容，解除 message.content_json 的 64 KiB TEXT 上限。
-- 已存在数据库需显式执行一次；不会删除或重写任何消息。
ALTER TABLE `message`
  MODIFY COLUMN `content_json` MEDIUMTEXT NOT NULL;
