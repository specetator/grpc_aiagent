#include "persist_handler.h"

#include "logging.h"

#include <algorithm>
#include <chrono>
#include <thread>

namespace sparkpush {

PersistHandler::PersistHandler(const Config& cfg)
    : cfg_(cfg),
      persist_pool_(cfg.mysql_pool_size > 0 ? cfg.mysql_pool_size : 4,
                    "persist_pool") {}

bool PersistHandler::Init() {
  // 初始化MySQL连接池
  if (cfg_.mysql_host.empty()) {
    LogError("[PersistHandler] MySQL host not configured");
    return false;
  }

  MySqlConfig mysql_cfg;
  mysql_cfg.host = cfg_.mysql_host;
  mysql_cfg.port = cfg_.mysql_port;
  mysql_cfg.user = cfg_.mysql_user;
  mysql_cfg.password = cfg_.mysql_password;
  mysql_cfg.db = cfg_.mysql_db;
  mysql_cfg.pool_size = cfg_.mysql_pool_size > 0 ? cfg_.mysql_pool_size : 8;

  mysql_pool_ = std::make_unique<MySqlConnectionPool>();
  if (!mysql_pool_->Init(mysql_cfg)) {
    LogError("[PersistHandler] MySQL pool init failed");
    return false;
  }
  LogInfo("[PersistHandler] MySQL pool initialized");

  // 持久化消费组：spark_push_persist_group（独立于推送消费组）
  if (cfg_.kafka_brokers.empty()) {
    LogError("[PersistHandler] Kafka brokers not configured");
    return false;
  }

  KafkaConsumer::Options opts;
  opts.enable_auto_commit = false;
  std::string persist_group = cfg_.kafka_consumer_group + "_persist";

  if (!persist_consumer_.Init(cfg_.kafka_brokers,
                              persist_group,
                              cfg_.kafka_push_topic,
                              std::bind(&PersistHandler::HandleMessage, this,
                                        std::placeholders::_1, std::placeholders::_2),
                              opts)) {
    LogError("[PersistHandler] Kafka persist consumer init failed");
    return false;
  }
  LogInfo("[PersistHandler] Persist consumer initialized, group=" + persist_group);

  persist_pool_.Start();
  return true;
}

void PersistHandler::Start() {
  persist_consumer_.Start();
  LogInfo("[PersistHandler] Started");
}

void PersistHandler::Stop() {
  persist_consumer_.Stop();
  persist_pool_.Stop();
  if (mysql_pool_) {
    mysql_pool_->Stop();
  }
  LogInfo("[PersistHandler] Stopped");
}

void PersistHandler::HandleMessage(const std::string& key, const std::string& value) {
  (void)key;
  persist_pool_.Submit([this, payload = value]() {
    PushToCometRequest req;
    if (!req.ParseFromString(payload)) {
      LogError("[PersistHandler] Failed to parse PushToCometRequest");
      return;
    }
    // 只处理需要落盘的消息
    if (req.need_persist()) {
      PersistMessage(req);
    }
  });
}

// MySQL 消息落盘：INSERT ... ON DUPLICATE KEY UPDATE（幂等写入）
// 重试策略：与 PushHandler 一致，指数退避 100ms → 400ms → 1600ms
// SQL 注入防护：使用 mysql_real_escape_string 转义 content_json
void PersistHandler::PersistMessage(const PushToCometRequest& req) {
  static constexpr int kMaxRetries = 3;
  static constexpr int kBaseBackoffMs = 100;  // 100ms → 400ms → 1600ms

  if (!mysql_pool_) return;

  const auto& msg = req.message();
  std::string session_id = msg.session_id();

  for (int attempt = 0; attempt <= kMaxRetries; ++attempt) {
    auto conn_guard = mysql_pool_->Acquire(1000);  // 1秒超时
    MYSQL* conn = conn_guard.get();
    if (!conn) {
      if (attempt < kMaxRetries) {
        int backoff_ms = kBaseBackoffMs * (1 << (attempt * 2));
        LogError("[PersistHandler] No MySQL connection, retry " +
                 std::to_string(attempt + 1) + "/" + std::to_string(kMaxRetries) +
                 " after " + std::to_string(backoff_ms) + "ms");
        std::this_thread::sleep_for(std::chrono::milliseconds(backoff_ms));
        continue;
      }
      LogError("[DLQ][Persist] All retries exhausted (no connection), msg_id=" +
               msg.msg_id() + " session_id=" + session_id);
      return;
    }

    // 转义内容防止SQL注入
    char escaped_content[4096];
    mysql_real_escape_string(conn, escaped_content, msg.content_json().c_str(),
                             std::min(msg.content_json().length(), (size_t)2000));

    char sql[8192];
    snprintf(sql, sizeof(sql),
             "INSERT INTO message (session_id, msg_seq, sender_id, msg_type, "
             "content_json, timestamp_ms, client_msg_id) "
             "VALUES ('%s', %ld, %ld, '%s', '%s', %ld, '%s') "
             "ON DUPLICATE KEY UPDATE session_id=session_id",
             session_id.c_str(), (long)msg.msg_seq(), (long)msg.sender_id(),
             msg.msg_type().c_str(), escaped_content, (long)msg.timestamp_ms(),
             msg.client_msg_id().c_str());

    if (mysql_query(conn, sql) == 0) {
      return;  // 成功
    }

    std::string err = mysql_error(conn);
    if (attempt < kMaxRetries) {
      int backoff_ms = kBaseBackoffMs * (1 << (attempt * 2));
      LogError("[PersistHandler] MySQL persist retry " +
               std::to_string(attempt + 1) + "/" + std::to_string(kMaxRetries) +
               " after " + std::to_string(backoff_ms) + "ms, error: " + err);
      std::this_thread::sleep_for(std::chrono::milliseconds(backoff_ms));
    } else {
      LogError("[DLQ][Persist] All retries exhausted, msg_id=" + msg.msg_id() +
               " session_id=" + session_id + " error: " + err);
    }
  }
}

}  // namespace sparkpush
