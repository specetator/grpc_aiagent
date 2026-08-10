# E2E 性能与一致性回归（2026-08-09）

## 测量口径

本报告使用根目录 `load_test/e2e_bench.cpp`。工具建立独立接收账号的 WebSocket，并同时统计：

- `sent`：发送端成功写出的消息；
- `accepted_ack`：发送连接收到 accepted ACK；
- `delivered_ack`：发送连接收到 delivered ACK；
- `delivered`：接收端实际读到本轮唯一 `client_msg_id`；
- `delivery_qps`：以接收端实际送达数计算；
- p50 / p95 / p99：从发送时间到接收端读到消息的延迟。

只有 `sent == accepted_ack == delivered_ack == delivered == expected`，且 `send_failed=0`、`ack_errors=0` 才算通过。旧 `load_tester ws_send` 只代表 socket send，不作为系统投递 QPS。

## 本机环境

Logic、Comet、Job、Redis、MySQL、Kafka 和压测客户端在同一台机器运行；Job 使用 `push_single` + `spark_push_group_single`，持久化使用 `persist_message` + `spark_push_group_persist`，Job→Comet 开启双向 `PushStream`，按 `request_id` 等待逐条 reply。

## E2E 结果

| 场景 | sent / accepted / delivered_ack / delivered | 实际投递 QPS | p50 | p95 | p99 |
|---|---|---:|---:|---:|---:|
| 2 连接 × 10 条（双向流 smoke） | 20 / 20 / 20 / 20 | 344.6 msg/s | 44.7 ms | 56.3 ms | 56.3 ms |
| 8 连接 × 100 条（最终源码） | 800 / 800 / 800 / 800 | 535.5 msg/s | 753.8 ms | 1411.1 ms | 1469.0 ms |

8×100 进入队列平台区后，尾延迟明显上升；这正是需要同时观察 Kafka lag、stream queue、p99 和 loss rate 的原因。最终源码的 800 条测试同时验证了双向 PushStream 的逐条 reply，数据只用于本机版本回归，不能外推生产集群容量。

## 持久化和游标结果

当前源码 8×100 这轮结束后等待 5 秒，Job metrics 显示：

```text
spark_push_delivery_attempt_total        800
spark_push_delivery_success_total        800
spark_push_persist_success_total         800
spark_push_stream_write_success_total    800
spark_push_delivery_cursor_update_total  800
```

测试会话最终数据库复核：

```text
session_id                 = s_22_23
message total rows         = 2720
distinct msg_seq           = 2720
distinct client_msg_id     = 2720
min/max msg_seq            = 1 / 2720
session.last_msg_seq       = 2720
receiver delivered_seq     = 2720
```

其中最后一轮新增 800 条；数据库中还包含前序回归数据。关键结论是本轮新增事件全部由 `persist_message` 消费并幂等落库，接收方 delivered cursor 也推进到同一会话末端。

## 真实发现并修复的问题

第一次改造后，E2E 可以 100/100 实时送达，但测试命令立刻停止 Job 时，持久化 topic 尚未被 Job 消费，数据库暂时看不到新行。单独让 Job 继续运行并从 `persist_message` 重放后，事件全部落库。

随后又发现实时下发成功后没有推进接收方 `delivered_seq`，导致每次重连都会把已经在线收到的消息再次补推。当前 Comet 在目标连接实际发送成功后批量上报 `UserDeliveredCursor`，客户端仍按 `msg_id` 去重作为最后一道保护。

这两个问题说明：

1. accepted、realtime delivered、persistent stored 是三个不同阶段；
2. 离线补推必须有“目标用户游标”，不能只依赖发送方 ACK；
3. E2E 必须包含数据库和 cursor 复核，不能只看 WebSocket send。

## Redis 水位故障注入

在 MySQL `MAX(msg_seq)=100` 时，将 Redis `session:msg_seq:{sid}` 和 `session:last_seq:{sid}` 降到 1，重启 Logic 后继续发送 20 条。

结果：新消息使用 101～120，MySQL 共 120 个唯一序号，Redis 恢复到 120，无唯一键冲突。该结果验证 `ConversationStore::AppendMessageHotPath` 会以 MySQL 最大序号作为 Redis Lua 的 floor。

## 指标计算口径

```text
delivery_loss_rate = delivery_lost / (delivery_success + delivery_lost)
duplicate_rate     = delivery_duplicate / delivery_attempt
```

当前 exporter 是进程内轻量 Prometheus text 格式，重启会清零；生产环境需要 Prometheus / OpenTelemetry 多实例聚合。

## 仍需关注

- `delivered_ack` 表示交给目标在线连接，不是客户端阅读回执；
- Job 重试耗尽目前只有 `[DLQ]` 审计日志，还没有可重放的独立 DLQ topic；
- 本地 Kafka 是单 broker，不能据此宣称副本容灾；
- 8×100 的尾延迟表明还需要多 Job、多 Comet、多 partition 和多用户压测；
- 普通 HTTP API 的统一 Bearer 鉴权、TLS 和生产级监控告警仍需继续完善。
