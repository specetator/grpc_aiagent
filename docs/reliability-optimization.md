# 实时消息可靠性与投递优化

> 这份文档记录根目录当前源码已经完成的优化，以及仍然没有被夸大的边界。更新日期：2026-08-09。

## 1. 优化目标

原始链路可以完成“Logic 接收 → Kafka → Job → Comet → WebSocket”，但 ACK 语义、断线恢复和落库时机不够清晰。当前版本围绕下面几个问题改造：

| 问题 | 当前实现 | 验证方式 |
|---|---|---|
| ACK 只表示“收到” | `accepted_ack` / `delivered_ack` 两阶段 ACK | `load_test/e2e_bench` 分别计数 |
| 客户端断线或乱序 | `session_id + msg_seq` 游标、缺口补偿 | Web Demo 发送 `sync` |
| 每条消息一次 Unary RPC | Job 到每个 Comet 复用 `PushStream` | Job metrics / 重连测试 |
| Logic 崩溃丢本地持久化任务 | `persist_message` Kafka 持久化事件 | Job 重放 topic 后落库 |
| 离线用户只能查历史 | 握手时按 `delivered_seq` 自动补推 | `user_session_state` 查询 |
| 广播挤占单聊资源 | single / group / broadcast 独立限流和 topic / consumer group | 各场景 429 与 Kafka lag |
| 出问题只能看日志 | Logic、Job、Comet 均提供 `/metrics` | 9101 / 9202 / 9203 |

## 2. 当前消息状态机

```text
客户端发送 client_msg_id
        │
        ▼
Logic Redis Lua：去重 + 分配 session 内 msg_seq
        │
        ├── persist_message：Kafka delivery report 成功
        │                         │
        │                         └── accepted_ack
        │
        └── push_single / push_group：进入实时投递队列
                                  │
                                  ▼
                         Job PushStream → Comet
                                  │
                                  ├── 目标连接 send 成功
                                  │       └── delivered cursor 更新
                                  │
                                  └── 来源 Comet 收到 delivered_ack
```

`accepted_ack` 的含义是：Logic 已经把持久化事件交给 Kafka，并收到 delivery report；它不表示目标客户端已经看到消息。

`delivered_ack` 的含义是：Job/Comet 已把消息交给目标 Comet 的在线 WebSocket 连接，并把目标用户的 `delivered_seq` 单调推进。它仍不是“用户打开聊天窗口并阅读”的应用层回执；如果需要阅读回执，应再增加 read ACK。

## 3. 关键改动

### 3.1 ACK 拆分

协议在 `proto/spark_push.proto` 中增加 `AckStage`、`accepted_at_ms`、`request_id` 和 `delivered_count`。Comet 给发送方返回：

```json
{
  "type": "accepted_ack",
  "ack_stage": "accepted",
  "msg_id": "s_22_23-281",
  "session_id": "s_22_23",
  "msg_seq": 281,
  "client_msg_id": "client-generated-id"
}
```

Job 成功调用来源 Comet 的 `PushDeliveryAck` 后，来源连接收到：

```json
{
  "type": "delivered_ack",
  "ack_stage": "delivered",
  "msg_id": "s_22_23-281",
  "session_id": "s_22_23",
  "msg_seq": 281,
  "client_msg_id": "client-generated-id"
}
```

Comet 另外向 Logic 批量上报目标用户的 `UserDeliveredCursor`，所以发送方 ACK 和接收方离线游标不会混为一谈。

### 3.2 游标和缺口补偿

- Redis 负责热路径分配会话序号；MySQL `message` 表按 `(session_id,msg_seq)` 唯一约束保存历史。
- `user_session_state.delivered_seq` 保存每个用户交给连接的最大游标，更新使用 `GREATEST`，乱序回调不会回退；客户端仍以 `msg_seq` 检查缺口。
- Web Demo 收到同一 session 的消息时检查 `msg_seq`。发现 `last_seq + 1 < msg_seq`，就发送：

```json
{"type":"sync","session_id":"s_22_23","after_seq":last_seq,"limit":100}
```

- Comet 调用 `Logic.SyncMessages`，服务端校验单聊双方或聊天室成员权限，再从 MySQL 查询 `after_seq` 之后的消息。
- 补偿消息仍带原始 `msg_id`、`client_msg_id` 和 `msg_seq`，客户端按 `msg_id` 去重。
- WebSocket 握手会调用 `SyncOffline`，按 `delivered_seq` 查询离线消息。只有实际找到在线连接并发送成功的消息才推进游标。

### 3.3 Job → Comet 双向长连接

Job 为每个 `comet_id` 建立一条双向 streaming gRPC `PushStream`，而不是只
调用 `ClientWriter::Write` 后就认为消息完成。其内部包含：

- 有界请求队列 `push_stream_queue_max`，默认 10000；
- writer 线程串行写请求，reader 线程按 `request_id` 匹配每条
  `PushToCometReply`；
- Comet 对每条请求返回 `request_id / delivered_count / error`，Job 只有收到
  对应 reply 后才返回 Kafka consumer 成功并允许提交 offset；
- 写失败或读端断流后按 200ms、400ms、800ms…退避，最多四次重连尝试；
- 每条请求使用 `request_id=msg_id@comet_id`，Comet 保存有界最近 ID 集合，
  重连重放不会重复下发；
- 长连接回复超时或不可用时回退到带 deadline 的 Unary RPC，只有两条路径都失败
  才记为最终投递失败。

gRPC Channel 本身通常会复用 HTTP/2 连接，因此这里不把 Unary 描述成“每条消息新建
TCP”。双向流减少的是逐调用 `ClientContext`、metadata 和调用状态机开销，同时让应用层
拥有一条可做有界排队、request/reply 关联与重连控制的持久流，并避免把客户端库的
`Write()` 成功误当成服务端已经处理。

### 3.4 持久化 topic 消除 Logic 崩溃窗口

Logic 不再把落库任务只放在线程池。新消息先构造 `PersistMessageRequest`，写入 `persist_message`，并等待 Kafka delivery callback；Job 使用独立的 `spark_push_group_persist` 消费组：

1. 根据 scene 补建单聊或聊天室 session；
2. 插入 message；
3. 用 `GREATEST` 推进 `session.last_msg_seq`；
4. 业务回调成功后才同步提交 Kafka offset；
5. MySQL 唯一键冲突时比较完整消息字段，相同内容视为幂等重放。

因此 Logic 在返回 `accepted_ack` 后重启，Job 仍可从 Kafka 重放持久化事件。当前仍是“至少一次事件 + 幂等落库”，不是分布式 exactly-once；重试耗尽目前写审计日志，还没有独立 DLQ topic。

### 3.5 场景隔离、限流与排队

根目录默认配置：

| 场景 | 令牌桶 key | 速率 | burst | Kafka topic | Job consumer group |
|---|---|---:|---:|---|---|
| single | sender user | 1000/s | 200 | `push_single` | `spark_push_group_single` |
| group / chatroom | room id | 300/s | 60 | `push_group` | `spark_push_group_group` |
| broadcast | scope | 20/s | 5 | `broadcast_task` | `spark_push_group_broadcast` |
| persist | session id | Kafka delivery report | - | `persist_message` | `spark_push_group_persist` |

单聊和群聊 topic、producer、consumer group 分离；Job 到 Comet 的 stream 队列按 `comet_id` 隔离。超出令牌桶直接返回 429，Kafka 和 Job 的有界队列负责吸收短时峰值，不把无限内存队列伪装成可靠性。

这组参数是根据本机 4×25、8×100 E2E 的吞吐与尾延迟重新设定的保护线，不代表生产容量。线上应根据 Comet 连接数、Kafka partition 数、Job 消费能力和 p99 延迟重新压测。

### 3.6 前后端体感优化

通用聊天页面也做了与消息可靠性配套的优化：

- 单聊和聊天室发送后先显示乐观气泡，收到 `accepted_ack` / `delivered_ack` 后更新状态；
  失败帧带回 `client_msg_id`，可以精确标记失败消息；
- 首屏历史加载期间暂存实时消息，历史完成后按序号刷入；历史、实时和乐观消息均按
  `msg_id` / `client_msg_id` 去重，切换会话会取消旧 HTTP 请求；
- 浏览器的 Hermes 增量按 `requestAnimationFrame` 合并 DOM 更新，只在用户接近底部时
  自动滚动；单聊与聊天室 WebSocket 都使用指数退避重连，并在重连后触发游标补偿；
- Comet WebSocket 开启 `TCP_NODELAY`；多目标 delivered cursor 合并成一次 RPC；Logic
  对 active 用户状态做 2 秒短缓存，减少小消息热路径的重复数据库查询；
- `/api/session/list_single`、`/api/session/unread`、`/api/session/mark_read` 和聊天室
  用户接口统一要求 Bearer Token，并把请求体中的 user_id 与认证身份比对。

这些优化改善的是页面连续性、重复消息和链路开销，不会改变 `accepted`、`delivered` 和
`read` 三种语义，也不会把 Hermes 模型本身的推理时间伪装成实时投递延迟。

## 4. 可观测性

```bash
curl http://127.0.0.1:9101/metrics  # Logic
curl http://127.0.0.1:9202/metrics  # Job
curl http://127.0.0.1:9203/metrics  # Comet
```

重点指标：

| 指标 | 含义 |
|---|---|
| `spark_push_reconnect_total` | Job → Comet 长连接重连次数 |
| `spark_push_kafka_lag_ms` | 最近消费记录的 Kafka record age，毫秒 |
| `spark_push_delivery_latency_ms_{count,sum,max}` | 消息时间戳到 Job 投递处理的观测值 |
| `spark_push_delivery_attempt_total[_scene]` | 投递尝试数 |
| `spark_push_delivery_success_total[_scene]` | Comet 接受并交给连接数 |
| `spark_push_delivery_lost_total[_scene]` | 重试耗尽或队列失败数 |
| `spark_push_delivery_duplicate_total` | Comet request_id 去重次数 |
| `spark_push_delivery_cursor_update_failed_total` | 目标 delivered 游标更新失败数 |
| `spark_push_persist_success_total` | Job 成功落库数 |
| `spark_push_comet_push_stream_ready` | Job → Comet 长连接已进入 Comet 服务端处理函数 |
| `spark_push_hermes_ttft_ms_{count,sum,max}` | Hermes 请求开始到首个增量发布的延迟 |
| `spark_push_hermes_total_latency_ms_{count,sum,max}` | Hermes 请求从开始到完整回答的延迟 |
| `spark_push_hermes_prompt_chars_{count,sum,max}` | 实际发送给 Hermes 的 prompt 字符数 |
| `spark_push_hermes_prompt_messages_{count,sum,max}` | 实际发送给 Hermes 的 prompt 消息数 |

可用下面的比值观察趋势：

```text
delivery_loss_rate = delivery_lost / (delivery_success + delivery_lost)
duplicate_rate     = delivery_duplicate / delivery_attempt
```

当前注册表是轻量 Prometheus text exporter，未引入 prometheus-cpp；指标按进程内存统计，进程重启后清零，生产版应接入 Prometheus remote storage 或 OpenTelemetry。

## 5. 回归方法

```bash
cmake --build build -j"$(nproc)"
(cd build && ctest --output-on-failure)
(cd build && SPARK_PUSH_RUN_REDIS_TESTS=1 \
  ctest -R redis_sequence_integration_test --output-on-failure)

./build/load_test/e2e_bench \
  --sender-account <sender> --sender-password '<password>' \
  --receiver-account <receiver> --receiver-password '<password>' \
  --connections 4 --messages-per-conn 25 --timeout-ms 30000
```

E2E 只有下面条件同时满足才返回 0：

```text
sent == accepted_ack == delivered_ack == delivered
send_failed == 0
ack_errors == 0
```

E2E 结束后不要立即杀 Job；至少等待持久化 topic 消费完成，再查库：

```sql
SELECT COUNT(*) AS total_rows,
       COUNT(DISTINCT msg_seq) AS unique_seq,
       MIN(msg_seq), MAX(msg_seq)
FROM message
WHERE session_id = 's_<小uid>_<大uid>';

SELECT last_msg_seq
FROM session
WHERE session_id = 's_<小uid>_<大uid>';
```

2026-08-09 本机回归曾观察到：E2E 已经 100/100 实时送达，但立刻停止 Job 时持久化 topic 还未落库；让 Job 继续运行并从 topic 重放后，`s_22_23` 的 140 条事件全部落库。这个现象正好验证了“实时 ACK”和“持久化消费完成”是两个不同阶段，也应在文档和面试中分开解释。

## 5.1 Hermes Bot 的异步边界

第一阶段 Hermes 集成不把外部 HTTP 调用放进 Logic 的 WebSocket 热路径，而是增加
三个隔离 topic：

```text
Logic
  ├─ persist_message：先保存用户输入事件
  └─ ai_request ──► hermes_bridge ──HTTP SSE──► Hermes
                         ├─ ai_delta ──► Logic 临时事件 ──► push_single：实时追加
                         └─ ai_reply ──► Logic：最终答案
                                         ├─ persist_message：Bot 回答并分配 msg_seq
                                         └─ push_single：复用普通单聊投递
```

这样用户输入的 accepted 边界不会依赖 Hermes 响应时间，Hermes 超时也不会阻塞
Comet/Logic 的其他单聊。Bridge 以 `request_id` 缓存已经生成的回答，Logic 以
`hermes:<request_id>` 做消息幂等；Kafka 重试可以重新发布回复而不重复生成或落库。
Logic 另外保留每个会话最多 100 条的进程内短期上下文，覆盖回答已推送但 Job 尚未
完成 MySQL 落盘的窗口；重启后仍回退到 MySQL 历史。增量只用于在线体验，不分配
`msg_seq`，断线时不补发半截文本；完整回答仍经过普通
`persist_message → Job → push_single → Comet`，因此离线补推、delivered cursor、缺口
补偿和历史查询不需要为 AI 另写一套最终消息协议。

当前明确的可靠性边界：HTTP 客户端是本地明文 HTTP，Bridge 内部同步处理一个 Hermes
请求，回答和增量状态主要在进程内；Bridge 全部重启后，未消费的 `ai_request` 会重放，但已经处理
且尚未成功发布 `ai_reply` 的窗口仍应在下一阶段增加持久化 request/outbox 或独立
DLQ 重放工具。完整配置和 Windows 虚拟机网络检查见
[`hermes-integration.md`](hermes-integration.md)。

## 6. 尚未宣称完成的部分

- `delivered_ack` 是服务端交给目标 WebSocket 连接，不是客户端 read receipt；
- Job 重试耗尽目前只有不带正文的 `[DLQ]` 审计日志，还没有可重放的独立 DLQ topic；
- Kafka topic 在本地 compose 中是单 broker，不能据此宣称副本容灾；
- HTTP 普通业务接口仍有教学性质，公开部署前要统一 Bearer 鉴权、对象级授权、TLS、CORS 和入口限流；
- metrics 目前是进程内轻量 exporter，没有 Prometheus 告警规则和多实例聚合；
- 需要进一步做多用户、多 session、多 Comet、多 partition 的压力和故障注入，单机 E2E 数字不能外推集群容量。
