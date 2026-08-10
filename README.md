# Spark Push

C++17 分布式实时消息推送 / IM 教学项目。系统拆分为 Comet、Logic、Job 三个进程，支持 WebSocket 长连接、Kafka 异步投递、Redis 序号与幂等、MySQL 历史消息、双阶段 ACK、游标补偿和离线补推。

## 架构

```text
Client ──WebSocket──► Comet ──MessageStream──► Logic
  ▲                     ▲                       │
  │                     │                       ├── Redis：token / 路由 / seq / dedup
  │                     │                       ├── persist_message：持久化事件
  │                     │                       ├── push_single：单聊实时队列
  │                     │                       ├── push_group：群聊实时队列
  │                     │                       └── broadcast_task：广播队列
  │                     │                                  │
  │                     └──── PushStream ◄──── Job ◄────────┘
  └──────────────────── WebSocket delivery + delivered cursor

单聊 Hermes Bot（可选）：

Logic ── ai_request ──► hermes_bridge ──HTTP/SSE──► Windows Hermes
  ▲                         │       │
  │                         │       └─ ai_delta ──► Logic 临时推送 ──► Job/Comet
  └──── ai_reply ◄──────────┴─────── push_single ──► Job/Comet（最终落库消息）
```

| 组件 | 默认端口 | 作用 |
|---|---:|---|
| Logic | gRPC 9100 / HTTP 9101 | 鉴权、序号、幂等、Kafka 生产、历史/游标同步、`/metrics` |
| Comet | WebSocket 9000 / gRPC 9105 / metrics 9203 | 长连接接入、上行流、本机下推、离线补推 |
| Job | metrics 9202 | 消费四类 Kafka topic，长连接投递 Comet，持久化 MySQL |
| Web Demo | HTTP 9010 | 浏览器演示 |
| Hermes Bridge（可选） | 无监听端口 | 消费 `ai_request`，调用 SSE `/v1/chat/completions`，发布 `ai_delta` 和最终 `ai_reply` |

技术栈：C++17、Muduo、WebSocket、gRPC/Protobuf、Kafka、Redis、MySQL、OpenSSL、CMake/CTest、Docker Compose、GitHub Actions。

## 当前实现的可靠性设计

- `accepted_ack`：Logic 收到 `persist_message` 的 Kafka delivery report 后返回，表示消息事件已进入可重放队列；
- `delivered_ack`：Job/Comet 将消息交给目标在线 WebSocket 连接后回传，和 accepted ACK 分离；
- `session_id + msg_seq`：客户端按会话游标排序、去重，发现缺口时通过 `SyncMessages` 补偿；
- `user_session_state.delivered_seq`：握手后 `SyncOffline` 补推未交给该用户连接的消息；
- `push_single` / `push_group` / `broadcast_task`：单聊、群聊、广播分别限流、排队和消费，防止广播挤占单聊；
- `hermes_bot(900000000001)`：Logic 启动时幂等创建的系统用户；`ai_request/ai_delta/ai_reply` 只承载 AI 编排，最终回答仍走普通单聊 `push_single`；
- Hermes 增量体验：Bridge 将文本 SSE 增量发布到 `ai_delta`，Logic 只实时推送临时气泡；完整回答经 `ai_reply` 分配正式 `msg_seq`、落库并作为离线/重连恢复事实；
- Job 为每个 Comet 复用双向 `PushStream`，由 reader 按 `request_id` 匹配逐条 reply，带有界队列、指数退避重连、`request_id` 去重和 Unary fallback；
- 启动脚本不只检查端口，还等待 `spark_push_comet_push_stream_ready 1`，避免 Job/Comet 冷启动时把“可连接”误判为“可投递”；
- Logic、Job、Comet 都可抓取 Prometheus text metrics；
- MySQL 唯一键、字段比对和单调水位更新提供幂等落库兜底。

完整的优化决策、状态机、指标定义和边界见 [`docs/reliability-optimization.md`](docs/reliability-optimization.md)。
配套的运行、排障、优化操作和面试讲解见 [`../新手上手指南.md`](../新手上手指南.md)。
原始 Demo 到当前工程化版本的逐阶段对比、简历写法和面试追问见
[`docs/interview-project-evolution.md`](docs/interview-project-evolution.md)。

## 用户中心与管理员操作

用户主数据保存在 MySQL，用户状态支持 `active`、`disabled`、`deleted`（软删除）；登录
Token 在 Redis 中维护正向映射和 `user:tokens:<uid>` 反向索引，管理员可以批量撤销用户
会话。管理员用户列表、禁用/恢复/软删除、昵称修改、Token 撤销和审计日志接口见
[`docs/user-center.md`](docs/user-center.md)。

管理员前端控制台入口为
[`http://127.0.0.1:9010/admin_chatroom.html`](http://127.0.0.1:9010/admin_chatroom.html)，
也可以从普通聊天首页左侧的“管理员控制台”进入。登录后默认打开“用户中心”，还可以切换到
运行概览、审计日志、房间与广播；普通用户聊天入口仍是
[`http://127.0.0.1:9010/index.html`](http://127.0.0.1:9010/index.html)。

这部分实现保留了消息历史和审计引用，避免用物理 `DELETE` 造成消息发送者孤儿记录；生产集群
还应把管理员鉴权、审计写入和踢线通知接入统一 IAM、可靠日志管道和跨节点会话服务。

## 快速开始

```bash
cp .env.example .env.local
${EDITOR:-vi} .env.local

./scripts/start_demo.sh --foreground
# 浏览器打开 http://127.0.0.1:9010/index.html

# 另开终端执行；或在前台启动脚本中按 Ctrl-C
./scripts/stop_demo.sh
```

`.env.local` 至少设置 `SPARK_PUSH_MYSQL_PASSWORD`；要启用管理员控制台，还需设置
`SPARK_PUSH_ADMIN_ACCOUNT` 和 `SPARK_PUSH_ADMIN_PASSWORD`。真实口令不会从环境变量进入配置仓库。
`sql/schema.sql` 只创建不存在的表；开发环境确实需要清库时，才显式执行 `sql/reset_dev.sql`。

启动脚本会自动拉起 Redis / MySQL / Kafka、创建 topic、编译服务并检查端口。只想复用已启动依赖或已确认二进制最新时，可分别使用 `--skip-deps`、`--skip-build`；完整的参数、浏览器操作和排障说明见 [`scripts/00_prepare_and_run.md`](scripts/00_prepare_and_run.md)。

## Hermes Bot（第一阶段）

Windows Hermes 的源码目录 `F:\hermes` 不需要挂载到虚拟机；虚拟机中的
`hermes_bridge` 通过 HTTP 访问 Hermes API。设置 Hermes API Server 后，在
`.env.local` 开启：

```dotenv
SPARK_PUSH_HERMES_ENABLED=true
SPARK_PUSH_HERMES_BASE_URL=http://host.docker.internal:8642/v1
SPARK_PUSH_HERMES_API_KEY=与 Hermes API_SERVER_KEY 相同
SPARK_PUSH_HERMES_MODEL=hermes-agent
SPARK_PUSH_HERMES_STREAMING=true
```

重新启动 `scripts/start_demo.sh` 后，浏览器单聊页面可点击“与 Hermes Bot 对话”，
也可输入 UserID `900000000001`。默认使用流式 `/v1/chat/completions`：文本增量经
`ai_delta` 及时显示，最终完整回答经 `ai_reply` 重新进入
`persist_message → push_single → Job → Comet`，因此刷新、重连和离线补推仍然可靠。
设置 `SPARK_PUSH_HERMES_STREAMING=false` 可回退到非流式兼容模式。Logic 把最近 50 条
单聊历史组装为 `messages`，虚拟机网络、Windows 防火墙、多轮验证和边界说明见
[`docs/hermes-integration.md`](docs/hermes-integration.md)。

## E2E 验证

先通过 `/api/register` 创建两个账号，再执行：

```bash
./build/load_test/e2e_bench \
  --sender-account <sender> --sender-password '<password>' \
  --receiver-account <receiver> --receiver-password '<password>' \
  --connections 4 --messages-per-conn 25 --timeout-ms 30000
```

成功条件：

```text
sent == accepted_ack == delivered_ack == delivered
send_failed == 0
ack_errors == 0
```

这里的 `delivered` 是独立接收 WebSocket 实际读到消息的数量；旧版 `load_tester ws_send` 只代表 socket send 成功，不能当作系统实际投递 QPS。

E2E 结束后应让 Job 继续运行一小段时间，再检查 `message` 表；持久化 topic 的消费是独立阶段，不能用“实时 delivered 已完成”替代“历史已落库”。

## 指标

```bash
curl http://127.0.0.1:9101/metrics  # Logic
curl http://127.0.0.1:9202/metrics  # Job
curl http://127.0.0.1:9203/metrics  # Comet
```

重点指标包括：

- `spark_push_reconnect_total`：Job → Comet 重连次数；
- `spark_push_kafka_lag_ms`：最近消费记录的年龄；
- `spark_push_delivery_latency_ms_{count,sum,max}`：投递延迟观测；
- `spark_push_delivery_duplicate_total`：`request_id` 重放去重数；
- `spark_push_delivery_lost_total[_scene]`：重试耗尽/队列失败数；
- `spark_push_delivery_cursor_update_failed_total`：目标 delivered 游标更新失败数；
- `spark_push_rate_limited_total_single/group/broadcast`：场景限流数。
- Hermes 接入还可观察 `spark_push_hermes_requests_total`、`spark_push_hermes_replies_total`、`spark_push_hermes_errors_total`、`spark_push_hermes_stream_deltas_total`、`spark_push_hermes_stream_delta_enqueued_total` 和重复/失败计数。

## 代码阅读顺序

1. `proto/spark_push.proto`：ACK、游标、持久化事件和 gRPC 服务；
2. `comet/comet_server.cpp`：WebSocket 握手、上行解析、缺口同步和离线补推；
3. `logic/grpc_service.cpp`：序号、幂等、场景路由、持久化 topic 和限流；
4. `logic/conversation_store.cpp`、`logic/redis_store.cpp`：Redis Lua 和 MySQL 水位；
5. `job/service.cpp`、`common/kafka_consumer.cpp`：场景 consumer group、PushStream、offset 和落库；
6. `load_test/e2e_bench.cpp`：如何区分 accepted、delivered ACK 和真实接收。

教学快照 `02_auth_subset`～`06` 只用于分阶段学习；运行和面试说明以根目录源码为准。

## 简历表述边界

可以写：

- 设计 Comet / Logic / Job 三层 C++ 实时消息架构，以 Kafka 解耦业务受理和下行投递；
- 使用 Redis Lua 实现会话内 `msg_seq` 与 `client_msg_id` 原子幂等，引入 MySQL 水位和唯一键兜底；
- 将 ACK 拆为 accepted / delivered，引入游标缺口补偿、离线补推和 delivered cursor；
- 使用独立 persist topic、场景 topic、PushStream、重连重放和可抓取指标，建立可复现 E2E 验证；
- 单机 E2E 报告必须同时给出并发参数、accepted/delivered 数量、延迟分位数和数据库复核结果。

不要写“exactly-once”“消息绝不丢”“百万并发”或把 socket send 速度当系统 QPS。当前仍是单 broker 教学环境，DLQ、统一 HTTP 鉴权、TLS 和多机容量验证尚未完成。
