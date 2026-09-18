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

单聊 Pi Agent Bot（可选）：

Logic ── ai_request ──► hermes_bridge ──HTTP/SSE──► Pi gateway ──RPC──► Pi Agent
  ▲                         │       │
  │                         │       ├─ cann-advisor ──► 独立本地 RAG
  │                         │       └─ ai_delta ──► Logic 临时推送 ──► Job/Comet
  └──── ai_reply ◄──────────┴─────── push_single ──► Job/Comet（最终落库消息）
```

| 组件 | 默认端口 | 作用 |
|---|---:|---|
| Logic | gRPC 9100 / HTTP 9101 | 鉴权、序号、幂等、Kafka 生产、历史/游标同步、`/metrics` |
| Comet | WebSocket 9000 / gRPC 9105 / metrics 9203 | 长连接接入、上行流、本机下推、离线补推 |
| Job | metrics 9202 | 消费四类 Kafka topic，长连接投递 Comet，持久化 MySQL |
| Web Demo | HTTP 9010 | 浏览器演示 |
| Agent Bridge（可选） | 无监听端口 | 消费 `ai_request`，调用 Pi gateway 的 SSE `/v1/chat/completions`，发布 `ai_delta` 和最终 `ai_reply` |

技术栈：C++17、Muduo、WebSocket、gRPC/Protobuf、Kafka、Redis、MySQL、OpenSSL、CMake/CTest、Docker Compose、GitHub Actions。

## 当前实现的可靠性设计

- `accepted_ack`：Logic 收到 `persist_message` 的 Kafka delivery report 后返回，表示消息事件已进入可重放队列；
- `delivered_ack`：Job/Comet 将消息交给目标在线 WebSocket 连接后回传，和 accepted ACK 分离；
- `session_id + msg_seq`：客户端按会话游标排序、去重，发现缺口时通过 `SyncMessages` 补偿；
- `user_session_state.delivered_seq`：握手后 `SyncOffline` 补推未交给该用户连接的消息；
- `push_single` / `push_group` / `broadcast_task`：单聊、群聊、广播分别限流、排队和消费，防止广播挤占单聊；
- `hermes_bot(900000000001)`：Logic 启动时幂等创建的系统用户；`ai_request/ai_delta/ai_reply` 只承载 AI 编排，最终回答仍走普通单聊 `push_single`；
- Agent 增量体验：Bridge 将文本 SSE 增量发布到 `ai_delta`，Logic 只实时推送临时气泡；完整回答经 `ai_reply` 分配正式 `msg_seq`、落库并作为离线/重连恢复事实；
- CANN 引用：Pi gateway 的 `hermes.final`/`pi.final` 携带权威最终文本和结构化引用，Logic 校验后随最终消息落库；浏览器把 `cannkb://` 安全转换为受 Token 保护的同源原文页；
- Job 为每个 Comet 复用双向 `PushStream`，由 reader 按 `request_id` 匹配逐条 reply，带有界队列、指数退避重连、`request_id` 去重和 Unary fallback；
- 启动脚本不只检查端口，还等待 `spark_push_comet_push_stream_ready 1`，避免 Job/Comet 冷启动时把“可连接”误判为“可投递”；
- Logic、Job、Comet 都可抓取 Prometheus text metrics；
- MySQL 唯一键、字段比对和单调水位更新提供幂等落库兜底。

设计文档分成两层：

- 大框架（模块职责、完整链路、输入输出）：[`docs/spark-push-architecture.md`](docs/spark-push-architecture.md)
- 小模块（Lua 取号、连接表、PushStream 队列、SHA1 握手、TreeMap 合并等逐步算法）：[`docs/spark-push-internals.md`](docs/spark-push-internals.md)
- 阅读索引：[`docs/spark-push-implementation.md`](docs/spark-push-implementation.md)

完整的优化决策、状态机、指标定义和边界见 [`docs/reliability-optimization.md`](docs/reliability-optimization.md)。
配套的运行、排障和验证操作见 [`scripts/00_prepare_and_run.md`](scripts/00_prepare_and_run.md)。
原始 Demo 到当前工程化版本的逐阶段对比、简历写法和面试追问见
[`docs/interview-project-evolution.md`](docs/interview-project-evolution.md)。
CANNBot Agent 分层思路的 Pi 接入、项目知识 Skill、RAG 引用和二次开发流程见
[`docs/cannbot-integration.md`](docs/cannbot-integration.md) 与
[`docs/pi-agent-integration.md`](docs/pi-agent-integration.md)。

## 原生 Android 客户端与输出长度链路

Android 客户端已经从 WebView 壳迁移为 Kotlin/Jetpack Compose 原生实现，覆盖登录/注册、单聊历史与分页、实时消息、离线补推、Agent 命令卡片、模型/思考等级、CANN 知识原文和管理员控制台。Android 使用独立的 Pi/Hermes 联系人 ID，避免和 PC/WebDemo/Telegram 的 session、模型偏好和上下文混用。

~~~bash
cd /home/peco/cppcode/fenbushi/11.2-spark_push/android-app
./gradlew assembleDebug --no-daemon --console=plain
~~~

模块地图见 [`docs/spark-push-architecture.md`](docs/spark-push-architecture.md)；TreeMap 合并、envelope 序号、MEDIUMTEXT 动态读取等逐步算法见 [`docs/spark-push-internals.md`](docs/spark-push-internals.md)。Android 的构建/安装说明见 android-app/README.md。

单条 Agent 回复的长度排查要区分 12 KiB prompt 输入预算、provider 输出、Bridge 拼接、Logic 写入、MySQL 读取和 Android 展示；sql/migrations/002_message_content_mediumtext.sql 将历史消息字段提升为 MEDIUMTEXT，SPARK_PUSH_LENGTH_AUDIT=1 可记录各阶段 UTF-8 字节和 Unicode 字符数。迁移不会扩大模型上下文窗口，也不会改变 Android 输入框的可见高度。

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

./scripts/sparkctl.sh doctor
./scripts/sparkctl.sh up
# Ubuntu 虚拟机内浏览器打开 http://127.0.0.1:9010/index.html
# Windows 主机请使用虚拟机网卡 IP，例如：
# http://192.168.32.128:9010/index.html

# 查看状态、健康和日志
./scripts/sparkctl.sh status
./scripts/sparkctl.sh health
./scripts/sparkctl.sh logs all -f

# 停止业务和本机 Hermes；默认保留 Docker 数据依赖
./scripts/sparkctl.sh down
```

启动脚本会自动打印 Windows 可访问地址。Windows 中的 `127.0.0.1` 指向 Windows
本机，不是 Ubuntu 虚拟机；若无法访问，先在 Windows PowerShell 执行
`Test-NetConnection <虚拟机IP> -Port 9010`、`-Port 9101` 和 `-Port 9000`，再检查
虚拟机网络模式及 Ubuntu 防火墙。

`.env.local` 至少设置 `SPARK_PUSH_MYSQL_PASSWORD`；要启用管理员控制台，还需设置
`SPARK_PUSH_ADMIN_ACCOUNT` 和 `SPARK_PUSH_ADMIN_PASSWORD`。真实口令不会从环境变量进入配置仓库。
`sql/schema.sql` 只创建不存在的表；开发环境确实需要清库时，才显式执行 `sql/reset_dev.sql`。

启动脚本会自动拉起 Redis / MySQL / Kafka、创建 topic、编译服务并检查端口。只想复用已启动依赖或已确认二进制最新时，可分别使用 `--skip-deps`、`--skip-build`；完整的参数、浏览器操作和排障说明见 [`scripts/00_prepare_and_run.md`](scripts/00_prepare_and_run.md)。

日常运维统一使用 `scripts/sparkctl.sh`：

```bash
./scripts/sparkctl.sh up                 # 完整启动，必要时自动启动本机 Pi Agent Gateway
./scripts/sparkctl.sh up --fast          # 跳过编译
./scripts/sparkctl.sh restart --fast     # 快速重启
./scripts/sparkctl.sh logs bridge -f     # 跟踪 Agent Bridge
./scripts/sparkctl.sh knowledge --source custom-docs
./scripts/sparkctl.sh regress            # 编译 + 协议测试 + 知识源校验
./scripts/sparkctl.sh down --with-deps   # 连 Docker 依赖一起停止，不删除卷
```

底层 `start_demo.sh`、`stop_demo.sh` 仍保留，便于自动化程序只管理 Spark 业务进程。
`sparkctl down` 默认还会停止本机 Pi Agent Gateway。

## Pi Agent Bot

Agent 基座是 WSL 中的 Pi coding agent。`hermes_bridge` 进程名保持不变，指向本机
Pi gateway（`http://127.0.0.1:8643/v1`）。先执行 `cannbot/scripts/setup_pi_agent.sh`，
再在 `.env.local` 开启：

```dotenv
SPARK_PUSH_HERMES_ENABLED=true
SPARK_PUSH_HERMES_BASE_URL=http://127.0.0.1:8643/v1
SPARK_PUSH_HERMES_API_KEY=与 Pi gateway Bearer 相同
SPARK_PUSH_HERMES_MODEL=pi-agent
SPARK_PUSH_HERMES_STREAMING=true
```

重新启动后，浏览器单聊页面可点击“与 Pi Agent 对话”，也可输入 UserID
`900000000001`。默认使用流式 `/v1/chat/completions`：文本增量经 `ai_delta` 及时显示，
最终完整回答经 `ai_reply` 重新进入 `persist_message → push_single → Job → Comet`。
CANN RAG、`cannkb://` 超链接和 `/cann` `/kb` 命令仍然可用。细节见
[`docs/pi-agent-integration.md`](docs/pi-agent-integration.md)。

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

1. [`docs/spark-push-architecture.md`](docs/spark-push-architecture.md)：三进程职责和两条消息链路；
2. `proto/spark_push.proto`：ACK、游标、持久化事件和 gRPC 服务；
3. [`docs/spark-push-internals.md`](docs/spark-push-internals.md)：从 Redis Lua、连接表、PushStream 队列追到函数；
4. `comet/comet_server.cpp`：WebSocket 握手、上行解析、缺口同步和离线补推；
5. `logic/grpc_service.cpp`、`logic/redis_store.cpp`、`logic/conversation_store.cpp`：序号、幂等、场景路由；
6. `job/service.cpp`、`common/kafka_consumer.cpp`：consumer group、PushStream、offset 和落库；
7. `load_test/e2e_bench.cpp`：如何区分 accepted、delivered ACK 和真实接收。

教学快照 `02_auth_subset`～`06` 只用于分阶段学习；运行和面试说明以根目录源码为准。

## 简历表述边界

可以写：

- 设计 Comet / Logic / Job 三层 C++ 实时消息架构，以 Kafka 解耦业务受理和下行投递；
- 使用 Redis Lua 实现会话内 `msg_seq` 与 `client_msg_id` 原子幂等，引入 MySQL 水位和唯一键兜底；
- 将 ACK 拆为 accepted / delivered，引入游标缺口补偿、离线补推和 delivered cursor；
- 使用独立 persist topic、场景 topic、PushStream、重连重放和可抓取指标，建立可复现 E2E 验证；
- 单机 E2E 报告必须同时给出并发参数、accepted/delivered 数量、延迟分位数和数据库复核结果。

不要写“exactly-once”“消息绝不丢”“百万并发”或把 socket send 速度当系统 QPS。当前仍是单 broker 教学环境，DLQ、统一 HTTP 鉴权、TLS 和多机容量验证尚未完成。
