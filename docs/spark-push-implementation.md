# Spark Push 设计与实现文档索引

文档分成两层，不要混着读：

| 层 | 文档 | 写什么 | 不写什么 |
| --- | --- | --- | --- |
| 大框架 | [`spark-push-architecture.md`](spark-push-architecture.md) | 模块职责、完整链路、输入输出、可靠性边界 | 容器字段、Lua 逐步执行、帧字节 |
| 小模块 | [`spark-push-internals.md`](spark-push-internals.md) | 队列/哈希/有序树如何配合、Redis Lua、握手 SHA1、PushStream 匹配、TreeMap 合并 | 简历口径、启动命令大全 |

专题文档仍然有效，互不替代：

| 问题 | 文档 |
| --- | --- |
| ACK / 游标 / 指标语义 | [`reliability-optimization.md`](reliability-optimization.md) |
| 启动、排障、健康检查 | [`../scripts/00_prepare_and_run.md`](../scripts/00_prepare_and_run.md) |
| Pi Agent / CANN 引用 | [`pi-agent-integration.md`](pi-agent-integration.md)、[`cannbot-integration.md`](cannbot-integration.md) |
| 多 Agent 路由与隔离 | [`multi-agent-integration.md`](multi-agent-integration.md) |
| 用户中心与管理员 | [`user-center.md`](user-center.md) |
| 教学演进与简历边界 | [`interview-project-evolution.md`](interview-project-evolution.md) |

源码真源永远是根目录 `comet/` `logic/` `job/` `common/` `proto/` `hermes_bridge/` `android-app/`。教学快照 `02_auth_subset`～`06` 只用于对比演进。

---

## 推荐阅读顺序

1. [`spark-push-architecture.md`](spark-push-architecture.md) 第 1–2 节：三进程和两条消息链路。
2. `proto/spark_push.proto`：`AckStage`、`MessageStream`、`PushStream`、`SyncMessages`。
3. [`spark-push-internals.md`](spark-push-internals.md) 第 4–5、17–19 节：Lua 取号和两条 gRPC 流。
4. 同一文档第 14–16、27、31.4 节：WebSocket 帧、去重窗口、Android TreeMap、会话并行调度。
5. [`reliability-optimization.md`](reliability-optimization.md)：哪些数字可以写进简历，哪些不能。
6. 统一回归：`./scripts/sparkctl.sh regress`。

---

## Android 与输出长度（实现要点）

Android 已从 WebView 壳迁到 Kotlin/Jetpack Compose，覆盖登录、单聊历史分页、实时消息、离线补推、Agent 卡片、模型/思考等级、CANN 原文和管理员控制台。联系人 ID 与 PC 隔离：Pi `900000000201`，Hermes technical `900000000211`。

~~~bash
cd android-app
./gradlew assembleDebug --no-daemon --console=plain
~~~

模块落点：

- `SparkClient.kt`：HTTP Bearer、WebSocket、连接代数、16 MiB 单帧保护
- `SparkViewModel.kt`：历史游标、有界 `TreeMap` 流式合并、envelope 校验、弱网保留登录、40 ms 刷新
- `MainActivity.kt`：Setup/Auth/Home/Chat/Knowledge/Admin
- `SparkModels.kt`：`singleSessionId()` 必须与 Logic 公式一致

单条 Agent 回复变短时，按 [`spark-push-internals.md`](spark-push-internals.md) 第 21、28 节区分：12 KiB prompt 预算、provider 输出、Bridge 拼接、Logic 写入、MySQL 读取、客户端展示。`sql/migrations/002_message_content_mediumtext.sql` 把历史字段提升为 MEDIUMTEXT；`SPARK_PUSH_LENGTH_AUDIT=1` 记录 UTF-8 字节和 Unicode 字符数。迁移不会扩大模型窗口，也不会改变输入框可见高度。

---

## 数据边界

| 数据 | 权威位置 |
| --- | --- |
| Android 地址 / token / 昵称 | SharedPreferences `"spark_native"`，不存聊天历史 |
| 最终消息 | MySQL `spark_push.message` |
| Agent 路由 / turn / event | `~/.pi-spark-agent/im-router/routing.sqlite3` |
| token / 序号 / 热路由 | Redis |
| accepted → persist/push | Kafka |

API key、密码和 Windows profile `.env` 不进入源码、文档、AgentEvent 或客户端卡片。

---

## 验证入口

~~~bash
cmake --build build-wsl -j2
ctest --test-dir build-wsl --output-on-failure
PYTHONPATH=cannbot/scripts python3 cannbot/scripts/test_agent_router.py
python3 cannbot/scripts/validate_knowledge.py --strict
scripts/sparkctl.sh health
~~~

E2E 成功条件：`sent == accepted_ack == delivered_ack == delivered`，且 Job 继续消费 persist 后再查 `message` 表。跳过 Redis 外部测试、没有 ADB 设备，都不能写成“该项已在本次运行中验证”。
