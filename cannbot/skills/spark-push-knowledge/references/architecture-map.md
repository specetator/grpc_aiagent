# 权威来源导航

按问题读取最小文件集。源码仓中的 `cannbot/knowledge-sources.json` 是可机器校验的登记表；
安装包中的 `references/installed-source-map.md` 记录原始路径、快照路径与 SHA-256。用
`skill_view("spark-push-knowledge", file_path="references/sources/<原始路径>")` 读取快照。

| 问题领域 | 首选文档 | 当前代码真源 |
|---|---|---|
| 总体架构、组件边界、启动入口 | `README.md`、`docs/spark-push-architecture.md`、`scripts/00_prepare_and_run.md` | `CMakeLists.txt`、各组件 `main.cpp` |
| 队列/哈希/Lua/握手等逐步算法 | `docs/spark-push-internals.md` | `common/thread_pool.cpp`、`common/mysql_pool.cpp`、`logic/redis_store.cpp`、`comet/comet_server.cpp`、`job/service.cpp` |
| WebSocket 接入、ACK、断线补偿 | `docs/reliability-optimization.md`、`docs/spark-push-internals.md` | `comet/comet_server.cpp`、`comet/comet_grpc_service.cpp` |
| 序号、幂等、会话历史、AI 请求编排 | `docs/reliability-optimization.md`、`docs/spark-push-internals.md`、`docs/hermes-integration.md` | `logic/grpc_service.cpp`、`logic/redis_store.cpp`、`logic/conversation_store.cpp` |
| Kafka 消费、持久化、Job→Comet 推送 | `docs/reliability-optimization.md`、`docs/spark-push-internals.md` | `job/service.cpp`、`common/kafka_consumer.cpp` |
| Wire contract 与 RPC | — | `proto/spark_push.proto` |
| Agent HTTP/SSE 适配 | `docs/hermes-integration.md`、`docs/pi-agent-integration.md` | `hermes_bridge/hermes_client.cpp`、`hermes_bridge/sse_parser.cpp`、`hermes_bridge/main.cpp`、`cannbot/scripts/pi_gateway.py` |
| Agent 最终回复重试、持久化死信与恢复 | `docs/pi-agent-integration.md` 第 6 节 | `logic/hermes_reply.cpp`、`logic/grpc_service.cpp`、`common/kafka_consumer.cpp`、`job/service.cpp` |
| Agent 常用命令卡片、模型/思考等级/新建/重试与通用事件 | `docs/pi-agent-integration.md` 第 7、8 节 | `cannbot/scripts/agent_events.py`、`common/agent_event.h`、`web_demo/static/agent_channel.js`、`cannbot/scripts/pi_gateway.py` |
| Pi ↔ IM 架构审查、通用事件、会话队列与隔离方案 | `docs/pi-im-architecture-review.md`（2026-09-06 实施前审查快照）；当前路由/turn/事件合同见 `docs/multi-agent-integration.md` | `cannbot/scripts/agent_events.py`、`cannbot/scripts/agent_router.py`、`cannbot/scripts/pi_gateway.py`、`hermes_bridge/`、`logic/grpc_service.cpp` |
| WSL Pi Agent 安装、配置与二次开发 | `docs/pi-agent-integration.md`、`docs/cannbot-integration.md` | `/home/peco/.pi-spark-agent`、`cannbot/pi/` |
| Agent 命令和多轮上下文 | `docs/hermes-integration.md` | `logic/hermes_command.*`、`logic/grpc_service.cpp` |
| 多 Agent 选择、粘滞路由、事件回放、所有者配置和 Windows technical 隔离接入 | `docs/multi-agent-integration.md` | `cannbot/scripts/agent_events.py`、`cannbot/scripts/agent_router.py`、`cannbot/scripts/pi_gateway.py`、`cannbot/scripts/hermes_adapter.py`、`cannbot/scripts/windows_agent_transport.py` |
| Android 原生客户端、历史游标、流式渲染与输出长度审计 | `docs/spark-push-architecture.md`、`docs/spark-push-internals.md`、`docs/spark-push-implementation.md` | `android-app/app/src/main/java/com/peco/sparkim/SparkClient.kt`、`SparkViewModel.kt`、`MainActivity.kt`、`SparkModels.kt`、`logic/message_dao.cpp` |
| Hermes 延迟优化与 WSL 移植 | `docs/hermes-wsl-migration.md`（2026-09-07 评估） | `cannbot/scripts/windows_agent_transport.py`、`cannbot/scripts/hermes_adapter.py`、`cannbot/scripts/audit_hermes_wsl.py` |
| 用户、管理员、Token、审计 | `docs/user-center.md` | `logic/http_server.cpp`、`logic/user_dao.*`、`logic/audit_log_dao.*`、`common/security.*` |
| 数据库表与迁移 | `docs/user-center.md`、`docs/reliability-optimization.md` | `sql/schema.sql`、`sql/migrations/` |
| E2E、负载和性能证据 | `load_test/README.md`、`docs/performance-report-2026-08-09.md` | `load_test/`、`tests/` |
| 教学演进与简历边界 | `docs/interview-project-evolution.md` | 根目录与历史 subset 的差异 |

安装后的文档和代码统一位于 `references/sources/`，并保持原始相对路径。例如：

- `README.md` → `references/sources/README.md`
- `docs/hermes-integration.md` → `references/sources/docs/hermes-integration.md`
- `logic/grpc_service.cpp` → `references/sources/logic/grpc_service.cpp`
- `hermes_bridge/hermes_client.cpp` → `references/sources/hermes_bridge/hermes_client.cpp`

不要读取未登记的构建产物或从旧路径推测实现。若需要的实时文件不在快照中，明确说明“知识包
未覆盖或可能过期”，并建议在源码仓登记后重新同步。

## 判断优先级

1. 当前根目录代码与协议定义；
2. 与代码同步维护的 canonical 文档；
3. 带日期的性能/测试快照；
4. 教学历史目录和演进说明；
5. 尚未验证的建议。

性能报告只代表报告中记录的环境和参数，不能外推为生产容量。若文档和代码不一致，说明差异
并以当前代码描述“已经实现”的行为。
