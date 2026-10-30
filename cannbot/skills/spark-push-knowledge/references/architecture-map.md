# 权威来源导航

按问题读取最小文件集。源码仓中的 `cannbot/knowledge-sources.json` 是可机器校验的登记表；
安装包中的 `references/installed-source-map.md` 记录原始路径、快照路径与 SHA-256。用
`skill_view("spark-push-knowledge", file_path="references/sources/<原始路径>")` 读取快照。

| 问题领域 | 首选文档 | 当前代码真源 |
|---|---|---|
| 总体架构、组件边界、启动入口 | `README.md`、`docs/spark-push-architecture.md`、`scripts/00_prepare_and_run.md` | `CMakeLists.txt`、各组件 `main.cpp` |
| 完整项目复习、简历表述、GitHub 发布与验证口径 | `docs/project-guide-and-resume.md` | 对应章节列出的当前源码和带日期测试报告 |
| 源码分发、全新克隆、独立 IM 启动与验收 | `README.md`、`docs/benchmarks/2026-10-12-release-smoke.json` | `scripts/ops/doctor.sh`、`tests/run_release_smoke.py` |
| Logic 内部三层、业务规则复用、Repository 接口与回归 | `docs/logic-business-layer.md` | `logic/services/`、`logic/repositories/`、`logic/main.cpp`、`tests/logic_business_service_test.cpp` |
| Inference Gateway、Worker、容量准入、会话路由、ROCm 和批处理 | `docs/inference-serving.md` | `inference/proto/inference.proto`、`inference/gateway/`、`inference/worker/`、`hermes_bridge/grpc_inference_client.cpp`、`inference/scripts/` |
| RX 9070 XT 真实 CPU/ROCm 性能、优化消融和简历证据 | `docs/performance-report-2026-09-30-rx9070xt.md` | `inference/benchmarks/`、`docs/benchmarks/2026-09-30-rx9070xt/` |
| RTX 3060 真实 CPU/CUDA 性能、本地模型复用、显存采样与算子验证 | `docs/performance-report-2026-10-07-rtx3060.md` | `inference/worker/gpu_memory_probe.*`、`inference/scripts/`、`inference/tests/real_runtime_test.py`、`docs/benchmarks/2026-10-07-rtx3060/` |
| 队列/哈希/Lua/握手等逐步算法 | `docs/spark-push-internals.md` | `common/thread_pool.cpp`、`common/mysql_pool.cpp`、`logic/redis_store.cpp`、`comet/comet_server.cpp`、`job/service.cpp` |
| WebSocket 接入、ACK、断线补偿 | `docs/reliability-optimization.md`、`docs/spark-push-internals.md` | `comet/comet_server.cpp`、`comet/comet_grpc_service.cpp` |
| 序号、幂等、会话历史、AI 请求编排 | `docs/reliability-optimization.md`、`docs/spark-push-internals.md`、`docs/hermes-integration.md` | `logic/services/message_service.cpp`、`logic/services/conversation_service.cpp`、`logic/message_reservation.cpp`、`logic/redis_store.cpp`、`logic/conversation_store.cpp` |
| Kafka 消费、持久化、Job→Comet 推送 | `docs/reliability-optimization.md`、`docs/spark-push-internals.md` | `job/service.cpp`、`common/kafka_consumer.cpp` |
| 工程审查修复、迁移、故障回归 | `docs/review-remediation.md` | `logic/message_reservation.cpp`、`common/internal_rpc.h`、`sql/migrations/20261011_message_reservations.sql`、`tests/` |
| Wire contract 与 RPC | `docs/review-remediation.md` | `proto/spark_push.proto`、`common/internal_rpc.h` |
| Kernel Agent 产品身份、子任务编排与权限 | `docs/kernel-agent.md`、`docs/codex-subagent-reference.md` | `cannbot/pi/extensions/kernel-subagents.ts`、`cannbot/skills/spark-push-agent/` |
| Agent HTTP/SSE 适配 | `docs/hermes-integration.md`、`docs/pi-agent-integration.md` | `hermes_bridge/hermes_client.cpp`、`hermes_bridge/sse_parser.cpp`、`hermes_bridge/main.cpp`、`cannbot/scripts/pi_gateway.py` |
| Agent 最终回复重试、持久化死信与恢复 | `docs/pi-agent-integration.md` 第 6 节 | `logic/hermes_reply.cpp`、`logic/services/message_service.cpp`、`common/kafka_consumer.cpp`、`job/service.cpp` |
| Agent 常用命令卡片、模型/思考等级/新建/重试与通用事件 | `docs/pi-agent-integration.md` 第 7、8 节 | `cannbot/scripts/agent_events.py`、`common/agent_event.h`、`web_demo/static/agent_channel.js`、`cannbot/scripts/pi_gateway.py` |
| Pi ↔ IM 架构审查、通用事件、会话队列与隔离方案 | `docs/pi-im-architecture-review.md`（2026-09-06 实施前审查快照）；当前路由/turn/事件合同见 `docs/multi-agent-integration.md` | `cannbot/scripts/agent_events.py`、`cannbot/scripts/agent_router.py`、`cannbot/scripts/pi_gateway.py`、`hermes_bridge/`、`logic/grpc_service.cpp` |
| WSL Kernel Agent 安装、配置与二次开发 | `docs/pi-agent-integration.md`、`docs/cannbot-integration.md` | `/home/peco/.pi-spark-agent`、`cannbot/pi/` |
| Agent 命令和多轮上下文 | `docs/hermes-integration.md` | `logic/hermes_command.*`、`logic/services/message_service.cpp` |
| 多 Agent 选择、粘滞路由、事件回放、所有者配置和 Windows technical 隔离接入 | `docs/multi-agent-integration.md` | `cannbot/scripts/agent_events.py`、`cannbot/scripts/agent_router.py`、`cannbot/scripts/pi_gateway.py`、`cannbot/scripts/hermes_adapter.py`、`cannbot/scripts/windows_agent_transport.py` |
| Hermes Runs API 原生运行时（默认关闭，不替换 Pi 或 Legacy） | `docs/hermes-native-runtime-integration.md` | `cannbot/scripts/hermes_native_adapter.py`、`cannbot/scripts/windows_agent_http.py`、`cannbot/scripts/agent_router.py`、`logic/hermes_command.cpp`、`hermes_bridge/sse_parser.cpp` |
| 多人共享 CANN 房间、算子任务交接、开发授权与版本化测试回执 | `docs/room-agent-collaboration.md` | `logic/services/room_agent_service.*`、`logic/repositories/room_agent_repository.*`、`cannbot/scripts/room_agent.py`、`cannbot/pi/extensions/room-workspace.ts`、`tests/room_agent_e2e.py` |
| 会话内串行、会话间并行、排队超时与 Agent 看板 | `docs/spark-push-internals.md` 第 31.4 节 | `cannbot/scripts/agent_scheduler.py`、`cannbot/scripts/agent_router.py`、`cannbot/scripts/pi_gateway.py` |
| 图片发送、截图问答、附件不进 Kafka/WS | `docs/spark-push-architecture.md` 第 2.5 节、`docs/spark-push-internals.md` 第 32、31.5 节、`docs/pi-agent-integration.md` | `common/image_attachment.cpp`、`logic/attachment_dao.cpp`、`logic/http_server.cpp`、`logic/services/message_service.cpp`、`hermes_bridge/main.cpp`、`cannbot/scripts/pi_gateway.py`、`web_demo/static/index.html`、`android-app/.../SparkViewModel.kt` |
| Logic 重启后发消息停在「发送中」 | `docs/spark-push-internals.md` 第 18、31.6 节 | `comet/comet_server.cpp` `ReconnectStream` |
| Android 原生客户端、历史游标、流式渲染与输出长度审计 | `docs/spark-push-architecture.md`、`docs/spark-push-internals.md`、`docs/spark-push-implementation.md` | `android-app/app/src/main/java/com/peco/sparkim/SparkReliability.kt`、`SparkClient.kt`、`SparkViewModel.kt`、`MainActivity.kt`、`SparkModels.kt`、`logic/message_dao.cpp` |
| Hermes 延迟优化与 WSL 移植 | `docs/hermes-wsl-migration.md`（2026-09-07 评估） | `cannbot/scripts/windows_agent_transport.py`、`cannbot/scripts/hermes_adapter.py`、`cannbot/scripts/audit_hermes_wsl.py` |
| 用户、管理员、Token、审计 | `docs/user-center.md` | `logic/http_server.cpp`、`logic/user_dao.*`、`logic/audit_log_dao.*`、`common/security.*` |
| 数据库表与迁移 | `docs/user-center.md`、`docs/reliability-optimization.md` | `sql/schema.sql`、`sql/migrations/` |
| 当前 SQL 消息链路回归、三轮负载和大突发超时证据 | `docs/benchmarks/2026-10-11-current/README.md`、`docs/project-guide-and-resume.md` 第 6 节 | `tests/run_current_benchmarks.py`、`load_test/`、同目录 JSON 回执 |
| 历史 E2E、负载和性能实验 | `load_test/README.md`、`docs/performance-report-2026-08-09.md` | `load_test/`、`tests/`；旧数据不代表当前容量 |
| 教学演进与简历边界 | `docs/interview-project-evolution.md` | 根目录与历史 subset 的差异 |

安装后的文档和代码统一位于 `references/sources/`，并保持原始相对路径。例如：

- `README.md` → `references/sources/README.md`
- `docs/hermes-integration.md` → `references/sources/docs/hermes-integration.md`
- `docs/hermes-native-runtime-integration.md` → `references/sources/docs/hermes-native-runtime-integration.md`
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
