# 权威来源导航

按问题读取最小文件集。源码仓中的 `cannbot/knowledge-sources.json` 是可机器校验的登记表；
安装包中的 `references/installed-source-map.md` 记录原始路径、快照路径与 SHA-256。用
`skill_view("spark-push-knowledge", file_path="references/sources/<原始路径>")` 读取快照。

| 问题领域 | 首选文档 | 当前代码真源 |
|---|---|---|
| 总体架构、组件边界、启动入口 | `README.md`、`docs/spark-push-architecture.md`、`scripts/00_prepare_and_run.md` | `CMakeLists.txt`、各组件 `main.cpp` |
| 完整项目复习、简历表述、GitHub 发布与验证口径 | `docs/project-guide-and-resume.md` | 对应章节列出的当前源码和带日期测试报告 |
| 源码分发、全新克隆、独立 IM 启动与验收 | `README.md`、`docs/benchmarks/2026-10-12-release-smoke.json` | `scripts/ops/doctor.sh`、`tests/run_release_smoke.py` |
| 固定提交的 GHCR 候选、安装包、真实验收与同镜像推广 | `docs/releasing.md`、`docs/benchmarks/2026-11-01-release-tooling.json` | `.github/workflows/release.yml`、`scripts/package_release.py`、`scripts/verify_release.py`、`tests/clean_linux_release_e2e.py` |
| 候选制品 Kafka 匿名卷与显式平台备份回归 | `docs/benchmarks/2026-11-01-release-candidate-fixes.json` | `docker-compose.yml`、`scripts/spark_backup.py`、`tests/kafka_compose_volume_e2e.py`、`tests/backup_restore_e2e.py` |
| 单机 IM 冷备、全新项目恢复及 Runtime 范围 | `docs/backup-restore.md` | `scripts/spark_backup.py`、`tests/backup_restore_e2e.py` |
| 公共 Runtime 与 IM 的整批冷备、原路径恢复 | `docs/backup-restore.md`、`docs/benchmarks/2026-11-01-team-backup-restore.json` | `scripts/spark_team_backup.py`、`cannbot/scripts/public_runtime_archive.py`、`tests/team_backup_scenario.py`、`tests/runtime_archive_native_e2e.py` |
| 公共与 legacy CANN 部署边界、恢复后的双次准入检查 | `docs/optional-agent-connector.md`、`docs/benchmarks/2026-11-01-agent-deployment-modes.json` | `docker-compose.legacy-cann.yml`、`logic/services/room_agent_service.cpp`、`scripts/verify_public_agent_mode.py` |
| 公共 Runtime 停机、原生进程锁和冻结条件 | `docs/optional-agent-connector.md` | `cannbot/scripts/public_runtime.py`、`cannbot/scripts/public_runtime_maintenance.py`、`tests/public_runtime_lifecycle_e2e.py` |
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
| 用户自助配置 DSH/Codex/Claude Code/Hermes/OpenClaw | `docs/customer-agent-connections.md` | `logic/services/agent_connection_service.cpp`、`cannbot/scripts/user_agent_connections.py`、`cannbot/scripts/agent_connector.py`、`cannbot/scripts/agent_cli_adapter.py`、`web_demo/static/agent_connections.js` |
| 多 Agent 选择、粘滞路由、事件回放、所有者配置和 Windows technical 隔离接入 | `docs/multi-agent-integration.md` | `cannbot/scripts/agent_events.py`、`cannbot/scripts/agent_router.py`、`cannbot/scripts/pi_gateway.py`、`cannbot/scripts/hermes_adapter.py`、`cannbot/scripts/windows_agent_transport.py` |
| Hermes Runs API 原生运行时（默认关闭，不替换 Pi 或 Legacy） | `docs/hermes-native-runtime-integration.md` | `cannbot/scripts/hermes_native_adapter.py`、`cannbot/scripts/windows_agent_http.py`、`cannbot/scripts/agent_router.py`、`logic/hermes_command.cpp`、`hermes_bridge/sse_parser.cpp` |
| 多人共享 CANN 房间、算子任务交接、开发授权与版本化测试回执 | `docs/room-agent-collaboration.md` | `logic/services/room_agent_service.*`、`logic/repositories/room_agent_repository.*`、`cannbot/scripts/room_agent.py`、`cannbot/pi/extensions/room-workspace.ts`、`tests/room_agent_e2e.py` |
| Docker 公开 CANN 原文、独立文档导出与旧私聊回归 | `docs/cann-docker.md` | `scripts/export_cann_documents.py`、`docker-compose.cann.yml`、`tests/legacy_private_runtime_e2e.py`、`tests/legacy_private_runtime_fixture.py` |
| 公开 CANN 文档冷备、恢复路径与只读挂载证据 | `docs/benchmarks/2026-11-01-cann-document-backup.json` | `scripts/spark_backup.py`、`scripts/export_cann_documents.py`、`tests/backup_restore_e2e.py`、`docs/backup-restore.md` |
| CANN 发布重投、旧回执补建、reservation 清理与真实私聊证据 | `docs/benchmarks/2026-11-01-legacy-cann-private.json` | `logic/services/room_workspace_service.cpp`、`logic/message_reservation.cpp`、`tests/space_mysql_integration_test.cpp` |
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

## 协作空间与通用 Room Agent

- `docs/collaboration-spaces.md`：正式成员、邀请、角色、撤销。
- `docs/generic-room-agents.md`：Catalog、多绑定、原生 Session 隔离、任务及恢复 API。
- `docs/web-collaboration-client.md`：统一 Web 入口、按账号邀请、任务/工作区表单、消息缓存恢复与三浏览器验收。
- `docs/benchmarks/2026-11-01-web-collaboration.json`：真实三浏览器、Pi/Hermes 只读代码、Job 延迟持久化、断网/重启与未覆盖项。
- `docs/room-workspaces.md`：批准项目、不可变文件快照、Pi/Hermes 原生只读工具与最终发布决定；区分真实 Runtime 和完整 IM E2E。
- `docs/mvp-v1-acceptance.md`：M1–M6 / T01–T14 的证据类型与未覆盖边界。
- `docs/benchmarks/2026-10-31-generic-room-agents.md`：本轮真实/受控验收回执、变更清单与源文件指纹。
- `docs/v0.1.0-delivery.md`：v0.1.0 已发布制品、分阶段交付和保留的历史失败；不等同于 MVP V1.0 全量验收。
- `docs/benchmarks/2026-11-01-v0.1.0-release.json`：正式 Git 标签、镜像、安装包及八类真实制品回执；含发布权限失败、维护者完成发布及未覆盖范围。
- `docs/install.md`：完整 Docker 同源入口、凭据/数据卷、增量迁移和旧 WSL 开发模式。
- `docs/optional-agent-connector.md`：可选 Connector overlay、独立授权回连、凭据导出及配置故障隔离；正式公共 Runtime 与完整浏览器验收状态见交付跟踪。
- `tests/public_cross_room_scenario.py`：正式公共启动器下的两私有空间、原生工具与会话隔离专项；复用 `tests/public_room_runtime_e2e.py --cross-room`，消费屏障和冷重启后核对原始执行记录。
- `docs/benchmarks/2026-11-01-public-cross-room.json`：两个真实公共 Agent、四个原生会话、聊天与执行并存、Kafka 重投/冷重启的限定回执；逐项列出未覆盖的主动越权读取及上下文断言方向。

## 多模态资料（当前交付图文，音视频暂缓）

- `docs/benchmarks/2026-11-02-document-scope.json`：用户明确暂缓音频/视频；当前入口聚焦图文，27 项受控浏览器检查通过。旧 ASR 失败仍保留，但不再阻塞此阶段，也不再要求录音输入。

- `docs/media-agent-context.md`：已实现 context generation、请求/授权、首次发布、背景筛选与 namespace，以及服务端资料 API/来源回执；Native 资料工具、真实视觉、最终/任务引用仍待接入。
- `docs/room-context-tools.md`、`logic/services/media_context_service.*`、`logic/repositories/media_context_repository.*`：canonical request 驱动的房间聊天/媒体搜索、固定版本片段/JPEG 读取、事务来源复验与回执；无代码项目也可授权资料。
- `sql/migrations/20261101_media_context_sources.sql`：增量来源回执表；不清理旧 request、消息或 Session。
- `tests/media_context_service_test.cpp`、`tests/media_context_http_test.py`：受控撤权合同与真实 HTTP/MySQL/Redis/资料解析分列；聊天/request 由夹具建立，不代表 Kafka/Native/Web E2E。
- `docs/benchmarks/2026-11-01-room-context-tools.json`：资料 API 阶段的命令、源文件指纹、实际行锁竞争、来源预算/摘要/重启测试与未覆盖项。
- `docs/benchmarks/2026-11-01-media-context.json`：代次合同与真实 SQL 迁移、发布/撤销锁竞争、旧 generic 结果收养和回归；不等于共享删除、Native 媒体或 Web 完成。
- `cannbot/media/parser.py`、`cannbot/media/sandbox.py`：隔离文档/图片/音视频解析、输出复验与精确 execution 回收；真实 HTTP 发布和持久 worker 接管已贯通。
- `cannbot/media/provision_asr.py`：管理员预置固定版本的公共 ASR 模型，校验 Git/LFS 身份；运行时禁网且不使用私人模型配置。
- `tests/media_parser_test.py`、`tests/media_parser_e2e.py`：输出边界合同与真实解码/ASR（合成资料）；不等于原生 Agent 或多人浏览器 E2E。
- `docs/multimodal-collaboration.md`：全范围设计、上传 API、权限与尚未完成的解析/原生理解/Web 验收。
- `logic/services/media_service.cpp`、`logic/repositories/media_repository.cpp`、`logic/repositories/media_blob_store.cpp`：有界分片、正式成员授权、原文件与处理任务入队。
- `tests/media_blob_store_test.cpp`、`tests/media_mysql_integration_test.cpp`：真实文件及真实 SQL 范围，不等于多模态模型 E2E。
- `logic/services/media_processing_service.cpp`、`logic/repositories/media_processing_repository.cpp`：内部领取/租约、片段与原文验证、原子发布，正式成员可查当前索引。
- `cannbot/scripts/media_text_worker.py`：本地 UTF-8/Markdown pipeline、持久领取身份与崩溃恢复；不是 PDF/ASR/视频或 Agent Loop。
- `tests/media_processing_http_test.py`、`docs/benchmarks/2026-11-01-media-processing.json`：真实 HTTP/MySQL/Redis、正式登录/成员、worker 强杀与控制到期测试，逐项列出证据限制。
- `tests/pi_media_capability_probe.py`、`docs/benchmarks/2026-11-01-pi-media-capability.json`：隔离 Pi 原生工具传递真实图像；不代表 Hermes Native 或房间媒体 E2E。

- `logic/repositories/media_artifact_repository.cpp`、`sql/migrations/20261101_media_artifacts.sql`：派生物持久预留、确定性身份、失败回收、完整发布与原件聚合配额；有限候选扫描仍经 Reclaim 复查。
- `tests/media_artifact_mysql_test.cpp`：真实 SQL 连接终止、配额并发/锁等待、GC 失败及文件恢复。
- `docs/benchmarks/2026-11-01-media-parser.json`：12 项边界单测、22 项真实解析 E2E、20 分钟合成语音完整转写及各项限制。
- `docs/benchmarks/2026-11-01-media-artifact-storage.json`：实际 SQL 断连、GC 回执失败、配额竞争与回归；基础仓储通过不等于媒体发布/原生工具完成。
- `docs/media-publication.md`、`sql/migrations/20261101_media_publication.sql`：独立媒体 profile/批准指纹、claim/reserve、原子 manifest 发布、正式成员派生物读取。
- `logic/services/media_manifest.cpp`、`tests/media_manifest_test.cpp`：页/节/图/音视频覆盖、引用和规范摘要合同。
- `tests/media_pipeline_http_test.py`、`tests/media_large_payload_test.py`：真实解析→HTTP→SQL/文件→授权读取，20 分钟合成录音与大 JPEG；不等于生产 worker 或原生 Agent E2E。
- `docs/benchmarks/2026-11-01-media-publication.json`：发布阶段源码、命令、真实回执、跳过与未覆盖边界。
- `cannbot/scripts/media_worker.py`、`docs/media-worker.md`：独立生产 worker CLI、持久 claim/execution、续租硬期限、配额等待、原 bundle 复验、丢回执恢复与有限派生物 GC；尚未加入 Compose 默认启动。
- `docs/media-worker-operations.md`、`scripts/ops/media.sh`、`deploy/media-worker.json.example`：私有配置与批准指纹、sparkctl 独立启停、systemd 用户进程管理；不加载个人环境，不隐式随基础 IM 启停。
- `tests/media_worker_test.py`、`tests/media_worker_http_test.py`：假时间/传输的合同测试，与真实 Docker/HTTP/MySQL/Redis 强杀恢复分开；无完整 bundle 时允许重新做只读解析。
- `docs/benchmarks/2026-11-01-media-worker.json`：16 项实际 worker 场景（含完整 20 分钟合成语音及运行中 fencing）、全量回归和逐项范围；不等于原生媒体理解、Web 或完整产品验收。

## 原生房间资料工具（开发工作树）

- `logic/services/room_task_service.cpp`、`logic/repositories/room_context_source_sql.h`：当前成员按原 request 查询服务端来源；任务 JSON/历史保存 source_ids，事务内复验，失效后允许更新既有任务。
- `docs/benchmarks/2026-11-01-source-links.json`：来源预览/持久任务阶段的定向构建、2 项 Service 回归、23 项受控浏览器检查和 13 组真实 HTTP/MySQL/Redis；没有模型/Kafka/三浏览器新媒体验收。
- `web_demo/static/room_media.js`、`docs/web-collaboration-client.md`：正式页与旧聊天室共用的资料上传/续传、状态、预览、下载与结构化 Agent Mention 草稿；网页 256 MiB 上限，不自动调用模型。
- `docs/benchmarks/2026-11-01-multimodal-service.json`：真实服务部分成功与失败、受控浏览器检查和本地镜像交付；会议负责人识别、视频整链路及媒体恢复尚未通过，不代表多模态目标完成。
- `docs/multimodal-service-e2e.md`、`tests/public_media_scenario.py`：现有三用户真实 Runtime harness 的可选媒体场景，独立 worker 凭据与内部完整路径接线；合成素材协议验收不等于浏览器验收。
- `docs/benchmarks/2026-11-02-audio-task-recovery.json`：同次真实模型输出中合格音频任务的保存、来源与内容恢复；整份回答仍因术语错误失败，不代表音频语义或浏览器端到端验收通过。
- `docs/benchmarks/2026-11-02-web-media-analysis.json`、`tests/collaboration_web_media_analysis.cjs`：三浏览器 PDF 上传→真实 Hermes 分析→图片来源→保存交接→重启/撤权的 7 组定向检查；保留前两次断言失败，不覆盖 Pi 或音视频网页分析。
- `docs/benchmarks/2026-11-02-web-video-analysis.json`、`tests/collaboration_web_video_analysis.cjs`：复用网页上传入口与真实视听校验，7 组检查覆盖画面来源、原视频定位播放、重启与撤权；只选短合成视频，不代表长录音准确性通过。
- `docs/benchmarks/2026-11-02-asr-domain-context.json`：相同短录音及 turbo 模型加入通用技术会议背景仍识别错误，未改变正式 parser；没有向模型提供预期姓名或正确术语。

- `docs/room-context-native-tools.md`：Pi/Hermes 三工具、显式配置、原生去图错误、todo hydration 兼容与真实失败边界。
- `cannbot/room-context-tools.json`、`cannbot/scripts/room_context.py`、`cannbot/pi/extensions/room-context.ts`：共享模型参数、受信 request 注入、有界传输和原生图像输出。
- `cannbot/scripts/hermes_public_workspace.py`：Native 精确工具集、ContextVar 隔离及实例级恢复兼容；不修改私人源或新造 Loop。
- `tests/room_context_runtime_probe.py`：真实 Pi/Hermes 模型、provider 图片/来源对应、同图复跑、原生历史/todo 隔离；HTTP authority 是夹具。
- `tests/room_context_native_policy_test.py`：已安装 Native 的真实 converter/strip 合同，能力谓词受控，不是 provider E2E。
- `tests/room_context_http_client.cjs`、`tests/media_context_http_test.py`：真实客户端→Logic→MySQL/Redis/资料解析与撤权；不含模型/Kafka/Web。
- `docs/benchmarks/2026-11-01-room-context-native.json`：原始失败图片与回执、当前验证范围；Pi 视觉语义未通过，不能将图像送达当作整体验收通过。
