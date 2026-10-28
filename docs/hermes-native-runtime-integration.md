# Hermes Native Runtime 集成与验收记录

验证日期：2026-10-28（Asia/Shanghai）。Spark 基线 HEAD：
`0c4f1f2c4204481ce77a4a16162a360bbf2bf2fc`，开发分支 `hermes-native-runtime`。
本次进入仓库时该分支已有 18 个未提交文件（包括 Native 草稿与测试）；保留原工作，
在其上修正协议、恢复和控制缺口，没有 reset、清理、提交或推送。
原文件副本与初始 SHA-256 在本机 `/tmp/spark-native-baseline/initial-manifest.json`。

**交付状态：默认关闭的 Runs API 实现及契约测试已完成；没有切换现网联系人。**
本次对实际 technical 服务只做了只读 capabilities 探针。
尚未完成隔离真实 Agent 工具测试及完整 Native IM E2E，不能把本报告当作生产启用验收。
PiRpcClient、Pi 配置、Skills/Provider、Session 文件和所有原联系人均未改动。

## 1. 现状调查与关键事实

已阅读 `AGENTS.md`、README、架构文档、multi-agent/pi-agent/pi-im-review/
hermes-wsl-migration，以及 Router、AgentStore、Scheduler、AgentEvent、Legacy Adapter、
Windows 中继、Bridge 与对应 Python/C++ 测试。旧评估文档的版本和性能数据不代表本次安装。

旧链路：

```text
用户 → Comet → Logic（认证、联系人/房间权限、消息序号）
     → Kafka ai_request → hermes_bridge::HandleRequest
     → pi_gateway.make_handler → AgentRouter.chat
     → HermesHttpAdapter.chat → WindowsRelayPool / WindowsResponse
     → 已运行的 technical /v1/chat/completions → Hermes AIAgent
     → SSE / AgentEvent → Bridge
     ├→ ai_delta → Logic / Job / Comet 临时预览
     └→ ai_reply → Logic 正式回复编排 → Job / MySQL / 离线同步
```

目标链路只把 `hermes_http` 分支换为显式注册的 `hermes_native`：

```text
原 Kafka ai_request → 同一个 Bridge → 同一个 Gateway / Router
  → HermesNativeAdapter → 原 Windows stdio 中继
  → 原 technical API：POST /v1/runs
  → GET /v1/runs/{run_id}/events（工具/审批/增量）
  → run.completed 或 GET 原 Run 确认 completed
  → AgentStore 成功快照 → 原 ai_reply → Kafka delivery confirmation
  → 内部 confirm_reply → AgentStore broker_confirmed
```

没有第二个消息入口、Kafka topic、消费者组、Hermes Gateway 或 IM 历史数据库。
控制通道仍由同一 Kafka consumer 接收；它只提前执行指定 Native 联系人的控制记录。

### 八项源码核对

| 问题 | 源码结论 |
|---|---|
| Hermes 已原生实现什么 | 本机 `api_server_runs._execute_run/_run_agent_sync` 创建并运行 `AIAgent`；Agent Loop、工具、Skills、Memory、压缩、模型调用由 Hermes 持有。Runs 暴露其中一部分控制，不能据此推断所有 Gateway 功能都有 HTTP API。 |
| Spark 重复了什么 | `HermesHttpAdapter._state/_creative_prompt/control` 保存模型选择、上次输入、上下文边界、创作设定和世界书；`AgentStore.append_context` 保存有界展示投影。它们不是 Hermes Agent Loop 或长期 Memory。本次保留这些业务投影，不新增工具注册或 Memory 引擎。 |
| 哪些命令真正调用 Runtime | Legacy `/model` 查询 `/api/model/options` 并用 `_lock_model` 等待会话模型确认；聊天经 Chat Completions；`/restart` 原有固定 CLI 生命周期操作保留在 Legacy。创作命令为 Spark 本地状态，`/new` 原为本地轮转、下次惰性创建。Native `/new now` 还实际创建独立 API Session，`/status` GET 原 Run，`/stop`、审批调用 Runs 原生端点，推理等级随 `model_options.reasoning_effort` 进入每次 Run。 |
| 上下文、模型、路由在哪里 | Hermes SessionDB 保存真实 transcript；Spark `routing.sqlite3` 的 states 保存本地控制投影，agent_routes 保存可信路由，agent_turns 保存请求幂等结果。Pi 保持原 JSONL Session/偏好文件。 |
| 旧路径有原生工具/审批事件吗 | Legacy 的 Chat Completions 文本流及 `hermes.tool.progress` 不等于 Runs 的 `tool.started/completed` 和 `approval.request`。新分支处理实际 Runs 字段。 |
| Windows/WSL 限制 | Windows loopback 不等于 WSL loopback；中继只能访问既有 127.0.0.1:8644 白名单，无公网端口。每个 worker 同时处理一个 HTTP 请求，SSE 长占连接；Native 单独预留两个控制 worker，普通 SSE 仍用原池。 |
| Pi/Hermes 耦合点 | Registry、Router、Scheduler、通用 HTTP Gateway、AgentEvent、Bridge/IM。Native 通过能力标志选择附加接口；Pi 调用参数和 PiRpcClient AST 保持原样。新增聊天命令对 Pi/Legacy 返回不支持，不进入 Pi RPC。 |
| 能复用多少账本 | 原 states/update 事务、agent_turns、agent_events、完成结果回放、启动 reclaim、原 Kafka/DLQ、SQL 回复幂等全部复用。新增 Native journal 放在同一个 AgentStore 的独立 namespace，没有新建数据库。 |

## 2. 本机 Hermes 与官方方案选择

本机源码：`/mnt/f/hermes/hermes-agent`，版本 `0.21.1`，提交：
`67764dc0863349a384c16425e73ee8571f3a94b7`。
检查的 `api_server.py`、`api_server_runs.py`、`api_server_run_idempotency.py` 和
`pyproject.toml` 没有未提交改动。本次未更新安装。

通过现有 Windows Python/stdin relay 对运行中 technical `GET /v1/capabilities` 的只读结果：

```json
{
  "model": "technical",
  "runtime": {"mode": "server_agent", "tool_execution": "server", "split_runtime": false},
  "features": {
    "run_submission": true,
    "run_status": true,
    "run_events_sse": true,
    "run_stop": true,
    "run_approval_response": true,
    "runs_idempotency": {"supported": true, "durable": true, "retention_seconds": 86400}
  }
}
```

Native 每次新准入先检查这些能力及入口模型身份；缺失则明确拒绝，不降级执行其他 Agent。

官方来源（同时核对上述本机源码，不能仅依赖线上 main）：

- [Programmatic integration](https://github.com/NousResearch/hermes-agent/blob/main/website/docs/developer-guide/programmatic-integration.md)：TUI Gateway、ACP、HTTP 均驱动 AIAgent。
- [Platform adapter](https://github.com/NousResearch/hermes-agent/blob/main/website/docs/developer-guide/adding-platform-adapters.md)：`BasePlatformAdapter` 的 connect/disconnect/send/handle_message 以及 `ctx.register_platform()`。
- [API Server](https://github.com/NousResearch/hermes-agent/blob/main/website/docs/user-guide/features/api-server.md)：原生 HTTP 能力及 Session 范围。
- [WeCom](https://github.com/NousResearch/hermes-agent/tree/main/plugins/platforms/wecom) 与 [IRC](https://github.com/NousResearch/hermes-agent/blob/main/plugins/platforms/irc/adapter.py)：平台收发桥接，不重建 Agent Loop。
- [DSH QQ Bot](https://github.com/tencent-connect/dsh-qqbot)：QQ 接入层通过 `ctx.agents` 驱动 Runtime，作为职责分离参考，没有将该插件代码移植进 Spark。

| 指标 | A：Platform Plugin | B：Runs API（本次主架构） | C：TUI Gateway JSON-RPC |
|---|---|---|---|
| 完整 Agent 核心 | Gateway Runner → AIAgent | 现有 API Server → AIAgent | TUI session → AIAgent |
| technical 复用 | 需在现有 Gateway 加载插件并安排生命周期 | 直接使用已运行服务 | 要确认现有可接入的 TUI 服务及身份 |
| Core 修改 | 插件可以零 Core 修改 | 无 | 无，需独立协议客户端 |
| Session writer 风险 | 加载进现有 Gateway 并使用独立 Session 可避免冲突；不是必然冲突 | 新 API Session，与 CLI/TUI 不合并 | 不得启动额外 writer 去占用现有 Session；接入现有 owner 才安全 |
| 事件/审批/停止 | 可复用平台/Gateway 机制，需要交互 UI 实现 | 已确认工具事件、once/deny、stop、GET 状态 | 丰富，包括 clarify/respond、session.compress、interrupt |
| Kafka/Bridge 结合 | 要改变入站归属与出站持久化桥接 | 延续现有 HTTP/SSE 和 IM 协议 | 新增跨 Windows stdio/WebSocket 协议 |
| 幂等恢复 | 平台需自行处理准入账本 | 原生 24h 幂等＋Spark 持久结果；未知不重 POST | 会话控制较完整，不能据此假定有 durable admission |
| Pi 影响 | 接口改动较多 | 能力分支、默认关闭 | 新宿主与协议增大共享层复杂度 |
| 多用户扩展 | 平台 ACL/映射工作较多 | Spark ACL＋独立 API Session；共享 Profile 仍不是 OS 沙箱 | 需管理 subscriber、owner 与会话生命周期 |
| 维护成本 | 中高 | 当前最低 | 高 |

选择 B；不同时实现 A/C。A 并非设计上不可行，只是本次不能安全替换在用 Gateway 的部署和
消息入口。C 的本机方法定义在 `tui_gateway/server.py`，丰富交互能力不等于当前 HTTP 端点已存在。

已确认的接口：POST `/v1/runs`、GET `/v1/runs/{id}`、GET `/events`、POST `/stop`、POST `/approval`，
以及 `/v1/capabilities`、`/v1/skills`、`/v1/toolsets`、`/v1/models`、`/api/model/options`、
`/api/sessions`、`/api/sessions/{id}/model`。
Runs 的 `/steer` 存在，本次未开放。Runs 没有 `/compact` 或 clarify 回答端点，不伪造成功。

## 3. 可信身份、Session 与所有权

Logic 从认证用户及已授权联系人生成 `s_<min(uid,bot)>_<max(uid,bot)>`；Gateway 只接受内部
Bearer 服务调用；Router `_identity/_visible/_selected` 校验联系人与 owner ACL。
消息正文不能指定 owner、Profile、URL 或 Run 所有权。共享房间原有 Pi 规则不扩展到 Native。

`canonical_route` 的 tenant/channel/conversation/thread 来自受信任配置和 Logic。
Native 的 backend ref 使用 `session_key + :u<authenticated-user>`，包含 Agent 选择及用户；
`_scope` 再加入 Native namespace/Profile 做 SHA-256。现部署 tenant=`local`、channel=`spark_pc`、
thread=`_`；Android 通过 201/211 联系人与 PC 隔离，不冒称已实现多租户认证服务。
Pi/Legacy backend ref 仍为原 Spark session，绝不迁移旧上下文。

- PC/Web Pi `900000000001`、Hermes `900000000101`；Android Pi `900000000201`、Hermes `900000000211` 原样。
- `hermes:<agent-id>` 与 `hermes-native:<agent-id>` 的状态完全分离。
- Native 使用 `POST /api/sessions` 创建确定性 ID，仅接受响应的匹配 ID 且 `source=api_server`。
  不借用 CLI/TUI/Gateway 的 transcript，不绕过 API 的源/所有权限制。
- `/new` 更换 transcript，并实际确认 API Session 创建；保留模型/创作投影、长期记忆及 IM 历史。
- Header `X-Hermes-Session-Key` 在该会话内稳定；`/new` 不清空 Memory。
- 原 Run 状态未知或仍运行时，禁止新输入、新上下文和模型变更抢占同一 transcript。
- Hermes 的 Run 权限 scope 是 API credential/Profile，**不是 Spark 用户 ACL**；用户隔离由 Router
  加本地 journal 绑定完成。个人 technical 仍只应开放给获准 owner，不能当作不可信多用户沙箱。

## 4. 执行账本与状态机

复用 `AgentStore.states`；Native 准入、结果及当前任务映射写入使用 SQLite `synchronous=FULL`，
Pi/预览仍用原写入策略。Native journal namespace 为 `hermes-native:<agent-id>:run`，key 为
Spark request_id。另用 `:current` 映射当前 session → request_id。记录字段包括：

```text
version, request_id, input_hash, payload_hash, session_id, hermes_session_id,
run_id, idempotency_key, runtime, profile, endpoint_hash,
status, created_at_ms, updated_at_ms, event_position,
approval{prompt_id,generation,expires_at_ms}, output, metadata, delivery_state
```

`input_hash` 绑定逻辑输入；`payload_hash` 绑定最终的创作投影、模型和 Run body。
Router 的输入 hash 在 Native 分支额外纳入 runtime/Profile/端点指纹，防止同 Agent ID 切换
Legacy/Native 后把旧请求当新输入执行。Pi 的原输入 hash 算法不变。

```text
admitting（先持久化） → POST 一次 → queued → running / waiting_for_approval / stopping
                                               ├→ completed + output
                                               ├→ failed / cancelled / interrupted
                                               └→ unknown → GET 原 Run → 上述状态
```

只有原生 completed 的非空 output 可以成为 assistant_final。增量拼接、EOF、消息文本结束、
普通工具结束均不是成功终态。已持久终态不能被迟到 timeout/事件降级。
本地成功快照优先于远端保留期，即便 Hermes 已回收 Run 仍可恢复投递。

`delivery_state`：`not_ready` → `result_ready` → `broker_confirmed`。
Bridge 只有 `ai_reply` 的 SendAndWait 成功后才通过内部 `confirm_reply` 写最后一步。
回执失败会重试同一缓存结果及回执；IM 继续按 request_id 幂等。它不是 delivered_ack 或已读状态。
崩溃可能令本地回执暂时落后于 Kafka 事实，不能把 `result_ready` 解读成“Kafka 一定未收到”。

Spark 仍是至少一次消息处理加幂等结果账本，**没有端到端 exactly-once**。

### 故障窗口

| 窗口 | 处理 |
|---|---|
| POST 已发出但无可信 run_id | 保留 admitting/unknown，不进行第二次 POST；不能用 POST 充当无副作用查询。人工核对准入结果。 |
| 幂等记录过期/服务丢失幂等库 | 不重新 POST；稳定 `spk-<64 hex>` 只能降低已知保留期内的重复风险，不能替代本地账本。 |
| Hermes 完成但 Router 未保存 | Native 先保存 output；重启 reclaim → unknown → 本地结果或 GET 原 Run恢复。 |
| SSE 断开/网络 reset | 不再订阅消耗型 SSE 队列；GET 同一个 Run。未结束保持 unknown，禁止启动另一个 writer。 |
| 同进程未知任务重投 | Router `mark_turn_unknown`，不误记 failed；下一次查询原 Run。 |
| Gateway 回传 unknown | SSE 标记 `native_unconfirmed`；Bridge 不缓存/发布永久错误答案，走原 Kafka 重试/DLQ。恢复仍不重执行。 |
| Spark 成功而 Kafka publish 失败 | Bridge 缓存结果重发；进程丢失则 Router/Native 成功快照回放。 |
| Broker 确认后客户端离线 | 原 Logic/Job/MySQL 历史与离线恢复；ai_delta 不补写历史。 |
| Hermes 重启 | 原生持久状态可能标为 interrupted；明确非成功，不自动重跑工具。 |
| 另一个用户/联系人引用相同请求 | Router hash/ACL、Native session/Profile/端点绑定拒绝；不接受客户端直传 run_id 执行控制。 |

没有 run_id 的未知准入、过期的远端结果且无本地快照仍需人工核对。
`/retry` 是明确的新执行，显示副作用风险；原 request_id 重投是恢复结果，两者不能混称。

## 5. 事件合同与安全投影

Gateway 继续产生 `sparkpush.agent_event.v1` 和现有 envelope：
request_id、route_key、session_key、event_id、sequence、terminal 等；Native 准入后增加经过校验的
run_id。C++ `AgentEnvelopeValid` 只接受 1–80 位字母数字/下划线/连字符的 run_id。

| Hermes 原生输入 | Spark 投影 |
|---|---|
| message.delta.delta | assistant_delta；实时预览 |
| tool.started / tool.completed / tool.failed | tool_start/tool_end；只显示安全工具名与完成/失败提示 |
| subagent.start / subagent.complete | tool_update；固定提示，不带 goal/输出 |
| approval.request | tool_update；只带一次性决定令牌，不输出原始 command/args |
| reasoning.available | 固定“正在思考”，不展示私有推理 |
| run.completed.output | 完整 assistant_final，进入 ai_reply |
| run.failed/cancelled/interrupted | 非成功终态，绝不使用部分回答冒充最终答案 |
| queued/running/waiting/unknown | 准入提示、审批提示、GET 真实状态及持久账本；不编造服务没有发送的事件 |
| clarify / 逐步工具 stdout | 本机 Runs 不提供该完整能力；不虚构端点或透传内部内容 |

Bridge 仍将工具事件投影为 `progress=true` 的 assistant_progress，保持 Logic、Web/Android
现有增量契约。工具事件不能携带可执行 presentation。IM 中是短文本工具进度，不是新的工具结果 UI。

本机 `api_server_runs._run_event/_handle_run_events` 的 SSE 是消耗型队列，没有事件 ID、
序号或 Last-Event-ID 回放。**不能按 JSON 内容哈希去重**：两个完全相同的 delta 也可能是合法
重复词。Native 保留这些文本；重连只 GET 结果，不重放 SSE。event_position 是本地诊断计数，
不是远端 resume cursor。Spark 自己的事件 ID/sequence 仍处理客户端重放去重。

严格 UTF-8 增量解码；跨网络片段累计限制 1 MiB/frame、100000 events、16 MiB preview；
HTTP JSON/relay 单帧限制 1 MiB，超过会失败而非截断成功。明确终态后忽略后续过期片段。
工具参数、路径、环境变量、token、reasoning.available 原文不进入投影。
断开的下游预览连接不停止已准入 Run；结果优先记账，再由正式消息恢复。
Native 的临时 HTTP 错误不写成永久 terminal error，避免污染恢复后的成功事件回放。

## 6. 审批、停止与 Kafka 控制通道

默认 `native_interactive_approvals=false`。没有支持的 UI 回调、没有合法 prompt ID、
不提供 once 选择、身份不符或超时：拒绝/取消，不默认同意。
开启交互时，原生 prompt_id 只保存在服务端；用户看到 `ap_<random>` 令牌。
它绑定 session、Spark request_id、run_id、prompt_id、generation 和截止时间；
用户必须通过原会话发送 `/approve <token>` 或 `/deny <token>`。

适配器只发送 `{"choice":"once"|"deny","request_id":<native prompt>}`；
核对响应 run/request/choice 和 resolved=1。不开放 session/always/resolve_all。
令牌被原子占用，一次性消费；迟到/重复代次拒绝。独立 Timer 默认 60 秒（1–300 秒可配），
即使 SSE readline 阻塞也拒绝。审批回执不明时尝试停止原 Run，不重试允许决定。
Gateway 重启后不恢复授权令牌；Hermes 自身超时拒绝，管理员可按原 journal 停止任务。

停止不等待 Router 的会话执行锁；状态与内部发布回执同样不占长任务锁。
Windows 控制池独立于 SSE 池。`/stop` 只表示请求停止；已结束的 Run 返回实际终态，
不把“停止请求已接受”伪装成“工具已撤销”。Native 不通过 `/restart` 重启共享 Profile。

只绕过 Router 锁还不够：原 Kafka 批次等待仍会挡住下一条控制请求。
为此新增 `KafkaConsumer::Options.interrupt_predicate`（默认空）：

- 同一个 consumer 持续 poll，普通任务由一个受控线程按 FIFO 执行。
- 仅指定 Native 联系人的 stop/approve/deny/status 可在等待期间执行。
- 所有记录按消费顺序保留，只有已完成的连续前缀才 commit；控制先成功也不能跳过长任务。
- 失败/DLQ/offset 确认仍沿用原策略，退出时 join worker，没有 detached 执行。
- 有界 lookahead：`max_batch_records`，另有 16 MiB 待处理载荷高水位。超载则背压。
  **控制记录在窗口之外时不保证即时送达**；审批 Timer 仍独立拒绝。需要负载验收后再扩大灰度。
- 此可选模式要求 `processing_workers=1`，不会静默改变已配置的多 worker 批次；不满足则启动拒绝。
  未启用时原单线程/并行批次路径不变。Pi/Legacy 的记录不会绕过 FIFO。

`accepted_ack` 与 `delivered_ack` 的定义、SQL/Redis 消息序号、持久化及离线同步保持原样。

## 7. Windows/WSL2 灰度与回滚

当前注册表仍只有原四个 Pi/Legacy 联系人；默认示例 `agents.json.example` 没变。
新条目模板在 `cannbot/pi/hermes-native-agent.json.example`，**不是自动安装或完整注册表**。

1. 先准备隔离测试账号和管理员分配的未占用联系人 ID；保留原四个联系人及 owner ACL。
2. 核对已运行服务的 capabilities 与本报告协议；不启动第二个共享 Profile Gateway。
3. 在测试用注册表中添加模板条目，调整 id、bot_user_id、owner_user_ids、已有 credential 文件引用。
   保持 `hermes_native` 显式选入；先让交互审批保持 false。不把真实密钥复制到模板。
4. 要在完整 IM 测试交互控制，Bridge 增加以下**可选配置**（示例 ID 必须先由管理员核验）：

   ```ini
   native_control_bot_users=900000000301
   processing_workers=1
   max_batch_records=64
   ```

   或环境变量 `SPARK_PUSH_NATIVE_CONTROL_BOTS`、`SPARK_PUSH_BRIDGE_WORKERS`、
   `SPARK_PUSH_BRIDGE_MAX_BATCH_RECORDS`。不要把原 Pi 联系人加入 Native allowlist。
   本次没有修改运行中的 conf、注册表或环境文件，也没有重启服务。
5. 在 Logic 已有 `agent_bot_users` 配置中登记测试联系人，走既有无密码机器人账户流程。
6. 以隔离数据测试文本、只读工具、Session 恢复、超时/unknown、停止及消息离线恢复；
   审批测试仅在隔离工作区，确认完整链路后将测试条目的 `native_interactive_approvals` 设为 true。
7. 完成真实 E2E 与故障注入后才决定是否扩大灰度。当前结果不足以自动切换默认。

中继白名单只增加上述已确认端点；保持 Windows loopback 8644，不改防火墙，不开放公网。
凭据继续从私有 env 文件读取，不记录到测试日志。Native 不修改长期 Memory/Skills 安装或原 Profile。

回滚：停止向 Native 联系人提交新请求，等待原 Run 终态；unknown 先查询/停止并人工核对。
移除测试 Native 注册条目及 Bridge 的 Native allowlist，按正常运维重新加载配置。
保留 Native journal 供历史任务恢复；不要删除数据库、迁移 Pi Session、合并 Legacy transcript。
若同一联系人要切换 runtime，必须排空在途请求；模式指纹阻止同一 request_id 被新旧分支重复执行。
原 `hermes_http` 和中继路径一直保留，无需重装 Pi。

## 8. 测试、基线和性能

测试回执见 `docs/benchmarks/2026-10-28-hermes-native.json`；本机完整输出存放
`/tmp/spark-native-baseline/`。原基线基于进入时的未提交工作树，不把已有草稿误称为纯 HEAD。

| 检查 | 修改前 | 修改后 |
|---|---|---|
| Native unit（fake Hermes，无网络） | 11 | 38 |
| Native Gateway HTTP/SSE（真实本机 HTTP，fake Hermes） | 无 | 6 |
| Router（含 Legacy、中继超时/重建） | 31 | 31 |
| Pi Gateway（单聊/上下文/stream/model/new/retry/RAG/image） | 40 | 40 |
| Pi citations | 通过 | 通过 |
| knowledge --strict | 74 来源通过 | 74 来源通过 |
| sync_pi_agent --check | 退出 1 | 退出 1，同样的知识快照/settings 差异 |
| cmake --build build-wsl -j2 | 通过 | 通过 |
| CTest | 44 项，41 通过、3 跳过 | 47 项，44 通过、3 跳过 |

`sync_pi_agent --check` 的差异不是通过结果：spark-push-knowledge 安装快照与源码不同，settings
也与模板不同；spark-push-agent、cann-advisor 为 current。遵守不改变 Pi 运行配置的要求，
未执行 --install/--replace。默认 CTest 的 3 项跳过不计为通过；本次发布复核使用仓库已有
`tests/run_mysql_review_tests.py build-wsl`、`tests/run_redis_review_tests.py build-wsl`，
在自建临时容器中分别补跑 1 项和 2 项，全部通过。另执行
`tests/run_kafka_volume_test.py build-wsl`，真实 Kafka 容器替换后消息、已提交 offset 保留与重放确认通过。
这些测试仅清理自身创建的容器/卷，未修改现有服务。

新增确定性故障覆盖：提交丢响应仅一次 POST、准入预写、24h 以后仍不重跑、同 ID 异输入冲突、
模式/用户/Profile/端点隔离、真实字段模型选择、/new 独立 API Session、无法借用 CLI Session、
UTF-8 拆片与累计超限、合法重复文本、重复 Final/迟到 Delta、网络 reset、缺失终态、
failed/cancelled/interrupted、Bridge 提交前崩溃、下游断线、未知状态恢复、发布回执与执行分离、
默认拒绝/once/重复审批/超时、控制池独立、HTTP 停止抢占、同 key Kafka 控制优先与 offset 前缀、
业务失败后的 Kafka 原始记录回放。

现有断线/历史/SQL/DLQ 合同通过原测试复测，**不等于 Native 真模型完整 IM E2E**。
真正的 AIAgent Tools/Skills/Memory 模型调用和端到端性能对照未执行；无隔离真实 Profile/工具沙箱
可直接使用。本次只读探针确认了在用 API 能力，不证明工具执行或真实审批已经验收。
没有宣称任何延迟提升；mock 测试耗时不代表生产性能。

修正了原草稿测试对“丢响应后再次 POST”和“按相同文本去重”的错误预期，新增更严格的不重执行、
合法重复内容保留断言；没有放宽 Pi/IM 既有测试断言。

2026-10-28 发布前再审查补齐了 7 项边界测试：新建 API Session 失败保留旧上下文并稳定重试，
已完成输出及 broker_confirmed 不被迟到恢复覆盖，准备阶段取消不提交 Run，准入期间取消在获得
run_id 后送达，无 run_id 的停止保持 unknown，拒绝审批回执无效时停止原 Run，以及旧计时器
不能拒绝新审批。`/new` 仅在原生创建确认后持久切换映射；Pi/Legacy 不共享这些变更。
发布复核日志在 `/tmp/spark-native-release/`；回执的 `release_validation` 记录本轮源码指纹及结果。

## 9. 变更范围与剩余风险

新增：Native adapter、Native 单元与 HTTP 测试、Native 注册条目模板、控制筛选 header、
Kafka 控制抢占测试、Bridge 配置测试、本文与测试回执。
修改：Router 的 Native 能力接口、Gateway 事件/恢复投影、Windows HTTP 白名单、
AgentEvent run_id 校验、可选 Kafka 控制循环、Bridge 控制与发布回执、命令解析和测试登记。
进入时已有的 AGENTS/知识导航/multi-agent 文档改动保留并同步事实。
没有修改 PiRpcClient、Legacy Adapter、Scheduler、WindowsRelayPool、Comet/Job、SQL schema、
消息权限/序号逻辑或真实运行配置。

仍需完成的验收：

1. 在隔离真实 Hermes 环境下验证 Tools/Skills/Memory、模型、审批和多轮恢复，再做完整 IM E2E。
2. 对有界控制窗口做洪峰/慢 Broker/重平衡测试；目前已验证正常容量内抢占和 offset 安全，
   不保证窗口之外的低延迟，且单个普通 worker 的吞吐需要对照。
3. Native 单 JSON/事件帧上限 1 MiB，超大输出会明确失败；没有原生 SSE resume cursor。
   超过服务保留期且本地结果尚未落账的未知任务需人工处理。

后续最有价值的三个方向：隔离实机验收；可持久且可观测的控制准入/队列压力治理；
更丰富的审批/工具 UI 和官方协议支持下的 clarify/compact（仍不重造 Hermes Runtime）。

## 10. 完整交付文件清单

下表是相对 HEAD 的工作树集合；区分进入时已有工作，不把所有差异归为本轮新增。
初次实现未提交；用户随后授权完善并更新 GitHub。本轮按测试通过后的源码提交，发布不启用现网 Native。

| 文件 | 归属 |
|---|---|
| `AGENTS.md` | 保留进入时已有改动 |
| `README.md` | 补充 Native 入口与验收边界 |
| `cannbot/knowledge-sources.json` | 保留进入时已有改动 |
| `cannbot/pi/hermes-native-agent.json.example` | 本次新增/修改 |
| `cannbot/scripts/agent_router.py` | 已有改动，本次继续修改 |
| `cannbot/scripts/hermes_native_adapter.py` | 已有改动，本次继续修改 |
| `cannbot/scripts/pi_gateway.py` | 已有改动，本次继续修改 |
| `cannbot/scripts/test_hermes_native.py` | 已有改动，本次继续修改 |
| `cannbot/scripts/test_hermes_native_gateway.py` | 本次新增/修改 |
| `cannbot/scripts/windows_agent_http.py` | 已有改动，本次继续修改 |
| `cannbot/skills/spark-push-knowledge/references/architecture-map.md` | 保留进入时已有改动 |
| `common/agent_event.h` | 已有改动，本次继续修改 |
| `common/kafka_consumer.cpp` | 本次新增/修改 |
| `common/kafka_consumer.h` | 本次新增/修改 |
| `docs/benchmarks/2026-10-28-hermes-native.json` | 本次新增/修改 |
| `docs/hermes-native-runtime-integration.md` | 已有改动，本次继续修改 |
| `docs/multi-agent-integration.md` | 已有改动，本次继续修改 |
| `docs/project-guide-and-resume.md` | 同步架构、算法、测试与简历表述 |
| `hermes_bridge/config.cpp` | 本次新增/修改 |
| `hermes_bridge/config.h` | 本次新增/修改 |
| `hermes_bridge/hermes_client.cpp` | 本次新增/修改 |
| `hermes_bridge/hermes_client.h` | 本次新增/修改 |
| `hermes_bridge/main.cpp` | 已有改动，本次继续修改 |
| `hermes_bridge/native_control.h` | 本次新增/修改 |
| `hermes_bridge/sse_parser.cpp` | 已有改动，本次继续修改 |
| `logic/hermes_command.cpp` | 保留进入时已有改动 |
| `logic/hermes_command.h` | 保留进入时已有改动 |
| `tests/CMakeLists.txt` | 已有改动，本次继续修改 |
| `tests/hermes_command_test.cpp` | 保留进入时已有改动 |
| `tests/hermes_native_config_test.cpp` | 本次新增/修改 |
| `tests/hermes_sse_parser_test.cpp` | 已有改动，本次继续修改 |
| `tests/kafka_native_control_integration_test.cpp` | 本次新增/修改 |

仓库外 `/home/peco/cppcode/fenbushi/项目指引与简历书写.md` 同步正文，并将链接转换为本地
`sparkpush_project/` 路径；GitHub 保存的是 `docs/project-guide-and-resume.md`。
