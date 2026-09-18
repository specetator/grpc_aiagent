# 用 Pi Agent 替换 Hermes 基座

Spark Push 的 IM 可靠性边界不变：Logic 仍负责鉴权、`msg_seq`、Kafka、幂等和投递。
`hermes_bridge` 仍消费 `ai_request`、发布 `ai_delta`/`ai_reply`。变化只在 Agent 运行时：
WSL 中的 Hermes Gateway 换成 Pi coding agent，前面加一个兼容 OpenAI Chat Completions 的
本地 gateway。

```text
Spark Push 用户
    │
    ▼
Logic ──Kafka ai_request──► hermes_bridge
                                 │ OpenAI-compatible HTTP/SSE
                                 ▼
                         Pi gateway  :8643
                                 │ RPC JSONL
                                 ▼
                         Pi coding agent
                          ├─ spark-push-agent / spark-push-knowledge
                          ├─ cann-advisor 扩展（RAG 工具）
                          └─ custom provider ──► 原 CLI Relay / 第三方 LLM
```

CANN 知识库、`cannkb://` 引用和 `knowledge.html` 原文页全部保留。Pi 扩展只负责检索；
gateway 在最终回答上做和原先 Hermes `transform_llm_output` 相同的引用改写，再通过
`hermes.final` / `pi.final` 交给 Bridge。

## 目录与拉起

| 内容 | 路径 |
|---|---|
| Pi 配置 / Skills / 扩展 / 密钥 | `/home/peco/.pi-spark-agent` |
| 启动器 | `~/.local/bin/pi-spark-agent` |
| Pi CLI 本体 | `~/.local/node/bin/pi` |
| Linux Node | `~/.local/node/` |
| 项目源码（cwd） | `/home/peco/cppcode/fenbushi/11.2-spark_push` |
| CANN RAG | `/home/peco/cppcode/fenbushi/cann-agent-knowledge` |
| Gateway 日志 | `logs/pi_gateway.out` |

```bash
# 交互式 TUI（在项目根目录，可读实时源码）
cd /home/peco/cppcode/fenbushi/11.2-spark_push
pi-spark-agent

# 只拉起给 Spark 用的 HTTP API（127.0.0.1:8643）
pi-spark-agent gateway

# 随 Spark 整栈一起拉起
./scripts/sparkctl.sh up --fast
```

`PI_CODING_AGENT_DIR` 固定指向 `~/.pi-spark-agent`，不会读 `~/.pi` 或已删除的 Hermes 目录。

## 1. 一次性安装

需要 Linux Node（不要用 Windows 的 `node.exe`）和全局 `pi`：

```bash
cd /home/peco/cppcode/fenbushi/11.2-spark_push
bash cannbot/scripts/setup_pi_agent.sh
```

这会创建隔离状态目录 `/home/peco/.pi-spark-agent`，安装 Skills 与 `cann-advisor`
扩展，并写入 `~/.local/bin/pi-spark-agent`。第三方模型密钥放在该目录的 `.env`：

```dotenv
CLI_RELAY_API_KEY=<原 Hermes 使用的 CLI Relay 密钥>
```

`models.json` 默认指向 `http://127.0.0.1:8317/v1`（openai-responses）。若 CLI Relay
实际是 Chat Completions，把 `api` 改成 `openai-completions`。

## 2. 接到 Spark Push

`.env.local` 保持原来的 Bridge 变量，只是 8643 上现在是 Pi gateway：

```dotenv
SPARK_PUSH_HERMES_ENABLED=true
SPARK_PUSH_HERMES_BASE_URL=http://127.0.0.1:8643/v1
SPARK_PUSH_HERMES_API_KEY=<与 Spark 侧约定的 Bearer，可继续用原 API_SERVER_KEY>
SPARK_PUSH_HERMES_MODEL=pi-agent
SPARK_PUSH_HERMES_STREAMING=true
```

启动：

```bash
./scripts/sparkctl.sh up --fast
```

`up` 会先尝试停掉占用 8643 的旧 Hermes Gateway，再启动 `pi-spark-agent gateway`。
Web 页面按钮现为「与 Pi Agent 对话」，机器人 UserID 仍是 `900000000001`，历史会话外键不变。

交互式二次开发（可读实时源码）：

```bash
cd /home/peco/cppcode/fenbushi/11.2-spark_push
pi-spark-agent
```

## 3. 保留的 CANNBot / RAG 能力

| 能力 | 现在由谁提供 |
|---|---|
| `cann_knowledge_search/get/neighbors/status` | Pi 扩展 `cannbot/pi/extensions/cann-advisor.ts` |
| `[[CANN_REF:ref_...]]` → markdown 链接 + 资料来源 | `cannbot/scripts/pi_citations.py`（gateway） |
| `cannkb.citation.v1` 校验 | Logic `citation_validation` |
| 点击引用打开原文 | WebDemo `knowledge.html` + `/api/knowledge/document` |
| 建库/ingest/build | 仍只允许 `cann-rag` CLI，聊天命令拒绝写操作 |
| `/cann` `/kb` `/kb status` | Logic 解析 + gateway/扩展改写成强制检索 |

知识工作区路径不变：`/home/peco/cppcode/fenbushi/cann-agent-knowledge`。

项目知识或 Skill 变化后：

```bash
python3 cannbot/scripts/validate_knowledge.py --strict
python3 cannbot/scripts/sync_pi_agent.py --install --replace
```

## 4. 排障

```bash
curl -fsS http://127.0.0.1:8643/health
tail -f logs/pi_gateway.out logs/hermes_bridge.out
python3 cannbot/scripts/test_pi_citations.py
```

常见问题：

- 8643 被别的进程占用：`ss -ltnp | grep 8643`，停掉占用进程后重新 `sparkctl.sh up`。
- `Pi RPC process is not running`：确认 `pi` 在 `~/.local/node/bin`，且
  `PI_CODING_AGENT_DIR` 指向 `~/.pi-spark-agent`。
- 模型 401：检查 `~/.pi-spark-agent/.env` 的 `CLI_RELAY_API_KEY` 与 8317 上的 Relay。
- 有回答但没有知识库超链接：看最终消息是否带 `citations`；模型必须原样使用本轮工具返回的
  `[[CANN_REF:...]]`。

## 5. IM 完整适配进度：第一批运行正确性修复

目标是让自研 IM 承载 Pi 的会话、执行状态、停止、文件交互和多用户调度。
整体方案见 [Pi ↔ IM 架构审查](pi-im-architecture-review.md)。当前仍使用官方
`pi --mode rpc`，API 基线为本机安装的 `@earendil-works/pi-coding-agent 0.84.4`。

本批已落地：

- 会话切换失败、被扩展取消或实际 sessionFile 不一致时终止请求，不再向旧会话发送输入。
  Pi 重启清除绑定缓存，重新握手和绑定；旧 reader 的事件不能进入新进程。
- RPC 严格匹配 request ID 和 command；超时、EOF、无效 JSON、流式回调失败后销毁进程组，
  下次请求重新创建。等待全局执行锁超时不会取消另一个正在执行的请求。
- 只使用本轮 `message_end` 的成功 assistant 结果，等 `agent_settled` 后生成 final。
  error、aborted、length、缺失结果均返回失败；不再读取旧的 last assistant 文本兜底。
- Bridge 的 `HermesSseParser` 独立验证 SSE：error、截断、冲突 final 或缺少成功终态会失败，
  不把 partial 内容当作最终答案。兼容 `pi.final/hermes.final/agent.final`，要求 `[DONE]`；
  标准 Chat Completions 流要求 `finish_reason=stop` 后再 `[DONE]`。
- 没有审批 UI 时，扩展 confirm 返回 false，select/input/editor 返回 cancelled。
  这仅修复“自动同意”，**没有**为内置 bash/write 建立完整授权或沙箱。
- 请求必须包含非空 session_id、非负 int64 context_start_seq 和非空 user 文本。
  暂不支持的多模态内容返回错误；不把图片块转成字符串，也不重发旧用户消息。
  HTTP 请求体上限 1 MiB；RPC 单行和事件队列有上限；health 反映已握手进程的可用状态。
- 现有 Spark 单聊文件名保持兼容。特殊自定义 session ID 使用哈希避免替换/截断碰撞；
  若以前曾用特殊 ID 创建会话，需要显式迁移旧文件，不能自动猜测别名。
- 显式模型切换失败不再被吞掉；裸 model override 现在要求同时指定 provider。
  IM 本地 `/model` 确认、default 恢复及真实状态同步仍需后续会话控制阶段完成。

gateway 不再原样记录 Pi stderr 和工具参数，以免复制敏感数据。协议失败记录安全错误原因；
需要诊断扩展内部错误时，在受控环境使用独立 Pi 调试会话。

验证入口：

```bash
python3 cannbot/scripts/test_pi_gateway.py
python3 cannbot/scripts/test_pi_citations.py
cmake -S . -B build-wsl
cmake --build build-wsl -j2
ctest --test-dir build-wsl --output-on-failure
```

`pi_gateway_test` 使用假 Pi 子进程，不需要 API key，不调用模型；覆盖切换、重启、错误终态、
retry、超时、子进程清理、确认拒绝及 HTTP 入站约束。SSE 测试覆盖分片 UTF-8、CRLF、
多行 data、final 覆盖、引用、错误与 EOF。另已针对真实 Pi 0.84.4 在临时隔离配置下验证
离线启动、切换和重启重绑，没有发送 prompt。

本批验证结果：全项目构建通过；CTest 9 项通过、1 项 Redis 集成测试因未设置
`SPARK_PUSH_RUN_REDIS_TESTS=1` 按约定跳过；其中 gateway suite 含 22 个用例。
引用测试、知识校验通过。只读安装同步检查显示知识包尚未同步，运行 settings 与模板不同；
本批未覆盖运行配置或重新安装知识包。

上述第一批完成时尚未覆盖：持久化 final 的失败窗口修复、真实 DLQ、通用 AgentEvent、用户 Stop、FIFO/Worker Pool、
原生会话控制、图片与附件、workspace/多租户隔离。当前仍是共享进程串行执行；本批不能视为
完整多用户 Agent 平台。下一批先处理可靠结果，再贯通通用状态事件与控制链路。

修改源码不会自动重启已有 gateway/Bridge；部署或本地重启后才生效。

## 6. 第二批：最终回复重试与持久化死信（2026-09-06）

本批覆盖 `ai_request → ai_reply → persist_message` 三个消费阶段的失败保留，不修改
`accepted_ack` / `delivered_ack` 的含义。Kafka 确认只代表持久化事件已接收，MySQL 落库仍由 Job
异步完成；实时送达仍依赖后续 Comet ACK。

### 已实现

- `logic/hermes_reply.*` 将输入验证、确定性的消息构造与持久化重试从 gRPC 服务中提取。
  `completed_at_ms` 是稳定的消息时间，不再在每次消费时读取当前时间；Redis 分配序号后即便
  `is_new=false`，仍发布同一 `msg_id/msg_seq/client_msg_id` 的持久化事件。
- 只有持久化 topic 的 delivery report 确认成功后，Logic 才标记流式请求完成并更新 Agent 上下文。
  重试会再次尝试在线投递；`ai_delta` 仍不落历史、不推进游标。
- 验证回复的 user、bot 和单聊 session 一致，拒绝缺失字段、错误类型、非正整数/溢出的身份及
  完成时间。缺少稳定时间的旧消息不会被临时补一个时间继续处理，而会保留在 DLQ 待检查。
- `KafkaConsumer::Options.dead_letter_topic` 开启可靠失败模式，要求关闭自动提交与自动 offset
  store。回调异常按失败重试，异常正文不写日志。重试耗尽后必须收到 DLQ 的 delivery report
  才能提交原消息位点；DLQ 不可确认或位点提交失败时停止消费并离组，防止后面的提交跳过失败消息。
- Bridge 的 request、Logic 的 reply、Job 的 persistence 消费者分别启用
  `<request_topic>.dlq`、`<reply_topic>.dlq`、`<persist_topic>.dlq`。默认名为
  `ai_request.dlq`、`ai_reply.dlq`、`persist_message.dlq`。Job 的变更也保护普通 IM 消息落库；
  ephemeral delta 和其他实时推送消费者保留现有策略。
- request DLQ 在进程内答案缓存仍存在时附带 `recovery`，包含可直接补发到 `ai_reply` 的完整答案。
  修复缓存达到容量时清掉当前答案的问题。HTTP 超时及三次处理尝试也纳入 Bridge 的
  `max.poll.interval.ms` 预算；这不是 Session Worker Pool，Bridge 仍同步串行消费。

### 运行与恢复

开发启动脚本及 Compose 初始化已加入三个默认 DLQ topic。已有集群需先创建对应 topic、设置
与原始业务消息相同级别的访问权限和保留周期；自定义源 topic 时按上述后缀创建。DLQ 保存用户
消息和可能含文件内容的工具结果，应限制读取权限。DLQ 的 hex 编码、JSON 元数据和可选答案会
扩大消息体，需按最大输入和最大答案调整 broker/topic/producer 的消息大小限制；超限将安全停止
消费，不会绕过 DLQ 提交。

死信格式为 `sparkpush.dead_letter.v1`，包含消费组、topic、partition、offset、Kafka timestamp、
尝试次数和稳定 `id`（由 group/topic/partition/offset 的 JSON 数组编码）。`key_hex`、`payload_hex`
保留原始字节，包括无效 UTF-8 和 Protobuf；`key_is_null`、`payload_is_null` 区分 null 与空值。
`recovery`（若有）包含目标 topic、key_hex、payload_hex。hex 是编码，不是脱敏或加密。

恢复流程：

1. 先定位故障并检查消息是否已落库/送达；网络超时可能发生在 broker 已接收之后。
2. request DLQ 有 `recovery` 时优先补发其中的 ai_reply，保留全部身份字段和完成时间。
   没有 recovery 时必须检查 Pi 会话和工具副作用，再决定是否重试请求；本批不自动重新执行 Agent。
3. reply DLQ 修复原因后可按原 key/payload 补发；persistence DLQ 按原始 Protobuf 字节补发，
   不重新分配消息序号。恢复工具必须保留 null/empty 区别；尚未提供自动重放服务。
4. DLQ 无法写入或 commit 不确定时，Bridge 返回非零退出；Logic/Job 的对应消费者停止并离组，
   其他服务继续运行。修复后重启相应组件或由替代实例接管原消费组。监控
   `spark_push_kafka_consumer_failed_<source_topic>`，以及
   `spark_push_kafka_dead_letters_total_<source_topic>`；后者只统计已确认写入的死信。

这是至少一次处理，DLQ 与源位点没有 Kafka 事务绑定，DLQ 和最终回复都可能重复。Redis 的当前
去重 TTL 仍为 24 小时，Redis 丢失、超过 TTL 的重放、跨版本消息格式改变不在本批幂等保证内。
尤其旧版本使用消费时的时间：部署前应排空在途回复；对旧版本已分配/已落库消息重放前，应核对
并保留历史的原始消息字段，不能假定新的完成时间与旧记录一致。

尚缺持久化 TurnStore / outbox：如果进程在生成答案后、写入 ai_reply 或 DLQ 前崩溃，内存缓存
仍会丢失；重放 ai_request 可能重复执行有副作用的工具。本批不宣称端到端 exactly-once。

### 验证范围

`hermes_reply_test` 故障注入覆盖 Redis 已取号、持久化首次超时、重新构造回复后重试，以及序号、
消息内容和时间戳一致；覆盖错会话、错误身份、字段类型和整数溢出。`kafka_failure_integration_test`
使用 librdkafka 的临时本地 mock broker，运行真实 producer/consumer，验证异常重试、二进制死信、
答案恢复字段、死信失败不提交、不越过失败记录、同消费组重启重放，以及提交失败时停止消费。
测试无需已有 Kafka/MySQL 或模型凭据，但需要允许创建本地 socket。生产 Kafka 副本持久性、
MySQL 故障恢复、在线/离线客户端完整端到端链路仍需独立联调。

本批验证结果：全量 `cmake --build build-wsl -j2` 通过；CTest 11 项通过、1 项 Redis 集成测试因
未设置 `SPARK_PUSH_RUN_REDIS_TESTS=1` 跳过。知识校验通过（23 个来源），引用测试、启动脚本
语法检查通过。只读安装检查仍显示知识包待同步、运行 settings 与模板不同；未覆盖配置、
重装知识包或重启服务。这里的 Kafka 测试结果来自本地 mock broker，不是生产集群演练。

下一阶段：通用 AgentEvent 与 Channel/Agent Adapter 边界，再引入 SessionRouter、每会话 FIFO、
跨会话 Worker 并发、Stop 和原生 session 控制。附件与 workspace/多租户授权随后分阶段落地。

## 7. 第三批：模型选择卡片与 AgentEvent 初始协议（2026-09-06）

在 Pi 单聊发送 `/model` 后，Agent 返回模型选择气泡卡片：显示当前模型、可用模型、搜索、
每页 8 项、翻页、恢复默认与刷新按钮。点击选项通过现有 IM WebSocket 鉴权链路提交
`/model provider:model`，保留输入草稿。按钮不会在点击时显示切换成功，而是等待新的 Agent
确认消息。断线时保留重试入口；旧会话卡片不能向当前其他会话发命令；历史消息和实时消息使用
同一渲染器，普通用户的消息不能伪造 Agent 控制卡片。

### 控制流程和状态

`Logic → ai_request → Bridge → POST /v1/agent/control → Pi RPC`。
控制请求不会调用 LLM，Pi 0.84.4 的 `get_available_models` 提供真实可用列表，
`set_model` 后用 `get_state` 验证结果。`GET /v1/models` 也返回真实列表，模型 ID 使用
`provider:id`。本批使用本机安装包的 `docs/rpc.md` 和 `dist/modes/rpc/rpc-mode.js` 核对 API，
没有引入 Telegram 的非官方 patch 或旧 import。

控制请求包含 `session_id`、`context_start_seq` 和下列字段：

| operation | 参数 | 行为 |
|---|---|---|
| `list_models` | 可选已确认的旧 `provider/model` | 恢复目标会话状态并列出模型 |
| `set_model` | `target_provider`、`target_model`、正整数 `command_seq` | 校验可用性、切换、回读确认并保存 |
| `reset_model` | 正整数 `command_seq` | 恢复当前 gateway 启动时的默认模型并确认 |

`command_seq` 来自 Logic 分配的用户消息 `msg_seq`。选择以 session/context 为范围保存到
Pi session 旁的 `*.model.json`，通过临时文件、fsync、原子替换写入；不写 API key。重复的相同
revision/目标可重试，较旧 revision 或同 revision 的冲突目标被拒绝，避免旧消息重放回滚选择。
路径沿用原来的 session 文件映射并拒绝 symlink。默认选择也显式保存，从而压过旧 IM 历史中的
model override。每轮 prompt 前绑定 session 并恢复模型；新用户会话不会继承上一个用户的模型。
`/new` 的新 context 使用独立状态文件并恢复默认。

模型状态已验证后才写入 `hermes_model_state_confirmed=true`。Logic 不再把历史用户 `/model`
文本当作已经成功的配置；新版本中未确认的失败/本地回复也不能覆盖已确认快照。模型列表过期、
无效模型或 Pi 回读不一致会返回错误，保留原已保存选择。裸模型名改为提示选择卡片或明确指定
`provider:model`，避免同名模型跨 provider 歧义。

### Agent 和 Channel 的边界

`agent_events.py` 定义初始 `AgentAdapter` port（chat、model_control、available_models、readiness），
HTTP 层依赖该接口。当前实现仍由 `PiRpcClient` 承担；create/resume/abort/compact 的独立 port
待 Session Worker 阶段引入。本批没有把占位函数当作已经可用的生命周期 API。

通用事件 envelope：

```json
{
  "schema": "sparkpush.agent_event.v1",
  "type": "assistant_final",
  "data": {
    "text": "当前模型：provider:model-id",
    "presentation": {
      "kind": "model_picker",
      "current": {"provider": "provider", "id": "model-id", "name": "Model"},
      "models": [{"provider": "provider", "id": "model-id", "name": "Model"}],
      "truncated": false
    }
  }
}
```

Bridge 用 `agent_events=true` 协商 `event: agent.event` SSE，当前实际支持
`assistant_delta {text}` 和 `assistant_final {text, metadata}`。旧客户端不带该标志时仍收到
Chat Completions 文本 delta；Bridge parser 保留旧 final 协议兼容。请求身份、会话身份和增量
序号仍由现有 IM/Kafka 外层 envelope 承载；本批尚未统一成持久化的跨组件事件日志。

模型控制结果也是 `assistant_final`，通过 `presentation.kind=model_picker` 声明通用交互界面。
`common/agent_event.h` 校验并只投影公共模型字段；headers、API 地址、任意 action 和 HTML 不会
下发。`agent_channel.js` 是当前自研 IM 的展示适配器，只消费通用事件，并返回
`select_model/list_models/reset_model` 意图；`index.html` 将意图映射到当前 IM 命令。
没有 Telegram-specific callback/attach。普通文本 final 保留现有持久化格式，避免重复存储整段
答案；带模型卡片的 final 额外保存 `content.agent_event`，因此历史可以重建交互。

模型名通过 DOM `textContent` 展示。卡片目录限制为最多 512 项、约 24 KiB 模型数据，匹配当前
MySQL TEXT 和历史读取缓冲；超出时显示截断提示，手动命令仍可选择完整目录里的模型。
`user_message/assistant_start/thinking_delta/tool_start/tool_update/tool_end/attachment/abort/error`
已保留事件名，但尚未作为通用事件贯通 IM；工具进度仍沿用原文本提示，不能把预留类型视为已实现。

### 验证与部署边界

Gateway 回归测试覆盖目录脱敏、不产生 prompt、切换/重启恢复、两会话隔离、默认恢复、旧 revision
拒绝、重复命令、切换未确认、symlink 拒绝、忙碌时不误停另一请求及通用 SSE。
`agent_control_integration_test` 使用真实本地 HTTP、C++ Bridge client、fake Pi，覆盖查询、选择、
下一轮采用所选模型、通用 SSE、卡片转成历史内容及失败/默认恢复。浏览器测试使用真实 Chromium
加载项目页面与样式，所有 HTTP 请求在测试中拦截，不接触正在运行的 IM。

可复现浏览器测试（Playwright 可安装在临时目录，不加入项目生产依赖）：

```bash
SPARK_PLAYWRIGHT_MODULE=/tmp/spark-model-ui-test/node_modules/playwright \
PLAYWRIGHT_BROWSERS_PATH=/tmp/spark-model-ui-test/browsers \
node tests/agent_channel_browser_test.cjs
```

真实 Pi 0.84.4 已在临时配置、无扩展/Skills、离线模式下验证目录查询、切换、重启恢复与默认恢复，
未发送 prompt。全链路实际模型调用、生产 Kafka/MySQL、移动端人工验收仍需联调。

本批验证：全量构建通过，CTest 12 项通过、1 项 Redis 集成测试按约定跳过；其中 gateway suite
包含 30 个用例。Chromium 交互测试、真实 Pi 离线控制 smoke、引用测试和知识校验（26 个来源）
均通过。安装同步的只读检查仍显示知识包待同步、settings 与模板有差异；未覆盖运行配置或
重启已有服务。完整构建已将新前端资产复制到 `build-wsl/web_demo/static`。

本批仍为单 gateway/单 worker 的串行模型；模型控制在已有 Agent turn 后排队，忙碌超时提示重试。
`*.model.json` 依赖当前部署磁盘持久化，不支持多个 gateway 实例同时写同一 session 状态。
当前认证仍是 IM 用户鉴权加 Bridge 内网 Bearer，尚无 tenant/provider 级模型授权；不能据此开放
不可信多租户共用 coding tools。SessionRouter、跨会话 Worker 并发、Stop、tool/thinking 状态事件、
完整 TurnStore/outbox 和附件机制仍为后续阶段。

修改只在源码中完成；已有服务需部署新的 gateway、Bridge、Logic 与前端资产后生效。
新增 `message_seq` 字段用于控制幂等；旧在途 `/model provider:model` 消息缺少该字段时会失败并
提示重新选择，不会猜测 revision。仍需按第 6 节处理跨版本在途 final 与历史幂等兼容。

## 8. 第四批：常用命令卡片与真实会话控制（2026-09-06）

参考 [Hermes 官方命令表](https://github.com/NousResearch/hermes-agent/blob/main/website/docs/reference/slash-commands.md)
和 [Telegram 适配说明](https://github.com/NousResearch/hermes-agent/blob/main/website/docs/user-guide/messaging/telegram.md)
中的命令菜单、模型选择和新建确认模式。底层语义以本机 `@earendil-works/pi-coding-agent 0.84.4`
的 `docs/rpc.md`、`dist/modes/rpc/rpc-mode.js`、`dist/core/agent-session.js` 为准。
本批接入自研 IM 的通用卡片，不引入 Telegram callback payload 或 Hermes runtime。

| 命令 | 卡片和实际行为 |
|---|---|
| `/model` | 沿用真实模型目录、搜索、分页、选择和恢复默认 |
| `/reasoning`、`/thinking` | 查询当前模型支持的等级，显示选中项，点击发送 `/reasoning <level>` |
| `/retry` | 显示重试说明和确认/取消；`/retry now`（也接受 `--yes`、`-y`）才调用 Agent |
| `/new`、`/reset`、`/clear` | 显示新建说明和确认/取消；`/new now`（也接受 `--yes`、`-y`）创建空白上下文 |
| `/status` | 从运行时读取实际模型、思考等级、上下文消息数、工具调用数和累计 token 数 |
| `/help`、`/commands` | 常用命令菜单，保留 `/cann`、`/kb` 的帮助说明 |
| `/agent` | Router 列出/打开独立 Agent 会话（Pi 与 Hermes technical 隔离） |
| `/restart` | Windows Hermes technical 才重启 runtime；Pi 返回说明并引导 `/new` |
| 剧情命令 `/scene` 等 | 仅 Hermes · technical；Pi 上引导 `/agent` 切换 |

DeepSeek（`deepseek-flash` / `deepseek-v4-pro`）按官网 Thinking Mode 配置：上下文 1M、最大输出 384K，
`compat.thinkingFormat=deepseek`。`/reasoning` 的 Pi 等级映射为 `off→none`（关闭思考）、
`minimal/low→low`、`medium/high/xhigh→high`、`max→max`。默认思考强度为 `high`。
密钥只在 `~/.pi-spark-agent/.env` 的 `DEEPSEEK_API_KEY`。修改 `models.json` 后必须重启 Gateway。

输入框旁增加「/ 命令」按钮，在 Agent 单聊发送 `/help`。卡片点击走原有鉴权、限流、消息序号、
Kafka 和最终回复链路，不新增前端直连 gateway 的控制接口。保留输入草稿；提交后禁用按钮，
等待新消息确认；取消只关闭本卡片。历史和实时消息复用渲染，旧会话卡片不能向其他会话发送。

### 通用协议与安全投影

`AgentAdapter.control` 是通用控制入口，`model_control` 保留为 Python 兼容别名。
`POST /v1/agent/control` 增加 `help/status/new/new_session/retry/list_reasoning/set_reasoning`。
除显式确认的 retry 进入普通 streaming turn 外，以上控制不发送 prompt。新增卡片：

```json
{
  "schema": "sparkpush.agent_event.v1",
  "type": "assistant_final",
  "data": {
    "text": "当前思考等级：high",
    "presentation": {
      "kind": "command_card",
      "title": "选择思考等级",
      "actions": [
        {"id": "reasoning_set", "label": "high", "value": "high", "selected": true},
        {"id": "model", "label": "选择模型"}
      ]
    }
  }
}
```

公共 action 白名单为 `model/reasoning/new/retry/status/help/new_confirm/retry_confirm/cancel/reasoning_set`。
C++ 和浏览器都校验；额外的任意 command、URL、HTML、Pi 内部字段不进入公共投影。
`reasoning_set.value` 仅接受 `off/minimal/low/medium/high/xhigh/max`，可点选的子集由
Pi `get_available_thinking_levels` 决定。非推理模型只显示实际支持的等级。

`set_thinking_level` 后必须 `get_state` 回读确认，才保存会话偏好。每轮绑定会话、恢复模型后再恢复
思考等级。`*.model.thinking.json` 按 context 保存偏好和 revision，原子替换与 fsync；不写全局
settings。某模型不支持偏好时临时采用返回列表的首个合法等级，展示实际等级与未生效偏好，
切回支持的模型时恢复偏好。这个设置不代表向 IM 展示原始 thinking 内容。

### 新建、重试与历史恢复

`new_session` 以确认命令的正整数 `command_seq` 为新 generation，通过 Pi 原生 `switch_session`
绑定新的确定性 session 文件（不存在的文件由 Pi 创建空白上下文）。确认路径、检查新上下文消息
数为 0、恢复默认模型/等级后，才写 `*.route.json` 的 `context_start_seq` 并返回成功。
不删除 IM 消息或旧 Pi 文件；不支持给新上下文命名，未知参数返回用法。

路由文件优先于延迟的旧 IM history：正常请求解析出的旧边界不能回滚已经确认的新边界。
重复同一 new revision 不重复清空；旧 revision 的 new/model/reasoning 变更被拒绝。
成功控制和正常回答重复携带已确认的 `context_start_seq`，最终落入 bot 消息的
`hermes_context_start_seq`，Logic 只从 bot 的确认状态恢复上下文，不从新版本用户 `/new` 文本推断成功。
兼容历史中原版 `/new` 的明确成功回复；若升级前边界已滚出最近历史且没有确认元数据，本批无法
凭空恢复已丢失的路由，需发送一次 `/new now` 建立新的持久边界。

retry 明确定义为**重新发送**。确认后在 session lock 内调用 `get_messages`，取当前 Pi 上下文中
最后一条用户消息，使用当前模型和思考等级再次 prompt。即使 IM 窗口没有原问题，也不会回退到
其他 session。它不撤销旧回答或工具的文件修改，确认卡片提示工具可能再次执行。当前附件输入
尚未贯通；若原 Pi 消息含非文本附件，retry 拒绝静默丢附件，提示重新发送完整输入。
空上下文返回失败，不执行旧上下文问题。

### 验证与剩余边界

Gateway suite 35 个用例覆盖命令查询不调用模型、思考能力与持久化、两会话切换、重启、偏好降级、
新建预览无副作用、新边界持久化、重复/过期命令、旧历史不能回绑以及路由/配置 symlink 拒绝。
C++ HTTP 集成覆盖 command_card 的历史投影、reasoning 确认、流式与非流式 retry、新建后禁止
重试旧问题；命令规划和消息校验测试覆盖未确认命令、升级边界与 action 注入。
Chromium 实测命令菜单、等级按钮、确认/取消、草稿保留、提交禁用与跨会话保护。
真实 Pi 0.84.4 在临时配置和离线模式下验证状态、思考等级、重启、新建及全局 settings 不变，未调用模型。

当前仍是一个 Pi worker 和全局锁，控制操作在活动 turn 后执行；`/status` 是排队完成时的实际
状态，不能用它实时观察或停止运行中的 turn。每个 session 的路由/偏好依赖本地持久磁盘，禁止
多 gateway 同写。完整 Session Worker/多会话并发、Stop、Steer、Compact、Resume、附件和
tool/thinking 通用事件仍按架构审查后续阶段实施；本批没有把这些功能列成可用按钮。

部署需要同时更新 gateway、Bridge、Logic 和前端。`new`/`retry` 的无参数命令现在只打开卡片，
脚本调用要显式加 `now`。本批不自动重启服务或覆盖 Pi 配置。

本批最终验证：完整 CMake 构建通过；CTest 12 项通过、1 项 Redis 集成测试跳过（未配置测试服务）；
35 个 gateway 用例、Chromium 交互、真实 Pi 离线控制 smoke、引用校验及 26 来源知识校验均通过。
同步只读检查仍提示知识包待同步和 settings 模板差异；未执行配置覆盖或服务重启。

## 9. 思考深度与卡片回显修正（2026-09-06）

本地 Relay 的 `internal/registry/codex_model_capabilities.go` 声明 `gpt-5.6-luna` 的实际 wire
等级为 `low/medium/high/xhigh/max`。原 Pi 自定义模型遗漏 `reasoning`（默认 false），因此 RPC
仅返回 off。本项目 `models.json.example` 已补齐 `reasoning=true`、模型级
`thinkingLevelMap` 与 `supportsReasoningEffort=true`；不支持的 off/minimal 标记 null。
不把 ultra 映射成其他等级，也不推断其他自定义模型的能力。

已安装配置可用下面的定点迁移更新；只修改 cli-relay/gpt-5.6-luna 能力字段，备份权限 0600，
保留密钥、端点、其他模型和全局 settings。首次部署曾因工具额度不足中断；额度恢复后已执行
运行配置迁移和 Gateway/IM 服务重启。以下命令可幂等重复执行：

```bash
python3 cannbot/scripts/enable_relay_reasoning.py
SPARK_PUSH_BUILD_DIR="$PWD/build-wsl" bash scripts/sparkctl.sh restart --fast
```

卡片操作在原命令文本旁保存仅用于显示的 `content.agent_action`。前端验证 action 对应命令
与 text 完全一致后，显示“选择模型：…”或“选择思考深度：high”，实时回显和历史使用相同逻辑。
服务端仍以原 text 解析和鉴权；metadata 不参与执行，不接受任意显示字符串或可执行动作。
手工输入的斜杠命令保持原样。模型/等级变更成功返回简短确认和后续操作按钮，查询才显示选择列表。

本地已通过 36 个 gateway 用例、Pi 原生 Responses 请求序列化检查（五档 effort 原样传输，
在 onPayload 中停止，无网络）、配置迁移临时文件测试、显示与注入校验、知识及引用校验。
额度恢复后完成部署验收（2026-09-06）：

- 已安装 `~/.pi-spark-agent/models.json` 完成定点迁移，保留私有备份；既有全局设置未覆盖。
- 完整构建、隔离 Chromium 卡片回归，以及四项相关 CTest 均通过（gateway 含 36 个用例）。
- 真实 Chromium 页面通过独立“思考深度部署自检”账号注册/登录，沿真实 WebSocket、Kafka、
  Bridge、Pi RPC 路径依次选择 high/xhigh/max/medium/low，五档均得到实际运行时确认。
- 验证选择操作正常回显、不重复展示选择列表、保留草稿；真实历史 API 返回保存的 action 元数据，
  页面重新加载后显示文字不退回原始命令，并正确恢复当前选中项。此次验收未调用模型。
- 全部 15 项服务健康检查通过。线上效果请刷新页面后用新 `/reasoning` 卡片检查；旧历史卡片
  是当时目录的快照，不会凭空新增选项。
- 知识/引用校验通过；知识包与 settings 模板只读同步检查仍提示差异，没有覆盖用户设置。

## 10. 长时间检索后看似卡住的诊断与修复（2026-09-06）

截图对应的请求在 18:17:28 开始，12 次 CANN 工具调用均正常完成，最后一次工具结束于
18:18:17；Pi 在 18:21:07 正常返回最终答案，总耗时约 3 分 39 秒。只读数据库检查确认最终
回复已经持久化。该会话采用 max 思考深度；工具返回文本累计 214710 字符，末次模型回答
记录的 reasoning token 为 6214。证据表明停顿发生在工具结束后等待模型的阶段，而非 Kafka
宕机或消息未落库。不能仅由 token 数精确推导模型耗时。

修复内容：

- 新增通用 `assistant_progress {text}` 事件。Gateway 显示开始、工具开始/完成、自动重试
  以及每 15 秒的当前阶段；不转发原始 thinking。C++ SSE parser 的独立 progress callback
  不把状态加入回答文本，Bridge 经现有 `ai_delta` 的 `progress=true` 转发至 IM。
- 进度沿用增量序号去重、msg_seq=0、实时推送、不落历史、不推进游标；最终 reply 保持原来的
  可靠持久化链路。前端将状态与正文分开，完成后移除进度行。无新事件超过 30 秒时明确显示
  “未收到新进度”，本地等待计时不冒充后端存活证明。最终消息到达后丢弃迟到进度。
- `cann_knowledge_get` 请求指定片段时只保留对应 chunk 内容，避免同时返回整篇文档。
  search/get/neighbors 的正文预算统一为 6000 字符，搜索每项最多 1600 字符，不重复携带 snippet；
  超出明确标记 truncated，保留 citation 和证据元数据，可继续按片段获取缺失内容。
  工具 details 不再复制整份正文，JSON 使用紧凑编码。
- 普通问题指导先做一次聚焦检索、按需补充少量指定片段后作答；用户要求全面研究时仍允许
  扩展。保留用户 max 偏好，不通过偷偷降低等级来缩短响应时间。
- Gateway 新增不含问题/答案正文的 turn 开始与完成日志，包含思考等级、耗时、工具完成数。

验证：37 个 Gateway 用例、C++ 进度与回答分离测试、完整 CTest（12 通过，1 Redis 跳过）、
Chromium 进度显示及终态清理、`node tests/cann_context_test.cjs` 的片段/预算/引用保留均通过。
已备份并更新安装的 CANN 扩展，重启 Gateway、Bridge 和 IM 服务。原会话历史保持完整，
用户可刷新页面查看已完成的回答。本修复不提供即时 Stop，也不保证 max 推理在固定秒数内结束。

上线真实验证：独立测试账号使用 max、限定一次搜索和三条简短结论的 CANN DMA 问题，约
17 秒返回 784 字符最终结果、2 条引用，3 个进度事件包含工具完成状态；最终正文没有混入
进度信息，最终回复已在历史 API 中查到，页面完成后清除进度行。该受限问题与原长问题工作量
不同，不能据此宣称固定提速倍数。部署后 15 项服务健康检查全部通过。

## 11. 保留完整条件的分段读取（2026-09-06）

本节替代第 10 节的固定正文截断和少量补读建议。保留去重、紧凑 JSON 和搜索预览预算，
但正文长度限制只控制每次返回的页大小，不丢弃剩余内容，也不限定必要的补读次数。

- `cann_knowledge_get` 新增 `offset/length/expected_hash`，默认每页 6000、最大 12000 个
  Unicode 码点；优先在换行处分页，返回 `next_read` 供原样续读。每页携带
  `coverage {unit,scope,source_hash,start,end,total,complete,preview_only}`。`complete` 只表示
  本次响应是否含整份正文；多页是否读全按累计区间判断，不能只看最后一页是否到达末尾。
- 非首页必须携带正文 SHA-256；来源变化拒绝续读，要求从零开始，防止不同内容版本拼接。
  chunk 读取保留标题、位置、来源修订及引用，额外返回 `parent_read/neighbors_read`，用于
  核对前提、例外、表头及型号/版本条件。完整 chunk 不代表完整父文档上下文。
- 搜索返回的完整正文预览携带精确范围和续读位置；neighbors 只有摘要，因此明确标记
  `preview_only=true`，只能通过指定 chunk 的 get 获取完整内容，不能把短摘要误判为全文。
- Agent 指令要求在确定参数、对齐、限制和适用型号/版本之前读完整支持片段，并按需核对
  父文档和关联证据。条件不足时说明具体不确定性，不因延迟目标停止必要检索。
- Gateway 在当前 turn 按引用与内容哈希合并已读区间。引用缺页或仅有摘要时，最终正文
  附加“证据未读全”提示，`verified=false`，并记录 `unread_evidence`；重复页不填补缺口，
  不同内容版本不能累加，未使用的检索预览不影响已经读全的引用。旧版无 coverage 工具响应
  保留原契约兼容行为。该门禁只能证明已返回文本的覆盖范围，不能证明模型理解正确或语义
  条件已经穷尽；知识库检索的 sufficient 也不等于答案正确性保证。

回归命令：

```bash
node tests/cann_context_test.cjs
SPARK_CANN_LIVE_TEST=1 node tests/cann_context_test.cjs
python3 cannbot/scripts/test_pi_citations.py
python3 cannbot/scripts/test_pi_gateway.py
```

测试覆盖超过旧上限的尾部限制、中文/emoji/换行无损重建、摘要与全文区别、缺页/重复页、
跨内容版本拒绝与读取状态重置。真实知识库测试经注册的 search/get 工具分页读取，并与
原始 chunk 逐字比较，不调用模型。

本批验收：上述分页与引用回归、37 个 Gateway 用例、26 个来源的知识校验通过；真实知识库
经注册工具用 4 页无损重建 931 个码点的原始片段。已备份并定点部署 CANN 扩展、重启现有
Pi Gateway 和 IM 服务，15 项健康检查全部通过。用户模型和思考等级设置未调整；未以这次
文本覆盖测试宣称模型回答准确率或固定提速幅度。

## 12. 多 Agent 注册与 Windows Hermes（2026-09-06）

新增 `/agent` 联系人卡片、独立对话框和 Hermes HTTP Adapter，现有 Pi RPC Adapter 保留。
Pi 与 Hermes 分别使用 900000000001、900000000101，历史及上下文按独立联系人隔离。
Windows technical 通过 loopback API 和本地 stdio 管道接入，所有者权限在路由层校验。
详细使用方法、配置字段、隔离范围及已验证边界见 [多 Agent 接入文档](multi-agent-integration.md)。
