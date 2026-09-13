# Agent Bot 集成（Pi 基座）

当前 WSL 中的 Agent 基座是 Pi coding agent，不再是 Hermes。`hermes_bridge` 进程名和
Kafka topic 保持不变，以免改动 Spark 的可靠性边界。Pi 只负责生成回答，用户鉴权、
消息序号、持久化、实时投递和离线补推仍由 Spark Push 负责。安装与切换步骤见
[`pi-agent-integration.md`](pi-agent-integration.md)。

```text
浏览器/WebSocket
      │
      ▼
Comet ── MessageStream ──► Logic
                             ├─ persist_message（用户输入）
                             ├─ ai_request ──► hermes_bridge
                             │                    │ HTTP POST
                             │                    ▼
                             │               WSL Hermes
                             │                    │ /v1/chat/completions
                              │                    ├─ SSE delta ──► ai_delta
                              │                    │                    │
                              │                    │                    └─► Logic 临时事件
                              │                    │                         └─► push_single → Job → Comet
                              │                    ▼
                              └─ ai_reply ◄── hermes_bridge（最终答案）
                                  │
                                  ├─ persist_message（Hermes 最终回答）
                                  └─ push_single → Job → Comet → WebSocket
```

## 1. WSL Hermes API

本机使用 `/home/peco/cppcode/fenbushi/hermes-spark-agent` 中的独立 Hermes 源码，状态目录是
`/home/peco/.hermes-spark-agent`。它不读取 Windows `F:\hermes`。先完成第三方 provider 配置，
再启动 API：

```bash
cd /home/peco/cppcode/fenbushi/11.2-spark_push
hermes-spark-agent gateway run
```

默认监听 `127.0.0.1:8643`，`API_SERVER_KEY` 位于独立 Hermes 的 `.env`。完整布局、模型配置和
二次开发流程见 [`wsl-hermes-development.md`](wsl-hermes-development.md)。Hermes 官方接口说明见：
<https://hermes-agent.nousresearch.com/docs/user-guide/features/api-server>。

该接口会进入 Hermes 的完整 Agent loop，并按 API Server 平台启用的 toolsets 使用 Skills、
工具和子 Agent；它不是第三方模型的透明代理。项目 Agent 与自包含知识包的安装方式见
[`cannbot-integration.md`](cannbot-integration.md)。第三方模型 endpoint 和密钥配置在 Hermes
provider 中，Bridge 始终连接 Hermes API。

先在 WSL 验证：

```bash
curl --fail-with-body http://127.0.0.1:8643/health
set -a; source /home/peco/.hermes-spark-agent/.env; set +a
curl --fail-with-body http://127.0.0.1:8643/v1/models \
  -H "Authorization: Bearer ${API_SERVER_KEY}"
unset API_SERVER_KEY
```

Bridge 与 Hermes 都在 WSL 时应保持 loopback 绑定，不需要开放 Windows 防火墙。只有 Bridge
移入独立容器后，才需要把地址改成容器能访问的宿主机地址，并限制暴露范围。第一阶段 Bridge
本身只实现本地 HTTP。

## 2. Spark Push 配置

复制配置模板并填写真实值：

```bash
cd /home/peco/cppcode/fenbushi/11.2-spark_push
cp .env.example .env.local
```

`.env.local` 至少包含：

```dotenv
SPARK_PUSH_MYSQL_PASSWORD=本机 MySQL 密码
SPARK_PUSH_HERMES_ENABLED=true
SPARK_PUSH_HERMES_BASE_URL=http://127.0.0.1:8643/v1
SPARK_PUSH_HERMES_API_KEY=与 API_SERVER_KEY 相同
SPARK_PUSH_HERMES_MODEL=hermes-agent
SPARK_PUSH_HERMES_STREAMING=true
```

`start_demo.sh` 会创建 `ai_request`、`ai_delta`、`ai_reply` topic，并在 Logic 之后启动
`hermes_bridge`。Bridge 配置位于 `conf/hermes_bridge.conf`，其中 API 地址、Key、模型
和流式开关允许被环境变量覆盖。

```bash
bash scripts/start_demo.sh --foreground
```

启动后访问 <http://127.0.0.1:9010/index.html>。登录普通用户后，点击“与 Hermes
Bot 对话”，或手动输入固定机器人 UserID：

```text
900000000001
```

`hermes_bot` 是 Logic 启动时幂等写入 MySQL 的系统用户：不能注册、不能普通登录，
禁用/软删除后 Hermes 请求会被拒绝。停用 Hermes 集成时，Bot 用户记录仍保留，便于
历史消息和会话外键保持稳定。

## 3. 第一阶段的消息语义

- 默认调用 `POST /v1/chat/completions` 的 SSE 流式模式（`stream=true`）。Bridge 读取
  `data:` 事件，把文本增量按小批次发布到 `ai_delta`；首个增量到达后即可显示在浏览器，
  不必等待完整回答；
- Logic 先把用户输入写入 `persist_message`，再写入 `ai_request`；
- Bridge 消费 `ai_request`，将增量写入 `ai_delta`，将完整结果写入 `ai_reply`；
- Logic 消费 `ai_delta` 时只构造临时 `hermes_delta` 推送事件，不分配 `msg_seq`、不写入
  `message` 表，因此增量不会污染历史和游标；
- Logic 消费最终 `ai_reply`，将完整回答构造成普通单聊消息，写入 `persist_message`，
  再走 `push_single → Job → Comet`。最终消息是历史、离线补推和重连后的唯一事实来源；
- 用户输入的 `accepted_ack` 只代表输入已进入持久化 Kafka 队列，不代表 Hermes 已经
  生成回答；Hermes 回答到达后，按普通单聊消息进入实时/离线投递链路；
- Bridge 进程内以 `request_id` 缓存回答，Logic 的 Redis 热路径以
  `hermes:<request_id>` 对回答幂等，Kafka 重试不会在正常进程生命周期内重复调用和
  重复显示；
- `ai_delta` 是尽力而为的实时体验通道：用户在生成期间断线时不补发半截文本，重连后由
  最终 `ai_reply` 的落库消息和游标补偿恢复完整答案；前端收到最终消息后会替换临时气泡，
  不产生重复消息；
- 浏览器历史加载、WebSocket 实时消息和发送时的乐观气泡统一按 `msg_id` /
  `client_msg_id` 去重；切换会话时会丢弃旧历史请求的返回，避免同一条消息在页面出现两次；
- `/api/session/history` 要求普通用户 Bearer Token，并校验该用户属于目标会话，不能只凭
  `session_id` 读取其他会话；
- Hermes HTTP 失败不会丢掉用户输入，Bridge 会发出失败回复，Logic 把“暂时不可用”
  作为机器人消息写入历史，便于用户看到失败原因。

如果要临时回退到原来的完整回答模式，将下面配置设为 `false`；`ai_reply`、历史和离线
补推语义不变：

```dotenv
SPARK_PUSH_HERMES_STREAMING=false
```

## 4. 多轮和历史消息

每次用户发给 Bot 的新消息，Logic 从 `message` 表读取该会话最近 50 条历史，按
`msg_seq` 从旧到新恢复顺序，以发送者区分 `user` / `assistant`；为控制长期会话的
首 token 延迟，最终 prompt 还限制为约 12 KiB 的文本字节数，再把当前输入放在末尾
发送给 Hermes：

```json
{
  "messages": [
    {"role":"user", "content":"第一轮"},
    {"role":"assistant", "content":"第一轮回答"},
    {"role":"user", "content":"第二轮"}
  ],
  "stream": true
}
```

因此第一阶段不依赖 Hermes 的服务端会话 ID，而是由 Spark Push 自己维护单聊历史。
Logic 还保留每个会话最多 100 条的短期热上下文，用来覆盖上一轮回复已经推送但
Job 尚未完成 MySQL 落盘的短窗口；进程重启后仍以 MySQL 历史为准。只有文本消息会
进入 Prompt；Bridge 当前只转发文本增量，Hermes 的工具进度/非文本 SSE 事件不会被当作
聊天气泡发送。若关闭流式，仍兼容 `stream=false` 的完整 JSON 响应。

手工验证建议：

1. 登录普通用户，打开 Hermes Bot 单聊；
2. 发送“我叫 Alice”；
3. 再发送“我叫什么？”并检查回答是否引用第一轮；
4. 刷新页面或重新登录，重新打开 Hermes 会话，确认历史和第二轮上下文仍在；
5. 查看 `logs/hermes_bridge.out`、Logic metrics 和 `/api/session/history`，确认
   用户消息、机器人消息的 `msg_seq` 连续且均已落库。

命令行检查历史（把 `<uid>` 替换为登录用户 ID）：

```bash
USER_TOKEN='登录接口返回的 token'
curl -sS -X POST http://127.0.0.1:9101/api/session/history \
  -H 'Content-Type: application/json' \
  -H "Authorization: Bearer $USER_TOKEN" \
  -d '{"session_id":"s_<uid>_900000000001","anchor_seq":0,"limit":50}'
```

历史接口会校验 Bearer Token，并确认登录用户属于目标会话；不能只凭
`session_id` 查询其他用户的消息。

## 5. 常用命令

Logic 在生成 Agent 请求前解析以下命令；控制卡片走 `/v1/agent/control`，知识检索和
确认后的 retry 走普通 turn。控制命令文本及其确认回复不进入模型 Prompt：

| 命令 | 行为 |
|---|---|
| `/new` | 新建确认卡片；`/new now` 才切换空白 Pi 上下文，别名 `/reset`、`/clear` |
| `/status` | 查询 Pi 实际模型、思考等级、消息/工具/token 统计 |
| `/retry` | 重试确认卡片；`/retry now` 重新发送当前 Pi 上下文最后一条用户消息 |
| `/reasoning`、`/thinking` | 可用思考等级选择卡片；`/reasoning high` 显式设置 |
| `/cann <问题>` | 强制检索 CANN 知识库 |
| `/kb <问题>` | 强制检索知识库 |
| `/kb status` | 只读查看来源、文档数和当前 index generation |
| `/model` | 可用模型选择卡片（搜索、分页、恢复默认） |
| `/model provider:model` | 运行时确认后保存当前会话模型 |
| `/model default` | 恢复 Pi 启动默认模型 |
| `/help`、`/commands` | 常用命令卡片与帮助；也可点击输入框旁的「/ 命令」 |

`/new` 保留 Spark Push 聊天记录，在 Pi 确认空白上下文后持久化路由边界。确认按钮发送
`/new now`，也接受 `--yes`、`-y`。`/retry` 不撤销旧答案或工具副作用。
完整控制协议、会话恢复语义与验证见 [Pi 集成文档第 8 节](pi-agent-integration.md#8-第四批常用命令卡片与真实会话控制2026-09-06)。
模型选择和上下文边界会通过持久消息恢复，不依赖 Bridge 的进程内缓存。

当前 OpenAI 兼容接口运行 Pi Agent loop、Skills 和工具，命令由平台显式解析并映射到 Pi RPC。
未在上表中的 slash command 返回明确提示；即时 Stop、审批、后台任务和会话分支等控制
仍按 Pi Session Worker 方案逐步实现。

`/kb build`、`/kb ingest`、`/kb sync`、来源增删和删除命令会在 Logic 本地拒绝，聊天命令
不会改变知识库。建库只能在知识工作区通过 `cann-rag` CLI 明确执行。

## 6. CANN RAG 与引用

独立知识工作区为 `/home/peco/cppcode/fenbushi/cann-agent-knowledge`。Hermes 用户插件
`cann-advisor` 提供 search/get/neighbors/status 四个只读工具；Spark 不直接读取检索索引。
流式阶段仍只传纯文本增量，结束前 Hermes 额外发出 `hermes.final`，Bridge 以其中的最终文本
覆盖临时累积内容，并把 `response_metadata.citations` 放入 `ai_reply`。

Logic 只接受 `cannkb.citation.v1`、固定长度 ID、允许的定位类型和严格绑定的
`cannkb://document/<doc_id>?chunk=<chunk_id>`。最终消息以 `format: markdown` 和
`citations: [...]` 落库。WebDemo 只对 Hermes 最终消息启用本地固定版本的安全 Markdown
子集；链接会变成 `knowledge.html`，再使用登录 Token 调用
`POST /api/knowledge/document`。该接口只接受 catalog ID，不接受路径，并限制原文文件大小。

建库和检查示例：

```bash
cd /home/peco/cppcode/fenbushi/cann-agent-knowledge
bin/cann-rag source list
bin/cann-rag ingest --full
bin/cann-rag build
bin/cann-rag verify
bin/cann-rag eval
```

## 7. 观察与排障

```bash
curl -sS http://127.0.0.1:9101/metrics | rg hermes
tail -f logs/hermes_bridge.out logs/logic.out
```

重点检查：

- WSL `/health` 成功但 Bridge 连接失败：检查 Gateway 是否仍在运行、Bridge 是否真的运行在
  WSL 宿主机，以及 `SPARK_PUSH_HERMES_BASE_URL` 是否为 `http://127.0.0.1:8643/v1`；
- `Hermes API key is required`：检查 `.env.local` 是否被启动脚本加载；
- 用户消息有 accepted 但没有 Bot 回答：依次检查 `ai_request`、Bridge 日志、Hermes
  HTTP 响应和 `ai_reply`；如果有最终回答但没有逐字显示，再检查 `ai_delta` topic、
  Logic 的 Hermes delta consumer 和 `spark_push_hermes_stream_delta_*` 指标；
- Bot 回答历史存在但页面没收到：检查用户是否仍有 Comet 路由、Job→Comet
  PushStream 和 `spark_push_delivery_lost_total`；重新连接后应由 delivered cursor
  和离线补推补回来；
- `ai_request` 重复：不要直接清 Kafka offset；先确认同一 `request_id` 是否为重试，
  Bridge/Logic 会按请求 ID 做幂等。

第一阶段的可观测指标包括：

```text
spark_push_hermes_requests_total
spark_push_hermes_replies_total
spark_push_hermes_errors_total
spark_push_hermes_duplicate_replies_total
spark_push_hermes_realtime_enqueued_total
spark_push_hermes_stream_deltas_total
spark_push_hermes_stream_delta_enqueued_total
spark_push_hermes_stream_delta_publish_errors_total
spark_push_hermes_stream_delta_errors_total
spark_push_hermes_stream_delta_duplicate_total
spark_push_hermes_stream_delta_offline_total
spark_push_hermes_ttft_ms_{count,sum,max}
spark_push_hermes_total_latency_ms_{count,sum,max}
spark_push_hermes_prompt_chars_{count,sum,max}
spark_push_hermes_prompt_messages_{count,sum,max}
```

排查体感速度时，把一次请求的三个时间点分开记录：`accepted_ack`、首个
`hermes_delta`、最终 `single_chat`。第一段衡量 Spark Push 受理，第二段是首字节/首批
增量（TTFT），第三段是完整答案。若 accepted 很快而首个 delta 仍很慢，瓶颈在 Hermes
模型推理、工具调用或上下文长度，不在 Kafka 到浏览器的投递链路。

若回答中出现“引用无效”或“未包含可验证引用”，检查模型是否原样使用了本轮工具返回的
`[[CANN_REF:...]]`，并检查 Bridge 是否收到 `hermes.final`。若原文页 401，先回聊天页登录；
若 404，检查消息中的 generation 是否仍能在当前 catalog 找到对应稳定 ID。

## 8. 当前边界与下一阶段

当前实现明确保留以下边界：Bridge 是单进程 Kafka consumer，HTTP 客户端只支持明文
HTTP；Bridge 内部仍同步占用一个消费线程处理一次 Hermes 请求，但 HTTP 响应已经按 SSE
增量转发；历史上下文最多取 50 条且有约 12 KiB 字符预算，尚未加入 AI 并发池、取消请求和跨 Bridge 分布式
请求状态。增量通道不持久化，最终 `ai_reply` 才是可靠恢复边界。

下一阶段可在不改变单聊最终消息协议的前提下增加：AI 请求独立限流与超时队列、取消请求、
持久化请求状态、独立 DLQ/重放工具、流式背压、Hermes Sessions/Runs 控制面和工具进度事件转发。
