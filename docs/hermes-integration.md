# Hermes Bot 集成（第一阶段）

本阶段把 Windows 上运行的 Hermes Agent 接入 Spark Push 的单聊链路。Hermes
只负责生成回答，用户鉴权、消息序号、持久化、实时投递和离线补推仍由 Spark
Push 负责。

```text
浏览器/WebSocket
      │
      ▼
Comet ── MessageStream ──► Logic
                             ├─ persist_message（用户输入）
                             ├─ ai_request ──► hermes_bridge
                             │                    │ HTTP POST
                             │                    ▼
                             │             Windows Hermes
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

## 1. Windows Hermes API

`F:\hermes` 是 Windows 主机上的 Hermes 源码/运行目录，虚拟机中的 Bridge
不会直接读取这个 Windows 路径；Bridge 只通过 HTTP 访问 Hermes API。

在 Windows Hermes 的环境配置中开启 API Server（具体配置位置以 Hermes 版本为准，
通常是 `%USERPROFILE%\.hermes\.env`）：

```dotenv
API_SERVER_ENABLED=true
API_SERVER_HOST=0.0.0.0
API_SERVER_PORT=8642
API_SERVER_KEY=替换为随机长密钥
```

然后在 `F:\hermes` 按该版本的官方方式启动 Gateway/API Server。Hermes 官方接口
说明见：<https://hermes-agent.nousresearch.com/docs/user-guide/features/api-server>。

先在 Windows 本机验证：

```powershell
curl.exe http://127.0.0.1:8642/v1/models `
  -H "Authorization: Bearer 替换为随机长密钥"
```

再从本项目所在虚拟机验证。`host.docker.internal` 只在当前虚拟化/容器网络中
可用时才成立；如果不通，应改成虚拟机能访问的 Windows 主机 IP：

```bash
curl --noproxy '*' --fail-with-body \
  http://host.docker.internal:8642/v1/models \
  -H 'Authorization: Bearer 替换为随机长密钥'
```

如果 Windows API 只绑定 `127.0.0.1`，虚拟机无法访问。需要让 API 监听可达地址，
并在 Windows 防火墙中只允许来自虚拟机网段的 TCP 8642；不要把 API 端口直接暴露
到公网。若网络环境不允许明文 HTTP，可在 Windows/虚拟机侧用受控反向代理终止
HTTPS，但第一阶段 Bridge 本身只实现本地 HTTP。

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
SPARK_PUSH_HERMES_BASE_URL=http://host.docker.internal:8642/v1
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

## 5. 观察与排障

```bash
curl -sS http://127.0.0.1:9101/metrics | rg hermes
tail -f logs/hermes_bridge.out logs/logic.out
```

重点检查：

- Windows 本机 `/v1/models` 成功，但虚拟机失败：检查 API 是否监听 `0.0.0.0`、
  Windows 防火墙、虚拟机到 Windows 的路由和 `SPARK_PUSH_HERMES_BASE_URL`；
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

## 6. 当前边界与下一阶段

当前实现明确保留以下边界：Bridge 是单进程 Kafka consumer，HTTP 客户端只支持明文
HTTP；Bridge 内部仍同步占用一个消费线程处理一次 Hermes 请求，但 HTTP 响应已经按 SSE
增量转发；历史上下文最多取 50 条且有约 12 KiB 字符预算，尚未加入 AI 并发池、取消请求和跨 Bridge 分布式
请求状态。增量通道不持久化，最终 `ai_reply` 才是可靠恢复边界。

下一阶段可在不改变单聊最终消息协议的前提下增加：AI 请求独立限流与超时队列、取消请求、持久化请求状态、独立 DLQ/重放工具、流式背压，以及
Hermes 会话/工具调用能力。
