# 用户自助接入 Agent

实现与协议核对日期：2026-10-30。此功能显式启用，保留现有 Pi、Hermes technical 和 Hermes Native 配置。

登录 Web Demo 后，点击侧栏「连接我的 Agent」。用户可填写 Connector 地址和连接密钥，
自动识别或指定 DeepSeek Harness（DSH）、Codex、Claude Code、Hermes、OpenClaw，
检测连接、保存、打开聊天、更新或断开连接。每个用户每种 Agent 保存一个连接；暂不支持同类型多个实例。

## 架构与识别边界

```text
浏览器配置表单 → Logic /api/agent/connections（现有 Bearer 登录鉴权）
              → AgentConnectionService（固定 loopback Gateway，内部密钥）
              → Gateway Connections（用户所有权、连接配置、私有凭据文件）

普通聊天 → 原 Comet / Logic / Kafka / Bridge / Router
        → UserConnectionAdapter（按认证会话确定用户）
        → 客户侧 Spark Agent Connector
          ├─ DSH headless JSONL
          ├─ Codex exec JSONL
          ├─ Claude Code print stream-json
          ├─ Hermes Agent HTTP（复用 HermesHttpAdapter）
          └─ OpenClaw Gateway Chat Completions HTTP/SSE
```

**表单地址是 Spark Agent Connector 地址，不是模型 API，也不是裸 Agent CLI/原生服务地址。**
各 Agent 没有共同的身份发现协议；统一 Connector 提供带认证的 `sparkpush.connector.v1`
能力接口。自动识别依据该接口声明的 runtime，并核对客户端显式选择的类型；它不是二进制身份认证，
不扫描端口，也不根据模型名猜 Agent 类型。检测仅检查配置/可执行文件或只读服务信息，不调用模型，
不能证明模型账号、费用额度、工具权限或完整执行链路可用。

Connector 运行在客户自己的 Agent 主机上。浏览器不会启动服务器上的任意程序，
也不能直接启动客户电脑上的 CLI。Connector 由部署者固定二进制、工作目录和权限，
来自 IM 的文本只经 stdin 送给 CLI；不会进入 shell 命令或可执行参数。

## 启用 Spark 端

Logic 与 Pi Gateway 都需要读取以下环境配置（例如由现有启动脚本读取的 `.env.local`）：

```dotenv
SPARK_PUSH_HERMES_ENABLED=true
SPARK_PUSH_AGENT_SELF_SERVICE=true
SPARK_PUSH_AGENT_GATEWAY_PORT=8643
# 使用部署中既有的内部 Gateway 密钥；此处不提供示例真实密钥。
SPARK_PUSH_HERMES_API_KEY=
# 本机联调时可由管理员显式允许此精确 origin；公网客户连接默认要求 HTTPS。
SPARK_PUSH_AGENT_ALLOWED_ORIGINS=http://127.0.0.1:8650
```

`SPARK_PUSH_AGENT_GATEWAY_PORT` 是 Logic 访问 Gateway 的本机端口，默认 8643；
若修改，应同时调整实际 Gateway 的 `SPARK_PUSH_PI_PORT` 和 Bridge base URL。
内部 Gateway 只允许受信任的服务访问；不能把内部密钥发给浏览器。
现有 Agent 注册表（默认 `~/.pi-spark-agent/agents.json`）必须存在，
启用自助功能但缺失注册表时，Gateway 会拒绝启动。注册表继续保留现有 Pi/Hermes 条目，**不要手工加入下表的自动条目**。

启用后 Logic 自动创建五个保留机器人账号，Router 注册对应的按用户分发 Adapter：

| 类型 | 联系人 ID |
|---|---|
| DeepSeek Harness | 900000000301 |
| Codex | 900000000302 |
| Claude Code | 900000000303 |
| Hermes | 900000000304 |
| OpenClaw | 900000000305 |

这些是跨用户共用的入口 ID，但配置、凭据和真实会话按用户隔离。
未配置的入口不出现在该用户的 `/agent` 目录中；手动输入 ID 也不能使用他人的连接。
如果保留 ID 已被普通账号或配置占用，启动拒绝覆盖。当前房间共享仍只允许公共 Pi，
客户连接不自动成为房间 Agent。Web Demo 是本次自助配置入口，Android 未增加配置表单。

编译与部署：

```bash
cmake -S . -B build-wsl
cmake --build build-wsl --target logic_server -j2
# 按部署流程重启 Logic、Gateway；Web 静态资源同时更新。
```

新增 Logic 代理使用 libcurl，准备脚本中已有 `libcurl4-openssl-dev` 和 `pkg-config` 依赖。
源代码更新不会自动修改运行环境文件、重启服务或切换现网联系人。

## 客户侧启动 Connector

在客户主机准备本仓库的 `cannbot/scripts`，以及已安装、已配置身份凭据的 Agent。
首版 CLI Connector 面向 Linux/WSL，依赖 POSIX 进程组与管道；不宣称支持原生 Windows CLI 托管。
Hermes 的既有 `windows_stdio` 中继仍可通过客户侧配置使用。

从 `cannbot/connectors/*.json.example` 复制对应模板到仓库外的私有配置目录，修改绝对工作目录等字段。
将 `SPARK_AGENT_CONNECTOR_KEY` 放入该目录中权限为 0600 的环境文件，值至少为 24 个非空白 ASCII 字符。
它是浏览器表单需要填写的**连接密钥**，与模型 API key、Spark 登录 token、内部 Gateway 密钥不同。

```bash
# 环境文件与状态目录放在仓库外；用自己的绝对路径替换。
set -a
. /absolute/path/to/private/connector.env
set +a
python3 cannbot/scripts/agent_connector.py \
  --config /absolute/path/to/private/connector.json \
  --state-dir /absolute/path/to/private/connector-state \
  --listen 127.0.0.1 --port 8650
```

远程部署通过 HTTPS 反向代理暴露 Connector；Spark 服务器必须可访问该地址。
`127.0.0.1` 表示 Spark 服务器自身，不能用它指代另一台客户电脑。
默认 URL 策略拒绝非 HTTPS、用户信息、query/fragment、私网/loopback/保留地址及重定向；
只有管理员允许列表中的精确 origin 可以访问私网或使用 HTTP。连接时固定已校验的 DNS IP，
HTTPS 仍验证原域名证书，不使用环境代理。不要把允许列表交由普通用户修改。

模板字段：

| 类型 | 客户侧配置 |
|---|---|
| Codex | `runtime=codex`、`binary`、`cwd`、`permission_mode`；可选 `model` |
| Claude Code | `runtime=claude_code`、`binary`、`cwd`、`permission_mode`；可选 `model` |
| DSH | `runtime=dsh`、`binary`、`cwd`、`permission_mode`；模型由 headless profile 配置 |
| Hermes | `runtime=hermes`、`base_url`（含 `/v1`）、`expected_model`、凭据环境文件引用；可选既有 Windows 中继字段 |
| OpenClaw | `runtime=openclaw`、`base_url`（含 `/v1`）、`agent_id`、`token_env` 指定上游凭据环境变量 |

CLI 默认 `read-only`，客户部署者可以选择 `workspace-write`。Codex/DSH 明确不等待人工审批，
Claude Code 使用 `dontAsk` 和固定工具列表；不使用跳过权限/沙箱开关。
Claude 的只读工具列表不是操作系统沙箱，配置中的 hooks、插件和认证环境仍属于客户部署的信任范围。
OpenClaw 需先启用其 Chat Completions HTTP 端点；该端点默认可能未开启。
一个 Connector 承载一个 runtime，接入不同 runtime 时使用不同 Connector 实例及状态目录。
**同一状态目录不得同时运行多个 Connector 进程，启动时通过文件锁强制单写者。** 客户侧 Agent 的项目/长期记忆不是操作系统租户隔离。

## 协议、所有权与恢复

公共配置入口仅接受 POST `/api/agent/connections`：

```json
{"action":"probe","runtime":"auto","endpoint":"https://agent.example.com","token":"<connector-key>"}
```

`action` 支持 `list/probe/save/delete`；`save/delete` 使用 `revision` 作乐观并发检查。
客户端不能提交 `actor_user_id`、owner、可执行文件、工作目录或任意配置字段。
Logic 从现有登录 token 取得 actor，并通过内部认证请求传给 Gateway。
保存前重新检测；类型不一致、未知协议、未就绪或错误密钥均拒绝保存。
编辑时密钥留空仅在地址不变时保留原密钥。更新连接生成新 revision 和新上游上下文，IM 历史保留。

Gateway 将非秘密配置保存在现有 `routing.sqlite3`；密钥写入同目录下的
`connection_credentials/<随机引用>.env`（目录 0700、文件 0600），数据库只保存引用。
API 只返回 `has_key`，不返回密钥；浏览器不把连接密钥写入 localStorage。
断开移除配置和凭据文件，保留 IM 历史。备份时需同时保护状态目录及凭据文件。

Connector 协议端点全部要求连接密钥：

- GET `/v1/agent/capabilities`：协议版本、类型、能力及配置就绪状态。
- POST `/v1/agent/chat`：`session_id/request_id/message`，返回 NDJSON `delta/progress/final/error`。
- POST `/v1/agent/control`：只支持 `new/new_session/status/help`。
- POST `/v1/agent/result`：只查询原请求结果，绝不重新调用模型或工具。

Spark 根据认证用户、类型、IM 会话、连接 revision 生成上游会话标识。
CLI 续接使用返回的确切 Session ID，不使用 `--last/--continue`；
`/new` 在下一次消息时创建新 runtime 上下文，不清除长期记忆或 IM 历史。
配置变更与对应聊天串行，同 request_id 不能跨连接 revision 复用。

原 `accepted_ack/delivered_ack`、消息序号、最终回复落库和离线恢复链路保持不变。
预览继续是临时 `ai_delta`；只有原生成功终态和有效正文才能生成 `ai_reply`：
Codex 要求 `turn.completed` 和零退出码，Claude 要求成功 `result` 和零退出码，
DSH 要求 `final.reason.kind=completed` 和零退出码，OpenClaw 要求 `finish_reason=stop` 与 `[DONE]`。

Connector 先持久化执行意图，成功结果先持久化再发送；断线后 Spark 查询同一个结果，
并复用原 Router 幂等账本。不能确认执行结果时标记 unknown，不自动重跑工具，
新输入也不能直接继续一个未知的客户侧执行；需检查客户侧状态或显式 `/new`。
崩溃发生在 runtime 完成而 Connector 尚未保存结果之间时，仍可能需要人工核对，
没有宣称端到端 exactly-once 或所有 runtime 都能恢复未落盘的结果。

首版不支持通过这些客户连接发送附件、切模型、重试、停止、steering 或审批；
工具输出仅按运行时可用协议投影进度，DSH/Codex 不保证逐 token 输出。
不支持的操作明确返回提示。既有 Pi/Hermes Native 的功能不因此缩减。

## 验证与官方协议来源

自动化测试使用假 CLI 子进程、真实本地 HTTP、协议样本和隔离浏览器；
跨组件用例实际经过 C++ Logic 的 Bearer 鉴权和 libcurl、Gateway、客户 Connector，
覆盖配置操作、流式回复和原 request_id 回放。故障测试还覆盖 `/new` 本地写入失败、
同命令重放、Connector/Router 重启、重定向拒绝及 DNS IP 固定。
**没有把 fake Runtime 测试视为五个真实 Agent 的安装或模型联调成功。**

```bash
cmake --build build-wsl --target logic_server http_api_probe agent_connection_service_test -j2
python3 cannbot/scripts/test_user_agent_connections.py
ctest --test-dir build-wsl --output-on-failure -R 'agent_connection|user_agent_http|agent_router|pi_gateway|hermes_native|http_api_integration|logic_layer_contract'
SPARK_PLAYWRIGHT_MODULE=/path/to/playwright node tests/agent_connections_browser_test.cjs
```

当前未执行完整 Comet/Kafka/MySQL 在线投递、离线重连与历史落库实链路；上文可靠性说明是实现合同，
不能替代部署环境验收。旧 `tests/agent_channel_browser_test.cjs:129` 的按钮禁用断言，
在本次修改前的 Git HEAD 资源上也失败；本次新增配置页面浏览器测试独立通过。

核对来源：

- [Codex 非交互运行](https://learn.chatgpt.com/docs/non-interactive-mode)及本机 `codex exec --help`、`codex exec resume --help`。
- [Claude Code 程序化运行](https://code.claude.com/docs/en/headless)及 [CLI 参考](https://code.claude.com/docs/en/cli-reference)。
- [DSH headless](https://github.com/deepseek-ai/deepseek-harness/blob/master/packages/bundle/headless/README.md)、[JSON 事件实现](https://github.com/deepseek-ai/deepseek-harness/blob/master/packages/bundle/headless/src/json-stream.ts)。
- [OpenClaw Chat Completions](https://docs.openclaw.ai/gateway/openai-http-api)。
- Hermes 使用仓库已有 [HTTP Adapter](../cannbot/scripts/hermes_adapter.py)，Native 仍见 [独立运行时说明](hermes-native-runtime-integration.md)。
