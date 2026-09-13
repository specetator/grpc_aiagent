# 独立 WSL Agent 开发环境

> 2026-09 起 Agent 基座改为 Pi。本文件保留旧 Hermes 路径作为对照；日常请按
> [`pi-agent-integration.md`](pi-agent-integration.md) 使用 `/home/peco/.pi-spark-agent`
> 和 `pi-spark-agent`。

本机曾经建立一套与 Windows Hermes 完全分离的 WSL 开发实例。它不依赖 OpenCode、
CANNBot Toolkit、GitCode 登录或 Windows 的 `F:\\hermes`。现在同样的第三方模型仍走
CLI Relay，只是前面的 Agent 运行时换成 Pi。

## 1. 本机布局

| 内容 | 路径 |
|---|---|
| Hermes 源码 | `/home/peco/cppcode/fenbushi/hermes-spark-agent` |
| 个人开发分支 | `agent/spark-push` |
| Python 3.11 虚拟环境 | `/home/peco/.hermes-spark-agent/venvs/hermes-dev` |
| 独立状态目录 | `/home/peco/.hermes-spark-agent` |
| 配置 / 密钥 | `config.yaml` / `.env`（均位于独立状态目录） |
| Agent 身份 | `/home/peco/.hermes-spark-agent/SOUL.md` |
| 专用命令 | `/home/peco/.local/bin/hermes-spark-agent` |
| Agent API | `http://127.0.0.1:8643/v1` |
| CANN RAG 源码/数据 | `/home/peco/cppcode/fenbushi/cann-agent-knowledge` |
| CANN Advisor 插件 | `/home/peco/.hermes-spark-agent/plugins/cann-advisor` |
| CANN Advisor Skill | `/home/peco/.hermes-spark-agent/skills/cann-advisor` |

`hermes-spark-agent` 启动器会固定导出
`HERMES_HOME=/home/peco/.hermes-spark-agent`，因此配置、记忆、会话、Skills、插件、日志和
Gateway 状态不会落入默认的 `~/.hermes`，也不会读取 Windows 实例。源码采用 editable install；
修改源码后，下次启动即运行新代码。

## 2. 接入第三方 LLM

先编辑非敏感配置：

```bash
${EDITOR:-vi} /home/peco/.hermes-spark-agent/config.yaml
```

至少替换：

```yaml
model:
  provider: custom
  default: "你的模型 ID"
  base_url: "https://第三方服务地址/v1"
```

再把真实密钥写入仅当前用户可读的 `.env`：

```dotenv
OPENAI_API_KEY=你的第三方模型密钥
```

不要把第三方密钥写入 Spark Push 源码、Skill、示例配置或日志。也可以运行
`hermes-spark-agent model` 使用 Hermes 的交互式 provider 选择器；这不会要求 GitCode 登录。

如果第三方入口是本机 Docker Compose 中的 CLI Relay，Hermes 运行在 WSL 宿主机进程中，
应使用宿主机发布端口，而不是 Docker 内部服务名：

```yaml
model:
  provider: custom
  default: gpt-5.6-luna
  base_url: http://127.0.0.1:8317/v1
  api_key: ${HERMES_CUSTOM_127_0_0_1_8317_API_KEY}
  api_mode: codex_responses
```

`http://cli-proxy-api:8317/v1` 只适用于与 CLI Relay 位于同一 Docker 网络的容器；WSL
宿主机进程通常无法解析 `cli-proxy-api`，表现为 Hermes 连续重试后报告
`APIConnectionError`。修改 provider 配置后要重启现有 Hermes CLI/TUI 或 Gateway，使进程重新加载
endpoint 和密钥。

## 3. 启动与验证

从项目根目录启动交互式 Agent，可让 Hermes 读取实时源码和根目录 `AGENTS.md`：

```bash
cd /home/peco/cppcode/fenbushi/11.2-spark_push
hermes-spark-agent --tui
```

为 Spark Push Bridge 启动 OpenAI-compatible Agent API：

```bash
hermes-spark-agent gateway run
```

API 默认只监听 WSL loopback 的 `8643`，不会暴露到局域网。另开终端验证：

```bash
curl --fail-with-body http://127.0.0.1:8643/health

set -a
source /home/peco/.hermes-spark-agent/.env
set +a
curl --fail-with-body http://127.0.0.1:8643/v1/models \
  -H "Authorization: Bearer ${API_SERVER_KEY}"
unset API_SERVER_KEY
```

`/health` 只验证 Agent API 存活；真正发起对话前还必须完成上一节的模型 endpoint 和密钥配置。

## 4. 连接 Spark Push

`hermes_bridge` 与该 Hermes 都运行在 WSL 时，在项目的未跟踪 `.env.local` 中设置：

```dotenv
SPARK_PUSH_HERMES_ENABLED=true
SPARK_PUSH_HERMES_BASE_URL=http://127.0.0.1:8643/v1
SPARK_PUSH_HERMES_API_KEY=复制独立Hermes的API_SERVER_KEY
SPARK_PUSH_HERMES_MODEL=hermes-agent
SPARK_PUSH_HERMES_STREAMING=true
```

`API_SERVER_KEY` 位于 `/home/peco/.hermes-spark-agent/.env`。不要把它提交到仓库。Bridge 必须
连接 Hermes Agent API，不能把 `SPARK_PUSH_HERMES_BASE_URL` 改成第三方模型地址，否则会绕过
Agent loop、Skills、工具和子 Agent。

## 5. Spark Push Skills

当前实例已安装：

- `spark-push-agent`：复杂研发任务的角色和验收编排；
- `spark-push-knowledge`：项目文档、协议和关键源码的自包含知识快照；
- `spark-push-agent` bundle：同时启用上述两个 Skills。
- `cann-advisor`：独立的只读 CANN RAG 插件与触发 Skill，配置位于
  `plugins.entries.cann-advisor.settings`。

项目知识或 Skill 变化后执行：

```bash
cd /home/peco/cppcode/fenbushi/11.2-spark_push
python3 cannbot/scripts/validate_knowledge.py --strict
python3 cannbot/scripts/sync_hermes_skills.py --install --replace \
  --hermes-home /home/peco/.hermes-spark-agent
```

然后新建 Agent 会话，或在 CLI 中重载 Skills。通过 Spark Push Web Bot 调用时使用的是已安装
快照；从项目根目录运行 CLI 时还可以检查和修改实时源码。

CANN Advisor 的源码不放入 Hermes 上游内置 `plugins/`。插件从独立知识工作区安装到此
`HERMES_HOME`，支持 `/cann`、`/kb` 和自然语言自动检索。索引更新只切换 generation，
不会动态修改 system prompt 或工具集。

## 6. 二次开发边界

按变化类型选择最窄的扩展层：

1. 身份、语气、长期协作风格：修改 `SOUL.md`；
2. 项目知识、角色、流程：修改本仓 `cannbot/skills/` 并重新同步；
3. 自定义工具或外部系统：放入独立状态目录的 `plugins/`，不要先污染核心工具集；
4. 修改 Agent loop、provider、记忆或知识检索框架：在 `hermes-spark-agent` 源码分支开发。

修改 Hermes 核心前先阅读源码根目录 `AGENTS.md`。Python 测试必须通过 Hermes 自带包装器运行：

```bash
cd /home/peco/cppcode/fenbushi/hermes-spark-agent
scripts/run_tests.sh tests/相关测试文件.py -q
```

上游仓库是浅克隆；需要查看完整历史时先执行 `git fetch --unshallow origin`。同步上游时在个人
分支显式 fetch/rebase 并解决冲突，不要用更新命令覆盖未提交的二次开发改动。
