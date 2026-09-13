# CANNBot Agent 思路的 Pi 接入与二次开发

本项目只保留 CANNBot 中有价值的“Agent 角色 + Skills + 工作流”分层思想，执行引擎统一使用
Pi Agent。运行和二次开发不需要 OpenCode、CANNBot Toolkit 或 GitCode 登录；第三方模型由 Pi 的
provider / `models.json` 接入。原先 WSL Hermes 只作为已替换的基座，不要再把 Bridge 指回 Hermes。

> 名称说明：仓库继续使用 `cannbot/` 目录表示这套 Agent 分层的来源思路，但其中内容是本项目
> 自己维护的 Pi 适配，不是对 CANNBot Toolkit 的运行依赖。

## 1. 运行架构

```text
Spark Push 用户
    │
    ▼
Logic ──Kafka ai_request──► hermes_bridge
                                 │ OpenAI-compatible HTTP/SSE
                                 ▼
                         Pi gateway :8643
                          ├─ Pi Agent loop / tools
                          ├─ spark-push-agent
                          ├─ spark-push-knowledge
                          ├─ cann-advisor RAG 扩展
                          └─ custom provider ──► 第三方 LLM API
```

Pi gateway 的 `/v1/chat/completions` 在服务端进入完整 Agent loop；底层 provider 才负责访问
第三方 OpenAI-compatible 模型。`hermes_bridge` 因此应指向 Pi gateway，不能直接指向第三方
模型地址，否则 Skills、RAG 工具都会被绕过。完整安装步骤见
[`pi-agent-integration.md`](pi-agent-integration.md)。

Spark Push 继续负责用户鉴权、会话成员、`msg_seq`、Kafka 持久化、幂等、实时投递和离线恢复。
Hermes 只负责推理、工具和答案生成：`ai_delta` 是不落库的临时体验，最终 `ai_reply` 才进入普通
单聊消息链路。

## 2. 保留的 Agent 层

仓库内有两个 Hermes Skills：

| Skill | 作用 |
|---|---|
| `spark-push-agent` | 非简单研发任务的 Architect、Developer、Reviewer、Tester 角色编排 |
| `spark-push-knowledge` | 按权威等级检索项目文档、协议和关键源码，给出可引用证据 |

根目录 `AGENTS.md` 提供从项目目录启动 Hermes 时的稳定工程约束。复杂任务由
`spark-push-agent` 使用 Hermes 的 `delegate_task` 创建隔离子 Agent；简单问答或局部修改由主
Agent 直接完成，避免无意义地增加模型费用和延迟。

CANNBot 的角色文件格式不能被 Hermes 当作原生子 Agent 注册表直接执行。本项目将角色职责变成
`delegate_task` 的显式 `goal/context` 模板，位置在：

```text
cannbot/skills/spark-push-agent/
├── SKILL.md
└── references/
    ├── roles.md
    └── workflow.md
```

## 3. 自包含知识包

通过 Spark Push API 调用的 Hermes 会话不应假定工作目录就是项目根目录。若 Skill 只保存 `README.md` 之类的
相对路径，Agent 实际无法读取项目文件。同步脚本因此会把登记过的文档和关键源码复制到：

```text
<HERMES_HOME>/skills/spark-push-knowledge/references/sources/
```

并生成 `references/installed-source-map.md`，记录 source ID、权威等级、原始项目路径、安装路径
和 SHA-256。回答仍引用原始项目路径；哈希用于判断安装快照是否和当前仓库一致。

面向 CANN 开发顾问的正文不放在这个 Spark 项目 Skill 快照中，而是位于独立工作区：

```text
/home/peco/cppcode/fenbushi/cann-agent-knowledge
├── sources.json / sources.lock.json
├── cann_rag/                 # 抽取、BM25F/标签/关联召回、证据判断
├── hermes-plugin/cann-advisor
├── hermes-skill/cann-advisor
└── data/generations/         # 不进入 Git，current.json 原子切换
```

第一批语料只摄入 `cannbot-skills` 的 `ops/model/graph/runtime/tools` 中的 CANN Skill、
references 和代码样例，锁定上游 commit。许可证正文、版权和 NOTICE 保存在独立工作区。
新增自有资料必须先写入 `sources.json`；RAG 不会扫描任意磁盘目录。默认离线检索不依赖
LLM endpoint 的 embeddings 能力。

知识真源登记表是 `cannbot/knowledge-sources.json`。`authority` 含义如下：

- `code`：协议、schema 或直接决定运行行为的源码；
- `canonical`：随主线维护的设计、运行或 Agent 约束；
- `snapshot`：带环境和日期的测试/性能结果；
- `historical`：教学演进、旧实现或背景材料。

## 4. 安装到 Pi

先校验知识登记和 Skill 结构：

```bash
cd /home/peco/cppcode/fenbushi/11.2-spark_push
python3 cannbot/scripts/validate_knowledge.py --strict --list
```

预览当前 Pi 安装是否一致：

```bash
python3 cannbot/scripts/sync_pi_agent.py --check \
  --pi-home /home/peco/.pi-spark-agent
```

首次安装：

```bash
bash cannbot/scripts/setup_pi_agent.sh
```

项目知识变化后使用 `--replace`。脚本不会静默覆盖旧 Skill 内容，而是先把旧目录移动为带时间戳的
`backup-*`：

```bash
python3 cannbot/scripts/sync_pi_agent.py --install --replace \
  --pi-home /home/peco/.pi-spark-agent
```

完成后重启 Pi gateway，或新开 `pi-spark-agent` 会话。Spark Push 输入框的 slash command
当前由 Logic 自己解析，日常聊天直接用自然语言提问。

## 5. 第三方 LLM API

第三方密钥和 endpoint 只配置在 Pi，不进入 Spark Push 仓库。独立 WSL Pi 使用 `cli-relay`
provider 接入原 CLI Relay。Spark Push 侧只配置 Pi gateway 地址和 Bearer：

```dotenv
SPARK_PUSH_HERMES_BASE_URL=http://127.0.0.1:8643/v1
SPARK_PUSH_HERMES_API_KEY=<与 Pi gateway Bearer 相同>
SPARK_PUSH_HERMES_MODEL=pi-agent
```

`SPARK_PUSH_HERMES_MODEL` 对 Pi gateway 是虚拟模型名；真实模型由 `~/.pi-spark-agent`
的 `settings.json` / `models.json` 决定。会话内 `/model provider:model` 仍由现有适配层转发。
真实密钥放在 `/home/peco/.pi-spark-agent/.env`，不写进 `.env.example`、Skill 或日志。
完整说明见 [`pi-agent-integration.md`](pi-agent-integration.md)。

## 6. 二次开发

### 增加参考文档

1. 把文档放入项目，例如 `docs/new-reference.md`；
2. 在 `cannbot/knowledge-sources.json` 登记唯一 ID、路径、authority 和 topics；
3. 更新 `spark-push-knowledge/references/architecture-map.md` 的路由；
4. 增加 `evals/evals.json` 中的正向、反向或边界问题；
5. 执行严格校验和 `sync_pi_agent.py --install --replace`；
6. 重载 Skills 后用问题和文件引用验收。

### 修改知识库机制

先用可复现评测描述召回问题：问题文本、期望来源、错误来源、答案差异。修改独立工作区的
source、抽取、字段权重、标签、邻接关系或证据门槛后，运行 `bin/cann-rag eval`。当前实现为：

```text
DocumentSource → Chunker → Retriever → optional Reranker
```

该 RAG 已支持来源元数据、定向增量摄入、禁用来源、原子 generation、稳定文档/片段 ID、
离线评测和结构化引用。RAG 只是 Pi 的知识工具，不能接管 Spark Push 的消息可靠性状态。

### 修改角色或工作流

- 角色边界：`spark-push-agent/references/roles.md`；
- 阶段、门禁和回退：`spark-push-agent/references/workflow.md`；
- Pi 触发和总约束：`spark-push-agent/SKILL.md`；
- 项目级长期约束：根目录 `AGENTS.md`。

修改后同时验证简单问答不会无故委派、复杂任务会传完整上下文、Reviewer/Tester 不依赖
Developer 自报、只有实时项目可访问时才允许写代码。

## 7. 使用边界

通过 Spark Push Web/IM 调用时，Pi gateway 的工作目录就是项目根目录，可以使用 read/write/edit/bash
修改和验证代码；最终回答仍走 Spark 的 `ai_reply` 落库。从项目根目录启动 `pi-spark-agent` CLI
时同样能访问该工作树，`AGENTS.md` 会成为项目上下文。

同级的 `cannbot-skills` 上游克隆是首次建库和后续同步的显式来源，无需 GitCode 登录。
派生的检索实现和摄入内容仅用于华为昇腾/CANN 开发场景；独立工作区完整保留 CANN Open
Software License 2.0、上游版权、commit 和内容 SHA。分发或扩大用途前应重新确认许可证约束。
