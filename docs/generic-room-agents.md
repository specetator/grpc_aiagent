# 通用 Room Agent 与协作任务

验证日期：2026-10-31。以当前工作树为准。旧 `@CANN` 专用工作区和
`room_agent_task` 保留；本文的新任务不替代 CANN 的算子任务。

## 源码审查与最小设计

空间权限由 `RoomService` / `SpaceRepository` 和 MySQL 成员关系负责。
Redis 订阅与 Comet 在线连接只决定投递位置，不能授予访问权。
`RoomAgentService` 已具备 CANN 请求、交接和权限边界；本次扩展其 Catalog、
Binding 和任务用例，HTTP 继续只做协议适配。Runtime 复用 AgentRouter、
AgentStore、SessionScheduler、Hermes Bridge 和原 Kafka 链路。

管理员维护 `AgentDefinition`，房主维护 `RoomAgentBinding`。安装不创建私人
Profile 的副本，也不允许成员提交模型密钥、Runtime 地址或工作区。
稳定 `agent_id` 是机器标识，`bot_user_id` 是发言身份，名称仅用于展示。
Web 根据服务端 Catalog 和消息发送者共同校验机器人身份。

一次消息可显式选择最多四个不同 Agent。客户端传 `agent_mentions` 的 ID 数组；
仅有正文 `@Pi` 不触发。旧 `@CANN` 仍按原协议处理，不与结构化 mentions 混用。
普通消息和 Agent 最终回复均不会自动调用其他 Agent。

## 权限矩阵

所有接口使用 bearer 身份，客户端 `user_id` 不能代替鉴权。

| 操作 | owner | admin | member | 非成员/已移除 |
|---|---|---|---|---|
| 管理空间、安装/停用 Agent | 是 | 否 | 否 | 否 |
| 邀请/移除普通成员 | 是 | 是 | 否 | 否 |
| 提升成员为 admin | 是 | 否 | 否 | 否 |
| 发言、历史、附件、Agent 调用 | 是 | 是 | 是 | 否 |
| 查看 Agent 请求/Runtime 状态/协作任务 | 是 | 是 | 是 | 否 |
| 停止其他人发起的请求 | 是 | 是 | 否 | 否 |
| 停止本人请求 | 能力支持时 | 能力支持时 | 能力支持时 | 否 |
| 创建/接手未分配任务 | 是 | 是 | 是 | 否 |
| 更新他人负责的任务 | 是 | 是 | 否 | 否 |

成员移除会拒绝新的请求，Comet 订阅失效、推送目标重新校验。
已经进入 Runtime 的执行不等同于可自动撤销：Native 可按原 request_id 请求停止；
不能停止的 Runtime 明确返回不可用。剩余成员仍可接收已授权执行的最终结果。

## 增量迁移与请求可靠性

先备份并执行 `sql/migrations/20261031_collaboration_spaces.sql`，再执行
`sql/migrations/20261031_generic_room_agents.sql`。两者均可重复执行，保留旧数据。
新 Catalog 开启时 Logic 检查新表，缺迁移则拒绝启动。

新增表：

- `room_agent_binding`：房间/Agent 唯一绑定，安装状态与 generation。
- `room_agent_dispatch`：持久调用意图、源消息、发送尝试和撤销原因。
- `room_collaboration_task`：协作任务最新状态，revision CAS。
- `room_collaboration_task_history`：每次更新的不可变快照。

`room_agent_request` 继续保存不可变请求和首个确认结果。新请求 ID 为
`ra_ + SHA256(JSON(["room-agent-v1", source_msg_id, agent_id]))`，JSON 使用紧凑编码。
所有调用意图先于源消息 outbox 持久化；只有源消息处于 Kafka 已确认阶段，或
其历史记录已存在，恢复工作线程才投递 `ai_request`。停用/重装增加 generation，
未发出的旧授权不会复活。发送尝试结果不确定时保留 unknown，不伪造 cancelled。

Bridge 重启可能重新生成投递时间戳；Repository 保留首个完成时间，只允许语义
相同的结果重放，拒绝不同文本、成功状态、取消状态或工作区回执覆盖结果。
最终消息仍用 `hermes:<request_id>` 去重。`accepted_ack`、`delivered_ack`、
`msg_seq`、outbox、离线游标含义不变；流式 delta 不持久化、不推进正式游标。

IM 会话仍为 `r_<room_id>`。通用 Agent Runtime key 为
`roomagent_<room_id>_<agent_id>`，路由 thread 为 `agent:<agent_id>`。
锁、调度和原生 Session 使用同一隔离键；同房同 Agent 的多个成员共享该 Agent
上下文，不同 Agent/房间相互隔离。CANN 和私人会话键保持原样。

## API

以下均为 POST JSON，返回既有 `{code,message,data}` 格式；必填 `room_id`。

| 路径 | 额外输入 | 结果 |
|---|---|---|
| `/api/chatroom/agents/list` | 无 | Catalog、安装状态、最近请求、can_manage |
| `/api/chatroom/agents/install` | agent_id | 安装允许的 Agent |
| `/api/chatroom/agents/uninstall` | agent_id | 停用并使旧 generation 失效 |
| `/api/chatroom/agents/control` | request_id、operation | status/stop/approve/deny/recover 的真实回执 |
| `/api/chatroom/tasks/list` | 无 | 任务与修改历史 |
| `/api/chatroom/tasks/create` | request_id、title、description、steps[] | 关联源消息的任务 |
| `/api/chatroom/tasks/update` | task_id、revision、可选 claim/status/summary | 新 revision；冲突返回 409 |

`recover` 校验原请求发起者/管理员、当前成员关系和 binding generation，重新排队
同一不可变请求。它不生成新 request_id；Native 用原 Run GET 核实结果，Pi 未知
执行保持 unknown，不重新执行工具。已经确认或撤销的记录返回 409。
恢复前先查询原 Run：仍运行或无法确认时继续显示相应状态；终态后再投递同 ID
取回结果。Kafka 有限重试耗尽后不会持续后台轮询，需显式恢复；不能宣称自动恢复。

控制请求中的 approval_token 仅用于 Runtime 已存在的审批，不授予新能力。
无控制端口返回 503，不支持的操作返回 501，未授权返回 403。
公共 Pi 没有原生停止/查询运行中进程的接口：状态来自持久账本；重启后不明执行
保持 unknown。Native 状态来自原 Run 查询，取消/中断归为 cancelled。

WS 示例：

```json
{"type":"chatroom","group_id":123,"client_msg_id":"client-generated-id","content":{"text":"请总结讨论"},"agent_mentions":["public-pi","hermes-room-test"]}
```

服务端产生 actor_user_id、request_id、Agent 身份和执行回执，拒绝用户伪造这些
字段（包括 JSON 字符串包装的 content）。任务由成员确认保存，状态仅表示协作
进度；`completed` 不意味着测试已经通过。测试结论必须带真实执行回执。

## 部署边界

参考 `cannbot/pi/room-agents.json.example`，由管理员维护同一份注册表供 Gateway
和 Logic 使用。Logic 设置 `SPARK_PUSH_ROOM_AGENT_CATALOG`；Gateway 设置同一路径的 `SPARK_PUSH_AGENT_REGISTRY`。运行配置变化需重启相应服务，无公开 Catalog 修改接口。

公共 Pi 需要独立的 `SPARK_PUSH_PUBLIC_PI_ROOT`，以及管理员明确提供的
`SPARK_PUSH_PUBLIC_PI_ENV_FILE` / `SPARK_PUSH_PUBLIC_PI_MODELS_FILE`。
每个房间/Agent 有独立 workspace、HOME、agent-home 和 sessions。禁止默认
继承私人 Pi 目录；启动参数关闭工具、扩展、Skills、模板和主题。不提供 shell。
文件只读取允许的模型凭据键，不能注入 NODE_OPTIONS、PATH 或工作目录。

Hermes 默认关闭。测试需显式 `room_enabled=true`、`room_test_only=true`、
`spark-room-test-*` Profile、`SPARK_PUSH_ROOM_HERMES_TEST=true`，且交互审批关闭。
适配器查询真实工具清单，当前只允许独立 Agent 的 `todo` 计划工具，禁止文件、
终端、跨 Session 搜索和记忆工具。这个门禁不替代管理员对 Runtime 隔离的部署责任。
不得只给私人 technical Profile 换个名字后共享。

本地真实验收脚本 `tests/hermes_room_runtime_fixture.py` 在明确授权下仅读取 default
Profile 的模型 access token（不复制 refresh token），创建临时隔离配置、API key
和工作区；不复制私人聊天/记忆/Skills/.env。测试前后核对原配置和认证文件哈希。
若 access token 即将过期则拒绝测试，不刷新原账户。脚本完成后清理自身目录。

## 验证与后续接口

参见 [MVP 验收矩阵](mvp-v1-acceptance.md)。受控适配器测试、协议契约测试与真实
Pi/Hermes E2E 分开记载。自动自主协商、公共 shell、通用写入工具未开放。
后续可在原 Catalog 中增加管理员批准的能力，在 Runtime 的原生工具/审批接口
上扩展；不得借聊天字段提升权限，也不得另建 Agent Loop。
