# Kernel Agent

Kernel Agent 是 Spark Push 中的算子研发助手，基于 Pi coding agent 定制。Pi 提供模型访问、
Agent loop、内置工具和会话管理；本项目提供 CANN 知识检索、子 Agent 编排、消息接入、
任务权限、执行账本和多人工作区。产品名称统一使用 Kernel Agent，Pi 继续作为底层依赖注明。

## 产品与兼容边界

| 层次 | 名称或兼容值 | 职责 |
|---|---|---|
| 产品 | Kernel Agent | 面向用户的算子研发助手 |
| 运行时 | Pi coding agent / Pi SDK | 模型循环、工具执行、会话与 provider |
| Gateway | Kernel Agent gateway，`pi_gateway.py` | HTTP/SSE、Pi RPC、Router 接入 |
| 注册表 | `id=pi`、`runtime=pi_rpc` | 保留既有路由身份 |
| PC / Android 联系人 | `900000000001` / `900000000201` | 原会话历史、模型偏好和权限不迁移 |
| 安装状态 | `~/.pi-spark-agent` | 保留已有模型、凭据、Skills 与 JSONL 会话 |

兼容字段不代表另有一个对外的 Pi Agent 产品。Hermes technical 等独立接入仍保持自己的名称、
所有者和会话。新增 Kernel Agent 子会话也不会变成 IM 联系人，不参与消息序号分配。

## 已有能力

- 工作区代码读取与修改；主 Agent 可在其已有权限内执行命令和测试。
- CANN `search/get/neighbors/status` 工具、版本筛选、分页原文和引用溯源。
- 模型选择、思考等级、会话重置与重试、图片输入、工具进度和流式回复。
- Router 的身份绑定、turn 幂等和事件回放；同会话串行、跨会话有界调度。
- 多人共享算子任务、开发授权、版本化交接、工作区检查点和固定 runner 测试回执。
- 一个任务内创建独立上下文的子 Agent，分派研究、设计、修改与审查，收集结果后由主 Agent 验收。

房间测试机制不等于 Ascend NPU 验证。当前已记录的真实固定 runner 验收为 Linux C++ Add，
设备透传、远程 NPU 编译与性能闭环仍需对应执行环境和独立证据。

## 子 Agent 编排

入口是 [`kernel-subagents.ts`](../cannbot/pi/extensions/kernel-subagents.ts)。接口的生命周期设计
参考开源 Codex CLI，执行部分直接复用 Pi SDK `createAgentSession`，不另写模型协议或 Agent loop。
参考的源文件、固定版本和许可证见 [Codex 源码参考](codex-subagent-reference.md)。

```text
用户任务 → Kernel Agent 主会话
              ├─ spawn_agent(researcher) → 独立检索上下文
              ├─ spawn_agent(architect)  → 独立设计上下文
              └─ wait_agent 收集计划
                       ↓
                 developer 修改明确文件
                       ↓
                 reviewer / tester 只读核验
                       ↓
                 主 Agent 执行测试、检查结果、汇总回复
```

| 工具 | 行为 |
|---|---|
| `spawn_agent` | 指定角色、任务和必要上下文，立即返回子 Agent 身份；developer 需显式写入范围 |
| `send_input` | 向已完成子会话补充任务或证据；运行中需显式 `interrupt=true`，先中断再继续 |
| `wait_agent` | 任一指定子任务到达终态即返回；等待超时不取消任务，也不等于失败 |
| `list_agents` | 查看当前主 turn 拥有的子任务 |
| `close_agent` | 取消/关闭子会话并释放资源 |

子会话使用独立内存 Session，继承父会话的模型和思考等级；主 Agent 显式传递必要上下文，
不复制整个聊天历史。子 Agent 的输出是待核查的工作结果，主 Agent 必须检查代码和实际测试证据。
互无依赖的研究或审查可以并发；设计、修改和依赖这些修改的验证按依赖顺序执行。

CANN 子任务应返回文档/片段定位信息。子任务的工具事件不会自动进入主 turn 的引用账本；
主 Agent 对最终要引用的来源重新调用 `cann_knowledge_get` / `neighbors`，完整读取后再生成引用，
不能把子文本中的 `CANN_REF` 直接当作已验证证据。

## 工具权限与生命周期

- 子 Agent 的工具集合取父会话已启用工具的子集，不允许借委派扩大权限。
- researcher、architect、reviewer、tester 只读。tester 检查测试设计和已有证据，测试命令由主 Agent 执行。
- developer 只能写 `allowed_writes` 列出的精确文件路径；并发开发者不能持有重叠或祖先路径冲突的范围，
  要等 `close_agent` 释放会话后才释放该写入范围。
- 子 Agent 没有 shell，也不能继续派生孙 Agent；深度固定为一层。
- 房间子 Agent 只读工作区和 CANN 知识，不写文件、不执行 runner；房间主 Agent 继续遵循 Logic 的开发授权。
- 子任务仅属于当前主 turn；父 turn 结束、取消、会话切换或关闭时清理未结束的子会话。
- 子任务数量、轮数、执行时间、等待时间和返回文本都有上限，不能无限派生或无限积累输出。

这是工具权限和生命周期约束，不是操作系统多租户沙箱。主 Agent 仍需避免同时编辑已委派文件。
子会话不持久化为可恢复任务；进程重启后不恢复子 Agent，不自动重放可能产生副作用的子任务。
Gateway 的父 turn 账本和 IM 历史保持各自现有语义，不能用它们推断子任务支持 exactly-once。

## 安装与配置

```bash
bash cannbot/scripts/setup_kernel_agent.sh
kernel-agent             # 交互式入口
kernel-agent gateway     # 本机 HTTP/SSE 服务
```

安装器写入 `~/.local/bin/kernel-agent`，保留 `pi-spark-agent` 兼容启动器。Pi 运行时的安装目录、
模型和凭据保持原路径，项目 Skills 和扩展通过 `sync_pi_agent.py` 同步，工具库随扩展安装。
旧 `setup_pi_agent.sh` 也继续可用。安装或同步后重启 Gateway / 新开 CLI 会话才会加载新扩展；
提交源码不代表已经更新正在运行的服务。交互式 CLI 仍遵循 Pi 的项目信任流程，
应按实际工具发现结果判断是否已加载扩展；子 Agent 的角色提示无法绕过父会话工具限制。

| 配置 | 默认 / 范围 | 用途 |
|---|---|---|
| `KERNEL_AGENT_SUBAGENTS_ENABLED` | `true` | 设为精确值 `false` 禁用子 Agent 工具 |
| `KERNEL_AGENT_MAX_CHILDREN` | `4`；1～8 | 每个父 turn 保留的子会话名额 |
| `KERNEL_AGENT_CHILD_TIMEOUT_MS` | `120000`；1000～600000 | 单次子任务执行时限 |
| 固定上限 | 16 次创建 / turn；每子会话 4 轮 | 防止通过反复关闭再创建绕开资源预算 |
| 固定输出/工具上限 | 12000 字符；每子会话累计 48 次工具调用 | 限制结果和工具执行规模 |
| `wait_agent` 等待 | 默认 10 秒，最多 60 秒 | 等待期限与子任务执行期限分别控制 |

Gateway 接受 `SPARK_PUSH_KERNEL_HOST/PORT/API_KEY/CWD/PROVIDER/MODEL`，优先于对应旧
`SPARK_PUSH_PI_*` 配置；启动器可用 `SPARK_PUSH_KERNEL_COMMAND` 指向自定义路径。
Bridge 仍使用兼容的 `SPARK_PUSH_HERMES_*` 变量，Bearer 必须与 Gateway 一致。
不要把虚拟入口模型名当作真实 provider 模型；底层模型仍由现有配置决定。

注册表中旧默认显示名会转换为 Kernel Agent，`id=pi`、`runtime=pi_rpc` 与联系人 ID 保持不变。
存量数据库的机器人名称按需要执行
[`20261030_kernel_agent_identity.sql`](../sql/migrations/20261030_kernel_agent_identity.sql)：
只更新保留机器人账号匹配的旧默认名称，保留管理员自定义名称，不改变消息外键和历史。

## 与其他并发机制的区别

| 机制 | 处理的对象 | 典型用途 |
|---|---|---|
| 多 Agent 联系人 | Kernel Agent、Hermes 等独立服务 | 用户选择不同运行时 |
| Gateway worker 池 | 不同用户/会话的请求 | 跨会话吞吐与准入 |
| Kernel 子 Agent | 同一个主任务的子任务 | 独立上下文、分工、收集和审查 |
| 房间协作 | 多名用户共享的算子任务 | 授权、交接和版本证据 |

部署与配置见 [运行时集成](pi-agent-integration.md)；开发阶段与角色规则见
[`spark-push-agent`](../cannbot/skills/spark-push-agent/SKILL.md)。

## 2026-10-30 验收记录

独立发布工作树完成 C++ 构建；CTest 49 项中 46 项通过、3 项外部服务测试按配置跳过。
子任务生命周期/权限测试 53 项、真实 Pi SDK 配合本机受控 SSE 模型的集成检查 32 项、
Kernel Agent 浏览器名称检查 6 项通过；安装同步保留原模型和设置。
没有调用真实外部模型或验证 NPU。源码指纹、命令对应结果与跳过项见
[结构化验收记录](benchmarks/2026-10-30-kernel-agent/validation.json)。

额外执行的旧 `agent_channel_browser_test.cjs:129` 在未修改的 c5c396d 基线和本次分支均失败：
选择思考等级后，命令卡片按钮未保持禁用。本次没有把该已有问题计为通过，产品名称另有独立浏览器验证。
