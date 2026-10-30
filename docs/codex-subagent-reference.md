# Kernel Agent 子 Agent 的源码参考与复用边界

核对日期：2026-10-30。Kernel Agent 基于 Pi 运行时；子 Agent 的模型循环、工具执行和上下文管理
直接使用 Pi SDK，生命周期设计参考 OpenAI Codex CLI。这里记录实际查阅的上游版本，避免把
“参考 Codex 编排”写成“移植了整个 Codex”或“自行实现了模型运行时”。

## 1. 固定来源

- OpenAI Codex：提交 [`788b9d7bf6e37f735647b7f84b556c2a807df4d8`](https://github.com/openai/codex/commit/788b9d7bf6e37f735647b7f84b556c2a807df4d8)，
  [Apache-2.0](https://github.com/openai/codex/blob/788b9d7bf6e37f735647b7f84b556c2a807df4d8/LICENSE)。
- Pi：`@earendil-works/pi-coding-agent` **0.84.4**，对应 `v0.84.4` 提交
  [`b79e4cc834970cca69daebffab7df1da7d1e52c4`](https://github.com/earendil-works/pi/commit/b79e4cc834970cca69daebffab7df1da7d1e52c4)，
  [MIT](https://github.com/earendil-works/pi/blob/b79e4cc834970cca69daebffab7df1da7d1e52c4/LICENSE)。

以上提交及文件通过 GitHub 连接器读取；Pi API 另与本机已安装的同版本 SDK 类型定义、示例核对。
依赖升级后应重新运行子 Agent 回归，不能只根据 API 名称推定行为兼容。

## 2. 从 Codex 学习的生命周期约束

| 源码 | 观察到的机制 | Kernel Agent 的对应约束 |
|---|---|---|
| [`multi_agents/spawn.rs`](https://github.com/openai/codex/blob/788b9d7bf6e37f735647b7f84b556c2a807df4d8/codex-rs/core/src/tools/handlers/multi_agents/spawn.rs) | 先检查子线程深度，再构造子配置、创建独立线程，返回身份 | 创建真正独立的子会话；不把主会话中的角色切换称为子 Agent |
| [`agent/registry.rs`](https://github.com/openai/codex/blob/788b9d7bf6e37f735647b7f84b556c2a807df4d8/codex-rs/core/src/agent/registry.rs) | 启动前预留容量，启动失败自动释放；注册表维护身份和活跃数量 | 在异步创建前占用名额，关闭或创建失败释放，防止并发调用越过上限 |
| [`agent/child_config.rs`](https://github.com/openai/codex/blob/788b9d7bf6e37f735647b7f84b556c2a807df4d8/codex-rs/core/src/agent/child_config.rs) | 基于当前父 turn 构造配置，角色配置之后仍应用运行中的权限、cwd 等约束 | 继承当前模型与思考等级；子工具不得超出父工具范围，角色提示不能授予权限 |
| [`multi_agents/send_input.rs`](https://github.com/openai/codex/blob/788b9d7bf6e37f735647b7f84b556c2a807df4d8/codex-rs/core/src/tools/handlers/multi_agents/send_input.rs) | 向既有子线程发送后续输入，可先中断当前任务 | 已完成子会话可接收后续任务；正在运行时须显式中断，首版未实现普通输入排队 |
| [`multi_agents/wait.rs`](https://github.com/openai/codex/blob/788b9d7bf6e37f735647b7f84b556c2a807df4d8/codex-rs/core/src/tools/handlers/multi_agents/wait.rs) | 检查已完成状态并订阅状态变化；任一指定子线程终结即可返回，等待超时不取消任务 | 等待与取消分开；主 Agent 收集真实结果后再汇总，超时不能当作失败或成功 |
| [`multi_agents/close_agent.rs`](https://github.com/openai/codex/blob/788b9d7bf6e37f735647b7f84b556c2a807df4d8/codex-rs/core/src/tools/handlers/multi_agents/close_agent.rs) | 关闭子线程，并返回关闭之前的状态 | 明确取消和释放子会话资源；主会话切换或销毁时清理所属子会话 |

Codex 还有持久 Agent 图、恢复、环境绑定、完整历史分叉等机制。本项目没有因此自动获得这些
能力，也不将工具名相似视为 Codex API 完整兼容。这里没有复制或链接 Codex Rust 源码。

## 3. 直接复用 Pi 的部分

Pi 官方的 [`examples/extensions/subagent/index.ts`](https://github.com/earendil-works/pi/blob/b79e4cc834970cca69daebffab7df1da7d1e52c4/packages/coding-agent/examples/extensions/subagent/index.ts)
已展示独立子进程、有限并行、串行链路、输出收集和取消处理。它证明 Pi 不需要重新编写 Agent Loop
就能支撑子 Agent；本项目采用同包的 SDK 来保留可追加输入的子会话。

实现主要复用
[`examples/sdk/12-full-control.ts`](https://github.com/earendil-works/pi/blob/b79e4cc834970cca69daebffab7df1da7d1e52c4/packages/coding-agent/examples/sdk/12-full-control.ts)、
[`examples/sdk/11-sessions.ts`](https://github.com/earendil-works/pi/blob/b79e4cc834970cca69daebffab7df1da7d1e52c4/packages/coding-agent/examples/sdk/11-sessions.ts)
所示的公开 API：

- `createAgentSession`：使用现有 Pi 模型循环和工具执行器。
- `SessionManager.inMemory`：每个子 Agent 具有独立消息历史。
- 显式 `ResourceLoader` 和 `tools`：避免子会话自动发现额外扩展、再次加载编排器或扩大工具权限。
- 当前 `ModelRuntime`、模型和思考等级：沿用现有 provider 与认证，不增加另一套模型 HTTP 客户端。
- `AgentSession.prompt`、`abort`、`subscribe`、`dispose`：复用输入、事件和生命周期机制。

Kernel Agent 自己承担的是角色选择、身份注册、配额、父子权限边界和结果汇总协议。独立上下文
不等于独立操作系统沙箱；多个子会话仍可能共享工作目录，写入范围和房间测试权限需由项目
策略限制。实际启用范围、工具参数与测试结果以 Kernel Agent 实现文档及当前源码为准。

项目入口为 `cannbot/pi/extensions/kernel-subagents.ts`，生命周期实现位于
`cannbot/pi/extensions/lib/kernel-agent-control.ts`。工具为 `spawn_agent`、`send_input`、
`wait_agent`、`close_agent`、`list_agents`。首版使用 turn 范围内的子会话，不允许子 Agent
继续派生；没有跨 Gateway 重启恢复子会话的能力。房间子 Agent 保持只读检索；`tester` 角色
提供测试分析，实际命令由父 Agent 的既有授权工具执行，不额外取得 shell 权限。

## 4. 许可与归属

Pi SDK 保持上游 MIT 许可。对其官方 SDK 示例的适配保留
[`third_party/pi/LICENSE`](../third_party/pi/LICENSE)；新增的项目编排逻辑不能宣称为 Pi 或
OpenAI 官方产品。Codex 在本实现中是源码设计参考，其 Apache-2.0 归属与固定来源记录如上。
如果今后直接复制 Codex 实现，应再随分发保留相应许可证、版权及修改说明。
