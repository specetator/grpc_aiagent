---
name: spark-push-agent
description: Kernel Agent 的 Spark Push 子 Agent 研发编排。用于非简单的架构设计、跨组件实现、代码审查、可靠性影响分析和测试验收；普通知识问答只需 spark-push-knowledge。
version: 0.3.0
author: Spark Push project
license: MIT
platforms: [linux, windows]
metadata:
  tags: [Spark-Push, Agent, Orchestration, C++, Review, Testing]
  related_skills: [spark-push-knowledge]
---

# Kernel Agent · Spark Push workflow

Kernel Agent 基于 Pi coding agent，复用其 Agent loop、工具和模型访问；CANN RAG 通过
`cann-advisor` 扩展接入独立知识库。`kernel-subagents` 扩展提供独立上下文的子 Agent，
本 Skill 负责研发阶段、任务交接与验收规则，技能名保留兼容。

## 入口判断

1. 简单项目问答：加载 `spark-push-knowledge` 后直接回答，不切换角色。
2. 单文件、低风险修改：主 Agent 可直接检查、实现和验证。
3. 跨组件设计、可靠性协议修改或用户明确要求独立审查：读取
   [`references/workflow.md`](references/workflow.md)，按 Architect → Developer → Reviewer →
   Tester 的依赖关系组织任务。工具可用时使用 `spawn_agent` 创建独立子会话，
   `wait_agent` 收集结果，`close_agent` 释放资源；不能用角色切换冒充独立审查。
   未加载扩展时明确说明，只能由主 Agent 执行，不声称派出了子 Agent。
4. Spark Push Web Bot 经 Kernel Agent gateway 调用时，工作目录就是项目根目录，read/write/edit/bash
   可用。先检查实时代码再改；改完要跑相关验证。只有确认当前进程看不到项目树时，才改为只读分析。

## 强制上下文

- 先读取 `spark-push-knowledge`，再按 source map 加载最小证据集。
- 派发子任务时必须带上最小完整上下文：项目根目录、相关文件、用户目标、可靠性约束、
  验证命令及允许写入范围。
- 角色模板位于 [`references/roles.md`](references/roles.md)。把对应角色的职责和边界写入
  子任务提示，不依赖角色名称本身产生隐式行为；developer 的写入范围用 `allowed_writes` 明确列出。

## 质量门禁

- Architect 先给出证据、边界和计划，不写实现。
- Developer 只在计划明确后修改允许文件；子 Agent 不具备 shell，验证命令由主 Agent 执行。
- Reviewer 使用独立上下文检查 diff 和协议影响，不直接修改代码。
- Tester 子 Agent 只读检查测试计划与结果；主 Agent 执行可复现测试并交回真实证据，
  不把“编译成功”推断为“行为正确”。
- Reviewer 或 Tester 发现阻塞问题时回到 Developer；最多三轮仍未收敛则停止并向用户报告。
- 最终由主 Agent 核对工作区 diff 和测试结果，再向用户交付；子 Agent 的自报不能替代验证。
- 子 Agent 的 CANN 工具结果不会自动进入主 turn 引用账本。主 Agent 对最终引用重新执行
  `cann_knowledge_get` / `neighbors`，按分页要求完成证据读取，不直接转用子任务的引用标识。

## 并发边界

只有互不写同一文件、彼此没有先后依赖的研究或测试任务才并行委派。设计→实现→审查→修复
必须按依赖串行。子会话属于当前主 Agent turn，不跨 turn 保留；主 Agent 结束或会话切换会清理
未完成的子任务。深度固定一层，子 Agent 不能继续派生；房间子 Agent 只读检索，写入和测试
由已授权主 Agent 执行。将任务结果收齐后再答复，控制模型成本和响应时间。
