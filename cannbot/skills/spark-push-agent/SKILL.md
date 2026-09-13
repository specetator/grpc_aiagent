---
name: spark-push-agent
description: Spark Push 多角色研发编排。用于非简单的架构设计、跨组件实现、代码审查、可靠性影响分析和测试验收；普通知识问答只需 spark-push-knowledge。
version: 0.2.0
author: Spark Push project
license: UNLICENSED
platforms: [linux, windows]
metadata:
  tags: [Spark-Push, Agent, Orchestration, C++, Review, Testing]
  related_skills: [spark-push-knowledge]
---

# Spark Push Agent

把 CANNBot 的“角色 + Skill + 工作流”思想映射到 Pi Agent loop。运行引擎、工具调用和第三方
模型访问由 Pi 承担；CANN RAG 仍通过 `cann-advisor` 扩展使用独立知识库。

## 入口判断

1. 简单项目问答：加载 `spark-push-knowledge` 后直接回答，不切换角色。
2. 单文件、低风险修改：主 Agent 可直接检查、实现和验证。
3. 跨组件设计、可靠性协议修改或用户明确要求独立审查：读取
   [`references/workflow.md`](references/workflow.md)，在同一会话中按 Architect →
   Developer → Reviewer → Tester 顺序切换角色。Pi 默认没有 Hermes 式 `delegate_task`；
   不要假装已经派出隔离子 Agent，除非当前环境明确提供了 subagent 扩展。
4. Spark Push Web Bot 经 Pi gateway 调用时，工作目录就是项目根目录，read/write/edit/bash
   可用。先检查实时代码再改；改完要跑相关验证。只有确认当前进程看不到项目树时，才改为只读分析。

## 强制上下文

- 先读取 `spark-push-knowledge`，再按 source map 加载最小证据集。
- 切换角色时必须带上完整上下文：项目根目录、相关文件、用户目标、可靠性约束、
  验证命令及允许写入范围。
- 角色模板位于 [`references/roles.md`](references/roles.md)。把对应角色的职责和边界写入
  当前提示，不依赖角色名称本身产生隐式行为。

## 质量门禁

- Architect 先给出证据、边界和计划，不写实现。
- Developer 只在计划明确后修改代码，并报告实际执行的验证。
- Reviewer 使用独立上下文检查 diff 和协议影响，不直接修改代码。
- Tester 独立执行可复现测试，不把“编译成功”推断为“行为正确”。
- Reviewer 或 Tester 发现阻塞问题时回到 Developer；最多三轮仍未收敛则停止并向用户报告。
- 最终由主 Agent 核对工作区 diff 和测试结果，再向用户交付；子 Agent 的自报不能替代验证。

## 并发边界

只有互不写同一文件、彼此没有先后依赖的研究或测试任务才并行委派。设计→实现→审查→修复
必须串行。Spark Push IM 的无状态 HTTP 请求中，Pi gateway 会同步等待整轮结果，因此只在复杂任务
中启用多角色流程，控制第三方模型成本和响应时间。
