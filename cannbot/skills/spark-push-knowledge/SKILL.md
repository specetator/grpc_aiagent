---
name: spark-push-knowledge
description: Spark Push 项目知识查询与变更影响分析。适用于 Comet/Logic/Job 架构、Kafka 消息链路、ACK/游标/幂等、用户中心、Pi Agent 接入，以及项目参考文档维护；不用于 CANN 算子开发。
version: 0.3.0
author: Spark Push project
license: UNLICENSED
platforms: [linux, windows]
metadata:
  tags: [Spark-Push, C++, IM, Kafka, Reliability, Pi, Knowledge]
  related_skills: [spark-push-agent]
---

# Spark Push Knowledge

本技能是项目知识的路由层。源码仓中由登记表指向实时文件；安装到 Pi Agent 时，同步脚本会把
登记过的文档和关键源码打包成自包含快照。回答前先定位权威来源，再区分已实现事实、文档设计
和后续建议。

## 使用方式

1. 先读 [`references/architecture-map.md`](references/architecture-map.md)，按问题选择最小来源集。
2. 若存在 `references/installed-source-map.md`，先读该文件，再读取列出的
   `references/sources/...` 快照；不要猜测 WSL 项目路径。
3. 若 Pi 正在项目根目录运行，则同时检查实时代码；代码与快照或文档冲突时，以实时代码
   说明当前行为，并指出需要重新同步知识包。
4. 只有用户明确询问教学演进时才读取 `02_auth_subset/`～`06/` 或
   `5.1_room_subset/`。这些目录不是当前运行真源。
5. 涉及新增参考资料、检索范围或知识结构时，读取
   [`references/knowledge-maintenance.md`](references/knowledge-maintenance.md)。

## 必须保持的语义

- `accepted_ack` 与 `delivered_ack` 是两个阶段，不能相互替代。
- `ai_delta` 只提供流式体验，不落消息历史、不推进游标；最终 `ai_reply` 才进入普通单聊的
  持久化、实时投递和离线恢复链路。
- Spark Push 管理鉴权、会话、`msg_seq`、幂等和历史。Agent runtime 只生成答案。
- 当前 `hermes_bridge` 调用本机 Pi gateway 的 OpenAI-compatible Chat Completions HTTP/SSE；
  该接口进入 Pi Agent loop，而不是直接调用底层第三方模型。
- 根目录主线是运行真源；构建目录、依赖源码和教学快照不得混入日常 Codebase 检索结果。

## 回答要求

- 给出结论后附上原始项目文件路径；使用安装快照时也必须展示 source map 中的 original path。
- 无代码证据的内容标记为建议或待验证，不把设计目标描述成已经实现。
- 修改涉及消息可靠性时，至少检查实时在线、断线重连、重复投递、历史落库四种结果。
- 除非用户要求实施，否则知识查询只读，不修改代码或外部系统。
