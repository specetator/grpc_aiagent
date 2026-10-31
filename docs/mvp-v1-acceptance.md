# MVP V1.0 验收矩阵

日期：2026-10-31。目标基线 `c5c396dc51ca3ccb74ed321044dcc76885c9b2f7` 加当前工作树。
源码、迁移、测试均不代表自动发布。**本版本尚未标记 MVP V1.0 全部验收完成。**
最新执行结果见 [执行报告](benchmarks/2026-10-31-generic-room-agents.md) 与
[机器可读回执](benchmarks/2026-10-31-generic-room-agents.json)；本页说明覆盖边界。

本轮完整构建成功；CTest 51 项通过、4 项依赖环境跳过，随后真实 MySQL/Redis
各 2 项补跑通过。11 个 JavaScript 脚本通过。完整受控模式 305 项检查通过；
真实 Pi + 隔离 Hermes Native 模式 310 项通过，两者均包含三个独立浏览器的实际
HTTP/WS 操作。单独的真实 Native T14 通过 13 项检查，原 default Profile 未修改。
这些数字包含重复覆盖，不应相加成独立用例总数，也不等于所有产品目标均已完成。

## 分阶段交付

| 阶段 | 当前实现 | 证据与限制 |
|---|---|---|
| 空间权限 M1 | 私有默认、邀请接受、角色、成员撤销、对象访问校验、SQL 参数绑定 | Space 服务测试、真实 MySQL、完整 HTTP/gRPC/两个 Comet 实验 |
| 通用 Agent M2 | Catalog、多绑定、结构化 mentions、确定性请求、独立原生 Session、旧 CANN 兼容 | C++ 服务测试、Python Router 契约、真实 Pi/Hermes 路径 |
| 任务 M3 | 创建、接手、进度、交接、revision 历史；Runtime 状态/停止/原请求恢复 | MySQL 任务记录与真实 Native；协作 completed 与测试执行成功分开 |
| 隔离 M4 | 公共 Pi 无工具；独立 Hermes 测试 Profile；Catalog/成员服务端校验 | Pi 实际进程参数、Native 实际工具清单、未知恢复与原配置哈希 |
| 消息 M5 | 复用 outbox、ACK、序号、最终持久化、订阅撤销、离线同步 | CTest、真实 Kafka/MySQL/Redis、断线及重投实验 |
| Web M6 | 登录注册、空间/邀请、Agent 安装与选择、流式身份、请求、任务、错误/恢复 | 组件测试与三个独立浏览器上下文；以回执中实际结果为准 |

## T01–T14 对应脚本

| 编号 | 自动化位置 | 类型与验收边界 |
|---|---|---|
| T01 | spaces_e2e.py、room_agents_live_browser_test.cjs | 真 HTTP/SQL/WS；三用户与私有邀请 |
| T02 | generic_room_agent_e2e.py、live browser | 真群聊、客户端去重、三端最终时间线 |
| T03/T04 | generic_room_agent_e2e.py --real-runtimes | 真 Pi 进程/模型与隔离 Hermes API/模型；受控模式另计 |
| T05 | test_generic_room_agent.py、generic E2E | 同房不同 Agent 路由、调度、原生 Session 分离 |
| T06 | generic E2E、Router 契约 | 不同房间会话键及持久请求上下文隔离；不以模型口头否认泄漏为证明 |
| T07 | spaces_e2e.py、generic E2E | 移除后 WS/gRPC/历史/附件/Agent/任务拒绝，两个 Comet 旧订阅撤销 |
| T08 | generic E2E、Router/Native 测试 | 同 ID 重试；真实 Kafka 重投与 Bridge 重启；真实执行调用计数不增加 |
| T09 | generic E2E、room_agent_real_runtime.py | WS 离线补推；Native SSE 中断后的原 Run 恢复分别验证 |
| T10 | generic E2E、Router/Native 测试 | Logic/Bridge 重启和 Native 适配器账本恢复；不声称覆盖所有进程同时硬断电 |
| T11 | room_agent_service_test、generic E2E | 非 owner 安装、Runtime URL/权限伪造、非发起者停止/恢复均拒绝 |
| T12 | spaces_e2e.py 内原 CANN suite | 受控模型，真实 bubblewrap/g++ 工具；不是 CANN LLM/RAG 真实 E2E |
| T13 | logic_business_e2e.py、CTest、JS 回归 | 原单聊/群聊/附件/离线链路；私人 Pi/Hermes 单聊模型真实验收另列未覆盖 |
| T14 | room_agent_real_runtime.py | 真实隔离 Native、todo 工具事件、停止、真实模型错误、unknown 查询、重启/重放不重复 POST |

## 可重复执行

仅在测试拥有的容器与临时目录中执行；脚本使用随机名称/端口并清理自身资源。
Python E2E 需要 Python 3.11、grpcio、grpcio-tools、websockets、PyYAML。
浏览器测试需要在仓库外安装 Playwright/Chromium，并设置模块路径。

```bash
cmake --build build-wsl -j2
ctest --test-dir build-wsl --output-on-failure -j2
python3 tests/run_mysql_review_tests.py build-wsl
python3 tests/run_redis_review_tests.py build-wsl

# 受控模型 + 真 MySQL/Redis/Kafka/HTTP/WS
python tests/generic_room_agent_e2e.py --build build-wsl --browser --out /tmp/spark-room-controlled

# 真 Pi / 隔离 Hermes，仅读已授权模型凭据
python tests/generic_room_agent_e2e.py --build build-wsl --real-runtimes --browser --out /tmp/spark-room-real
python tests/room_agent_real_runtime.py --out /tmp/spark-native-real.json

node tests/room_agents_browser_test.cjs
node tests/spaces_browser_test.cjs
node tests/agent_channel_browser_test.cjs
python3 cannbot/scripts/validate_knowledge.py --strict
python3 cannbot/scripts/test_pi_citations.py
python3 cannbot/scripts/sync_pi_agent.py --check
```

真实测试公共 Pi 默认使用管理员现有模型文件中 DeepSeek/deepseek-flash，可通过
`SPARK_TEST_PI_PROVIDER` / `SPARK_TEST_PI_MODEL` 选择已配置模型。它不修改私人配置。
Hermes fixture 当前支持本机 Windows default 的 Codex 认证格式；其他部署应提供等价的
独立测试 Runtime，不能自动退回 personal technical。测试使用的路径见 fixture 源码，
不在报告写入凭据。真实测试会调用外部模型，普通 CTest 不会触发。

## 未覆盖与下一阶段

- 公共 Pi 禁用工具，Hermes 测试环境只开放 per-Agent 计划工具。本轮没有开放任意
  仓库文件工具或公共代码写入，不能把 todo 验收说成代码分析/修改沙箱已经验收。
- 真实个人 Pi/Hermes 私聊和真实 CANN LLM/RAG 回归未以本轮公共 Runtime 测试替代。
- `status` 的查询结果与最终消息恢复分开；未知执行须用原 request_id 恢复，不能自动
  换新 ID。先确认原 Run 终态再显式恢复；有限重试耗尽后没有无限后台轮询。
  Pi 无原生 unknown 恢复能力时明确保持 unknown，需要人工核对。
- 未测试全套依赖同时硬断电、跨主机网络分区或生产规模；回执只覆盖明确列出的故障。
- `sync_pi_agent.py --check` 允许报告知识包/settings 漂移。本次保留私人配置，不自动
  安装或覆盖 Skills、凭据及会话。

后续在原 Catalog、RoomAgentBinding、原生控制端口和持久任务 API 上扩展；无需第二套
Agent Loop。开放写入工具之前，需独立完成工作区沙箱、审批、真实操作回执与恢复验收。
