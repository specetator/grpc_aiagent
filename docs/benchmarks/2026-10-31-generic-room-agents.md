# 通用 Room Agent 阶段验收报告 · 2026-10-31

已实现范围的构建与测试门禁通过；**MVP V1.0 整体验收仍未完成**。
本报告对应 `c5c396dc51ca3ccb74ed321044dcc76885c9b2f7` 加未提交工作树。
不以 HEAD 单独标识本轮源码；源文件指纹为 `6504999b09ad6e2da6247b279c8bfe1de5785d85d7cd58155163a47517a4aee4`。
[JSON 回执](2026-10-31-generic-room-agents.json) 包含每个输入文件的 SHA256、完整检查名称和当前任务变更清单。
原有未提交修改、私人配置、凭据、工作区及 Session 保留；没有提交、发布或共享私人 Profile。

## 源码审查与阶段结果

| 阶段 | 审查起点与最终实现 | 本轮证据 / 下一阶段依赖 |
|---|---|---|
| 空间权限 | 复用已有正式成员、邀请、角色与撤权；Redis 订阅/在线状态不授予权限 | 真 SQL/HTTP/gRPC/双 Comet 撤权、附件、SQL 特殊字符、旧公开房间通过；权限基础可供 Agent 使用 |
| 通用 Agent | 原 CANN 单一专用路径扩展为管理员 Catalog、owner 安装、多 ID mentions；独立原生会话 | 双 Agent/双房间隔离、同房多人、稳定请求、真实 Pi/Hermes 通过；公共工具仍受限 |
| 任务协作 | 新增持久任务、接手、进度、交接、revision 历史；复用原请求与 Runtime 控制 | 真 SQL 三次 revision、重登恢复、Native stop/status/recover 通过；未验收通用仓库文件工具 |
| Web | 原聊天室增加空间、邀请、成员、Agent 选择/身份/流式状态和任务操作 | 三独立浏览器真实 HTTP/WS 通过；无需手输 RoomID；工具执行 UI 依据真实事件 |
| 集成验收 | 复用原 ACK/outbox/最终消息/离线链路，补身份伪造、结果重放和恢复回归 | 下表实测通过；真实私人单聊、CANN 模型与新增工具沙箱仍需独立验收 |

最小设计、权限矩阵、API、迁移顺序见 [通用 Agent 设计](../generic-room-agents.md)；
T01–T14 和重跑命令见 [验收矩阵](../mvp-v1-acceptance.md)。数据库仅使用增量迁移，
保留 `room_agent_task`、旧消息、CANN 工作区与 Session。

## 执行证据

| 验证 | 结果 | 模式与边界 |
|---|---|---|
| 完整 C++ 构建 | 通过 | 最终构建 `build16`，全部目标成功 |
| 完整 CTest | 51 通过，4 跳过，0 失败 | 共 55 注册；环境依赖跳过不能记作通过 |
| MySQL / Redis 补跑 | 各 2/2 通过 | 真实临时容器，覆盖上述全部 4 个跳过项 |
| JavaScript | 11 个脚本通过 | 组件/浏览器、游标、旧 CANN 工作区、私聊控制、子 Agent SDK；最终 UI 改动另做专项回归 |
| 完整受控栈 | 305 项检查通过 | 真 MySQL/Redis/Kafka/HTTP/gRPC/WS，受控模型，三独立浏览器 |
| 完整真实 Agent 栈 | 310 项检查通过 | 真 Pi 进程/模型、隔离 Windows Hermes Native API/模型、三独立浏览器 |
| 独立真实 Native T14 | 13 项通过，4 次 POST | 实际 todo 事件、stop、失败、unknown、重放与适配器重启；原 Profile 哈希不变 |
| 知识校验 / 引用 | 86 个来源通过 / 通过 | 包含本报告登记 |
| 已安装知识包同步检查 | **退出 1，未通过** | 发现知识包和 settings 漂移，保留私人安装未覆盖 |

305/310 各自已包含旧业务 93 项以及 CANN 受控模型、真实 bubblewrap/g++ 的 7 次执行；
两套结果有重叠，不能相加成独立用例总数。真实栈的 CANN 部分仍使用受控模型。
三个浏览器使用独立存储、真实注册鉴权、邀请接受、并发群聊、Agent 回复、相同最终时间线、
任务接手和退出重登；测试只映射临时栈端口，没有关闭浏览器安全检查或跳过后端权限。

关键故障证据：Native 实际停止后，`cancelled` 在共享消息与历史中保留；SSE 中断后
查询原 Run 终态，再显式恢复原 request_id，得到持久 final 且只有一次 Native POST。
实际 Kafka 重投与 Bridge 重启不增加 Pi chat / Native POST 次数，首份 SQL 结果保持不变。
Logic 重启后重新登录，任务、交接和最终消息恢复。`ai_delta` 不推进持久游标。

真实模型测试仅使用用户授权的 Windows default 模型认证，启动临时隔离 Profile；
不复制私人聊天、记忆、Skills 或工作区，不刷新原账户。测试前后检查原配置与认证哈希。
原始服务日志仅保留在本机临时目录，不纳入可分享回执。

独立源码 Reviewer 第三轮 PASS；Tester 对已实现范围的证据门禁 PASS，均明确不等于
MVP 完成。子代理未执行测试，实际构建、运行与回执核验由主代理完成。

## 未完成事项与下一阶段接口

- 通用公共 Pi 当前无工具；Hermes 仅开放独立 todo。未接入经验证的仓库文件分析或
  代码修改沙箱，不能宣称已完成通用只读源码分析和授权写入体验。
- 私人 Pi/Hermes 真实模型私聊、CANN 真实 LLM/RAG 回归未跑，不能以公共测试替代。
- 未覆盖全栈同时硬断电、跨主机网络分区或生产规模；已通过的是上述明确定义的故障。
- Native 有限重试耗尽后需要查询原 Run 再显式恢复；无无限后台轮询。Pi 不支持的
  unknown 恢复保持 unknown，不自动重执行。公共审批和不支持的控制明确返回不可用。
- 已安装知识包/settings 漂移待单独处理；本轮不覆盖私人配置。

下一阶段继续使用 Catalog / Binding generation、持久任务与 revision、原生控制端口和
AgentEvent 工具回执。通用文件分析需要管理员限定的只读仓库挂载与真实隔离验收；写入
需要额外审批和真实执行恢复测试。无需新 Agent Loop、数据库或自主协商工作流。

## 当前任务修改文件

下列 57 个文件相对通用 Agent 开始前的工作树快照变化，另新增本报告的 Markdown/JSON。
此前空间阶段及用户已有改动不冒充本轮变更；完整工作树的 Git diff 会更大。

- `README.md` (modified)
- `cannbot/knowledge-sources.json` (modified)
- `cannbot/pi/room-agents.json.example` (added)
- `cannbot/scripts/agent_router.py` (modified)
- `cannbot/scripts/hermes_adapter.py` (modified)
- `cannbot/scripts/hermes_native_adapter.py` (modified)
- `cannbot/scripts/pi_gateway.py` (modified)
- `cannbot/scripts/room_agent.py` (modified)
- `cannbot/scripts/test_generic_room_agent.py` (added)
- `cannbot/scripts/test_hermes_native.py` (modified)
- `cannbot/scripts/windows_agent_http.py` (modified)
- `cannbot/scripts/windows_agent_transport.py` (modified)
- `cannbot/skills/spark-push-knowledge/references/architecture-map.md` (modified)
- `common/message_payload.h` (modified)
- `docs/generic-room-agents.md` (added)
- `docs/hermes-native-runtime-integration.md` (modified)
- `docs/mvp-v1-acceptance.md` (added)
- `docs/room-agent-collaboration.md` (modified)
- `hermes_bridge/hermes_client.cpp` (modified)
- `hermes_bridge/hermes_client.h` (modified)
- `hermes_bridge/main.cpp` (modified)
- `hermes_bridge/sse_parser.cpp` (modified)
- `logic/CMakeLists.txt` (modified)
- `logic/hermes_reply.cpp` (modified)
- `logic/http_server.cpp` (modified)
- `logic/main.cpp` (modified)
- `logic/repositories/room_agent_repository.cpp` (modified)
- `logic/repositories/room_agent_repository.h` (modified)
- `logic/services/agent_connection_service.cpp` (modified)
- `logic/services/agent_connection_service.h` (modified)
- `logic/services/message_service.cpp` (modified)
- `logic/services/message_service.h` (modified)
- `logic/services/room_agent_catalog.cpp` (added)
- `logic/services/room_agent_service.cpp` (modified)
- `logic/services/room_agent_service.h` (modified)
- `logic/services/room_task_service.cpp` (added)
- `sql/migrations/20261031_generic_room_agents.sql` (added)
- `sql/schema.sql` (modified)
- `tests/CMakeLists.txt` (modified)
- `tests/agent_channel_browser_test.cjs` (modified)
- `tests/generic_room_agent_e2e.py` (added)
- `tests/hermes_reply_test.cpp` (modified)
- `tests/hermes_room_runtime_fixture.py` (added)
- `tests/hermes_sse_parser_test.cpp` (modified)
- `tests/http_api_integration_test.py` (modified)
- `tests/internal_rpc_test.cpp` (modified)
- `tests/logic_business_e2e.py` (modified)
- `tests/room_agent_real_runtime.py` (added)
- `tests/room_agent_service_test.cpp` (modified)
- `tests/room_agents_browser_test.cjs` (added)
- `tests/room_agents_live_browser_test.cjs` (added)
- `tests/space_mysql_integration_test.cpp` (modified)
- `tests/spaces_browser_test.cjs` (modified)
- `tests/spaces_e2e.py` (modified)
- `web_demo/static/agent_channel.js` (modified)
- `web_demo/static/chatroom.html` (modified)
- `web_demo/static/room_agents.js` (added)
