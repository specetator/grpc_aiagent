# 多人共享 CANN Agent 与算子任务交接

2026-10-31 增量扩展见 [通用 Room Agent](generic-room-agents.md) 与 [当前验收矩阵](mvp-v1-acceptance.md)。以下日期对应原阶段证据，不替代新报告。

实现与验证日期：2026-10-11。适用根目录当前主线，不适用教学 subset。

产品名称现为 Kernel Agent。房间内子 Agent 仅提供只读研究与审查，不能借委派获得写入或 runner 权限；
授权主 Agent 继续负责修改与测试。子任务生命周期和限制见 [Kernel Agent](kernel-agent.md)。

## 1. 能力与边界

一个聊天室绑定一个不可静默改绑的算子任务、一个共享 Pi 会话和一个独立工作区。
成员用 `@CANN 问题` 触发已有公共 Pi/CANN Agent，普通讨论不触发模型。
不是让几个人共用某个人的私聊身份；每轮仍记录真实发起者。
私人 Hermes technical 的所有者检查不变，不能作为公共房间 Agent。

已实现：共享上下文、按成员广播流式进度与最终消息、开发权限、持久任务摘要、CAS 版本更新、
请求与结果账本、不可变源码检查点、固定测试执行器回执、重连历史恢复。
房主绑定任务和授予开发权限；普通成员可提问/读代码，但不能写文件或执行测试。
取消、重置共享会话、换模型、重启 Agent 等房间控制命令暂不开放。

当前是单 Logic + 单 Gateway 的协作实现，不是多租户集群或完整 OS 沙箱。工作区文件工具
有路径/符号链接/隐藏路径拦截，测试子进程另在 bubblewrap 中隔离；Pi 本身仍持有部署账户的
模型凭据和 CANN 只读知识工具。普通聊天室依旧可自行订阅，因此不能把“加入房间”理解为
获得写权限。保密项目还需邀请制、独立运行账户及操作系统级隔离。

## 2. 三层实现与源码入口

```text
HTTP/WS → Comet → gRPC adapter → MessageService
                                   ↓
                     RoomAgentService / OperatorTaskService
                                   ↓
                        RoomAgentRepository → MySQL

MessageService → Kafka ai_request → Bridge HTTP/SSE → Gateway / AgentRouter
                                                      ↓
                                           RoomPiFactory / CANN RAG
                                                      ↓
                                        独立工作区 + 固定测试执行器
```

| 关注点 | 当前源码 |
|---|---|
| 房间绑定、权限、触发、不可变请求 | [`room_agent_service.cpp`](../logic/services/room_agent_service.cpp) 的 RoomAgentService |
| 摘要、待办、状态与版本冲突 | 同文件的 OperatorTaskService::Checkpoint |
| Repository 接口与事务持久化 | [`room_agent_service.h`](../logic/services/room_agent_service.h)、[`room_agent_repository.cpp`](../logic/repositories/room_agent_repository.cpp) |
| HTTP DTO 适配与依赖组装 | [`http_server.cpp`](../logic/http_server.cpp)、[`main.cpp`](../logic/main.cpp) |
| 房间请求与回复编排 | [`message_service.cpp`](../logic/services/message_service.cpp)、[`hermes_reply.cpp`](../logic/hermes_reply.cpp) |
| 请求级真实身份 | [`spark_push.proto`](../proto/spark_push.proto) 的 auth_token、[`comet_server.cpp`](../comet/comet_server.cpp) |
| 共享路由、Actor、turn 幂等 | [`agent_router.py`](../cannbot/scripts/agent_router.py) |
| FIFO 与单会话执行互斥 | [`agent_scheduler.py`](../cannbot/scripts/agent_scheduler.py) |
| Pi 工作区、版本副本、测试隔离 | [`room_agent.py`](../cannbot/scripts/room_agent.py) |
| 文件权限及同一轮写/测试互斥 | [`room-workspace.ts`](../cannbot/pi/extensions/room-workspace.ts) |
| 房间任务面板、流式预览 | [`chatroom.html`](../web_demo/static/chatroom.html)、[`room_agent_channel.js`](../web_demo/static/room_agent_channel.js) |

HTTP/gRPC 不直接操作 SQL、Redis、Kafka；共享规则在业务服务。Bot 不需要以可登录账号
加入聊天室，最终消息仍使用已有无密码机器人身份。

## 3. 两种身份、两种进度

- 消息会话：`r_<room_id>`，沿用房间 msg_seq、历史、游标。
- Agent 原生会话：`room_<room_id>_<task_id>`；用户更换不改变它。
- Actor：本次凭证解析出的用户。房间上行 RPC 新增传输字段 auth_token，Comet 从已鉴权
  连接注入；Logic 每次复核并拒绝伪造 from_user_id。该 token 不写入 Kafka、历史或提示词。
- 工具能力：由 Logic 按房主/开发授权构造 can_develop；Gateway 只接受可信 Bridge Bearer
  下的 scope，不从群聊正文推断权限。
- 人工交接：任务 summary、next_steps、status。修改必须携带最新 revision，防止并发覆盖。
- 执行证据：request_id、actor、最终结果、code_revision、源码文件 hash、测试回执。
  任务 status=done 或 Agent 说“通过”，都不是测试证据。

MySQL 的 room_agent_task 保存最新任务，room_agent_checkpoint 保存历次人工版本，
room_agent_request 保存接受的 AI 输入和最终回执。Configure/Checkpoint 的版本修改与
历史记录同一事务提交。SQL 使用连接字符集对应的转义，不拼接未转义的用户文本。
初始化 SQL 与 [`增量迁移`](../sql/migrations/20261011_room_agent.sql) 都只 CREATE IF NOT EXISTS；
Logic 启动也会确认这三张表，不删除既有会话。

## 4. 顺序、幂等与恢复

单 Logic 的房间提交锁覆盖序号分配、持久化确认、AI 请求入队，按同一 Kafka 会话 key
保持输入顺序；Gateway 调度器进一步限制同会话只执行一个 turn，并给等待者分配 FIFO ticket。
排队者超时只能移除自己的 ticket，不能释放其他 turn 的执行资格。

提示词标注不同发言者，合并 MySQL 历史与最近消息缓存，只包含触发消息之前的讨论。
最多取最近 20 条背景消息，按 UTF-8 边界裁剪；当前问题单独保留。Native Pi 会话和人工
交接记录继续提供长期上下文，不能理解为永远完整保留每个 token。

AI 请求先保存固定输入快照再发送 Kafka，重复 client_msg_id/request_id 使用同一快照，
不会因后续摘要改变而重复调用工具。重复已完成 turn 回放原结果；重启时未确认的执行标记
unknown，不自动重跑副作用。Kafka 入队失败与输入持久化之间仍有窗口：accepted_ack 只
确认人类消息 durable，不承诺 AI 已执行；需原请求重试/人工核对，不是事务 outbox 或 exactly-once。

成员掉线不取消房间任务。ai_delta 仍是临时预览，不落消息历史、不推进游标；最终 ai_reply
经 persist_message 进入房间历史、push_group、Job/Comet。投递明确枚举数据库中的成员，
不依赖可自行报告的 WS 在线名单。取消订阅后不能查询任务或历史；已获得的数据无法追溯收回。
权限撤销影响后续接收/新请求，不声称能撤销已经开始的工具副作用。

## 5. 工作区与测试回执

管理员配置的根目录下，workspaces/<room_id>_<task_id> 保存当前源码，sessions 保存 Pi
会话，checkpoints/<namespace>/<code_revision> 保存不可变源码副本，receipts 保存实际测试记录。
相同代码版本复用一个副本；每版本上限 512 个非隐藏文件、16 MiB。没有自动归档/总量回收，
长期部署必须另加存储配额和保留策略。源码不靠 LLM 摘要恢复；MySQL、SQLite、工作区需一起备份。

房间 worker 显式加载 CANN RAG 和工作区 guard，关闭自动扩展/Skill/模板发现，不复用私人
工作目录或 shell 工具。读成员只启用 read 与四个 CANN 知识工具；开发成员再启用 write、edit、
room_run_tests。路径越界、隐藏路径、符号链接均拒绝，同一 turn 的并行写/测试也互斥。

测试工具无命令/路径参数。仅执行管理员配置的 Python runner，放进 bubblewrap 的 PID、
挂载、网络等 namespace；只暴露系统运行库、任务工作区和只读 runner，不挂载 home/模型凭据。
缺少 sandbox/runner 时明确失败，绝不回退为不隔离执行。超时、非零退出或源码在测试过程中
变化都不能标为 passed。回执包含 run_id、前后代码 revision、runner hash、退出码及有界日志。

本次真实执行器验证的是 Linux 主机 C++ Add 示例，**不是 Ascend C 算子的 NPU 精度/性能测试**。
当前没有实现昇腾设备透传或远程 NPU 执行服务；实际 CANN 编译/设备验证需另接对应执行环境。
CANN 知识检索已实测，但不能把知识回答当作硬件验证。

## 6. 启用与使用

已有 Pi、Bridge、IM 依赖配置保持不变，新增以下部署环境（路径由管理员设置，不由用户提交）：

```dotenv
SPARK_PUSH_ROOM_AGENT_ENABLED=true
SPARK_PUSH_ROOM_WORKSPACE_ROOT=/home/peco/.local/share/spark-push-rooms
SPARK_PUSH_ROOM_TEST_RUNNER=/absolute/admin-owned/runner.py
```

测试执行器可留空，届时能协作问答/修改源码，但不能宣称运行测试成功。安装 Linux 依赖
`bubblewrap util-linux g++`，根目录使用部署账户独享的 0700 目录。公共 Pi 的 bot_user_id 必须与
Logic 主机器人一致，owner_user_ids 必须为空；私人 technical 不可用。
启动脚本读取 .env.local 后，重启 Logic、Comet、Bridge、Kernel Agent gateway，使新协议和环境生效。
Gateway 的模型/provider 要选择已配置且实际可用的入口。本次真实模型测试使用本机当前默认
deepseek/deepseek-flash，没有修改个人模型或凭据配置。

网页 chatroom.html 的“CANN 协作任务”面板支持房主创建任务/授权、刷新进度、开发成员
保存交接摘要。普通讨论照常发送，向共享 Agent 提问使用 `@CANN ...`。
新 HTTP 接口都要求 Bearer，业务错误查看 JSON code：

| POST 路径 | 请求要点 |
|---|---|
| /api/chatroom/agent/configure | room_id；首次 title/spec/developer_user_ids；更新授权需 revision，仅房主 |
| /api/chatroom/agent/task | room_id，返回 task、最近 20 个 request/result、can_develop |
| /api/chatroom/agent/checkpoint | room_id/revision/patch；patch 仅 summary/next_steps/status |

旧 gRPC 房间发送者须提供新增 auth_token；普通 WS 客户端不需提交它，由新版 Comet 注入。
当前任务面板首版在 Web；Android 未新增任务面板，不能宣称全端 UI 已完成。

## 7. 验证与复现

构建与 CTest 29/29 通过（含独立 Redis 6381），C++ 房间业务 36 条检查，Python 房间
与 sandbox 10 个测试。工作区 guard 的 Pi TypeScript loader 实测和房间 UI DOM 合同测试通过。
真实 HTTP/gRPC/WS 原有业务回归 88 条检查通过。
独立 MySQL 3308 / Redis 6381 DB11 / Kafka 29092 下，受控模型 E2E 44 条检查通过，
真实 Pi 完整聊天室 E2E 16 条检查通过。结果见
[`2026-10-11 结构化证据`](benchmarks/2026-10-11-room-agent/)。

```bash
cmake --build build-wsl -j2
SPARK_PUSH_RUN_REDIS_TESTS=1 SPARK_PUSH_REDIS_PORT=6381 \
  ctest --test-dir build-wsl --output-on-failure
python3 cannbot/scripts/test_room_agent.py
node tests/room_agent_channel_test.cjs
node tests/room_workspace_test.cjs # 需要本机安装 Pi 的 TS loader
# 需 websockets/grpcio/grpcio-tools，且只使用专用依赖库：
python3 tests/room_agent_e2e.py --secret-file /private/benchmark-secret.json --out /private/results
python3 tests/room_agent_e2e.py --live-pi --secret-file /private/benchmark-secret.json --out /private/live-results
```

审查/真实测试修复过三类问题：排队超时误释放执行锁；Pi --tools 白名单遗漏自定义 CANN/
测试工具；裸 RPC 可伪造房间发起者。另增加同一 turn 写/测试互斥和代码版本变化检测。
测试日志与 secret 留在本机，仅结构化无凭据报告上传 GitHub。这是功能验收，不是吞吐压测，
不把本次运行时长转换为 QPS、生产容量或多人开发效率提升百分比。
