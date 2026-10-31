# 授权项目工作区

v0.1.0 第二阶段，2026-10-31。当前已实现并验证服务端配置、撤销与数据库持久化，
快照文件边界、原生读取包装、两成员连续使用同一原生会话和发布决定持久化。
Pi/Hermes 已完成真实模型/工具验证；使用真实 Logic 授权的完整 IM 集成仍在验收；
此文不能作为整版发行通过的证据。
本阶段命令、源文件指纹和真实读取路径见
[2026-10-31 工作区回执](benchmarks/2026-10-31-room-workspaces.json)。

## 配置和权限

管理员通过 `SPARK_PUSH_ROOM_PROJECT_CATALOG` 指定
[`room-projects.json.example`](../cannbot/pi/room-projects.json.example) 格式的目录。
`id/name` 是用户可见标识，`root/paths` 只供可信 Connector 构建快照。
`paths` 明确列出可共享的源码子目录或文件，不能填写空路径、隐藏路径或 `..`。
批准项目表示管理员同意共享这些内容；文件名过滤不能识别源码中的所有硬编码秘密。

| 操作 | owner | admin/member | 非成员 |
|---|---|---|---|
| 查看项目名称和当前选择 | 允许 | 允许 | 拒绝 |
| 选择或清除项目 | 允许 | 拒绝 | 拒绝 |
| 调用已安装的只读 Agent | 允许 | 允许 | 拒绝 |
| 提交任意 root、Runtime URL、工作目录或写入授权 | 拒绝 | 拒绝 | 拒绝 |

API 都由 Logic adapter 调用 `RoomAgentService`；SQL 位于 Repository。

- `POST /api/chatroom/workspace/list`：`{room_id}`，返回项目标识、当前绑定、generation、can_manage。
- `POST /api/chatroom/workspace/set`：`{room_id, project_id, generation}`；空 project_id 清除绑定。
- `POST /internal/room-agent/authorize`：专用 Connector bearer key 与 `{request_id}`，
  从已存请求取得身份并重新检查当前用户状态、成员、Agent 安装和工作区代数。
  不接受 actor、scope 或文件路径作为授权依据。

内部授权密钥通过 `SPARK_PUSH_WORKSPACE_AUTH_KEY` 配置，至少 32 字节；Connector
通过 `SPARK_WORKSPACE_AUTH_ORIGIN` 连接 Logic。默认 Docker 公共入口尚不转发此内部
端点；正式可选 Connector 网络接入仍在本阶段实施中，不应直接暴露全部 Logic 路由。

增量迁移 `20261031_room_workspaces.sql` 增加 `room_workspace` 和
`room_agent_publication`。撤销保留记录并递增
generation；重新绑定不会复用旧授权。修改带客户端已看到的 generation，冲突返回 409。
既有 CANN 任务、请求、工作区和原生 Session 不清理、不迁移。

## 执行与文件边界

每个新请求保存 project_id/workspace_generation。执行 namespace 和持久路由同时区分
房间、Agent、Agent 安装代数和工作区代数；切换项目不复用旧项目的 Runtime Session。
旧请求缺少这些字段时只保留原来无文件权限的会话。

Connector 使用 Linux 目录描述符与 `O_NOFOLLOW` 读取批准源路径，拒绝 symlink、
hardlink、特殊文件和复制期间变化的文件；跳过隐藏目录、凭据文件名与非源码后缀。
单文件上限 1 MiB，总计 32 MiB、4096 个文件，遍历上限 16384 项。超限失败，
不把部分结果当作完整快照。整个可变仓库不承诺事务级快照一致性。

每个 request_id 对应不可变快照和 manifest，记录相对路径、内容 SHA-256 和 revision。
同请求重放加载原快照，不重新覆盖。Agent 只能读取 manifest 中的内容，不能选择任意
宿主机路径；每次真实读取前后都重新授权。

Pi 有工作区时使用 `--no-builtin-tools --no-extensions --no-skills`（无工作区仍为
`--no-tools`），显式加载唯一受信扩展。扩展成功加载
后才注册 Pi 原生 `read` 的受限包装；加载失败时没有可退回的无保护读取工具。

Hermes 公共集成保留原 Runs API、AgentLoop、read_file_tool、工具事件与恢复账本。
独立 Profile 只启用 `spark_workspace` 和 `todo`，并设置
`tools.tool_search.enabled: off`，让工具清单直接可核验。可信 request_id 经 HTTP header
传入；canonical scope 由 Logic 返回，通过每次执行的 ContextVar 传到原生执行线程。
缺失上下文默认拒绝。Hermes 仅加载 Connector 已构建的快照，不读取私人 source root。
串行和并行工具路径都必须经过受限 handler，不能依赖可能 fail-open 的前置 hook。
Native Session 和幂等键还需匹配 canonical scope 推导的值，不能将另一房间的
Session 与当前 request_id 混用。Windows 原生 read_file_tool 内部依赖 Git Bash，
测试夹具显式选择 Git Bash；不会向模型开放 shell 工具。

快照仓只允许可信 Connector 写入，Native 部署需将其只读挂载。路径/摘要检查不能
防御拥有快照仓写权限的恶意宿主机进程。快照按请求保留，目前不自动清理；运维必须
计入磁盘容量，不能删除 unknown 请求恢复所需的快照。

## 验收与尚未闭环

- 服务端：owner 选择、越权/额外字段拒绝、CAS、清除/重绑、移除成员和禁用账号后拒绝。
- 文件系统：12 项契约测试通过，包括真实文件读取、目录/文件链接、hardlink、FIFO、
  stat/open 间替换、内容篡改、限额与并发重复快照。
- Pi：使用安装的原生读取函数验证批准字节、读取前后授权和越界/篡改拒绝。
- Router：新工作区/安装代数不复用上下文，旧请求保持原 namespace。
- 真实 Runtime：Pi/Hermes 都读取了随机校验值和实际 `logic/message_reservation.cpp`，
  返回具体函数与审查结果，并产生原生工具事件。第二名成员的新请求恢复原生上下文；
  Hermes 完成请求重放不再次调用工具。授权 HTTP 是契约夹具，不能称为 IM E2E。
  原生工具回执还逐条验证相对路径、成功返回和 `Reserve` 实现文本，不仅检查回答中的文件名。
- 发布：MySQL 事务验证并发双发布、提交决定后重建 Repository、撤销后重投保持原文、
  语义冲突拒绝、未发布结果取消。首次发布与撤销同时竞争尚待故障集成补证。
- 待完成：Native 跨房间并发、执行中撤销和重启恢复的真实链路、真实 Logic 授权 E2E、
  可选 Connector 部署、正式公共 Hermes 启动入口、Web 工作区选择与最终三浏览器验收。

撤销不能收回已经公开的历史或模型已经读到的数据。首次最终回复先在 MySQL 中锁定
房间、账号和请求，检查当前成员/Agent/工作区授权，持久保存允许或取消决定；提交
决定是授权的生效边界。随后仍由原预约、outbox、Kafka、Complete 链路处理。
数据库故障会重试，不会被伪装为取消成功。决定提交后崩溃，重投取回完全相同的回复；
已获准的消息可能在后续撤销之后才送达，但不会被重写成另一个最终结果。

摘要同时覆盖消息和结果的持久投影，只忽略已有协议允许变化的完成时间。
project_snapshot 仅保存项目标识、SHA-256 revision 和文件数；通用只读 Agent 不能
提交旧 CANN 的写入/测试 checkpoint。旧 CANN 和工作区功能前已存请求保持原路径。
