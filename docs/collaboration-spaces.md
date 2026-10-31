# 协作空间：第一阶段

2026-10-31。沿用聊天室 `im_group`、正式成员 `group_member` 和 `r_<room_id>` 会话。
新增空间默认 private；旧房间升级后仍为 public。现有 `room_agent_task`、checkpoint、
request、消息序号、reservation/outbox、工作目录和 Agent Session 不迁移、不清空。

## 源码审查与分层

此前 `group_member` 同时承担订阅和部分读取授权；WS presence 只存 Redis/Comet，却能先
加入本地快照。消息发送的成员检查依赖 Room Agent，上传者本人读取附件跳过会话检查，
已读/未读没有会话权限检查。`CreateChatroom` 拼接房间名，`AddOrUpdateMember` 实际只插入。

当前 HTTP/gRPC 只做认证凭据提取、DTO 转换和响应序列化。`RoomService` 管理空间/邀请/角色
规则；`SpaceRepository::WithRoom` 在同一连接、同一事务、同一房间行锁内执行这些规则，
数据库失败回滚。Repository 负责参数绑定与持久化。房间创建、owner 成员和会话原子提交。
所有成员变更锁房间行，因此接受邀请、移除、角色调整不会相互穿透。

`GroupMemberDao` 是历史、消息、同步和下推资格查询的权威存储，角色只接受 0/1/2。
`RoomAgentRepository` 同样读取该成员表；Agent 的开发者列表不构成独立入会授权。
`AddOrUpdateMember` 现在真正更新角色，但公开加入和接受邀请只为不存在的成员写默认角色，
重复加入不会降级已有 owner/admin。

## 权限

| 操作 | owner | admin | member | 非成员 |
|---|---|---|---|---|
| 讨论、历史、同步、附件、成员/Agent 任务查看 | 是 | 是 | 是 | 否 |
| 邀请已有活跃用户 | 是 | 是 | 否 | 否 |
| 移除普通成员/撤回未接受邀请 | 是 | 是 | 否 | 否 |
| 移除 admin | 是 | 否 | 否 | 否 |
| 授予/撤销 admin | 是 | 否 | 否 | 否 |
| 修改名称、描述、可见性 | 是 | 否 | 否 | 否 |
| 配置共享 Agent 开发成员 | 是 | 是 | 否 | 否 |
| 正式退出 | 暂不支持 | 是 | 是 | 幂等 |

owner 不能被移除、降级或退出；所有权转让尚未实现。共享任务 checkpoint 仍要求 owner 或
显式 developer，admin 身份本身不会授予执行代码的权限。现有私人 Agent owner ACL 不变。

邀请独立于成员表：`pending → accepted` 才入会。只允许被邀请用户本人接受；再次接受不会
改角色。移除/撤回写 `revoked`，旧接受请求不能恢复成员；公开空间被管理员移除者也不能
立即自行加入。重新邀请后可以重新接受。主动退出记 `left`，公开空间可以重新主动加入，
私有空间必须重新邀请。第一阶段邀请没有自动到期；owner/admin 用 remove 撤回待接受邀请。

## 数据库升级

先备份业务库，在维护窗口执行（对目标数据库运行，不执行 reset_dev.sql）：

```bash
mysql --login-path=spark-admin spark_push < sql/migrations/20261031_collaboration_spaces.sql
```

`--login-path` 是运维预先配置的 MySQL 凭据配置名，按部署环境替换。脚本会：

1. 为旧 `im_group` 增加 description/visibility；旧行设 public，此后默认 private。
2. 创建 `room_invitation`（状态及邀请人，主键 room_id/user_id）。
3. 补齐旧 owner 成员，修正非 owner 的错误 owner 角色，补齐缺失房间会话。

脚本可重复运行，不删除旧成员、消息或 Agent 数据。新空库直接使用 `sql/schema.sql`。
Logic 启动检查字段/表，缺迁移会拒绝启动。同步升级 Logic、Job、Comet；旧 Comet 没有下推
成员复查，不能与新的私有空间安全语义混用。迁移后不要回退到缺乏权限检查的旧二进制。

所有带用户输入的房间 SQL（含 Agent JSON、request_id）使用预处理参数。事务控制语句是
固定 SQL；不依赖字符串转义或 MySQL 的反斜杠模式。

## HTTP API

均为 POST，使用 `Authorization: Bearer <token>`。actor 只从凭据获取；JSON 中的 user_id
在 invite/role/remove 中是目标用户，不能指定创建者或替别人接受邀请。响应保持既有
`{code,message,data}`，客户端必须检查业务 code；409 等仍采用现有 HTTP 200 外层约定。

| 路径 `/api/spaces/…` | 请求字段 | 返回 data |
|---|---|---|
| create | name, description（可选）, visibility（默认 private） | room_id, session_id |
| list | offset=0, limit=100（1..200） | spaces：room_id/session_id/name/description/visibility/owner_id/role |
| update | room_id, name, description, visibility | 空对象 |
| members | room_id | members：user_id/account/name/role |
| invite | room_id, user_id | 空对象 |
| invitations | 无 | 当前用户至多 200 条待接受邀请及空间信息 |
| accept | room_id | 空对象 |
| role | room_id, user_id, role=`member` 或 `admin` | 空对象 |
| remove | room_id, user_id | 空对象（亦可撤回 pending 邀请） |
| leave | room_id | 空对象 |

名称 1..128 UTF-8 字节，描述至多 2048 字节；不接受 NUL；visibility 只允许 public/private。
`/api/chatroom/join` 保留公开加入；已是成员时幂等。旧 `/api/chatroom/leave` 仍是页面离开
空操作，不能删成员。`/api/chatroom/unsubscribe` 现在走正式退出规则；admin create 仍创建
公开聊天室，并自动建立 owner。`/api/chatroom/list` 仍返回用户正式成员空间，旧客户端可用。

`chatroom.html` 提供纯原生 JS 空间管理面板：创建、我的空间、邀请收件箱、接受、成员角色、
移除和正式退出。显示字段使用 textContent；发生 room_revoked 清除当前消息/Agent 面板。

## 在线权限与可靠性边界

HTTP、gRPC 发送、历史/增量/离线同步、已读/未读、任务查看和房间附件读取使用当前成员关系。
房间附件上传者退出后同样无权读取。DB/授权 RPC 出错时拒绝房间访问，不回退到 presence。

Comet 加入先调用内部 `CheckRoomAccess`，允许后才登记连接及 ACK。每条连接单独登记房间，
一个标签页关闭不会移除其他标签页的订阅。连接断开只清在线状态，不删正式成员。
定向下推、房间广播、room-scoped 系统广播、ai_delta、离线消息在发送前复查接收人资格；
主动同步帧也复查。每秒扫描现有在线订阅，撤销后清本地成员和 Redis 在线计数，并发送
`room_revoked`。扫描间隔之外还有 RPC/当前扫描耗时，不是严格一秒 SLA。

新请求在数据库撤销提交后立即失去资格。已完成授权的在途操作可能结束；撤销已被 Comet
观察到时，本地代数检查会丢弃已排入 IO 队列的旧帧。此前交给 socket 的字节不可追回。
这不是跨 Logic/Comet 的同步撤销屏障；大房间逐次查询及周期扫描的容量仍需后续压测。
授权故障下拒绝的投递不推进 delivered 游标，Comet 向 Job 返回 503，释放当前去重占位以允许原 ID 重试。
处理中和已完成的去重状态分离；并发兜底请求遇到尚未完成的请求也回 503，不能提前确认 Kafka。
已有持久消息还可以由原有 gap/offline sync 补齐。

`accepted_ack` 仍表示 Kafka 确认持久化事件，`delivered_ack` 仍表示目标连接接受下推。
`msg_seq/client_msg_id`、reservation/outbox、safe_seq 保持原语义。已 accepted 的共享任务可
为剩余成员完成，成员退出不删除任务/工作区；移除者不能发起新任务、查询任务或接收回复。
第一阶段不增加运行中 Agent 工具强制取消、通用多 Agent 路由或私人 Hermes 公共共享。

## 验证与下一阶段接口

```bash
cmake --build build-wsl -j2
ctest --test-dir build-wsl --output-on-failure -j2
python3 tests/run_mysql_review_tests.py build-wsl
python3 tests/run_redis_review_tests.py build-wsl
python3 tests/run_kafka_volume_test.py build-wsl
# Python >=3.11，grpcio/grpcio-tools/websockets；Docker 中独立临时依赖，自动清理。
python3.11 tests/spaces_e2e.py --build build-wsl --out /tmp/spaces-e2e
# 需要 Playwright 和已安装 Chromium；不访问真实账户。
node tests/spaces_browser_test.cjs
node tests/room_workspace_test.cjs
```

`spaces_e2e.py` 包含旧库迁移重复执行、双 Comet、邀请/角色、WS/gRPC 权限、附件上传者撤销、
排队旧 targets 过滤、ACK/序号、断线离线恢复，并运行现有受控 @CANN 协作测试。受控模型
不等于真实模型/CANN 硬件验收。具体结果见本次交付回执，未执行项目不推断通过。

后续可复用 SpaceRepository 事务边界、成员角色、邀请状态机、内部批量授权 RPC 和稳定
room/session/task ID，增加所有权转让、邀请过期/分页、管理审计、持久化撤销世代及控制
outbox、同步撤销屏障、工具执行前授权复查和面向大空间的权限查询优化。
