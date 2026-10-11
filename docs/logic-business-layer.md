# Logic 三层架构与业务规则导航

适用范围：根目录 `logic/` 当前实现，2026-10-09 重构；不包含教学 subset。
本次是维护性重构，不是新协议、不新增部署进程，也不是吞吐优化实验。

## 1. 三层分别负责什么

这里的三层是 **协议适配层 → 业务服务层 → 数据访问层**，不是系统的
Comet / Logic / Job 三进程。它们是两种不同的拆分维度。

```text
Logic main.cpp：集中创建依赖，决定实例和生命周期
  ├─ HTTP adapter：HttpApiServer
  ├─ gRPC adapter：LogicServiceImpl（Unary / MessageStream）
  └─ Kafka reply/delta consumer
         │ 调用共享 Service，而不是入口之间互相调用
         ▼
  logic/services/：业务规则、用例编排、普通 C++ 输入输出
         │
         ├─ User / Conversation / Attachment Repository 接口
         │       └─ repositories/dao_repositories.cpp：生产适配器
         └─ 其他服务直接复用现有 DAO / Store / Redis / Kafka 封装
                 ▼
  *_dao.cpp / conversation_store.cpp / redis_store.cpp / common/
                 ▼
  MySQL / Redis / Kafka / 附件文件
```

适配层允许保留：路由、HTTP method、JSON 字段类型解析、Bearer 提取、Protobuf ↔ DTO、
响应序列化、CORS、gRPC 流读写与 request_id 对应。不能在这里新增 SQL 查询、Redis key 操作、
Kafka 生产或复制业务权限判断。

Service 决定：账号状态、会话可见性、附件归属、分页边界、ACK 条件、订阅与在线状态的区别、
管理员权限和用例执行顺序。DAO 决定：如何执行 SQL、读写文件、更新 Redis，以及返回存储结果。
`ConversationStore` 仍保留已有序号/消息存储组合能力，没有为了分层把成熟热路径重新实现一遍。

这是经典三层和核心 Repository 测试接口，不是全项目 Clean Architecture：部分 Service
实现仍直接依赖具体数据组件，Kafka 序列化仍在消息服务实现中使用 Protobuf；公共业务头文件
不暴露 HTTP/gRPC 请求类型。没有把每个 DAO 再包成一个只有转发作用的 Service。

## 2. 按业务找代码

| 业务 | 核心服务和入口方法 | 下层实现 |
|---|---|---|
| 注册/登录/鉴权 | `services/user_service.*`：Register、Login、Authenticate、AuthenticateAdmin | UserRepository → UserDao、RedisStore、AuditLogDao；`common/security.cpp` |
| 历史/缺口/离线/游标 | `services/conversation_service.*`：CheckAccess、History、SyncAfter、SyncOffline、MarkDelivered、MarkRead、Unread | ConversationRepository → ConversationStore、GroupMemberDao |
| 图片上传/读取/引用 | `services/attachment_service.*`：Upload、Read、AuthorizeMessageImages | AttachmentRepository → AttachmentDao；访问权复用 ConversationService |
| 上行消息与最终 Agent 回复 | `services/message_service.*`：Submit、HandleHermesReply、HandleHermesDelta | UserService、AttachmentService、ConversationStore、RedisStore、KafkaProducer |
| 房间订阅/在线状态 | `services/room_service.*`：Join、Leave、Unsubscribe、ListSubscriptions、ReportRoomJoin/Leave | GroupDao、GroupMemberDao、ConversationStore、RedisStore |
| 管理员用户中心 | `services/admin_service.*`：SetStatus、UpdateName、RevokeTokens、ListUsers、ListAudit | UserDao、RedisStore、AuditLogDao |
| 旧弹幕 API | `services/danmaku_service.*`：Send、List | DanmakuDao、ConversationStore、RedisStore、KafkaProducer |
| CANN 原文 | `services/knowledge_service.*`：Document | `knowledge_catalog.cpp` 与 catalog 文件 |
| 房间共享 Agent / 算子交接 | `services/room_agent_service.*`：RoomAgentService、OperatorTaskService | RoomAgentRepository → MySqlRoomAgentRepository |

2026-10-11 的新增协作服务、API、权限和真实验收见
[`room-agent-collaboration.md`](room-agent-collaboration.md)。房间上行使用 Comet 注入的 auth_token
复核真实 Actor；HTTP/gRPC 仍只做 DTO 转换，不能从消息正文取得开发权限。

共用定义：`model.h` 保存 User / Session / Message / StoredAttachment 等模型和账号状态；
`services/service_status.h` 返回业务 code/message，**不是 HTTP 状态或 grpc::Status**；
`services/business_rules.h::RequireAdministrator` 复用管理员规则；
`services/repositories.h` 定义存储接口；`message_codec.h` 统一消息 Protobuf 字段映射。

HTTP、gRPC 用 `main.cpp` 创建的同一批 Service 实例；用户热缓存、场景令牌桶、Agent 最近上下文
与增量去重状态不会为两个入口各创建一份。Service 生命周期长于适配器与 consumer，底层连接池
和 DAO 的生命周期长于 Service；这些指针是非拥有型依赖，不得注入请求局部对象。
业务实现编译为 `spark_push_logic` 静态库，服务器适配器和业务单测共同链接它。

## 3. 三个复习例子

### 3.1 会话权限只写一次

`ConversationService::CheckAccess` 从存储获取 Session：单聊要求当前用户是两端之一；
房间要求成员查询成功且确实为成员，查询错误不能视作授权。

HTTP History、gRPC SyncMessages、MarkDelivered、图片上传、非 owner 图片读取都复用这条规则。
附件服务允许上传时懒创建规范单聊 ID `s_<min>_<max>`，先确认调用方是参与者再写数据库。
`s_1_8junk` 等非规范拼写现在会在创建之前拒绝，修复了旧解析可能产生无关会话行的副作用；
这是本次明确的边界收紧，不是把所有旧接口权限悄悄改成同一种语义。

### 3.2 鉴权与热路径缓存区别

HTTP `requireUser` 和 gRPC VerifyToken 都调用 UserService::Authenticate。
它们共享 token 解析、MySQL 最新账号状态检查，但保留 HTTP/realtime 两种 audience：
实时入口禁止管理员和系统 Bot token。管理员入口再用 AuthenticateAdmin。

发送消息的 `GetActiveUser` 复用原有 2 秒短缓存；新 HTTP 请求和新 WS 握手不使用该缓存。
管理员禁用先撤销 Redis token 再更新 MySQL 状态，审计失败仍按原约定单独报告。
不能据此声称“已建立的所有长连接立即被踢下线”；旧热缓存和连接生命周期仍存在。

### 3.3 消息的成功边界不变

```text
UpstreamMessageRequest → UpstreamCommand → MessageService::Submit
  账号状态/限流/附件 → Redis Lua seq + dedup
  → PersistToTopic：Kafka SendAndWait delivery report
  → UpstreamResult.accepted=true → adapter ACK_STAGE_ACCEPTED
  → 异步实时 topic → Job → Comet → delivered_ack
```

accepted 不是 MySQL INSERT 已完成；历史必须等 Job persist consumer 落库。
同一个 sender/session/client_msg_id 保留同一 msg_id/msg_seq，重试不重复实时投递。
Kafka Agent consumers 现在直接调用 MessageService，不再调用 gRPC 类里的业务方法。
ai_delta 仍临时推送、不占序号、不进历史；最终 ai_reply 才进入持久化与离线恢复链路。
同步 RPC 和双向流都调用同一个 Submit，只有读取/写入协议形式不同。

## 4. 兼容约定与未完成的治理

- 没改 URL、Proto、SQL schema、Redis key、topic 配置和 WebSocket 消息类型。
- HTTP 错误 code 和 HTTP status 不是总相同：例如普通用户拿 token 请求管理员接口，
  是业务 403 + HTTP 400；禁用账号登录也是业务 403 + HTTP 400。映射由适配器保留。
- 历史/同步/附件授权已共享。MarkRead/Unread 仍保留旧的“请求 user_id 等于鉴权身份”规则，
  本次没有额外宣称补齐它们的对象级授权。
- `SendLegacyText` 专门保留旧 `/api/message/send`：同步存储、无实时 fanout、无 accepted/delivered
  合同，旧接口仍不要求 Bearer。它不是 WS Submit 的替代入口；正式使用应另开安全迁移任务。
- 弹幕仍接受请求 body 的 token、使用 Redis 解析，固定房间 1001；不自动升级为普通 HTTP
  新鲜账号状态鉴权。`pushed=true` 仍不是 broker 确认或最终客户端收讫。
- HTTP Leave 只返回旧的页面离开结果；Unsubscribe 才删持久订阅，gRPC ReportRoomLeave 才更新在线计数。
  OnlineCount 存储失败回退成员数量、在线计数部分失败/重复报告等旧局限没有在此次重构中解决。
- 管理员角色在可信适配器解析后传入服务；用户变更、房间管理、管理员广播再共享业务层角色检查。
  内部 gRPC 仍是可信内网身份输入，不是公开无鉴权 API，需要 TLS/mTLS 等另行治理。
- 仍有旧响应/弹幕/广播的字符串 JSON 拼接和解析异常边界，后续应单独修正并补兼容测试。
- MessageService 仍承载较多 Agent 编排；未来可再提取 AgentConversationService，但缓存和幂等状态
  必须保持唯一所有者，不能只拆文件后各自保存一份状态。

## 5. 新功能怎么写

1. 先找已有规则：用户用 UserService，会话用 CheckAccess，图片用 AttachmentService，
   管理员用 RequireAdministrator。身份从可信入口获得，不相信客户端声称的 actor。
2. 用普通 C++ 参数/DTO 增加 Service 用例；若需新存储行为先增加 Repository/DAO 方法。
3. 在 `main.cpp` 注入依赖。HTTP/gRPC handler 只做字段转换、调用、响应，不再直接操作 DAO。
4. 用内存 Repository 测真实 Service 的允许/拒绝/存储失败，再测实际接口和异步消息链路。
5. 若改变可靠性合同，必须同时验证在线、重复、离线、历史；Agent 还要验证临时增量与最终回复。

验证入口：`tests/logic_business_service_test.cpp`（无需 MySQL 的真实业务用例）、
`tests/logic_layer_contract_test.py`（文本依赖门禁，不是 AST 形式证明）、
`tests/logic_business_e2e.py`（显式启用、真实依赖库及 HTTP/gRPC/WS 的功能回归）。
实测证据见 `docs/benchmarks/2026-10-09-business-layer/`；不要把回归样本吞吐当容量结论。

2026-10-09 最终验证：构建通过；业务单测 81 项检查通过；全项目 CTest 26/26 通过
（43.27 秒，真实 Redis 启用、无跳过）；真实 HTTP/gRPC/WS 功能回归 88 项检查通过
（12.17 秒）；本地 CUDA/Qwen Agent 链路预热 2 + 正式 4 请求无失败，断连最终回复与相同
client_msg_id 重放恢复通过。知识登记/引用校验和临时目录知识包同步通过。
