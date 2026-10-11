# Spark Push 项目详解、技术复习与求职手册

> 被解释项目：`/home/peco/cppcode/fenbushi/11.2-spark_push`\
> 版本口径：只以根目录当前源码为准；`02_auth_subset`～`06`、`5.1_room_subset` 等目录是学习快照。\
> 更新日期：2026-10-11（同步工程审查修复；更新架构、消息契约、算法与简历；新增第 19 章）\
> GitHub 仓库：[specetator/grpc_aiagent](https://github.com/specetator/grpc_aiagent)；在线手册：[项目详解与简历手册](https://github.com/specetator/grpc_aiagent/blob/main/docs/project-guide-and-resume.md)。
> 当前行为与迁移以 [工程审查修复](review-remediation.md)、本手册第 19 章及根目录源码为准。大框架与小模块旧专题中尚有 Redis 取号等历史描述，阅读时按日期区分。

本文有四个目标：

1. 不打开源码，也能快速恢复对项目架构、数据流和可靠性语义的完整认识；
2. 按 Redis、Kafka、gRPC、Muduo/WebSocket、MySQL、C++ 并发等技术栈复习原理，并知道它们在项目中的代码落点；
3. 把队列、Lua、握手、PushStream、TreeMap 等小模块追到逐步算法，达到面试官追问“xx 处链表/哈希/序号到底怎么做”的颗粒度；
4. 把真实实现和实测结果整理成可用于简历、项目介绍和面试追问的材料，同时保留工程边界。

## 0. 阅读路线与速查索引

第一次阅读建议按以下顺序：

1. 第 1～3 章：项目是什么、为什么拆成 Comet/Logic/Job、消息模型是什么；
2. 第 4 章：完整走一遍登录、单聊、离线补推、房间消息和 Agent 路径；
3. 第 5 章：按技术栈复习原理和具体代码；
4. 第 13 章：小模块逐步算法（容器选择、Lua 全文、SHA1 握手、PushStream、Android TreeMap、图片附件）；
5. 第 6～8 章：源码定位、启动、验证、性能与排障；
6. 第 8.3 节：2026-09-01 性能优化的定位、实施、A/B 数据和边界；
7. 第 9～10 章：项目演进、简历写法和面试回答。
8. 第 15 章：本地 Qwen3-0.6B 的 CUDA 推理、显存观测、批处理、Flash Attention、KV Cache 和测试证据。
9. 第 16 章：Logic 内部三层结构、业务规则的共享入口、维护方法和重构测试。
10. 第 17 章：2026-10-10 GitHub 发布历史；当前同步与验证见第 19 章。
11. 第 18 章：多人共享 CANN Agent、任务/源码交接、开发权限和真实聊天室测试。
12. 第 19 章：工程审查修复、当前验证回执、存量迁移与后续生产验收。

如果只剩十分钟：先看 1.2、2.1、3.3、4.2、5.3、5.4、5.5、8.3、10.2 和 13.1。

| 想复习的问题                                 | 直接阅读                          |
| -------------------------------------- | ----------------------------- |
| 一条消息怎样从发送方到接收方                         | 4.2 单聊完整时序                    |
| Redis 当前保存什么、为什么不再负责消息取号 | 5.4、13.4～13.5、19.2 |
| Kafka topic、consumer group、offset 如何设计 | 5.3 Kafka、13.11 消费者           |
| gRPC Unary 和 Streaming 为什么同时存在         | 5.2 gRPC/Protobuf、13.18～13.19 |
| Muduo 如何管理大量 WebSocket 连接              | 5.1 Muduo/WebSocket、13.16 连接表 |
| WebSocket 握手 SHA1/Base64 怎么算           | 13.14                         |
| MySQL 表、索引、MEDIUMTEXT 二次读取             | 5.5 MySQL、13.21               |
| 没有手写链表时队列/哈希/有序树怎么配合                   | 13.1 容器选择                     |
| Android 流式增量如何按序号合并                    | 13.27 TreeMap                 |
| 图片发送、截图问答为什么不走 WebSocket base64        | 4.7、13.30                     |
| Agent envelope / Router 幂等             | 4.7、13.24～13.25               |
| accepted/delivered/read 三者区别           | 3.3 消息状态机                     |
| 重连后怎样补消息                               | 4.3 游标与离线补推                   |
| 如何启动、验证和排障                             | 7、8 章                         |
| 性能数字能怎样写进简历                            | 8.2、10.2                      |
| 面试官可能追问什么                              | 10.3                          |
| 面试题逐题回答稿                               | 10.3.1                        |
| RTX 3060 如何接入本地模型、如何衡量 AI 推理性能         | 15 章                          |
| HTTP/gRPC 的业务逻辑去哪里了、怎样复用规则 | 16 章、`logic/services/` |
| GitHub 发布版本、最新验证与迁移要求 | 17 章（历史发布）、19 章（当前修复） |
| 多人协作开发算子、共享 Agent 与进度交接 | 18 章 |

---

## 1. 项目定位：它解决了什么问题

### 1.1 一句话定义

Spark Push 是一个 C++17 分布式实时消息推送/IM 教学项目：客户端通过 WebSocket 保持长连接，Comet 负责连接接入，Logic 负责业务、序号、幂等和路由，Kafka 解耦受理、实时投递与持久化，Job 消费任务并通过 gRPC PushStream 下发到 Comet，Redis 保存热状态，MySQL 保存可恢复的业务事实。

它不是已经承载商业流量的微信或 Telegram，也没有完成跨机房、多副本和百万连接验证。项目价值在于：能把“消息发出去了”拆成可解释、可验证的多个阶段，并用代码、数据表、Kafka offset、游标和 E2E 测试闭环。

### 1.2 已实现能力

| 领域    | 当前能力                                                                                              |
| ----- | ------------------------------------------------------------------------------------------------- |
| 用户与安全 | 注册、登录、PBKDF2 密码哈希、安全随机 token、禁用/恢复/软删除、token 批量撤销、审计日志                                            |
| 实时接入  | WebSocket 握手（自实现 SHA1+Base64）、帧解析、心跳、多连接、指数退避重连、单帧大小限制                                            |
| 消息场景  | 单聊、聊天室/群聊 fanout、系统广播、弹幕时间轴                                                                       |
| 可靠性 | 两阶段 ACK、SQL 持久序号/原文幂等/outbox、消费后提交、persist DLQ 与显式重放工具、近期投递去重 |
| 恢复能力 | 历史查询、safe_seq 缺口补偿、逐条 receipt 连续 delivered cursor、多会话离线分页、未读和已读 |
| 性能治理  | Redis/MySQL 连接池、路由短缓存、场景令牌桶、Kafka topic 隔离、Job→Comet 长连接、有界队列                                     |
| 客户端   | WebDemo；原生 Kotlin/Compose Android（独立 Agent 联系人、TreeMap 流式合并、连接代数、图片发送）                            |
| 可观测性 | Prometheus text metrics、关键消费者 readiness、文件日志轮转、E2E/异常网络回归、可选长度审计 |
| 可选扩展  | 本机 Pi Agent / Hermes technical、HTTP/SSE 增量、`ai_request/ai_delta/ai_reply` 隔离、CANN 引用、截图问答（先上传再引用） |

### 1.3 核心设计问题

这个项目真正值得讲的不是“用了 Redis、Kafka、gRPC”，而是以下问题如何被解决：

- 长连接数量与业务计算怎样隔离？
- 在线路由怎样快速查询，又怎样支持用户多端连接？
- 同一会话怎样分配持久唯一的 `msg_seq`，旧入口混写和 Redis 丢失时怎么办？
- 客户端重试怎样避免生成两条消息？
- Logic 返回成功前，消息至少要跨过什么可靠边界？
- Kafka 消费到任务后，什么时候才能提交 offset？
- gRPC `Write()` 成功是否等于目标客户端已经收到？
- 接收端断线期间的缺口怎样发现、怎样补齐？
- 热点用户、广播、大房间怎样避免拖垮普通单聊？
- 性能测试怎样避免把客户端 `send()` 速度误报成系统投递能力？

### 1.4 项目边界

当前版本可以描述为“有工程化可靠性设计的单机可复现分布式消息系统”，不能描述为生产级 IM。尚未完成的关键能力包括：

- Kafka/MySQL/Redis 多节点高可用和跨机故障验证；
- 实时 push/broadcast 的可重放 DLQ、后台管理平台和告警闭环；persist/Agent 已有配置的死信 topic，CLI 重放已实现；
- 公网 HTTPS/WSS、mTLS、统一 IAM 与密钥托管；内部 RPC 已有角色密钥与远程 TLS 配置，真实证书互通仍待验证；
- 多 Comet 一致性路由、跨节点踢线、灰度和滚动升级验证；
- 多账号大群、多 partition、多机压测以及容量模型；
- 发送方可见的实时已读回执、消息撤回、媒体消息和完整群管理。

---

## 2. 总体架构

### 2.1 主消息架构图

```text
发送方 Client
    │ WebSocket 上行
    ▼
Comet ───── MessageStream(gRPC 双向流) ─────► Logic
  │                                             │
  │                                             ├─ Redis
  │                                             │   token / 在线路由 / 未读缓存
  │                                             │
  │                                             ├─ MySQL 事务
  │                                             │   msg_seq / 原文 / outbox
  │                                             │
  │                                             ├─ persist_message
  │                                             │      │
  │                                             └─ push_single / push_group
  │                                                    │
  │                                                    ▼
接收方 Client ◄── WebSocket ◄── Comet ◄── PushStream ─ Job
                                                       │
                                                       └─ MySQL 持久化
```

系统同时存在两条 Kafka 分支：

- `persist_message` 是可恢复事实的持久化分支；Logic 等待 broker delivery report 后才返回 accepted ACK，Job 再异步落 MySQL。
- `push_single`/`push_group` 是在线实时投递分支；目标不在线或该分支暂时失败时，仍可依靠持久化事实和游标在重连后补齐。

### 2.2 进程职责

| 进程                  | 默认端口                           | 核心职责                                                              | 不应该承担的职责                          |
| ------------------- | ------------------------------:| ----------------------------------------------------------------- | --------------------------------- |
| Logic | HTTP 9101、gRPC 9100 | 用户/权限、SQL 序号与原文预留、outbox 恢复、路由、限流、Kafka、历史/游标 | 不持有海量 WS；历史 message 由 Job 写入，接受路径仍同步访问 SQL |
| Comet               | WS 9000、gRPC 9105、metrics 9203 | WebSocket 接入、连接表、房间本机订阅、上行转发、本机下推、离线补推触发、delivered ACK            | 不做复杂业务和持久化决策                      |
| Job                 | metrics 9202                   | 消费实时/广播/持久化 topic、维护到 Comet 的 PushStream、处理重连重放、落 MySQL           | 不直接面向公网客户端                        |
| Web Demo            | HTTP 9010                      | 浏览器演示、用户聊天和管理员控制台                                                 | 不作为安全边界                           |
| Android             | 独立 APK                         | 原生 Compose 客户端，复用同一套 HTTP/WS 合同                                   | 不在本地保存聊天历史                        |
| Hermes Bridge       | 无监听端口                          | 消费 `ai_request`、调用本机 Pi gateway HTTP/SSE、发布 `ai_delta`/`ai_reply` | 不阻塞 Logic 普通 IM 热路径               |
| Pi gateway / Router | 本机 8643                        | Agent 路由、turn 幂等、事件账本、模型执行                                        | 不拥有 Spark 用户鉴权和 `msg_seq`         |

### 2.3 为什么要拆成三层

Comet、Logic、Job 的拆分对应三类完全不同的负载：

- Comet 是连接密集型：关心 fd、心跳、事件循环、连接生命周期和 socket 写缓冲；
- Logic 是业务密集型：关心鉴权、幂等、顺序、会话权限、限流和路由；
- Job 是异步任务密集型：关心 Kafka lag、重试、offset 提交、投递确认和落库。

如果全部放进一个进程，连接风暴可能挤占业务线程，MySQL 慢查询可能阻塞 WebSocket，广播 fanout 也可能拖慢单聊。拆分后可以独立扩容、独立限流、独立观察，并通过协议定义各自失败边界。

### 2.4 存储分工

| 存储    | 保存内容                                            | 为什么放这里                           |
| ----- | ----------------------------------------------- | -------------------------------- |
| Redis | token/撤销索引、用户/房间→Comet 路由、路由代次与 nonce、未读缓存、在线计数 | 热状态与可失效缓存；不再分配当前发送链 msg_seq |
| Kafka | 持久化事件、实时投递任务、广播任务、可选 AI 事件                      | 解耦生产消费、吸收突发、支持重放和 consumer group |
| MySQL | 用户/会话/消息、message_sequence、原文 reservation/outbox、逐条投递 receipt、读/投递游标、协作任务等 | 持久序号、事务幂等和可恢复事实 |

Redis 是“热状态”，MySQL 是“事实来源”，Kafka 是“阶段之间可重放的事件通道”。三者不是互相替代。

### 2.5 目录地图

```text
11.2-spark_push/
├── proto/           Protobuf 消息与 gRPC 服务定义
├── common/          配置、日志、连接池、Kafka、限流、metrics、安全
├── comet/           Muduo WebSocket 接入与 Comet gRPC 服务
├── logic/           业务 gRPC、HTTP API、RedisStore、ConversationStore、DAO
├── job/             Kafka consumer、PushStream、持久化消费者
├── hermes_bridge/   可选 AI HTTP/SSE 适配层（指向本机 Pi gateway）
├── android-app/     Kotlin/Compose 原生客户端
├── web_demo/        用户端与管理员静态页面
├── cannbot/         Pi/Hermes adapter、Router、知识 Skill
├── load_test/       功能客户端、E2E 和专项压测器
├── tests/           单元/集成测试
├── conf/            Logic、Comet、Job、Hermes 配置
├── sql/             schema、迁移与显式开发环境重置脚本
├── scripts/         启停、依赖准备和运行说明
└── docs/            可靠性、演进、用户中心、Hermes 专题文档
```

---

## 3. 核心数据、标识与消息状态机

### 3.1 `ChatMessage` 是贯穿全链路的数据模型

定义位置：`proto/spark_push.proto`。

| 字段              | 含义                                  | 主要用途             |
| --------------- | ----------------------------------- | ---------------- |
| `msg_id`        | 服务端消息 ID，当前通常为 `session_id-msg_seq` | 服务端与客户端去重、日志定位   |
| `session_id`    | 会话 ID                               | 分区消息顺序、历史查询、游标定位 |
| `msg_seq`       | 会话内递增序号                             | 排序、缺口发现、未读和补偿    |
| `sender_id`     | 发送者用户 ID                            | 权限、展示和审计         |
| `timestamp_ms`  | 服务端处理时间                             | 展示、延迟和 lag 计算    |
| `msg_type`      | `text/system/danmaku/...`           | 消息类型扩展           |
| `content_json`  | 业务 JSON                             | WebSocket 实际下发内容 |
| `client_msg_id` | 客户端生成的请求幂等键                         | 重试去重、ACK 对应本地气泡  |

### 3.2 四种容易混淆的 ID

| ID              | 谁生成         | 唯一范围               | 解决什么问题                    |
| --------------- | ----------- | ------------------ | ------------------------- |
| `client_msg_id` | 客户端         | 发送者+会话范围           | 网络重试时识别“同一次发送”            |
| `msg_seq` | Logic/MySQL 事务 | 单个会话内 | 持久唯一序号、顺序和缺口 |
| `msg_id`        | Logic       | 当前由 session+seq 推导 | 业务消息身份                    |
| `request_id`    | Logic/投递层   | Job→Comet 请求       | gRPC stream reply 匹配和重放去重 |

`client_msg_id` 与 `request_id` 不能混用：前者保护“业务发送”，后者保护“内部投递”。

### 3.3 消息状态机

```text
客户端本地创建
    │ WebSocket send
    ▼
Logic 鉴权 → SQL 原文预留/取号 → prepare outbox
    │ persist_message 已收到 Kafka delivery report
    ▼
accepted_ack
    │ 在线实时 topic → Job → Comet reply
    ▼
delivered_ack
    │ 用户真正阅读并主动上报
    ▼
read_seq 更新
```

语义边界：

- `accepted_ack`：消息持久化事件已经进入 Kafka 的可重放边界，不等于 MySQL 已落库，也不等于接收者在线。
- `delivered_ack`：目标连接所属 IO 线程已检查连接/写缓冲并接受发送操作；这仍不是客户端收讫、展示或已读证明。
- `read_seq`：应用层已读游标，需要客户端明确上报。
- `delivered_seq`：逐条 receipt 与 SQL 安全前缀共同证明的连续投递水位。高序号先到不会跳过低序号尚未持久化或尚未投递的消息。

因此项目没有声称端到端 exactly-once，也没有把 delivered ACK 夸大为“用户一定看到了”。

### 3.4 会话 ID 与顺序

单聊会话将两个 uid 排序后构造：

```text
s_<较小uid>_<较大uid>
```

聊天室会话：

```text
r_<room_id>
```

项目只保证“会话内 `msg_seq` 单调递增”，不保证跨会话全局顺序。IM 中不同会话通常不需要共享一条全局序列；强行全局排序会制造额外热点。

### 3.5 MySQL 核心关系

```text
user
  ├─ session(user1_id, user2_id)
  │    └─ message(session_id, msg_seq)
  │          └─ UNIQUE(session_id, msg_seq)
  ├─ user_session_state(user_id, session_id)
  │          ├─ read_seq
  │          └─ delivered_seq
  ├─ group_member(group_id, user_id)
  ├─ attachment(id, owner_user_id, session_id)  → 磁盘 data/attachments/<id>
  └─ audit_log(target_user_id)

im_group ── group_member
video_danmaku 独立按 video_id + timeline_ms 查询
```

`message.uk_session_seq` 是历史唯一键；`message_sequence` 在同一事务内锁住会话并分配序号，`message_reservation` 保存原文及二进制 outbox。`message_delivery_receipt` 保存逐条投递证明；read_seq 可用 GREATEST，delivered_seq 必须先计算安全连续前缀。

---

## 4. 核心业务链路

### 4.1 登录与 WebSocket 建连

1. 客户端调用 Logic HTTP `/api/login`；
2. Logic 从 MySQL 查询用户，校验 `active` 状态和密码；
3. OpenSSL 生成随机 token，Redis 写 `token:<token> → user_id`，并写 `user:tokens:<uid>` 反向索引；
4. 客户端携带 token 与 Comet 完成 WebSocket 握手；
5. Comet 将 Logic gRPC `VerifyToken` 放入有界工作池，3 秒 deadline，握手总等待 5 秒；完成后切回连接所属 IO 线程；
6. Comet 注册带 generation + 随机 nonce 的 Redis 路由，并在本机 `user_conns_` 保存该用户连接；晚到下线仅能删除自己那一代路由；
7. Comet 调用 `SyncOffline`，根据 `delivered_seq` 补推离线缺口。

核心代码：

- `logic/http_server.cpp`：登录、注册和 Bearer HTTP 协议适配；`logic/services/user_service.cpp` 执行账号、密码与 token 规则；
- `common/security.cpp`：PBKDF2、随机 token、常量时间比较；
- `comet/comet_server.cpp`：握手、连接注册和离线补推；
- `logic/redis_store.cpp`：token 与在线路由。

### 4.2 一条单聊消息的完整时序

假设 A 给 B 发一条文本消息，以下描述的是 2026-10-11 修复后的当前链路：

1. A 生成 1～128 字节 `client_msg_id`，经 WebSocket 发送 `single_chat`。
2. Comet 从已认证连接取得 actor 与 token，经有界 `MessageStream` 队列发送请求。
3. Logic 重新验证 token 与 actor，校验用户/目标/内容并执行限流，构造稳定的 session_id。
4. `ConversationStore::AppendMessageHotPath` 调用 SQL reservation：事务锁定 `message_sequence` 会话行，以 `(session_id,sender_id,client_msg_id)` 查原文。命中时返回原 timestamp/body/seq；同 ID 改正文则拒绝。
5. 新请求递增 SQL 序号并保存 canonical message。首次接入旧会话才从历史 MAX/会话水位初始化；Redis 不参加取号。
6. 将完整 `PersistMessageRequest` 的二进制写入 SQL outbox，再 `SendAndWait` 发布 `persist_message`。
7. 只有 Kafka delivery report 成功才返回 `accepted_ack`。确认超时仍可能已经发布，客户端用同 ID 重试；prepared outbox 由后台每秒扫描恢复。
8. 查 B 的 Redis 路由，按目标 Comet 创建相同业务消息身份的 `push_single` 实时任务；重试允许再次 fanout，下游去重。
9. Job 消费实时任务，经复用 `PushStream` 发送并等待相同 request_id 的 reply。
10. Comet 锁内复制目标连接，在其所属 IO 线程检查连接状态与 4 MiB 输出容量、接受发送后才计入 delivered_count；Job 据 reply 回传 `delivered_ack`。
11. Comet 上报逐条投递 receipt。SQL 只推进安全、连续且已经投递的前缀；不能直接取收到消息的 MAX。
12. 独立 persist consumer 核对消息全部字段、幂等写历史和更新 reservation stage=3，成功或已确认 DLQ 后才提交原 offset。
13. B 按消息 ID 去重、按 seq 排序；缺口通过 `SyncMessages` 的分页与 safe_seq 证明恢复。

面试一分钟：Comet 管连接，Logic 管身份和持久消息状态，Job 管异步历史与投递。SQL 事务统一取号、保存重试原文，并在 Kafka 前准备 outbox；accepted 以 Kafka 确认为边界。实时投递独立进行，Comet 的 IO 线程接受发送后才确认 delivered，历史可能晚于实时到达。逐条 receipt、safe_seq 和分页补拉负责处理这种乱序，整个系统仍是至少一次尝试与多层幂等。

关键边界：SQL 预留成功不等于 accepted；gRPC Write 成功不等于 Comet 完成；delivered 不等于客户端收讫；重复请求保持原文，但不能承诺永不重复 fanout 或无限期去重。

### 4.3 重连、缺口补偿与离线补推

项目使用两个互补机制：

**主动缺口补偿**

客户端已看到 seq=12，下一条却是 seq=15，就调用 `SyncMessages(session_id, after_seq=12)` 拉取 13～15。它解决在线期间丢帧、乱序或页面短暂不可用。

**握手离线补推**

建立 WS 后，Comet 按 `after_session_id + after_seq` 反复请求 `SyncOffline`，每页最多 200 条并受 2 MiB 估算预算限制，直到无下一页。下发被所属 IO 线程接受后，逐条 `MarkDelivered`，仅推进安全连续水位。Sync 的 `next_seq` 是翻页位置，`safe_seq` 才是已结算前缀证明；客户端在当前连续游标覆盖请求起点后使用它，SQL 暂缺的低序号保留并再次补拉。

`delivered_seq` 和 `read_seq` 必须分开：用户可能已经收到但没有打开会话；如果用 `read_seq` 做离线补推，会在每次重连重复推送大量“已送达但未读”的消息。

旧版本压测发现一个边界：同一个有大量历史的账号同时建立 1000 条连接时，每条握手都会触发离线同步，造成消息放大和约 2 GiB Comet RSS。当前虽增加输出上限，用户级同步合并和同账号连接限制仍未完成；不能把旧 RSS 当新版本实测。

### 4.4 聊天室与群 fanout

聊天室消息使用 `session_id=r_<room_id>`。Logic 优先从 Redis `room:comets:<room_id>` 得到有订阅者的 Comet，只为这些 Comet 创建任务；如果路由不存在，再回退到 MySQL 群成员并逐个查询用户路由。

Comet 本机维护：

- `room_users_[room_id]`：本机加入房间的用户集合；
- `user_conns_[uid]`：用户的所有本机连接。

`PushToRoom` 先在锁内复制目标连接，随后在锁外执行 send，避免慢 socket 写操作长期占用连接表锁。群聊/聊天室进入 `push_group`，与单聊 `push_single` 分离，降低大 fanout 对单聊的干扰。

### 4.5 广播与弹幕

广播由管理员 HTTP 或持有独立 ADMIN 角色密钥的内部 RPC 创建任务。房间 scope 的成员由 Logic 查询持久成员关系并写入任务；Job 遇到空/异常成员范围会拒绝，绝不退化为全局广播。广播仍有独立限流与 consumer group。

弹幕按 `video_id + timeline_ms` 存入 `video_danmaku`，索引 `idx_video_timeline` 支持时间区间查询。弹幕本质上是另一种按时间轴组织的消息，不应强行复用单聊会话顺序。

### 4.6 历史、未读和已读

`POST /api/session/history` 会：

1. 校验 Bearer token；
2. 查询 session 并验证请求者是单聊参与者或群成员；
3. 以 `(session_id, msg_seq)` 为条件倒序分页；
4. 返回最近一页消息。

未读数近似为：

```text
max(0, session.last_msg_seq - user_session_state.read_seq)
```

Logic 优先从 Redis 读取 `session:last_seq` 和 `user_session:read_seq`，未命中再回源 MySQL。已读上报同时更新 MySQL，并写 Redis 缓存。

### 4.7 可选 Pi Agent / Hermes 链路

Agent 是独立扩展，不改变普通 IM 主链路。`hermes_bridge` 进程名保持不变，实际打本机 Pi gateway（`http://127.0.0.1:8643/v1`）：

```text
用户 → Logic → persist_message（用户输入已 accepted）
              └→ ai_request → hermes_bridge → Pi gateway HTTP/SSE
                                    ├→ Router：route_key / turn 幂等 / 事件账本
                                    ├→ ai_delta → Logic → 临时实时气泡（不落库）
                                    └→ ai_reply → Logic
                                                  ├→ Redis 分配正式 msg_seq
                                                  ├→ persist_message
                                                  └→ push_single → Job → Comet
```

`ai_delta` 只优化首字体验，不分配正式 `msg_seq`、不写历史，也不参与重连补偿；完整 `ai_reply` 才是事实消息。Bridge 以 `request_id` 缓存最终回答，Logic 以 `hermes:<request_id>` 做消息幂等。Android 使用独立联系人 `900000000201`（Pi）和 `900000000211`（Hermes technical），避免和 PC/WebDemo 的 session、模型偏好混用。

图片和截图问答不改变这条 accepted 边界：客户端先 `POST /api/attachment/upload`，WebSocket 只带 `content.attachments[].id`。发给 Agent 时 `ai_request.images` 仍是 id；Bridge 读 `data/attachments/<id>` 后才把 base64 交给 Pi RPC。无文字时默认「请查看这张图片。」当前视觉模型是 `deepseek-flash`。每条最多 4 张、每张 4 MiB。

逐步算法见第 13.23～13.27、13.30 节。配置见 `11.2-spark_push/docs/pi-agent-integration.md`。大框架见仓库 `docs/spark-push-architecture.md` 第 2.5 节。

---

## 5. 核心技术栈复习：原理、项目实现与面试重点

### 5.1 Muduo、Reactor 与 WebSocket

#### 5.1.1 为什么客户端侧使用 WebSocket

普通 HTTP 是请求/响应模型，服务端不能自然地随时向客户端主动推送。WebSocket 在一次 HTTP Upgrade 后复用同一条 TCP 连接，双方都能主动发送 frame，适合聊天、通知和在线状态。

WebSocket 仍然建立在 TCP 上：

- TCP 保证单连接字节流可靠有序，但不提供消息边界；WebSocket frame 提供消息边界。
- 客户端发出的 frame 必须 mask，服务端 frame 通常不 mask。
- 应用仍要处理半包、粘包、断线、心跳、重连和业务去重。
- TCP send 成功只代表数据进入本机/内核缓冲，不等于对端应用已经处理。

#### 5.1.2 Reactor 模型

Muduo 的典型 Reactor 思路是：

```text
EventLoop
  └─ Poller(epoll)
       ├─ 监听连接可读/可写事件
       ├─ 回调 onConnection
       └─ 回调 onMessage
```

一个 I/O 线程通过 epoll 管理大量 fd，只有事件就绪时才执行回调，避免“一连接一线程”的巨大线程栈和上下文切换成本。项目配置 `comet_io_threads=8`，主 loop 接受连接，工作 EventLoop 分担 I/O。

项目代码落点：

- `comet/app.cpp`：创建 EventLoop、CometServer 和独立 gRPC server；
- `comet/comet_server.cpp`：连接建立/断开、连接映射、上行 frame 和下行 fanout；
- `comet/websocket_utils.cpp`：WebSocket 握手与消息元信息解析；
- `muduo/net/`：仓库内 Muduo 网络实现。

#### 5.1.3 连接表与锁粒度

`user_conns_` 的值是连接集合而不是单连接，因为同一用户可能有手机、网页和多个标签页。向用户或房间推送时，Comet 在锁内把智能指针复制到局部 vector，释放锁后再 send。这样可以避免：

- 持有全局连接锁执行网络 I/O；
- 一个慢连接阻塞其他连接的增删；
- 连接断开时裸指针失效。

需要记住的风险：热点用户的多连接会把一条业务消息复制 N 次；房间 fanout 的容量应按 deliveries/s 而不是 messages/s 衡量。

#### 5.1.4 心跳与重连

心跳用于：

- 发现 NAT、代理或网络静默断开；
- 维持连接映射的准确性；
- 尽早触发路由清理和重连。

重连使用指数退避，避免服务恢复瞬间所有客户端同时重试形成 thundering herd。仅有退避还不够，还应在生产中加入随机 jitter、连接速率限制和同账号连接上限。

#### 5.1.5 面试复习点

- LT/ET：水平触发未读完会继续通知，边沿触发必须读到 `EAGAIN`；
- Reactor 与 Proactor：Reactor 通知“可以做 I/O”，Proactor 通知“I/O 已完成”；
- 为什么不能一连接一线程：线程栈、调度、缓存失效和同步成本；
- WebSocket 与 TCP：WebSocket 是 TCP 上的应用层协议，不替代 TCP；
- 背压：socket 写缓冲持续增长时应限制单连接队列、丢弃低优先级消息或断开慢消费者。

### 5.2 Protobuf 与 gRPC Streaming

#### 5.2.1 为什么内部不用 JSON/HTTP

Protobuf 提供明确 schema、紧凑二进制编码和跨语言生成代码；gRPC 基于 HTTP/2，提供复用、流控、deadline、状态码和 Unary/Streaming 调用。内部服务之间比手写 JSON 更容易保持字段兼容和错误语义。

`proto/spark_push.proto` 定义三类服务：

| Service        | 主要 RPC                                                     | 方向与作用                               |
| -------------- | ---------------------------------------------------------- | ----------------------------------- |
| `LogicService` | `VerifyToken`、`SyncMessages`、`SyncOffline`、`MarkDelivered` | Comet 调用 Logic 的鉴权和恢复接口             |
| `LogicService` | `MessageStream`                                            | Comet↔Logic 双向上行长连接                 |
| `CometService` | `PushStream`、`PushToComet`、`PushDeliveryAck`               | Job→Comet 实时投递、Unary fallback 和 ACK |
| `JobService`   | `EnqueuePushTask`、`EnqueueBroadcastTask`                   | 保留的任务 RPC 接口                        |

#### 5.2.2 Unary 与 Streaming 的取舍

Unary 每条消息创建一次逻辑 RPC，请求和响应天然一一对应，简单但高频调用有重复调度和元数据开销。双向 Streaming 长期复用 HTTP/2 流，适合连续消息，但必须自己管理：

- 每条消息的 `request_id`；
- 单 writer/reader 并发约束；
- pending map；
- 逐条 reply、超时和取消；
- 流断开后未完成请求怎样失败并重试；
- 有界队列和慢下游背压。

项目有两条核心流：

- Comet→Logic `MessageStream`：减少上行逐条 Unary RPC；当前 `grpc_stream_count=8` 分摊等待 Kafka persist delivery report 的请求。
- Job→Comet `PushStream`：每个 Comet 复用一条双向流，writer 发送，reader 按 `request_id` 唤醒等待者。

#### 5.2.3 为什么 `Write()` 成功不够

`stream->Write(request)` 返回 true，只代表 gRPC 接受了写操作。Comet 可能还没有处理请求，连接也可能紧接着断开。因此 Job 的正确完成条件是：

```text
Write 成功
  + reader 收到相同 request_id 的 PushToCometReply
  + reply.error.code == 0
```

Job 的 `pending` map 保存 `request_id → completion`；reader 收到 reply 后填充结果并唤醒 `SendToStream`。只有此时 Kafka 业务回调才返回成功。

#### 5.2.4 deadline、fallback 与重连

- `push_rpc_deadline_ms` 限制等待 reply 的时间；
- stream 超时或不可用时，Job 使用 `PushToComet` Unary 重试；
- writer 遇到断流使用指数退避重建 stream；
- 旧流 pending 请求统一标记失败，交回 Kafka 回调重试路径；
- Comet 的近期 `request_id` 集合防止 stream 重放和 Unary fallback 双重投递。

当前内部 gRPC 默认仅允许本机明文并校验角色密钥；跨机连接/监听要求配置 TLS。实际证书互通仍待测，生产还需服务发现、轮换及按需求配置 mTLS。

### 5.3 Kafka：解耦、可靠边界与 offset

#### 5.3.1 Kafka 基础模型

Kafka 由 topic、partition、record、producer、consumer group 和 offset 构成：

- 同一 partition 内 record 有序，不同 partition 没有全局顺序；
- 同一 consumer group 中，一个 partition 同时只分配给一个 consumer；
- 不同 group 可以各自完整消费同一 topic；
- offset 是“该 group 消费到哪里”，不是消息本身的全局状态；
- 扩 partition 可以提高并行度，但 key 的选择决定同一业务对象是否仍进入同一 partition。

#### 5.3.2 当前 topic 设计

| Topic             | Producer       | Consumer group               | 作用            |
| ----------------- | -------------- | ---------------------------- | ------------- |
| `persist_message` | Logic          | `spark_push_group_persist`   | 消息事实异步落 MySQL |
| `push_single`     | Logic          | `spark_push_group_single`    | 单聊在线投递        |
| `push_group`      | Logic          | `spark_push_group_group`     | 群聊/聊天室投递      |
| `broadcast_task`  | Logic HTTP/RPC | `spark_push_group_broadcast` | 广播任务          |
| `push_to_comet`   | 兼容旧配置          | 旧统一链路                        | 根目录默认链路不使用    |
| `ai_request`      | Logic          | Hermes Bridge group          | 可选 AI 请求      |
| `ai_delta`        | Hermes Bridge  | Logic Hermes delta group     | 临时流式增量        |
| `ai_reply`        | Hermes Bridge  | Logic Hermes reply group     | 最终 AI 回答      |

场景拆 topic 的价值是资源隔离：即使大群或广播出现 backlog，单聊 consumer 仍有独立 partition/线程和 lag 指标。它不是绝对隔离，因为进程 CPU、网络和 broker 仍然共享。

#### 5.3.3 Producer 可靠性配置

`common/kafka_producer.cpp` 配置：

- `acks=all`：等待 ISR 中要求的副本确认；本机单 broker 环境仍不能等价于多副本高可用；
- `enable.idempotence=true`：减少 producer retry 导致的 Kafka 层重复；
- `retries=5`、`retry.backoff.ms=500`；
- `linger.ms=5`、`batch.num.messages=1000`：用少量等待换批处理吞吐；
- LZ4/Snappy/Gzip 逐级尝试压缩；
- 后台 poll 处理 delivery report，析构前 flush。

普通实时 topic 使用异步 `Send`；`persist_message` 使用 `SendAndWait`，等待 delivery report 后才越过 accepted 边界。

#### 5.3.4 Consumer offset 的正确提交点

`common/kafka_consumer.cpp` 关闭 auto commit。业务 callback 完成后 `commitSync(message)`：

- 实时投递 callback 只有收到 Comet reply，或 Unary fallback 成功后才返回 true；
- 持久化 callback 只有 MySQL 幂等落库成功后才返回 true；
- callback 失败最多处理 3 次并退避。

这样避免了“刚把任务丢进本地线程池就提交 offset，进程崩溃后任务和消息一起丢失”的窗口。

必须注意当前代码的真实边界，按 consumer 分开讲：

- **persist** 已配置独立 `persist_message.dlq`。业务 3 次失败后 `SendAndWait` 写入死信 JSON（key/payload 用 hex，避免非法 UTF-8）；DLQ 或随后 `commitSync` 失败则停消费并离组，**不会跳过**落库事件。
- **push / broadcast** 仍是 best-effort：重试耗尽后打印 `[DLQ]` 日志并提交 offset，避免毒消息卡住分区。实时投递没有独立可重放 DLQ topic。

简历应写“persist 有死信 topic，实时 push 仍是重试+死信日志”，不能写“全链路完整 DLQ”。

#### 5.3.5 交付语义

当前整体是：

```text
Kafka at-least-once 倾向
+ SQL canonical reservation / client_msg_id 幂等
+ Comet request_id 近期去重
+ MySQL 唯一键和完整字段比较
= 业务侧幂等消费
```

Producer 幂等不等于端到端 exactly-once；Kafka 与 MySQL 之间没有分布式事务，WebSocket 终端也不参加 Kafka 事务。正确表述是“通过 at-least-once + 多层幂等实现可重试投递”。

#### 5.3.6 lag 与背压

`spark_push_kafka_lag_ms` 表示最近消费记录的年龄。lag 上升说明生产速度大于处理速度，或者下游变慢。排查顺序：

1. 看是 single、group、broadcast 还是 persist group；
2. 看 Job stream queue、reply timeout 和 reconnect；
3. 看 Comet socket fanout/慢连接；
4. 持久化 topic 再看 MySQL pool 和慢 SQL；
5. 判断是否需要增加 partition 和 consumer 实例。

### 5.4 Redis：热状态、Lua 原子性和连接池

#### 5.4.1 当前 Redis key

| Key | 类型 | 当前用途 |
|---|---|---|
| `token:<token>` / `user:tokens:<uid>` | String+TTL / Set | 凭证与撤销反向索引 |
| `route:user:<uid>` | Set | 用户所在 Comet |
| `route:generation:<uid>:<comet>` | counter | 路由注册代次 |
| `route:lease:<uid>:<comet>` | String | generation + 随机 nonce，用于 compare-delete；没有 TTL 心跳租约 |
| `room:comets:<room>` / `room:online_count:<room>` / `room:comet_count:<room>:<comet>` | Set/counter | 房间 fanout 和在线计数 |
| `session:last_seq:<sid>` / `user_session:read_seq:<uid>:<sid>` | String | 未读/已读缓存 |

`session:msg_seq` 与 `message:dedup` 仅由保留的旧 Lua API/历史 benchmark 使用；当前发送链不依赖它们。

#### 5.4.2 Redis Lua 的当前用途与历史取号

当前上线/下线以 Lua 原子注册 generation+nonce，删除时只有租约值完全匹配才 SREM。它防止旧连接的晚到下线删除新连接路由；随机 nonce 还覆盖计数器丢失后数字复用。

旧 `AllocateSessionMsgSeq` 把 floor、dedup、INCR、last_seq 放在一个 Lua 操作中，原理和全文仍见 13.4，但它只保留为历史学习/组件基准。当前原文幂等和序号在 SQL 中完成，不能用旧 Lua ops/s 推算接受路径容量。

#### 5.4.3 Redis 丢数据或水位落后怎么办

SQL `message_sequence` 是当前唯一取号权威；WS、旧 HTTP 和弹幕入口统一使用它，Redis 清空不会造成序号回退。部署升级必须排空旧事件，不能混用旧 Redis 分配器和新 SQL 分配器。

Redis 全量丢失仍会影响 token/在线路由：没有为所有已在线连接自动重注册，也没有故障 Comet 的心跳租约。需要重建连接/路由及登录凭证；序号正确不代表全链路自动恢复。真实 Redis 停启/网络黑洞实验尚未执行。

#### 5.4.4 在线路由与短缓存

`route:user:<uid>` 使用 Set，因为一个用户可能同时连接多个 Comet。Logic 的 `RedisStore` 还使用带 TTL 的本地 route cache 和 `shared_mutex`：读多写少时并发读取共享锁，路由变化时失效缓存。

缓存的基本原则：

- 命中提高吞吐；
- TTL 限制陈旧时间；
- 路由增删必须主动失效本地缓存；
- 缓存只能优化，不能成为唯一事实；
- 多 Logic 实例之间没有本地缓存广播，因此 TTL 是一致性边界。

#### 5.4.5 Token 反向索引

登录时既写 `token:<token>`，又 `SADD user:tokens:<uid> token`。管理员禁用用户时就能以 O(该用户 token 数) 删除会话。为兼容旧版本没有反向索引的 token，代码还会低频 `SCAN token:*` 并核对 value。

生产环境不应在高频路径使用 `KEYS`；`SCAN` 虽然渐进式，数据量很大时仍有额外成本。更完整设计应保证所有 token 都有可靠反向索引，并在迁移后删除 SCAN 兜底。

#### 5.4.6 Redis 连接池

`common/redis_pool.cpp` 提供：

- 初始化最小连接数；
- 连接不足时增长到 max；
- `condition_variable` 等待可用连接；
- 连接/读写超时；
- 热连接直接复用，只对空闲超过 `redis_health_check_interval_ms` 的连接做 PING；
- 业务命令发现 `redisContext` I/O 错误后归还时直接淘汰；
- 空闲连接回收；
- RAII `RedisConnGuard` 自动归还。

历史混合压测出现过 `save token to redis failed`。当前连接池默认 Acquire 等待 3 秒，失效后每 250 ms 重试补回 min，即使禁用扩容也能恢复；connect/PING 移出池全局锁。等待期限不取消底层网络调用，总时长仍受原生超时影响。认证、历史和路由会竞争这个池，需要监控等待与失败。

### 5.5 MySQL：事实存储、索引与幂等

#### 5.5.1 表职责

| 表                    | 关键字段/索引                               | 作用                                |
| -------------------- | ------------------------------------- | --------------------------------- |
| `user`               | `UNIQUE(account)`、`status/deleted_at` | 账号、密码、生命周期和软删除                    |
| `session`            | `UNIQUE(session_id)`、user/group 索引    | 会话参与者和最新序号                        |
| `message` | `UNIQUE(session_id,msg_seq)` | 持久消息历史；图片只存 attachment id |
| `message_sequence` | `PRIMARY(session_id)` | 持久会话序号；事务持行锁 |
| `message_reservation` | 会话序号唯一、会话/发送者/client ID 唯一 | canonical 原文、二进制 outbox、发布/持久化阶段 |
| `message_delivery_receipt` | 用户/会话/序号唯一 | 逐条投递证明，推动连续水位 |
| `attachment`         | `PRIMARY(id)`、owner/session 索引        | 图片元数据；字节在 `data/attachments/<id>` |
| `user_session_state` | `UNIQUE(user_id,session_id)`          | `read_seq` 与 `delivered_seq`      |
| `im_group`           | owner 索引                              | 群/聊天室元数据                          |
| `group_member`       | `UNIQUE(group_id,user_id)`            | 成员关系与角色                           |
| `video_danmaku`      | `(video_id,timeline_ms)`              | 时间轴查询                             |
| `broadcast`          | `UNIQUE(task_id)`                     | 广播任务记录                            |
| `audit_log`          | target/action + created_at            | 追加式用户中心审计                         |

#### 5.5.2 为什么接受路径仍需要 SQL，但历史由 Job 写

2026-10-11 前的 Redis 取号方案虽快，却不能持久保留重试原文，也无法在缓存丢失和旧 SQL 入口混写时维持统一序号。当前明确选择正确性优先：

```text
Logic SQL 事务取号 + canonical reservation
  → SQL prepare 二进制 outbox
  → persist_message Kafka delivery report
  → accepted_ack
  → Job 幂等写历史、stage=3
```

因此不能再称“MySQL 完全移出热路径”。在线受理包含同步 SQL 往返、行锁和 prepare；message 历史仍异步落库。prepared 事件可以在 Logic 重启后恢复，Kafka 确认失败仍需同 ID 重试来消除不确定性。

旧 HTTP `/api/message/send` 已要求 Bearer、校验 actor/目标与限流，并使用同一 SQL 序号权威同步写历史。它仍保留旧响应，不宣称 WS 的两阶段 ACK/实时投递合同；弹幕也共用序号权威。

#### 5.5.3 幂等落库

Job 写 message 时可能遇到 Kafka 重放：

- 如果 `(session_id,msg_seq)` 不存在，正常插入；
- 如果唯一键已存在且完整字段一致，视为幂等成功；
- 如果相同序号对应不同内容，视为 sequence collision，不能静默覆盖；
- session last_seq 使用 `GREATEST`，避免乱序落库导致水位回退；
- read_seq 使用 GREATEST；delivered_seq 先按 receipt 和已结算前缀计算连续水位，再单调更新，避免跨过尚未落库的低序号。

“唯一键冲突”不应该全部忽略，否则真正的数据冲突会被伪装成幂等成功。

`content_json` 已从 `TEXT` 升为 `MEDIUMTEXT`（迁移 `sql/migrations/002_message_content_mediumtext.sql`）。读取仍先绑 64 KiB 缓冲；`MYSQL_DATA_TRUNCATED` 时按实际长度 `mysql_stmt_fetch_column` 再取一列，避免长 Agent 回复被静默截断。这不扩大模型上下文窗口。逐步算法见第 13.21 节。

#### 5.5.4 历史分页和索引

历史查询使用：

```sql
WHERE session_id = ? AND msg_seq < ?
ORDER BY msg_seq DESC
LIMIT ?
```

这是游标分页而不是大 offset 分页。`(session_id,msg_seq)` 唯一索引同时支持过滤和有序扫描；大页码不会像 `OFFSET 100000` 那样先扫描并丢弃大量记录。

#### 5.5.5 Prepared Statement 与连接池

DAO 使用 `MYSQL_STMT` 绑定参数，避免拼接输入造成 SQL 注入，并降低重复解析成本。`common/mysql_pool.cpp` 与 Redis pool 类似，支持最小/最大连接、超时等待、健康检查、空闲回收和 RAII guard。

连接池不是越大越好：过多连接会增加 MySQL 线程、内存和锁竞争。应根据请求并发、单 SQL 时延、数据库能力和等待指标计算，而不是简单等于 WebSocket 数量。

### 5.6 C++17 并发、RAII 与线程模型

#### 5.6.1 项目中的并发单元

| 位置                  | 并发模型                                        |
| ------------------- | ------------------------------------------- |
| Comet WebSocket     | Muduo EventLoop + I/O worker                |
| Comet→Logic         | 多条 gRPC MessageStream                       |
| Comet Unary gRPC 工作 | `ThreadPool`                                |
| Logic HTTP | 默认 4 个 IO worker；同步业务仍占工作线程；gRPC server 独立等待 |
| Kafka producer      | 调用线程 produce + 后台 poll thread               |
| 每个 Kafka consumer   | 一个阻塞 consume thread                         |
| 每个 Job→Comet stream | writer thread + reader thread + pending map |
| delivered ACK | 有界线程池 |
| Comet 鉴权/下线/房间 RPC | 可 join 的有界池；握手结果切回 IO 线程 |
| Logic outbox 恢复 | PeriodicWorker；每秒扫描 prepared 事件 |
| Comet 上行 deadline | 独立 100 ms watchdog；不依赖 writer 继续执行 |

#### 5.6.2 常用同步原语

- `std::mutex`：保护连接表、队列、pending map 和 metrics；
- `std::shared_mutex`：路由本地缓存读多写少；
- `std::condition_variable`：连接池等待、stream queue、请求完成通知；
- `std::atomic`：运行标记、压测计数器；
- `std::thread`：Kafka loop、gRPC Wait、stream reader/writer；
- `std::shared_ptr`：跨线程 send/startRead/stopRead 闭包持有连接与数据；定时器 weak_ptr 防止延迟回调延长或误用生命周期。

加锁原则：锁内只修改共享状态，网络 I/O、数据库 I/O 和耗时计算尽量放到锁外；多个 mutex 同时获取时使用 `std::scoped_lock` 降低死锁风险。

#### 5.6.3 RAII

RAII 把资源生命周期绑定到对象：

- `RedisConnGuard`/`MySqlConnGuard` 析构自动归还连接；
- `unique_ptr` 管理 producer、consumer、stub 和 stream；
- thread 在 Stop/析构阶段 join；
- gRPC context、MySQL statement、Redis reply 都应在所有分支释放。

面试时可强调：RAII 不只是智能指针，它适用于锁、fd、数据库连接、线程和任何必须成对释放的资源。

### 5.7 限流、背压与故障隔离

#### 5.7.1 令牌桶

令牌桶以固定速率补充 token，请求消耗 token；桶容量允许短时 burst。项目按场景配置：

| 场景             | 默认 rate/s | burst | key   |
| -------------- | ---------:| -----:| ----- |
| single         | 1000      | 200   | 发送用户  |
| group/chatroom | 300       | 60    | 房间或用户 |
| broadcast      | 20        | 5     | 广播场景  |

超限返回 429，并增加 `spark_push_rate_limited_total_<scene>`。当前桶在单个 Logic 进程内，多个 Logic 实例时不是全局限流；生产环境可按需求使用 Redis/网关限流或一致性哈希把同 key 固定路由。

#### 5.7.2 有界队列

Job 的 `push_stream_queue_max=10000`；Comet→Logic 每流队列+pending 共 512 项、排队数据 8 MiB；通用池排队 1024 项/提交权重 32 MiB；Comet 单连接输出 4 MiB；Kafka producer 10000 项/32 MiB。满时显式拒绝或关闭慢连接。上限要按消息大小与机器内存调整，并不保证默认 10000 个慢连接内存安全。

完整背压链路应同时观察：

```text
客户端发送速率
→ Logic 限流
→ Kafka backlog
→ Job stream queue
→ gRPC flow control
→ Comet socket write buffer
→ 慢客户端
```

### 5.8 安全与用户中心

#### 5.8.1 密码与 token

`common/security.cpp` 使用：

- PBKDF2-HMAC-SHA256；
- 16 字节随机 salt；
- 120000 次迭代；
- 32 字节 digest；
- `CRYPTO_memcmp` 常量时间比较；
- 兼容旧 MD5，成功登录后可触发 rehash；
- 32 字节 CSPRNG token，再 hex 编码。

密码哈希必须带盐并使用慢 KDF；普通 SHA256/MD5 即使加盐也太快，容易被 GPU 暴力破解。

#### 5.8.2 Bearer 鉴权和授权

受保护 HTTP API 从 `Authorization: Bearer <token>` 解析用户，再从 Redis 验证 token，并查询 MySQL 用户仍为 active。历史接口还校验请求者属于目标会话，这属于对象级授权。

管理员账号来自环境变量，不写入仓库。禁用/软删除会撤销 token、清理路由并写 audit_log。软删除保留消息外键语义和审计，不直接物理删除历史事实。

内部 RPC 已区分 COMET/JOB/可选 ADMIN 密钥，缺少配置时启动失败；默认仅监听 loopback。跨机监听/连接必须配置服务器证书/私钥与客户端 CA，真实证书互通仍待测。公网 HTTP/WS 默认仍是明文，需要部署 HTTPS/WSS；mTLS、统一 IAM 和密钥托管尚未实现。

### 5.9 可观测性

#### 5.9.1 三类指标

| 类型           | 例子                                               | 用途         |
| ------------ | ------------------------------------------------ | ---------- |
| Counter      | accepted、delivery_success、reconnect、rate_limited | 累计事件数，只增不减 |
| Gauge        | kafka_lag_ms、stream_ready                        | 当前状态或最近值   |
| 简化 histogram | latency `_count/_sum/_max`                       | 次数、总和和最大值  |

`common/metrics.cpp` 是轻量 Prometheus text registry，不是真正带 bucket 的 Prometheus Histogram，因此服务端本身不能直接给出 p95/p99；分位数由 E2E 压测器保留样本后计算。

#### 5.9.2 重点指标

- Logic：accepted、duplicate、persist failure、realtime enqueue failure、rate limited；
- Job：Kafka lag、delivery attempt/success/lost、stream write/reply timeout、queue rejected、reconnect；
- Comet：连接、离线补推、重复 request、cursor update failure、stream ready；
- Hermes：request/reply/error、delta、TTFT、总延迟、prompt 大小。
- Logic/Job：关键 consumer_failed、service readiness、DLQ 与 outbox 恢复失败；`/ready=200` 不证明 DLQ 为空或存储无积压。
- 日志：默认仅写文件，50 MiB 轮转并保留 5 个历史文件；原生 stderr 位于 .out，排查需同时看 .log/.out。

指标必须与日志和业务结果结合。例如 lag=0 只能说明 consumer 追平，不能证明客户端实际收到；delivery_success 增加也要结合独立接收端验证。

---

## 6. 源码导航：遇到问题应该看哪里

### 6.1 推荐阅读顺序

1. `docs/spark-push-architecture.md` 或本文第 1～4 章：三进程职责和两条消息链路；
2. `proto/spark_push.proto`：先理解 ChatMessage、ACK、游标和三组 Service；
3. `comet/comet_server.cpp`：看客户端怎样接入、上行怎样转发、下行怎样 fanout；
4. `logic/services/message_service.cpp`：看凭证/actor 校验、SQL 原文预留、Kafka 两条分支和限流；`grpc_service.cpp` 做 RPC/DTO 与工作负载身份检查；
5. `logic/message_reservation.cpp` / `logic/conversation_store.cpp`：看事务序号、outbox 和安全前缀；`redis_store.cpp` 看 token/路由代次；
6. `job/service.cpp`：看 consumer、PushStream、reply、fallback 和持久化；
7. `common/kafka_consumer.cpp`：看 retry、offset 和 persist DLQ；
8. `android-app/.../SparkViewModel.kt`：看 TreeMap 流式合并和连接代数；
9. `sql/schema.sql`：看数据约束和索引；
10. `load_test/e2e_bench.cpp`：看怎样定义“真实投递完成”。
11. 本文第 13 章：把上述文件追到逐步算法。

### 6.2 按问题定位

| 现象/问题                         | 先看                                                    | 再看                                                                  |
| ----------------------------- | ----------------------------------------------------- | ------------------------------------------------------------------- |
| 登录失败 | `logic/services/user_service.cpp`；HTTP 解析看 `http_server.cpp` | `common/security.cpp`、`UserRepository`、`user_dao.cpp`、`redis_store.cpp` |
| WebSocket 握手失败                | `comet/comet_server.cpp`                              | `LogicServiceImpl::VerifyToken`、Redis token                         |
| accepted ACK 没有返回 / 气泡停在「发送中」 | Comet `MessageStream` 是否 `broken`、是否已重连               | `ReconnectStream`；握手 Unary 成功不代表上行流活着                               |
| accepted 有、实时消息没有             | Redis route                                           | `push_single` lag、`JobRunner::ProcessPushRequest`、Comet target      |
| delivered ACK 没有              | Job `StreamReaderLoop`                                | `SendDeliveryAck`、来源 Comet 路由                                       |
| 重复消息 | SQL reservation 原文/清理窗口 | Comet AcceptPushRequest、历史唯一键、前端去重 |
| msg_seq 冲突 | message_sequence 是否被旧分配器混写 | Reserve 事务、历史唯一键与完整字段比较 |
| 重连后缺消息                        | `SyncOffline`                                         | `user_session_state.delivered_seq`、历史落库 lag                         |
| 重连后重复很多                       | Comet 握手补推                                            | `MarkDelivered`、同账号多连接                                              |
| 历史接口慢 | `ConversationService::History`；入口 `handleHistory` | `MessageDao::ListMessages`、MySQL pool 和索引 |
| 群聊拖慢单聊                        | topic/group 配置                                        | rate limiter、Job queue、broker partition                             |
| Job lag 增长                    | `common/kafka_consumer.cpp`                           | PushStream、MySQL 或 Comet 下游                                         |
| Hermes/Pi 首字慢                 | `hermes_bridge/hermes_client.cpp`、`sse_parser.cpp`    | Pi gateway、模型 TTFT、prompt 长度、48B/50ms 合并                            |
| Android 流式乱序/卡住               | `SparkViewModel.kt` `acceptDelta`                     | envelope sequence、最终 `ai_reply`、连接代数                                |
| 长回复被截断                        | `logic/message_dao.cpp` `AssignTextColumn`            | MEDIUMTEXT 迁移、`SPARK_PUSH_LENGTH_AUDIT`                             |
| 图片发不出去 / Agent 看不见图 | `logic/services/attachment_service.cpp`、`message_service.cpp::BuildHermesRequest` | HTTP 附件接口、`data/attachments/`、Bridge `LoadPiImagesFromDir`、Pi `prompt.images` |

### 6.3 关键类职责

| 类/文件                                      | 一句话职责                                     |
| ----------------------------------------- | ----------------------------------------- |
| `CometServer`                             | WebSocket 连接和本机 fanout 的所有者               |
| `CometServiceImpl`                        | 接收 Job gRPC 下推并调用 CometServer             |
| `LogicServiceImpl` | 内部 gRPC 协议适配：DTO/Protobuf 与 Unary/Stream |
| `HttpApiServer` | HTTP 路由、解析、凭证提取、调用 Service 与响应映射 |
| `logic/services/` | 八个业务服务集中规则与用例编排，详见第 16 章 |
| `logic/repositories/` | 实现核心业务存储接口，复用已有 DAO/Store/Redis |
| `AttachmentDao`                           | 图片元数据入库与本地文件原子 rename                     |
| `ConversationStore` | 串联 session/message/state DAO、SQL reservations 与未读缓存 |
| `MySqlMessageReservations` | 持久序号、canonical 原文、outbox 阶段与安全前缀 |
| `RedisStore`                              | 封装 Redis key、路由缓存、Lua 和在线计数               |
| `JobRunner`                               | Kafka 四类 consumer 与 Job→Comet stream 生命周期 |
| `KafkaProducer/Consumer`                  | librdkafka 可靠性参数、poll、retry 和 commit      |
| `MySqlConnectionPool/RedisConnectionPool` | 连接复用、扩缩、校验、等待和 RAII                       |
| `SceneRateLimiter`                        | 单进程分场景令牌桶                                 |
| `MetricsRegistry`                         | 线程安全的 Prometheus text 指标注册表               |
| `SparkViewModel`                          | Android 游标、TreeMap 流式合并、重连                |
| `HermesSseParser`                         | Pi/Hermes SSE 行状态机与终态校验                   |
| `AgentStore`                              | SQLite route/turn/event 账本                |

### 6.4 配置定位

| 配置    | 文件                  | 常用字段                                                           |
| ----- | ------------------- | -------------------------------------------------------------- |
| Logic | `conf/logic.conf`   | HTTP/gRPC 端口、Redis/MySQL pool、Kafka topics、rate limit、Hermes   |
| Comet | `conf/comet.conf`   | WS/gRPC/metrics 端口、Comet ID、I/O 线程、MessageStream 数             |
| Job   | `conf/job.conf`     | topic/group、Comet targets、PushStream queue/deadline、MySQL pool |
| 敏感变量  | `.env.local`        | MySQL 密码、管理员口令、Hermes key；不提交 Git                              |
| 数据结构  | `sql/schema.sql`    | 全量建表，默认不删除数据                                                   |
| 显式清库  | `sql/reset_dev.sql` | 仅确认开发数据可删除时手动执行                                                |

### 6.5 HTTP API 速查与兼容边界

路由集中注册在 `logic/http_server.cpp`：

| 类别   | API                                                              | 鉴权/说明                                     |
| ---- | ---------------------------------------------------------------- | ----------------------------------------- |
| 公开   | `/api/register`、`/api/login`                                     | 创建用户、签发 token                             |
| 用户会话 | `/api/session/history`、`mark_read`、`unread`、`list_single`        | Bearer token；历史还校验会话成员                    |
| 图片附件 | `/api/attachment/upload`、`/api/attachment/get`                   | Bearer；校验会话成员；二进制不进 WS                    |
| 聊天室  | `/api/chatroom/join`、`leave`、`unsubscribe`、`online_count`、`list` | 主要使用 Bearer，并核对 body user_id 与 token 用户   |
| 弹幕   | `/api/danmaku/send`、`list`                                       | send 兼容在 JSON body 传 token；不是统一 Bearer 形式 |
| 管理   | `/api/admin/user/*`、`audit/list`、`chatroom/*`、`broadcast`        | 管理员 Bearer token                          |
| 兼容发送 | `/api/message/send` | Bearer；from_user_id 必须等于认证用户；共用 SQL 序号同步写历史，仍保留旧响应 |

兼容边界仍需明确：弹幕保留 body token 形式，旧 HTTP send 不等于 WS 可靠发送流程；不能把已补鉴权写成所有协议和部署安全都已完成。HTTP parser 已加入分包推进、长度上限和异常响应，见第 19 章。

### 6.6 配套专题文档

本文可独立用于复习。仓库里的分层文档与本章对应关系：

- [大框架](spark-push-architecture.md)：模块职责与完整链路（含第 2.5 节图片），对应本文第 1～4 章；
- [小模块逐步算法](spark-push-internals.md)：Lua/握手/队列/TreeMap/图片附件，对应本文第 13 章；
- [阅读索引](spark-push-implementation.md)：两层文档入口、Android 与长度审计；
- [项目 README](../README.md)：最短启动与当前能力；
- [可靠性优化](reliability-optimization.md)：ACK、游标、PushStream 和指标；
- [工程审查修复](review-remediation.md)：当前 SQL 预留、生命周期、安全、迁移与验证回执；旧专题中的取号算法应按此校准；
- [项目演进与面试](interview-project-evolution.md)：从早期 Demo 到当前版本的阶段记录；
- [用户中心](user-center.md)：管理员 API、状态和审计；
- [Pi Agent 集成](pi-agent-integration.md)：本机 gateway、SSE、引用、截图问答和二次开发；
- [多 Agent](multi-agent-integration.md)：路由、粘滞、事件回放与渠道隔离；
- [压测工具说明](../load_test/README.md)：基础客户端参数；
- [启动说明](../scripts/00_prepare_and_run.md)：依赖和网络环境。

---

## 7. 构建、运行与功能验证

### 7.1 环境准备

```bash
cd /home/peco/cppcode/fenbushi/11.2-spark_push
cp .env.example .env.local
${EDITOR:-vi} .env.local
```

至少配置：

```dotenv
SPARK_PUSH_MYSQL_PASSWORD=<本机开发密码>
```

管理员和 Hermes 均为可选配置。真实口令、token、数据库 dump 和运行日志不要提交。

### 7.2 一键启动与停止

```bash
bash scripts/start_demo.sh
```

新建环境中脚本启动依赖、建 topic、Release 编译并载入 .env.rpc 的角色密钥，再检查 readiness 与两组 stream。存量环境必须先完成第 19.5 节 SQL/Kafka 升级；脚本不自动迁移业务数据。

只复用已经启动的依赖或二进制：

```bash
bash scripts/start_demo.sh --skip-deps
bash scripts/start_demo.sh --skip-build --skip-deps
```

停止业务：

```bash
bash scripts/stop_demo.sh
```

如果还要停止 Compose 中间件并保留数据卷：

```bash
docker compose --env-file .env.local stop redis mysql kafka
```

### 7.3 手动编译与测试

```bash
cmake -S . -B build \
  -DCMAKE_BUILD_TYPE=Release \
  -DBUILD_TESTING=ON
cmake --build build -j"$(nproc)"
ctest --test-dir build --output-on-failure
```

主要地址：

| 服务              | 地址                                          |
| --------------- | ------------------------------------------- |
| 用户页面            | `http://127.0.0.1:9010/index.html`          |
| 管理页面            | `http://127.0.0.1:9010/admin_chatroom.html` |
| Logic HTTP      | `http://127.0.0.1:9101`                     |
| Comet WebSocket | `ws://127.0.0.1:9000/ws`                    |
| Logic metrics   | `http://127.0.0.1:9101/metrics`             |
| Job metrics     | `http://127.0.0.1:9202/metrics`             |
| Comet metrics   | `http://127.0.0.1:9203/metrics`             |

从 Windows 访问 WSL/虚拟机时，应使用虚拟机网卡 IP；Windows 的 `127.0.0.1` 不一定映射到虚拟机服务。

### 7.4 最小功能验证

注册两个账号后运行：

```bash
./build/load_test/e2e_bench \
  --sender-account <sender> --sender-password '<password>' \
  --receiver-account <receiver> --receiver-password '<password>' \
  --connections 4 --messages-per-conn 25 --timeout-ms 30000
```

严格通过条件：

```text
sent == accepted_ack == delivered_ack == delivered
send_failed == 0
ack_errors == 0
```

`delivered` 是独立接收方 WebSocket 实际读取的本轮唯一消息数。旧 `load_tester ws_send` 只统计 socket send，不能代替 E2E 实际投递。

### 7.5 持久化验证

实时 E2E 结束后让 Job 继续运行几秒，再检查 persist consumer group 和 MySQL：

```bash
docker exec spark-kafka rpk group describe spark_push_group_persist \
  --print-commits
```

```sql
SELECT COUNT(*) AS total_rows,
       COUNT(DISTINCT msg_seq) AS unique_seq,
       MIN(msg_seq), MAX(msg_seq)
FROM message
WHERE session_id = 's_<小uid>_<大uid>';

SELECT last_msg_seq
FROM session
WHERE session_id = 's_<小uid>_<大uid>';
```

实时 delivered 与 MySQL stored 是两个阶段。测试结束后立刻停 Job，可能看到实时消息已到而持久化 consumer 尚未追平；这不应被误判为已经丢失，也不能因此忽略 persist lag。

### 7.6 故障注入建议

| 故障           | 操作                                  | 应验证的行为                                  |
| ------------ | ----------------------------------- | --------------------------------------- |
| Redis seq 落后 | 将 Redis seq/last_seq 调低，MySQL 保持高水位 | Logic 重启后以 MySQL max floor 继续分配，不冲突     |
| Job 重启       | 投递期间终止并重启 Job                       | 未提交 record 重放，Comet request_id 去重       |
| Comet 重启     | 中断 PushStream                       | Job reconnect 增加，pending 失败后重试/fallback |
| MySQL 暂停     | 停止 MySQL                            | persist lag 增长，实时链路可能继续，历史暂时落后          |
| 接收端断线        | 发送期间断开后重连                           | `SyncOffline`/缺口同步补齐，客户端去重              |
| 广播突发         | 单聊同时发广播                             | single topic 和延迟不应被完全挤占                 |

故障注入不能只看进程没有崩溃；还要核对消息数量、唯一序号、offset、游标、重复数和最终收敛。

---

## 8. 测试方法、性能结果与排障

### 8.1 性能测试方法

性能报告至少同时记录：

- 被测源码版本和是否有未提交改动；
- CPU、内存、磁盘、fd 上限和部署拓扑；
- Release/Debug 构建；
- 并发连接、消息总量、消息大小和发送模型；
- 吞吐、p50/p95/p99、成功/失败/重复；
- Kafka lag、队列拒绝、超时、进程 CPU/RSS；
- 是否包含 Redis、MySQL 持久化和真实接收端。

本项目专项工具：

| 工具                     | 测量内容                                 |
| ---------------------- | ------------------------------------ |
| `e2e_bench`            | accepted、delivered ACK、实际接收和 E2E 分位数 |
| `connection_bench`     | 空闲连接、心跳和重连风暴                         |
| `room_fanout_bench`    | Comet 房间 socket fanout               |
| `storage_bench`        | Redis Lua 与 MySQL InsertMessage 隔离性能 |
| `kafka_delivery_bench` | 隔离 Kafka→Job→Comet→WebSocket         |
| `history_bench`        | 带 Bearer 的历史查询吞吐与分位数                 |

### 8.2 2026-08-27 根目录版本本机实测

#### 环境与口径

| 项目    | 配置                                                 |
| ----- | -------------------------------------------------- |
| 源码    | 根目录当前工作区，基线提交 `01ab75d`，含测试前已有未提交功能改动              |
| 主机    | 12 逻辑 CPU、15 GiB 内存、4 GiB swap                     |
| fd 上限 | 1048576                                            |
| 部署    | Logic、Comet、Job、客户端同机；Redis/MySQL/Kafka 为本机 Docker |
| 口径    | 以独立 WebSocket 实际读取唯一 `client_msg_id` 为投递成功         |

所有数字只用于当前单机同部署版本，不能外推为生产集群 SLA。

#### 1）单聊 E2E 实际投递

所有档位均零发送失败、零 ACK 错误、零实际丢失。

| 发送连接 × 每连接消息 | 实际投递      | 耗时（s） | msg/s  | p50（ms） | p95（ms） | p99（ms） |
| ------------:| ---------:| -----:| ------:| -------:| -------:| -------:|
| 4 × 100      | 400/400   | 0.765 | 522.95 | 410.07  | 730.42  | 758.49  |
| 8 × 100      | 800/800   | 1.426 | 561.09 | 732.60  | 1355.42 | 1410.80 |
| 16 × 100     | 1600/1600 | 2.818 | 567.79 | 1394.68 | 2666.70 | 2772.57 |
| 32 × 100     | 3200/3200 | 5.535 | 578.13 | 2830.16 | 5256.25 | 5464.78 |

结论：无节流突发模型下约 550～580 条实际消息/秒进入平台区；继续增加并发主要增加排队和 p99。这里的延迟包含整批突发排队，不是低负载单条 RTT。

#### 2）空闲连接、活跃连接和重连

| 场景        | 结果                                                     |
| --------- | ------------------------------------------------------ |
| 基线        | Comet RSS 15084 KiB、VSZ 1526548 KiB、FD 52、33 threads   |
| 1000 空闲连接 | 1000/1000，0.505 s，1981.34 conn/s；RSS 19748 KiB、FD 1052 |
| 空闲连接增量    | RSS +4664 KiB，约 4.66 KiB/连接；VSZ +约 515 MiB，虚拟空间不等于常驻内存 |
| 1000 活跃连接 | 0.463 s 建连；10 秒共 10000 次 heartbeat，0 失败                |
| 3×1000 重连 | 3000/3000；三轮 2758.39、2804.44、2594.38 conn/s            |

异常样本：用有大量历史的同一账号建立 1000 条连接，Comet RSS 从约 23.3 MiB 升至约 2.05 GiB，`offline_repush_messages_total` 增加 28400。它反映的是离线补推放大，不是空闲连接固有成本。

#### 3）热点接收者和房间 fanout

热点用户持有 10 条连接，400 条消息形成 4000 次实际投递：4000/4000，5814.32 deliveries/s，p99 679.82 ms。

| 房间连接 | 实际投递        | deliveries/s | p50（ms） | p95（ms） | p99（ms） |
| ----:| -----------:| ------------:| -------:| -------:| -------:|
| 10   | 1000/1000   | 4721.04      | 123.24  | 203.00  | 209.81  |
| 100  | 10000/10000 | 37485.70     | 151.73  | 253.80  | 262.72  |
| 500  | 50000/50000 | 74988.70     | 341.95  | 622.56  | 646.17  |

这组大房间测试使用同一已入房用户的多条连接，准确测量 Comet 对 500 条本机 socket 的复制发送，不是 500 个独立账号的完整成员查询和多 Comet 路由能力。

#### 4）Redis Lua 与 MySQL 同步 INSERT

Redis `AllocateSessionMsgSeq`，每档 20000 次：

| 线程  | ops/s    | p50（ms） | p95（ms） | p99（ms） |
| ---:| --------:| -------:| -------:| -------:|
| 1   | 4162.38  | 0.216   | 0.385   | 0.553   |
| 4   | 12533.80 | 0.305   | 0.453   | 0.557   |
| 16  | 23697.50 | 0.643   | 1.015   | 1.261   |

MySQL `MessageDao::InsertMessage`，每档 5000 次：

| 线程  | rows/s  | p50（ms） | p95（ms） | p99（ms） |
| ---:| -------:| -------:| -------:| -------:|
| 1   | 257.19  | 3.653   | 5.533   | 7.917   |
| 4   | 775.59  | 4.928   | 7.045   | 9.982   |
| 16  | 2117.03 | 6.835   | 10.379  | 16.439  |

Redis 与 MySQL 操作语义不同，不能当作同类数据库横评。这组数据说明当时选择 Redis Lua 的性能动机；当前为原文幂等与恢复改用 SQL reservation，不能据此推算新链路吞吐。

#### 5）Kafka→Job→Comet 隔离链路

| 突发消息  | 实际收到        | msg/s   | p50（ms） | p95（ms） | p99（ms） |
| -----:| -----------:| -------:| -------:| -------:| -------:|
| 1000  | 1000/1000   | 1514.17 | 315.82  | 607.32  | 629.87  |
| 5000  | 5000/5000   | 1461.80 | 1493.29 | 3154.53 | 3265.65 |
| 10000 | 10000/10000 | 1771.76 | 2706.45 | 5113.90 | 5317.85 |

累计 metrics：16000 次 attempt、success 和 stream write 全部对应；没有 queue rejected、reply timeout 或 delivery lost。最后一档 `kafka_lag_ms=5371`，与客户端 p99 5.32 s 基本一致，说明突发队列积压直接推高尾延迟。

#### 6）历史查询读写混合

历史接口每次返回最近 50 条，8 线程共 4000 次；写入为 32×200 条完整单聊。

| 场景    | 成功/失败  | 吞吐           | p50（ms） | p95（ms）  | p99（ms）  |
| ----- | ------:| ------------:| -------:| --------:| --------:|
| 历史只读  | 4000/0 | 380.04 QPS   | 19.79   | 27.05    | 32.91    |
| 混合时历史 | 4000/0 | 345.23 QPS   | 21.66   | 30.08    | 32.74    |
| 纯写基线  | 6400/0 | 591.12 msg/s | 5488.22 | 10335.60 | 10715.20 |
| 混合时写入 | 6400/0 | 588.56 msg/s | 5448.32 | 10279.20 | 10729.80 |

混合时历史吞吐下降约 9.2%，p50/p95 上升约 9.4%/11.2%；单聊写入差异在本轮噪声范围。另一次先压历史再同时登录 33 个连接的样本出现 `save token to redis failed`，历史 3992 成功、8 失败，暴露 Redis 连接池共享竞争风险。

### 8.3 2026-09-01 性能优化实施记录

本轮只做不改变 accepted/delivered、序号、幂等和 Kafka offset 语义的优化。核心思路是：先用分层基准找到额外往返和串行点，再用同机 A/B 证明改动，不通过放宽限流伪造 QPS。

#### 8.3.1 优化前的定位

| 热点             | 代码现象                                                                                       | 影响                                            |
| -------------- | ------------------------------------------------------------------------------------------ | --------------------------------------------- |
| Redis 借连接      | `common/redis_pool.cpp` 原来每次 `Acquire()` 都执行 `PING`                                        | 一次 Lua/查询业务命令被放大成 `PING + 业务命令` 两次 Redis 往返   |
| 首条会话序号         | `ConversationStore::AppendMessageHotPath()` 用一把全局锁包住 MySQL `MAX(msg_seq)` 和 Redis floor 校准 | 多个不同会话首次发消息也必须排队                              |
| Comet→Logic 上行 | 每条 `MessageStream` 内同步处理，`PersistToTopic()` 又等待 Kafka delivery report                      | 每条 stream 同一时刻只有一条消息跨越 accepted 边界，4 条流的并行度偏低 |

优化前同日基线：Redis Lua 热路径 16 线程、2 万次为 `21270.6 ops/s`，p50/p95/p99 分别为 `0.690/1.108/1.407 ms`；16×100 单聊 E2E 为 `499.08 msg/s`，p99 `3165.83 ms`。后一项含 1600 条整批突发排队，不是低负载 RTT。

#### 8.3.2 代码改动

1. **Redis 热连接按空闲时间检测**

   - `RedisConnectionPool` 在空闲队列中已保存 `last_used`，现在只有连接空闲超过 `redis_health_check_interval_ms` 才 `PING`；根目录默认为 30000 ms。
   - 新建连接已经完成 connect/AUTH/SELECT，不再立即多做一次 `PING`。
   - 如果业务命令已将 `redisContext::err` 置为错误，`Release()` 直接销毁连接，不让断链连接回池后反复失败。
   - 保留建连、健康检查、借连接超时、空闲关闭和坏连接淘汰等低频 metrics。没有在每次借还时调用当前带全局 mutex 的 `MetricsRegistry`，避免为优化热路径又引入新的串行点。
   - 配置落点：`common/config.h/.cpp`、`logic/main.cpp`、`conf/logic.conf`；基准工具 `load_test/storage_bench.cpp` 也传递同一参数。

2. **会话序号 floor 初始化锁分片**

   - 将原来的一把 `seq_seed_mu_` 改为 64 个 shard，按 `hash(session_id) % 64` 定位。
   - 同一会话仍串行执行 MySQL max floor 校准，不会并发重复初始化；不同会话通常可并行。
   - 这一项主要改善多会话冷启/重启后的首消息，不改变同会话序号语义。

3. **Comet→Logic 上行流从 4 条增加到 8 条**

   - `conf/comet.conf` 的 `grpc_stream_count` 从 4 调到 8，与 Comet 8 个 I/O 线程对齐。
   - 单条 stream 内的处理和 `request_id` 响应匹配没有改，只增加等待 Kafka persist delivery report 时的并行度。
   - 没有直接把 persist producer 改成“发送后立即 accepted”，因为那会把 ACK 边界从“broker 已确认”退化为“进程内已排队”。

#### 8.3.3 复测结果

Redis 基准参数与优化前一致：16 线程、2 万次 `AllocateSessionMsgSeq` Lua 原子操作。优化后连续运行三次，用中位数降低单次抖动的影响。

| 阶段     | ops/s                       | p50（ms）               | p95（ms）               | p99（ms）               | 失败    |
| ------ | ---------------------------:| ---------------------:| ---------------------:| ---------------------:| -----:|
| 优化前    | 21270.6                     | 0.690                 | 1.108                 | 1.407                 | 0     |
| 优化后三次  | 31867.9 / 32556.4 / 37573.1 | 0.477 / 0.464 / 0.414 | 0.777 / 0.787 / 0.647 | 1.104 / 1.124 / 0.791 | 0     |
| 优化后中位数 | **32556.4**                 | **0.464**             | **0.777**             | **1.104**             | **0** |

相对优化前，Redis 热路径吞吐提升约 **53.1%**，p50/p95/p99 分别降低约 **32.7%/29.9%/21.6%**。

E2E 使用相同二进制、同一组账号、同机组件和 16×50=800 条突发，只切换 `grpc_stream_count`。这个规模没有触发既有 1000/s、burst 200 的单聊令牌桶。

| 上行 gRPC 流 | sent/accepted/delivered ACK/实际投递 | msg/s       | p50（ms）    | p95（ms）    | p99（ms）    |
| ---------:| --------------------------------:| -----------:| ----------:| ----------:| ----------:|
| 4（原配置）    | 800/800/800/800                  | 621.43      | 650.29     | 1221.22    | 1272.27    |
| 8（优化配置）   | 800/800/800/800                  | **1100.55** | **383.18** | **686.11** | **711.06** |

同机 A/B 下吞吐提升约 **77.1%**，p99 降低约 **44.1%**，两组均为零发送失败、零 ACK 错误、零实际缺失。

优化后另用 16×100=1600 条突发做过压力边界检查：1589 条成功消息的 p99 为 1410.41 ms，11 条被 Logic 以明确 429 拒绝，指标 `spark_push_rate_limited_total_single=11`。由于 benchmark 等待“本轮 1600 条全部投递”直到 30 s 超时，其输出的 52.99 msg/s 是被超时分母污染的数字，**不能当作系统 QPS**。这个样本反而说明优化后处理速度已能触发现有准入上限；性能测试不应在不说明的情况下放宽安全限流。

历史查询回归：8 线程共 4000 次、每次最近 50 条，4000/0 成功/失败，361.07 QPS，p50/p95/p99 为 21.68/26.52/31.72 ms。本轮没有保留与之完全同版本的优化前历史查询样本，因此只将它作为功能/性能回归，不宣称提升百分比。

#### 8.3.4 验证与当前边界

| 验证                                               | 结果                                                                         |
| ------------------------------------------------ | -------------------------------------------------------------------------- |
| `cmake --build build-wsl -j2`                    | 全部目标编译通过                                                                   |
| `ctest --test-dir build-wsl --output-on-failure` | 7 项通过，Redis 集成项按设计在未启用环境变量时 skip                                           |
| 启用 `SPARK_PUSH_RUN_REDIS_TESTS=1` 的 Redis 集成测试   | 通过，包括 floor 校准、800 次并发序号分配、幂等和 token 撤销                                    |
| `validate_knowledge.py --strict`                 | 17 个知识源校验通过                                                                |
| `sync_hermes_skills.py --check`                  | `spark-push-knowledge` 衍生 skill 存在本轮前已有的未同步状态；本轮未改写 `/mnt/f/hermes/skills` |

当前边界：

- 连接在健康检查间隔内突然断开时，当次业务命令可能失败；归还时会淘汰该连接，客户端仍应依赖 `client_msg_id` 做幂等重试。
- 8 条上行流是当前 8 I/O 线程、单机 Kafka 下的实测选择，不是越多越好；继续增加会增加 gRPC 线程、pending 和 broker 压力。
- 本轮没有将 Kafka `SendAndWait` 改为无等待异步 ACK。后续若要进一步提高吞吐，应使用有界 inflight 队列和 delivery callback 异步回应，并保证只在 broker 成功后发 accepted ACK。
- 所有数字都是本机同部署 Release 结果，不是生产 SLA；800 条 A/B 是单轮对照，正式容量报告应运行多轮并报告中位数和方差。

### 8.3.5 2026-09-12 压测结果（旧 Redis 取号版本）

本轮在服务完整启动后重新执行，覆盖此前文档中的单聊 E2E、连接、历史查询和 Kafka→Job→Comet 数据。环境为当前工作区构建产物；Logic、Comet、Job、Redis、MySQL、Kafka 和压测客户端同机运行。使用新建的隔离账号 `perf_run_20260912_s`、`perf_run_20260912_r`，不与历史压测账号混用。

| 场景                      | 成功/总数                                              | 吞吐                | p50       | p95        | p99        | 错误/丢失 |
| ----------------------- | --------------------------------------------------:| -----------------:| ---------:| ----------:| ----------:| -----:|
| 单聊 E2E（8×100）           | sent/accepted/delivered ACK/实际接收 = 800/800/800/800 | **747.76 msg/s**  | 582.47 ms | 1012.06 ms | 1059.44 ms | 0/0   |
| WebSocket 建连（200 连接）    | 200/200                                            | **628.56 conn/s** | -         | -          | -          | 0     |
| 历史查询（8 线程×50）           | 400/400                                            | **348.78 QPS**    | 20.13 ms  | 43.65 ms   | 73.43 ms   | 0     |
| Kafka→Job→Comet（1000 条） | sent/received = 1000/1000                          | **1385.98 msg/s** | 335.85 ms | 640.64 ms  | 666.47 ms  | 0/0   |

当时 E2E 四个计数一致，Kafka 分层无发送失败、丢失或解析错误。该组与 8.2/8.3 的数字都属于旧 Redis 取号链路；2026-10-11 已改为 SQL 持久预留，不可称当前 QPS。本轮没有重测完整 IM 吞吐。

本轮启动时 WebDemo 因 9010 端口已有旧进程而未重新拉起，但不影响 Logic、Comet、Job、Kafka、Redis、MySQL 和 Agent Gateway 压测链路。

### 8.4 性能结论应该怎样理解

- 2026-09-12 的 747.76 msg/s/p99 1059.44 ms，以及 8 月/9 月 A/B，仅证明旧版本在对应环境的结果；当前 SQL 预留链路需新容量与尾延迟测量；
- 吞吐平台后 p99 快速增长，说明系统正在排队，不应继续用增加客户端并发“压高 QPS”；
- 1000 个空闲连接很轻，但“重连+离线历史”很重，容量模型必须分连接状态；
- 房间应同时报告业务 messages/s 与实际 deliveries/s；
- Redis 单命令很快，不代表 Redis 连接池在认证、历史和消息混合时不会饱和；
- 测试在所有组件同机运行，客户端也抢占 CPU，因此结果适合相对分析，不适合外推生产容量。

### 8.5 常见排障顺序

| 现象                   | 排查顺序                                                         |
| -------------------- | ------------------------------------------------------------ |
| 注册/登录失败              | MySQL 3307与密码 → 用户状态 → PBKDF2 → Redis pool/token             |
| WS 握手失败              | Logic 9100 → token 是否存在 → Comet 日志 → 协议/端口                   |
| accepted 无 delivered | Redis 在线路由 → `push_single` lag → Job stream ready → Comet 连接 |
| delivered 但历史没有      | `persist_message` group lag → Job MySQL pool → 唯一键/冲突日志      |
| 重复消息 | client_msg_id/SQL 原文与清理窗口 → outbox 阶段 → request_id 窗口 → 前端去重 |
| msg_seq 冲突 | 是否混跑旧分配器 → SQL message_sequence/reservation → message 唯一键与原文字段 |
| 重连重复/缺历史 | receipt/安全前缀 → next_seq 与 safe_seq → continuation → 同账号连接数 |
| Kafka lag 增长         | topic/group → callback 时延 → stream queue/MySQL → partition 数 |
| 429                  | 场景、key、rate/burst；不要直接无限调大                                   |
| Comet 内存增长           | 连接数 → 写缓冲 → 离线补推数 → 热点用户多连接                                  |

日志位于 `logs/`，PID 位于 `.run/`。排查时先按 request/client/msg/session ID 串联日志，再看指标和数据库，不要只看某一个组件“没有报错”。

### 8.6 测试数据说明

2026-08-27 性能测试使用独立 `perf_*_20260827` 账号、会话和房间；MySQL 隔离测试保留带 `perf_mysql_*` 前缀的消息行，Redis 短期 dedup key 会自动过期，但 seq key 和测试账号仍可能保留。为避免误删已有数据，测试结束时只停止服务和容器，没有执行数据库清理。复测应使用新的 run id；确需清理时先用只读 SQL 确认前缀和数量，再执行精确删除，不能直接运行宽范围清库命令。

2026-09-01 优化复测新建了 `opt_sender_20260901` 和 `opt_receiver_20260901` 两个本地账号（user_id `900000000016/17`），消息保留在会话 `s_900000000016_900000000017`。本轮也没有自动删除账号或压测消息，以避免宽范围清理误伤用户数据。

---

## 9. 从教学 Demo 到当前版本：工程演进主线

面试介绍项目时，不要按“先用了 Redis，再用了 Kafka”罗列组件。更好的叙述是“发现问题 → 做出决策 → 用数据验证 → 说明边界”。

### 9.1 演进对照

| 原始/早期问题                 | 当前改造                                                | 解决效果                  | 尚存边界                         |
| ----------------------- | --------------------------------------------------- | --------------------- | ---------------------------- |
| socket send 成功就算消息成功    | accepted/delivered ACK + 独立接收 E2E                   | 消息阶段可解释、可验证           | delivered 仍不是用户已读            |
| 接受路径与异步历史缺少持久协调 | SQL reservation/outbox + persist Kafka + Job 历史 | 原文重试、重启恢复、统一序号 | 接受仍有 SQL 开销，跨系统非 exactly-once |
| 客户端重试原文/时间戳变化 | SQL canonical 原文与 client ID 唯一键 | 相同 ID 返回原 seq/正文/时间戳，改正文拒绝 | 已结算记录满一天后清理，非永久去重 |
| Redis 清空/旧 HTTP 混写造成序号冲突 | 当前所有入口用同一 SQL message_sequence | 缓存丢失不回退序号 | 升级要排空旧事件，禁止新旧分配器混跑 |
| Job 任务入线程池后就提交 offset   | callback 真正完成后同步 commit；persist 另有 DLQ topic        | 进程崩溃时未完成 record 可重放   | 实时 push 重试耗尽仍只有死信日志          |
| Job→Comet 逐条 Unary      | 双向 PushStream + request/reply                       | 复用连接、明确逐条完成           | 单 Comet stream/queue 仍会成为瓶颈  |
| 流断开可能重复投递               | pending 失败、Kafka retry、request_id 去重、Unary fallback | 可重试且减少重复              | 近期去重集合有容量边界                  |
| 高序号先投递/离线只补第一页 | receipt 连续水位 + safe_seq + 多会话分页 | 不因 MAX 游标跳过低消息 | 同用户多连接放大、晚落库需后续补拉 |
| 广播/大群挤占单聊               | 独立 topic/group/rate limit                           | 故障域和指标更清晰             | broker/CPU/网络仍共享             |
| 只有日志没有量化                | Prometheus text metrics + 六项压测                      | 能从吞吐、p99、lag 定位瓶颈     | 尚无 Grafana/告警和多机基准           |
| AI 调用阻塞 IM 风险           | 独立 Hermes Bridge 与 AI topics                        | 外部模型和普通聊天隔离           | 外部 API 可用性仍需治理               |

### 9.2 最值得讲的三次决策

#### 决策一：重新定义“成功”

问题不是先改代码，而是先定义消息状态：accepted、delivered、read、stored 各自代表什么。只有状态边界明确，才能决定 ACK 在哪里返回、offset 在哪里提交、测试应该观察什么。

#### 决策二：用持久原文和 outbox 统一序号与重试

早期用 Redis 取号、Kafka 异步落库降低延迟；工程审查发现 SQL 历史落后、缓存丢失和旧入口混写的组合无法仅靠 MAX floor 兜底，重试还可能改变时间戳/正文。当前增加 SQL reservation/outbox：先持久保存身份和原文，再等 Kafka 确认 accepted，Job 继续负责历史。它增加 SQL 往返，但让正确性与恢复有明确证据，容量数据必须重测。

#### 决策三：用 E2E 证据替代组件自证

Kafka producer 成功、Job counter 增长、Comet send 返回都只能证明一个局部阶段。独立接收 WebSocket 同时核对 accepted ACK、delivered ACK、实际消息和 MySQL，才能发现阶段之间的问题。

### 9.3 项目复盘模板

回答“你遇到的最大问题是什么”可以这样组织：

```text
现象：E2E 实时显示 100/100，但立即停止 Job 后 MySQL 数量不足。
定位：accepted、realtime delivered 和 persistent stored 被混为一个状态。
原因：持久化 consumer 尚有 lag；早期本地异步落库还存在进程崩溃窗口。
改造：增加 persist_message，Logic 等 delivery report 后 accepted，Job 消费后再提交 offset。
验证：同时检查两类 ACK、独立接收、consumer group offset 和 MySQL 唯一序号。
边界：仍是 Kafka+MySQL 业务幂等，不是跨系统 exactly-once。
```

---

## 10. 求职使用：项目介绍、简历和高频追问

### 10.1 30 秒和 2 分钟项目介绍

#### 30 秒版本

> Spark Push 是我在 C++17 教学 IM 基础上持续改进的消息系统，拆为 Comet 连接层、Logic 业务层和 Job 异步层。我主要做了 SQL 持久序号与原文幂等/outbox、Kafka accepted 边界、gRPC 双向流与 delivered 确认、分页安全游标，以及跨线程连接生命周期和异常网络回归。

#### 2 分钟版本

> Comet 基于 Muduo Reactor 管 WS，Logic 校验用户/成员关系并管理消息状态，Job 消费 Kafka 写历史或下推。最初用 Redis Lua 取号，审查发现缓存丢失、旧 HTTP 混写、重试时间戳变化及持久化晚到时的游标问题，于是把当前入口统一到 SQL 会话行锁和 canonical reservation，准备二进制 outbox 后等 Kafka 确认再发 accepted。prepared 事件后台恢复，重复消息保持原 ID/正文/时间戳。
>
> 投递由 Job 的 PushStream 匹配 request_id，Comet 所属 IO 线程检查连接和输出容量后才确认 delivered；逐条 receipt 与 safe_seq 防止高序号跳过低序号。离线跨会话翻页，队列和输出有界，握手鉴权移到可 join 工作池，内部 RPC 加角色密钥和远程 TLS 配置。43 项 CTest 中 40 通过、3 项 opt-in 在隔离 MySQL/Redis 补跑通过；真实 Kafka 重建保留消息和 offset。尚未完成多机故障、长期高并发与当前吞吐压测，不能声称生产级或 exactly-once。

### 10.2 简历写法

#### 项目标题

```text
Spark Push｜C++17 分布式实时消息与 Agent 协作系统
```

#### 技术栈

```text
C++17、Muduo、WebSocket、gRPC/Protobuf、Kafka、Redis、MySQL、
OpenSSL、CMake/CTest、Docker Compose、Prometheus Text Metrics
```

#### 推荐项目描述（通用后端岗位）

- 在 Comet/Logic/Job 架构中抽离共享业务 Service 与 Repository，复用 HTTP/gRPC 的身份、会话、附件和成员规则；新增协议保持业务规则有唯一入口。
- 将 WS、旧 HTTP 与弹幕序号统一到 MySQL 事务，持久保存 client_msg_id 对应的原文、时间戳与序号；Kafka 前准备二进制 outbox，支持确认失败重试及后台恢复，拒绝同 ID 改正文。
- 以 Kafka delivery report 定义 accepted，以目标连接 IO 线程接受发送定义 delivered；通过逐条 receipt、safe_seq 和跨会话分页避免乱序/晚落库造成永久漏补，保持至少一次尝试与多层幂等。
- 修复 HTTP 半包、JSON 异常、WS Ping/Pong、跨线程连接闭包和晚鉴权生命周期；实施有界任务/流队列、慢连接输出上限、可 join RPC 工作池和受控退出。
- 增加内部 RPC 角色认证、loopback/TLS 配置、路由 generation+nonce compare-delete、消费者 readiness、DLQ 显式重放和日志轮转；以 43 项 CTest、隔离 SQL/Redis 回归及真实 Kafka 容器替换验证对应边界。

简历篇幅不足时选 3～4 项，每项必须能追到代码与测试。可另写 Android、GPU 推理或第 18 章 Agent 协作，避免把所有领域堆成一条。

#### 偏 C++/网络岗位版本

- 用 Muduo EventLoop 管理 WS，跨线程 send 闭包持有连接与数据，状态转换归 owner loop；以 weak_ptr 定时器、可 join 工作池和传输先销毁顺序收敛回调生命周期。
- 用 gRPC reader/writer、pending map 和 request_id 管理双向流，Comet 上行按项数/字节限制积压，以独立 watchdog 从入队时计算超时并取消 stalled 流。
- 将握手鉴权移出 IO 线程，验证真实 TCP 分包、慢鉴权、异常断连、Ping/Pong/Close 与 SIGTERM；连接池失效后能在禁用扩容时补回最小连接数。

#### 偏中间件/分布式岗位版本

- 用 SQL canonical reservation、Kafka 确认和 Job 幂等历史建立受理/投递分离的消息状态；以 outbox 恢复 prepared 事件，不夸大跨系统 exactly-once。
- 按场景拆 topic/group/限流，在业务成功或已确认持久化死信后提交 offset，关键 consumer 失败撤销 readiness；提供默认 dry-run 的显式重放 CLI。
- 建立独立接收端 E2E 与分层基准，区分业务 messages/s、fanout deliveries/s 和组件 ops/s，历史结果绑定测试日期、版本与同机负载。

#### 历史优化数字怎样使用

2026-09-01 的 Redis Lua 21.27k→32.56k ops/s、800 条 E2E 621.43→1100.55 msg/s、p99 1272→711 ms 可以作为**旧 Redis 取号版本的单机 A/B 故事**。当前 SQL 预留链路没有新的 IM 吞吐结果，不能把历史数据标成当前容量。日志成本测量也不能写成消息 TPS 收益。

### 10.3 高频面试追问

#### 1. 为什么需要 accepted 和 delivered 两个 ACK？

因为“系统已经可靠受理”和“目标在线连接已经接到服务端下发”是不同阶段。accepted 的边界是 persist event 获得 Kafka delivery report；delivered 的边界是 Comet 处理下推并返回 delivered_count。把二者合并，会让发送方无法区分排队、离线和实时投递失败。

#### 2. delivered ACK 能说明用户看到了吗？

不能。它最多说明服务端已交给目标 WebSocket 连接的发送路径。客户端进程可能随即崩溃，页面也可能没有渲染。用户已读需要独立 read receipt/read_seq。

#### 3. 当前消息序号能保证全局有序吗？

不能。SQL 会话行锁保证同 session 唯一递增，per-session 提交锁只管单个 Logic 进程的发送顺序；跨 Logic 的 Kafka 发布顺序与跨会话顺序仍不由它保证。客户端仍需排序、去重和补拉。旧 Redis Lua 原子取号也不等于端到端投递有序。

#### 4. 为什么不用 MySQL 自增 ID 直接做消息序号？

表自增 ID 是全表维度，不能自然表达会话内序号。当前用 SQL message_sequence 按 session 锁行分配，接受路径仍有同步数据库延迟；序号可有封存缺号，不能要求物理上永远连续。

#### 5. 为什么 Redis 快，还要 MySQL？

Redis 保存可失效热状态，MySQL 保存消息身份/序号、原文/outbox、长期历史与 receipt 游标；幂等和补偿都需要持久事实，不能只依赖缓存。

#### 6. Kafka 能保证 exactly-once 吗？

Kafka 的幂等 producer/事务只覆盖特定 Kafka 范围；本项目还跨 MySQL、gRPC 和 WebSocket，没有统一事务。当前是至少一次尝试，依靠 SQL 原文 reservation、Comet 近期去重、历史唯一键与客户端去重处理重放；不能宣称端到端 exactly-once。

#### 7. 为什么消费后再 commit offset？

如果刚收到 record 或刚提交本地线程池就 commit，进程随后崩溃，Kafka 认为已消费但业务没有完成。callback 真正完成后提交，失败 record 可以重放。代价是处理完成、commit 之前崩溃会重复，所以必须幂等。

#### 8. 当前 DLQ 完整吗？

persist 已有独立 `persist_message.dlq`：三次处理失败后把 hex 编码的 key/payload 写入死信，确认后才提交原 offset；DLQ 本身失败则停消费。实时 push/broadcast 仍是重试耗尽后打 `[DLQ]` 日志并提交 offset。已有默认 dry-run、显式目标且需 --apply 的 CLI 重放；尚无管理界面或自动恢复平台。简历应写“persist 死信 topic + 实时路径死信日志”，不能写“全链路完整死信队列”。

#### 9. gRPC 已经复用 HTTP/2，为什么还需要 Streaming？

Unary 也可复用底层连接，但每条消息仍有独立 RPC 生命周期和调度。Streaming 让应用复用一条长期逻辑流，减少高频调用开销，同时允许批量连续发送；代价是需要自己处理逐条匹配、并发读写、超时、背压和断流恢复。

#### 10. 为什么 `Write()` 成功后还要等 reply？

`Write()` 只说明消息被 gRPC 接受写入，不证明 Comet 已处理。只有相同 request_id 的 reply 才能证明 Comet 执行了去重和 fanout，并给出 delivered_count。

#### 11. 如何防止大群/广播拖慢单聊？

项目使用独立 topic、consumer group、令牌桶和指标，把队列与处理线程隔离。生产中还可以分 broker/集群、优先级队列、分层 fanout、多 Comet 分片和慢消费者治理。当前单机 CPU/网络仍共享，所以不是绝对隔离。

#### 12. 离线补推为什么用 delivered_seq 而不是 read_seq？

read_seq 表示用户已经阅读，delivered_seq 表示服务端已经交给连接。用户可能收到但没读；若从 read_seq 补推，每次重连都会重复发送全部未读消息。

#### 13. 如何处理热点用户？

一条消息会复制到该用户所有连接，热点用户的成本随连接数线性放大。可以限制设备数、合并同用户离线同步、对单用户设置下行队列上限、按 user_id 分片，并监控单用户连接和写缓冲。

#### 14. 历史查询为什么使用游标分页？

`session_id + msg_seq < anchor` 能利用复合索引直接定位，随着页码增大成本相对稳定；大 OFFSET 需要扫描并丢弃前面的行。

#### 15. 性能瓶颈在哪里？

当前尚需重测 SQL reservation/outbox 接受路径；应观察会话行锁、SQL 往返/池等待、Kafka 确认、流队列和慢连接。747.76 msg/s 等数据属于 2026-09-12 旧 Redis 版本，不能当当前 QPS；令牌桶 429 也不能用来推断容量。

#### 16. 项目中真实发现过什么问题？

可以讲四条能追到测试的链路：HTTP header 半包循环不推进；相同 client ID 重试重建时间戳导致历史冲突；高 seq 投递后 MAX delivered 跳过尚未落 SQL 的低 seq；晚到旧下线删新路由。分别用 TCP 切分、首次 Kafka 发布失败注入、真实 SQL 晚落库/乱序 receipt、50 次路由并发与计数器丢失回归验证，细节见第 19 章。

#### 17. Hermes 为什么不直接放进 Logic？

外部模型延迟秒级且故障模式复杂，放进 Logic 会占用普通消息线程和连接。独立 Bridge 与 AI topics 可以隔离重试、SSE 解析和外部网络；最终回答再复用普通消息的持久化与离线恢复。

#### 18. AI delta 为什么不落库？

delta 是展示过程，不是稳定事实。逐 token 落库会制造大量消息、序号和历史噪声。最终完整 reply 分配正式 seq 并落库，重连时恢复完整答案即可。

### 10.3.1 面试题逐题回答稿

下面的回答按“先结论、再机制、最后证据和边界”的顺序组织。面试时先讲粗体结论，面试官继续追问时再展开实现细节。

#### 1）请先讲项目架构，架构怎么扩展？

> 这是一个 C++17 分布式实时 IM 系统，拆成 Comet、Logic、Job 三个进程。Comet 管理 WebSocket 长连接、心跳和本机连接；Logic 负责鉴权、会话、序号、幂等、路由、限流和 Kafka 生产；Job 消费 Kafka，通过 gRPC `PushStream` 投递到目标 Comet，或者把持久化事件落入 MySQL。Redis 保存热状态，MySQL 保存业务事实，Kafka 负责异步解耦和重放。

扩展时尽量保持核心消息协议不变：新增消息场景使用独立 topic、consumer group 和处理器；新增客户端协议在 Comet 增加适配层；新增 Pi、Hermes 等 Agent 实现统一 Agent Adapter，最终仍转换为普通 IM 消息。这样新增 Agent 不需要改动 WebSocket 可靠性和历史恢复链路。

#### 2）性能怎么测，QPS 是多少，如何改进？

> 我使用闭环 E2E 测试，不把发送端 `socket send` 当成系统 QPS。测试同时统计 `sent`、`accepted_ack`、`delivered_ack` 和接收端按唯一 `client_msg_id` 读到的消息，并记录 p50/p95/p99、失败、重复、Kafka lag 和队列状态。只有四个消息数量完全相等才算通过。

历史 2026-09-12 旧 Redis 版本：8×100 E2E 为 747.76 msg/s、p99 1059.44 ms；200 WS 建连 628.56 conn/s；历史查询 348.78 QPS；Kafka 分层 1385.98 msg/s。更早的 A/B 说明热连接 PING、floor 锁分片和上行并行度优化收益。它们都是对应版本的单机数据。

2026-10-11 的当前链路改成 SQL 原文预留与 outbox，本轮执行了功能/故障回归和日志成本测量，未重跑完整 IM 吞吐。下一次压测要覆盖 SQL 行锁和写入、不同会话并发、消息尺寸、在线率及慢客户端，不能沿用旧 Lua 数字。

#### 3）这个项目哪里比较难？

> 最难的是跨进程失败时仍然保持消息语义正确，而不是实现 WebSocket 收发本身。

一条消息会经过客户端、Comet、Logic、Kafka、Job、Comet、WebSocket 和 MySQL。任意阶段都可能崩溃或超时，所以必须定义 accepted、delivered、stored 的边界，决定 Kafka offset 何时提交，并用 `client_msg_id`、`request_id`、MySQL 唯一键和游标处理重复、乱序、断线和重放。AI Agent 还增加了 SSE 增量、模型超时和最终答案恢复问题，因此只把 `ai_reply` 作为可靠消息。

#### 4）怎么保证消息可靠性？

> 当前采用“至少一次传递 + 多层幂等”，没有声称端到端 exactly-once。

- 客户端用 client_msg_id 识别重试；Logic 用 SQL 行锁统一序号，保存 canonical 原文并 prepare outbox，后台恢复 prepared 事件。
- 持久化事件获得 Kafka delivery report 后才返回 `accepted_ack`。
- Job 在业务处理完成后提交 offset，崩溃时允许 Kafka 重放。
- Job→Comet 使用 `request_id` 匹配响应，Comet 对近期 request 做去重。
- MySQL 使用 `(session_id,msg_seq)` 唯一键和完整字段比对兜底。
- 客户端按 `msg_id/client_msg_id` 去重，发现序号缺口时调用补偿接口。
- 逐条 receipt 推进连续 delivered_seq，分页用 next_seq，结算证明用 safe_seq，跨会话 continuation 补全离线；read_seq 只表示已读。

#### 5）Comet 怎么和 Logic 交互？异步返回如何对应原请求？

> 客户端连接 Comet，Comet 通过 gRPC 双向 `MessageStream` 把上行消息转给 Logic；下行则由 Job 通过每个 Comet 的双向 `PushStream` 投递。Comet 不承担复杂业务判断，Logic 不持有海量 WebSocket。

异步关联依赖显式 ID，而不是依赖返回顺序：`client_msg_id` 对应客户端发送，`msg_id` 对应业务消息，`session_id + msg_seq` 对应会话顺序，`request_id` 对应 Job→Comet 投递请求。PushStream 的 writer 串行写入，reader 按 `request_id` 查 pending map；断线重连重放同一个 request_id 时，Comet 去重后返回同一请求的结果。

#### 6）Redis 起什么作用，为什么用？

> Redis 负责低延迟、高频访问的热状态，不替代 MySQL 的长期事实存储。

项目用 Redis 保存 token/撤销索引、用户/房间路由、在线状态、路由 generation+nonce 和未读缓存。令牌桶目前在 Logic 内存中，消息序号与原文幂等目前在 SQL，不能把它们描述成 Redis 当前职责。Redis 适合高频小对象、集合和过期缓存；其丢失仍影响登录/路由，但不会回退持久消息序号。

#### 7）面试中怎样避免回答过短？

每道题至少补齐四层：

```text
结论：我采用什么设计
机制：组件、协议、数据结构如何配合
证据：测试数字、故障注入或代码落点
边界：当前没有验证什么，生产还需要什么
```

例如不能只说“QPS 是 1100”，而应说“800 条 E2E 消息在同机 A/B 中达到 1100.55 msg/s，accepted、delivered 和实际接收全部为 800，p99 为 711 ms；这是单机回归数据，下一步还要在多 Comet、多 Job、多 Kafka partition 环境做容量测试”。

### 10.4 STAR 项目故事

#### 故事 A：可靠性改造

- Situation：原始 Demo 将发送成功、服务受理、在线投递和数据库落库混成一个状态；
- Task：建立可解释、可重放、可验证的消息状态机；
- Action：拆 ACK、persist Kafka、SQL 原文 reservation/outbox、消费后 commit、receipt 连续游标、安全分页和异常网络回归；
- Result：分别验证 accepted/delivered/stored；确认失败重试保持原文，隔离 SQL 测试覆盖晚落库与乱序投递，真实 Kafka 重建保留记录与 offset；多机与长期压力仍待验收。

#### 故事 B：性能定位

- Situation：只看 socket send 会得到虚高 QPS，无法解释 p99 和队列积压；
- Task：建立端到端和分层基准；
- Action：增加独立接收端、Kafka 隔离链路、连接/fanout/存储/历史混合压测；
- Result：识别 Kafka 下游约 1700 msg/s、离线补推放大和 Redis pool 额外往返；优化后 Redis 热路径提升 53.1%，800 条单聊 A/B 吞吐提升 77.1%、p99 降低 44.1%。

### 10.5 不能写进简历的表述

不要写：

- “支持百万并发”——只实测 1000 条本机连接；
- “消息绝不丢”——没有跨系统事务、正式 DLQ 和生产故障证明；
- “端到端 exactly-once”——当前是可重试+多层幂等；
- “7.5 万群聊 QPS”——该数字是 500 socket fanout 的 deliveries/s；
- “生产级安全”——内部角色密钥/TLS 配置只覆盖部分边界；公网 TLS、mTLS、密钥托管与真实证书互通仍待完成/验证；
- “Redis 比 MySQL 快十倍”——两项 benchmark 操作语义不同；
- “Kafka 保证客户端收到”——Kafka 只覆盖事件队列，终端投递还经过 Job、Comet 和 WebSocket。

---

## 11. 后续优化路线

本轮明确问题已按第 19 章收敛。继续按正确性、并发/生命周期、安全、性能、架构推进：

1. 在隔离预生产完成存量 SQL/Kafka/RPC/客户端升级演练，覆盖旧 backlog 排空、回退、late persistence、重试与多会话补拉；不能直接新旧分配器混跑。
2. 真实依赖停启/网络黑洞、kill -9、慢客户端、断连长期压力与 ASan/TSan；补 Redis 路由重注册、Comet 心跳租约、同账号连接数和用户级离线合并。
3. 验证远程 RPC 证书互通，配置公网 HTTPS/WSS、生产密钥托管及轮换，再开展多节点 Kafka/MySQL/Redis 容灾。
4. 对 DLQ、consumer_failed、prepared outbox、池等待、队列和内存建立告警；补实时 push/broadcast 可重放 DLQ 和管理入口。
5. 用新版本 E2E、steady/burst、消息尺寸/在线率矩阵重新测 SQL 接受路径；基于证据决定路由批量化、SQL 批处理及异步日志，不做无依据的微优化。
6. 多 Comet/Logic/Job、多 partition 与跨机容量验证后，再扩展业务能力和架构。

---

## 12. 最终复习清单

读完本文后，应能不看代码回答：

- [ ] Comet、Logic、Job 分别负责什么，为什么这样拆？
- [ ] 一条单聊消息经过哪些协议、topic、存储和 ACK？
- [ ] `client_msg_id`、`msg_id`、`msg_seq`、`request_id` 有何区别？
- [ ] Redis Lua 为什么原子，脚本具体做哪几步？
- [ ] 为什么当前改为 SQL 持久取号，WS/旧 HTTP 混写与 Redis 清空如何避免序号冲突？
- [ ] reservation/outbox 各阶段代表什么，Kafka 确认超时后如何重试？
- [ ] `persist_message` 与 `push_single` 为什么是两条分支？
- [ ] Kafka callback 和 offset 在什么位置提交，失败语义是什么？
- [ ] gRPC stream 为什么需要 pending map、reply 和 deadline？
- [ ] delivered_seq 与 read_seq 为什么不能合并？为什么不能直接 MAX delivered？
- [ ] safe_seq 和 next_seq 有何区别，跨会话离线怎么继续翻页？
- [ ] MySQL 哪些唯一键和 `GREATEST` 更新负责幂等？
- [ ] 热点用户、大房间、重连风暴分别会放大什么资源？
- [ ] E2E 吞吐为什么低于 Kafka 隔离链路，p99 为什么随突发增加？
- [ ] Redis 为什么不应在每次借连接时 `PING`，优化后怎样淘汰坏连接？
- [ ] 为什么增加 gRPC 上行流能降低排队，又不能无限增加？
- [ ] 为什么触发 429 后 benchmark 输出的吞吐不能当作系统 QPS？
- [ ] 哪些性能数字可以写进简历，必须附带哪些环境边界？
- [ ] 为什么当前不能宣称 exactly-once、百万并发和生产级安全？
- [ ] 当前实现有没有手写链表？FIFO、去重、乱序重排分别用什么容器？
- [ ] WebSocket Accept 的 SHA1 填充和 Base64 余字节怎么处理？
- [ ] Job PushStream 断流时 pending 请求为什么必须先失败再重连？
- [ ] persist consumer 和 push consumer 的 DLQ/offset 语义有何不同？
- [ ] Android `TreeMap` 如何从序号 0 连续消费，重复/旧序号怎样处理？
- [ ] `ai_delta` 为什么不能推进 cursor，最终 envelope 的 sequence 必须等于什么？
- [ ] 图片为什么必须先 HTTP 上传？热路径如何校验 attachment id？Agent 当前轮怎样把文件变成 Pi RPC images？
- [ ] Logic 重启后为什么握手成功、发消息却停在「发送中」？Comet 如何重连 MessageStream？

如果这些问题都能用“原理 → 项目代码落点 → 验证证据 → 当前边界”回答，这个项目就已经从一份源码变成了可以用于复习、讲解和求职的完整工程案例。

---

## 13. 小模块逐步算法

大框架见第 1～4 章。本章按**具体数据结构、逐步步骤、容量和失败路径**写，对应仓库 `docs/spark-push-internals.md`。面试官问“xx 处链表怎么实现、xx 算法怎么做”时，用这一章回答。

约定：当前实现**没有**自定义链表节点或手写 `LinkedList`。FIFO 用 `std::queue`，按时间淘汰用 `std::deque` + `unordered_set`，按 ID 匹配用哈希表，按序号重排用有序树。

### 13.1 容器选择原则

| 场景                  | 实际容器                                            | 为什么是它                      | 不是什么                        |
| ------------------- | ----------------------------------------------- | -------------------------- | --------------------------- |
| 线程池任务 | std::queue<Task>，含 function 与字节权重 | FIFO + mutex/cv；默认 1024 项/32 MiB 排队 | 无界队列 |
| MySQL / Redis 空闲连接  | `std::deque<ConnEntry>`                         | front 弹出最旧空闲，back 归还       | 手写双向链表                      |
| Comet 用户连接          | `unordered_map<int64_t, set<TcpConnectionPtr>>` | 一个用户多连接；set 去重指针           | 单连接覆盖                       |
| 投递 `request_id` 去重  | `unordered_set` + `deque`                       | set O(1) 查重，deque 记淘汰顺序    | LRU（命中不刷新）                  |
| Job PushStream 待写请求 | `queue<PendingRequest>`                         | 单 writer 串行 Write，上限 10000 | 无界内存队列                      |
| Job 已写出未回复          | `unordered_map<string, PendingRequest>`         | reply 可能乱序                 | 按下标等待                       |
| Android 流式乱序帧       | `TreeMap<Int, PendingStreamDelta>`              | 需要最小缺口序号                   | 链表、数组扫描                     |
| Agent 短期热上下文        | `unordered_map → deque<Message>`                | 每会话 FIFO 100 条             | 严格 LRU（满 1024 时删 `begin()`） |

### 13.2 会话 ID

```text
if (user1 > user2) swap(user1, user2)
session_id = "s_" + user1 + "_" + user2
msg_id     = session_id + "-" + msg_seq
聊天室     = "r_" + room_id
```

Logic 热路径、`SessionDao::GetOrCreateSingleSession`、Android `singleSessionId()`、WebDemo `buildSingleSessionId()` 必须得到同一值。Agent 的 `conversation_id` 必须等于这个 Spark session_id，否则 Logic 拒绝最终 envelope。

### 13.3 Redis 键空间

当前 token/用户路由/房间计数/未读缓存见 5.4.1。generation+nonce 的两个新键用于防止迟到删除：

```text
route:generation:{uid}:{comet} : INCR 注册代次
route:lease:{uid}:{comet}      : generation + 随机 nonce
```

只有 lease 完全匹配才删路由；它没有 TTL 心跳语义。旧 session:msg_seq 与 message:dedup 键属于保留的 Lua benchmark，生产发送链不再使用。

### 13.4 历史 Redis 取号 Lua（当前发送链已替换）

函数：`RedisStore::AllocateSessionMsgSeq`。保留用于学习与历史组件 benchmark；当前 `AppendMessageHotPath` 不调用它，序号算法见 13.5。

```lua
local current = tonumber(redis.call('GET', KEYS[1]) or '0')
local floor = tonumber(ARGV[1])
if current < floor then
  redis.call('SET', KEYS[1], floor)
end
if KEYS[3] ~= '' then
  local existing = redis.call('GET', KEYS[3])
  if existing then
    return {tonumber(existing), 0}   -- is_new = 0
  end
end
local next_seq = redis.call('INCR', KEYS[1])
local last_seq = tonumber(redis.call('GET', KEYS[2]) or '0')
if next_seq > last_seq then
  redis.call('SET', KEYS[2], next_seq)
end
if KEYS[3] ~= '' then
  redis.call('SET', KEYS[3], next_seq, 'EX', tonumber(ARGV[2]))
end
return {next_seq, 1}
```

EVAL：`KEYS[1]=session:msg_seq`，`KEYS[2]=session:last_seq`，`KEYS[3]=dedup`（空 client_msg_id 则空串），`ARGV[1]=MySQL MAX floor`，`ARGV[2]=86400`。

逐步：先抬 floor → 再查幂等（命中仍可能已经抬过 floor）→ `INCR` → 只在更大时写 last_seq → SET EX dedup。TTL 过后同一 `client_msg_id` 会拿到新序号，这是 24 小时窗口幂等，不是永久唯一。

### 13.5 当前 SQL 持久取号与原文预留

入口：ConversationStore::AppendMessageHotPath → MySqlMessageReservations::Reserve。

1. 开始事务，INSERT ... ON DUPLICATE KEY UPDATE 获得会话排他锁，再 SELECT last_seq FOR UPDATE；不能用 INSERT IGNORE 的共享锁升级制造并发死锁。
2. 新计数行的 last_seq=0 时读取历史 MAX/会话 last_seq 作为旧数据 floor；升级必须先排空旧事件。
3. 按 session/sender/client ID 查询 reservation。已有记录返回原文和时间戳；stage=4 或同 ID 改内容则拒绝。
4. 新请求检查 int64 上限，递增 message_sequence、保存 canonical 原文与 stage=0，同事务提交。
5. WS 路径 prepare 不可变二进制 outbox，然后等 Kafka；旧 HTTP 则用同序号同步写历史并标 persisted。

原 64 shard seed set 已从当前链路移除。SQL 历史唯一键只作最终防线，不靠“冲突后丢掉消息”处理 Redis 回退。

### 13.6 令牌桶

`SceneRateLimiter::Allow`，配置 `conf/logic.conf`：single 1000/s burst 200（key=sender）；chatroom 300/60（key=room）；broadcast 20/5。

1. `bucket_key = scene + ":" + key`，一把 mutex 保护全部桶
2. 首次：`tokens = burst`
3. `tokens = min(burst, tokens + elapsed * rate)`
4. `< 1` 拒绝（429，不进 Kafka），否则减 1

### 13.7 线程池

queue<Task> 保存 function 与提交字节权重，默认排队上限 1024 项/32 MiB。Submit 锁内检查 started/stopping 和容量，拒绝返回 false；worker 出队后锁外执行。Stop(true) 排空、Stop(false) 丢未执行任务，两者都 join 已运行任务。hardware_concurrency=0 时至少一个 worker。Comet 原 detached RPC 已纳入此类有界可等待池。

### 13.8 MySQL 连接池

空闲 deque<ConnEntry> + mutex/cv，默认 Acquire 等待 3 秒。创建/健康检查在池锁外执行；坏连接减计数后每 250 ms 重试补回 min，即使 autoGrowth=false。RAII guard 归还。Acquire 的期限不能取消底层 connect/ping 或 SQL，本轮没有跨 SQL 的请求取消。

### 13.9 Redis 连接池

同样限时等待、网络操作在池锁外、失效后补 min；空闲超过健康间隔才 PING，I/O 错误归还时淘汰。RedisConnGuard 不可复制、可移动，避免重复归还。每次 Redis 业务命令仍需 Acquire；当前发送链不再为序号执行 Lua。

### 13.10 Kafka 生产者

`acks=all`、`enable.idempotence=true`、`retries=5`、`linger.ms=5`。后台 `poll(5)`。

- `Send`（实时 push）：produce + poll(0)，不等 DR
- `SendAndWait`（persist / ai_request / DLQ）：`unordered_map<void*, DeliveryState>`，opaque 指针，`cv.wait_for`；`accepted_ack` 的边界就是 persist 这条成功，超时 5s

### 13.11 Kafka 消费者

关闭 auto commit，独立线程 `consume(1000)`。回调最多 3 次，退避 `100 * attempt` ms。

| consumer                       | 失败后                                              | 原因        |
| ------------------------------ | ------------------------------------------------ | --------- |
| persist（`persist_message.dlq`） | 先死信 `SendAndWait`，成功才 commit；DLQ/commit 失败则停消费离组 | 不能跳过落库    |
| push / broadcast               | 仍 commit（best-effort）                            | 避免毒消息卡住分区 |

非法 protobuf：persist 返回 false；push 返回 true（跳过并提交）。

### 13.12 口令与 Token

新口令：`pbkdf2_sha256$120000$<16B盐hex>$<32B摘要hex>`，`CRYPTO_memcmp` 比较。旧 32 位 MD5 校验成功后 rehash。Token：32 随机字节 → 64 hex，`SETEX` + `SADD user:tokens`。撤销先读反向索引，再 `SCAN MATCH token:* COUNT 256` 兜底。管理员和 Agent bot 不允许用用户 Token 建 WebSocket。

### 13.13 用户路由短缓存

`unordered_map<uid, {comets, expire}>` + `shared_mutex`，TTL 默认 1000 ms。命中 shared lock 拷贝；未命中 `SMEMBERS` 后 unique lock 写回。增删路由 `erase`。不是 LRU，无容量上限，无负缓存。

### 13.14 WebSocket 握手：自实现 SHA1 + Base64

不依赖 OpenSSL 做握手。`comet/comet_server.cpp`。

**Base64**：3 字节→24 bit→4 个 6 bit 查表；余 1 字节补 `==`，余 2 字节补 `=`。

**SHA1**：初值 `0x67452301, EFCDAB89, 98BADCFE, 10325476, C3D2E1F0`；追加 `0x80`，补 0 直到 `size%64==56`，再追加 64 bit 大端 bit 长度；每块 80 轮，四段常数 `5A827999 / 6ED9EBA1 / 8F1BBCDC / CA62C1D6`。

**Accept**：`Base64(SHA1(client_key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"))`。Header 大小写敏感。Token 从 token= 取，未做 URL decode。VerifyToken 放入有界工作池，3 秒 deadline；握手总等待 5 秒、header 上限 16 KiB。weak_ptr 捕获连接，结果切回 owner loop 并检查连接/退出状态；晚到成功需清理自己注册的路由。

### 13.15 帧解析与发送

入站需 FIN=1、客户端 mask、RSV=0，解析 7/16/64 bit 长度；文本上限 64 KiB。控制帧不分片且最多 125 字节：Ping 回相同 payload 的 Pong，Close 回应并在 1 秒后强制关闭；非法 opcode/控制长度关闭连接。

出站 text 为 FIN、不 mask。数据经 shared_ptr 持有并在 owner loop 检查输出容量，4 MiB 高水位关闭慢连接。未确认当前 loop 接受时不能计入投递或推进 receipt。

### 13.16 连接表与房间表

```text
user_conns_ : unordered_map<int64_t, set<TcpConnectionPtr>>
room_users_ : unordered_map<int64_t, set<int64_t>>
conns_mu_   : 同时保护 recent_push_*
```

握手成功 insert，不踢旧连接；最后一条连接断开才通过有界 RPC 池通知下线，并携带 generation+nonce。PushToUsers 锁内复制、owner loop 实际接纳后汇总结果；传输对象销毁先等待 IO 线程，再释放连接表/回调依赖。房间成员权限仍取持久关系，不以 WS presence 代替。

### 13.17 request_id 定长去重窗口

容量 100000。空 id 放行不记录。set 已有 → `delivery_duplicate_total`，返回 false。否则 insert + push_back；超限 `erase(front)+pop_front`。FIFO 窗口不是 LRU。Job 默认 `request_id = msg_id@comet_id`。重复仍回 `error=0, delivered_count=0`。

Logic 同类结构：`hermes_stream_completed_` 10 万、`hermes_delta_seen_` 20 万、`hermes_history_cache_` 最多 1024 会话 × 100 条。

### 13.18 Comet → Logic MessageStream

**问题**：握手走 Unary `VerifyToken`，发消息走 `MessageStream`。旧实现只在启动时建流，Write 失败就退出，Logic 重启后页面仍显示在线，气泡停在「发送中」。

**解决**：与 Job PushStream 对称。每流 `broken` + 本流 `pending`。Reader 读失败只标坏并 503 掉等待中的 callback；Writer 负责 `ReconnectStream`（join 旧 reader，新 context/stream，再拉 reader）。Write 最多 4 次，退避 200ms→5000ms；耗尽才回 503。配置 `grpc_stream_reconnect_base_ms/max_ms`。指标 `spark_push_comet_logic_stream_ready` / `_reconnect_total`。

grpc_stream_count=8；每流 send_queue+pending 共最多 512 项、排队数据 8 MiB，满时显式失败。从入队时计 deadline，100 ms 独立 watchdog 到期失败回调并取消 stalled 流。context 创建/替换/取消用同锁，锁内再次检查 stream_running；停止后 join reader/writer。握手 Unary 与上行流分开恢复，channel/stub 生命周期保持稳定。

### 13.19 Job → Comet PushStream

每 comet 一条流。queue 上限 10000；满则拒绝，Kafka 重试。`SendToStream` 等待 2000 ms。

Writer 最多 4 次：流坏先重连；退避 200ms 倍增到 5000ms；Write 前放入 `pending[request_id]`。

Reader 按 reply id 匹配；`error==0 && delivered_count>0` 才异步 `SendDeliveryAck`；读循环退出把 remaining pending 全部 `ok=false`。

**ReconnectStream 关键步骤**：TryCancel → WritesDone → join reader → Finish → **先失败所有 pending**（让 Kafka 重试，绝不在无 reply 时提交 offset）→ 新建 stream。

Unary 兜底 3 次，间隔 100/200ms。两条路径都失败才 `delivery_lost_total`。

### 13.20 落库幂等与连续投递水位

Job persist 重试，唯一键冲突时逐字段核对 session、seq、sender、type、content、client ID 和 timestamp；内容/标识按字节比较，只有完全一致才幂等成功。历史写入标 stage=3，晚到 Kafka 确认只做 GREATEST(stage,2)，不能降级已经 persisted 的记录。

session.last_msg_seq/read_seq 可 GREATEST 更新。delivered 则锁 user_session_state 行、保存逐条 receipt、读取已结算前缀，找出其中第一个未投递的历史行，只推进此前连续水位。低序号尚未落库或未投递时，高序号 receipt 不能把它跳过。

### 13.21 MEDIUMTEXT 动态读取

`AssignTextColumn`：普通消息走 64 KiB 快路径；`reported_length > capacity` 或 `MYSQL_DATA_TRUNCATED` 时，按实际长度分配 string，再 `mysql_stmt_fetch_column`。解决读侧静默截断，不改变 12 KiB prompt 预算和 Compose `maxLines=4`。

### 13.22 离线补推与客户端缺口

Offline 每页最多 200 条并限制 2 MiB 估算字节，按已授权 session 排序，以 after_session_id/after_seq 继续，直到 has_more=false；超预算单行明确失败，不推进游标，需用 HTTP history 读。

Sync 在读历史前取得 SQL 安全前缀，并用 limit+1 判断是否有下一页；next_seq 只负责分页，safe_seq 证明本页已覆盖的结算前缀。Web/Android 只有当前连续 cursor 覆盖请求 after_seq 后才用证明跳过封存缺号；其他缺口保留并定时重拉。

Comet 在 owner loop 接受发送后才逐条上报 receipt；旧 HTTP 文本由 DownstreamMessageJson 补上存储 msg_id/seq/timestamp 元数据。ai_delta 不分配序号、不参与游标。离线查询不是一次性快照，晚落库仍需后续缺口补拉或重连重扫。

### 13.23 SSE 解析状态机

上限 16 MiB。`Feed` 按 `\n` 切行；空行 `ProcessEvent`；`event:`/`data:` 去掉一个可选前导空格。`[DONE]` 必须已有 final 或 `finish_reason=stop`。`pi.final`/`hermes.final`/`agent.final` 重复终态必须文本和 metadata 一致。普通 choices 只取第一项，`finish_reason` 必须是 `"stop"`。缺 DONE、非 stop、空回答都不能写成成功 `ai_reply`。

### 13.24 Bridge 增量合并

第一帧立即发送（TTFT）；之后 `>=48` 字节或 `>=50` ms 才 flush。progress 先 flush 文本再单独发，`delta_index` 仍递增。delta 异步 Send；最终 reply `SendAndWait`。进程内 `reply_cache_` 命中则跳过模型只重发 reply；满 10000 **整表清空**（不是 LRU）。最终 envelope 的 `sequence` 必须等于 `delta_count`。

### 13.25 Agent envelope 与 Router 账本

```text
route_key  = "rt_" + sha256(canonical_json(tenant,channel,conversation,thread))[:32]
session_key = "agent:{agent_id}:{channel_id}:{route_key 去掉 rt_}"
event_id    = "evt_" + sha256(request_id + "\0" + sequence)[:32]
```

`begin_turn`：同一 `request_id` 必须同一 `input_hash`；completed 回放不再调模型；running/failed 拒绝重复执行。`append_event`：`(request_id,sequence)` 主键 + `event_id` 唯一；payload 冲突抛错；终态或每 256 序号修剪非终态，只留最近 4096。replay 最多 4096。

### 13.26 WebDemo

`client_msg_id = Date.now() + '-' + random`；`bufferedAmount > 1MiB` 拒发；去重表上限 20000。历史未就绪时实时帧进 pending，完成后按 seq 刷入。流式缓冲超过 256 放弃缺口（最终消息仍权威）。`requestAnimationFrame` 合并 DOM；距底部 < 96px 才自动滚。重连 `wsGeneration`，延迟 `min(1000*2^n, 10000)`。图片：pending 最多 4 张，粘贴/点选后先 `POST /api/attachment/upload`，WS 只带 id。

### 13.27 Android：连接代数与 TreeMap

`SparkClient.connect`：`generation++`，旧 socket cancel；回调仅当 generation 匹配且 socket 是当前对象。单帧 16 MiB。

`StreamAccumulator.pending : TreeMap<Int, PendingStreamDelta>`，正文用 `StringBuilder` 只追加。

`acceptDelta`：

1. `index < 0`：无序号，立即 apply
2. `nextDeltaIndex` 从 0 开始
3. `index < expected` 或 key 已存在 → 丢弃，不回退
4. `pending[index] = delta`
5. `while pending.remove(cursor) != null`: apply，`cursor++`
6. `nextDeltaIndex = cursor`

第一帧强制刷新，之后 40 ms 一批。150 秒无事件把临时气泡标失败，正式 `ai_reply` 仍替换预览。事件队列有界 512：积压时丢掉后续 `hermes_delta` 并停止预览，靠最终消息和历史同步恢复。`TreeMap` 待补帧上限 256 / 512 KiB，超限同样放弃预览。有 `agent_event` 时校验 request、conversation、事件类型和 `terminal=false`，与 Web 一致。连接/历史/游标/流式状态都在同一 `eventDispatcher` 串行处理。弱网校验失败保留 Token，首页可点「重试连接」。重连延迟 1800 ms。联系人 Pi `900000000201`、Hermes technical `900000000211`。图片：`GetContent("image/*")` 上传后再发；`parseMessage` 读 `attachments`；预览按 id 懒加载。

### 13.28 长度审计

`SPARK_PUSH_LENGTH_AUDIT=1`。UTF-8 字节 = `size()`；Unicode 字符 = 非 continuation 字节数。比较顺序：provider → Bridge → Logic `db_before_write` → MySQL 读回 → 客户端展示。只修**第一个**下降点。

### 13.29 一次单聊实际走过的小模块

1. 客户端生成 UUID `client_msg_id`，乐观插入
2. Comet SHA1 握手（仅首次）+ 帧解码
3. Logic 凭证/actor/对象权限 → 令牌桶 → SQL 事务取号与 canonical 原文预留 → prepare outbox
4. Kafka `SendAndWait` persist → `accepted_ack`
5. Kafka 异步 `push_single`
6. Job persist：INSERT + GREATEST；冲突则字段比对
7. Job PushStream：queue → pending map → Comet FIFO 去重 → WS 帧
8. Comet owner loop 接受发送后逐条 MarkDelivered；Job 回传来源 delivered_ack
9. 客户端推进 cursor；发现缺口再 `sync`

Agent 路径在第 4 步之后分叉：`ai_request`（图片仅为 id）→ Bridge 读本地文件 → SSE → 48B/50ms 合并 → TreeMap/rAF 预览；终态再从第 4 步重走普通单聊。带图时第 1 步前多一次 HTTP 上传。

### 13.30 图片附件：上传、绑定、视觉输入

对应仓库 `docs/spark-push-internals.md` 第 32、31.5 节。

**魔数**（不信任客户端 mime）：JPEG `FF D8 FF`，PNG `89 50 4E 47 0D 0A 1A 0A`，GIF87a/89a，RIFF....WEBP。id：`RAND_bytes(12)` → `att_` + 24 hex；合法 `^att_[0-9a-f]{16,64}$`；路径永远是 `attachment_dir/id`。

**上传**：Bearer → 体 ≤ 8 MiB → 会话成员（没有 session 行则解析 `s_{u1}_{u2}` 并 `GetOrCreateSingleSession`）→ DecodeBase64 → 魔数 → 写 `<id>.tmp` 再 rename → INSERT `attachment`。每张 1 字节～4 MiB，每条消息最多 4 张。

**发送**：取号前 `AuthorizeMessageImages`：owner=sender、status=ready、session 为空则绑定否则必须匹配。`msg_type=image`。Kafka/WS 仍只含 JSON 引用。

**Agent**：无文字有图 → 「请查看这张图片。」；`ai_request.images` 只含本轮 id。Bridge `LoadPiImagesFromDir` 再魔数校验后 EncodeBase64。Gateway `normalize_prompt_images` 后放入 Pi RPC `prompt.images`。默认视觉模型 `deepseek-flash`。历史图不随后续 `ai_request` 重放。

测试：`tests/image_attachment_test.cpp`；`test_pi_gateway.py` 的图片转发。

---

## 14. 下一阶段推进顺序

按 **可靠性收敛 → 性能测量 → 并发扩展 → 部署工程化** 推进，不跳步。

### 14.1 已做的可靠性收敛（2026-09-18）

- Android 区分 Token 失效与服务器不可达；后者保留登录并可重试。
- 有界事件队列与有界 `TreeMap`；超限停止临时预览。
- 连接、历史、游标、流式在同一串行入口修改。
- envelope 校验与 Web 对齐。
- 契约测试：`tests/client_reliability_contract_test.py`（默认 CTest）。
- 计时线：`send → accepted → agent_start → first_text → final → history_readable`，并输出最近样本 p50/p95。
- Comet→Logic `MessageStream` 在 Logic 重启后自动重连；握手 Unary 成功不再掩盖上行流已死。重试耗尽回 503，气泡变为「发送失败」。

真机与完整在线链路仍需 `SPARK_PUSH_RUN_E2E=1` 的隔离账号/假 Agent 作业，不在默认 CTest 里跑真实模型。

### 14.2 端到端场景矩阵

| 场景           | 核心判定                             | 当前入口                                                       |
| ------------ | -------------------------------- | ---------------------------------------------------------- |
| 在线发送         | accepted、delivered、实际接收分别核对      | `load_test/e2e_bench`                                      |
| 重复发送         | 同一 `client_msg_id` 不产生第二条历史      | Redis Lua 集成测试；契约测试记录口径                                    |
| 生成途中断线       | 重连后获得完整最终回复                      | Android 停止预览 + `ai_reply`/`sync`；真机待 E2E 作业                |
| 服务进程重启       | persist 可重放；进行中的 Agent turn 结果未知 | persist DLQ；Router `running` 拒绝复用                          |
| 超过 64 KiB 回复 | 写入、历史、同步逐字一致                     | MEDIUMTEXT + `AssignTextColumn`；长度审计                       |
| 多 Agent      | 用户/会话/模型/上下文不串用                  | Android 独立联系人 ID                                           |
| 图片 / 截图问答    | WS/Kafka 无 base64；Agent 当前轮能看见图  | HTTP 上传 + attachment id；`image_attachment_test`、gateway 单测 |

真实模型调用必须作为单独集成测试，避免模型波动污染消息系统判定。

### 14.3 先量化延迟，再优化吞吐

不要先加线程。用现有 `SparkTiming` 日志区分：

- 模型慢：`first_text_ms` 大而 accepted 很快；
- Bridge/Agent 排队慢：`agent_start_ms` 大；
- Kafka/持久化慢：`accepted_ms` 大；
- Android 渲染慢：`final_ms` 已小但界面卡顿。

测出瓶颈后再改对应阶段。

### 14.4 会话内串行、会话间并行（已落地，仍单 Gateway）

- 同一会话：`SessionScheduler` + worker 亲和，保证上下文和工具顺序。
- 不同会话：`AdapterPool` 使用多个 Pi RPC 进程并行；**不**增加 Gateway 副本，SQLite 仍单进程写入。
- 上限：`SPARK_PUSH_PI_WORKERS`（默认 2）、`SPARK_PUSH_AGENT_MAX_INFLIGHT`、`SPARK_PUSH_AGENT_MAX_QUEUE`、`SPARK_PUSH_AGENT_QUEUE_TIMEOUT_S`、`SPARK_PUSH_AGENT_MAX_PER_AGENT`。
- 界面进度：排队中 / 正在执行 / 正在生成 / 已完成；崩溃遗留 turn 为 `unknown`，不自动重跑。
- 看板：`GET http://127.0.0.1:8643/v1/agent/metrics`（排队 p50/p95、in-flight、rejected）。
- 统一回归：`./scripts/sparkctl.sh regress`。
- DeepSeek 模型：上下文 1M、输出上限 384K；思考开关与 effort 按官网映射（`off→none`，默认 `high`）。
- Hermes 常用 IM 命令（`/model` `/reasoning` `/new` `/retry` `/status` `/help` `/cann` `/kb`）在 2026-09-06 已接到 Pi；`/restart` 与剧情命令在 Pi 上给出引导，不误打 Windows runtime。

### 14.5 小模块文档模板

每个关键模块统一写：

> 解决的问题 → 源码入口 → 数据结构 → 操作步骤/伪代码 → 锁与线程 → 时间/空间复杂度 → 失败恢复 → 对应测试

已按该模板展开的例子见第 13.4 节 Lua、第 13.17 节去重窗口、`docs/spark-push-internals.md` 第 31 节。

---

## 15. 本地 Qwen3-0.6B：CUDA AI Infra 开发与测试（2026-10-07）

本章以本机 **RTX 3060 12GB + i5-12400F + Ubuntu 22.04 / WSL2** 为准，沿用项目已有
ROCm 推理分层与消融口径。被测版本是根目录 `8579f0e` 加本次 CUDA 改动，不是教学 subset。
原始结果和完整复现见 [CUDA 实验报告](performance-report-2026-10-07-rtx3060.md)。

### 15.1 这次开发解决什么问题

IM 基础设施负责可靠传消息；AI Infra 负责让模型计算可控地进入这条消息链路。
前者不能因为模型慢而阻塞连接线程，后者不能靠“多开几个线程”无限接收任务。
本次复用已有本地模型，把 CUDA 算子运行时、流式 RPC、显存信息、容量准入和真实测试接起来。

```text
客户端 → Comet → Logic → Kafka ai_request → hermes_bridge
                                         → gRPC Gateway → Worker → llama.cpp CUDA
                        ai_delta ← 文本增量（临时 UI，不落库）
                        ai_reply ← 最终结果 → 原有 Kafka 持久化/Job 投递/历史查询
```

默认 Pi Agent 入口保持不变；本地 gRPC 模型是可选纯文本推理后端，不替代 Pi 的工具、Skills、
图片理解和会话控制。本次没有训练模型、合并 LoRA，也没有手写 CUDA 内核。
“算子支持”指固定 llama.cpp/ggml CUDA 后端的编译、部署、数值验证与性能验证。

### 15.2 技术栈选型与源码速查

| 环节     | 技术及选型原因                                                                  | 核心代码                                                                                           |
| ------ | ------------------------------------------------------------------------ | ---------------------------------------------------------------------------------------------- |
| GPU 编译 | CUDA Toolkit 12.9、nvcc；匹配现有驱动，显式 `sm_86`，只编译本机架构                         | `inference/scripts/setup_cuda_toolkit.sh`、`build_runtime.sh`                                   |
| 模型格式   | 本地 HF safetensors 转 GGUF/Q8_0；沿用 ROCm 的同一引擎与测试接口                         | `inference/scripts/prepare_local_model.sh`                                                     |
| GPU 计算 | llama.cpp/ggml CUDA；小模型无需新建完整框架，直接复用已有矩阵乘、RMSNorm、RoPE、Attention 内核      | 外部固定引擎 `ggml/src/ggml-cuda/`；项目 `serve_runtime.sh`                                             |
| 模型适配   | libcurl + HTTP/SSE，解析 token usage、文本增量、完成/错误事件                           | `inference/worker/llama_cpp_backend.cpp`                                                       |
| 远程生成   | gRPC Server Streaming；边生成边输出，携带 request_id、sequence、usage，支持 deadline/取消 | `inference/proto/inference.proto`、`inference_worker_service.cpp`                               |
| 准入/调度  | 有界队列、容量预留、Worker 心跳；防止 GPU 慢请求无限堆积                                       | `inference/gateway/gateway_service.cpp`、`worker_registry.cpp`、`scheduler.cpp`                  |
| GPU 观测 | 动态 NVML，按 PCI 地址匹配设备，Driver API 回退；业务二进制无需链接 SDK                         | `inference/worker/gpu_memory_probe.cpp`、`proto/inference.proto` 字段 17                          |
| IM 接入  | 可选 gRPC Bridge、Kafka 会话内串行/会话间并行；保持原持久化语义                                | `hermes_bridge/grpc_inference_client.cpp`、`hermes_bridge/main.cpp`、`common/kafka_consumer.cpp` |
| 测试     | 真实模型矩阵、上游算子 CPU 对照、真实取消/恢复、隔离 IM 依赖                                      | `inference/benchmarks/`、`inference/tests/real_runtime_test.py`                                 |

为什么不直接选择 vLLM：本次需要复用已有 ROCm 的 GGUF、相同 engine commit 和测试路径，
Qwen3-0.6B 在 12GB 单卡上也不需要先引入另一整套 serving 依赖。现有 vLLM adapter 只通过协议
fixture，不代表真实 vLLM 性能；未来大模型、高并发和 PagedAttention 可以另做同口径比较。

### 15.3 本地模型与环境的真实边界

已经定位并复用 `~/.cache/huggingface/hub/models--Qwen--Qwen3-0.6B` 的基础模型，
revision 为 `c1899de289a04d12100db370d81485cdf75e47ca`。转换设置 `HF_HUB_OFFLINE=1`，
不重新下载权重，不修改原始缓存。输出在 `/home/peco/models/`，引擎/toolkit 在 `/home/peco/tools/`，均不入 Git。

CPU 与 CUDA 固定 llama.cpp 提交 `6c7a87f7e5e5cd75b8a641c3471f2dee84a6ed17`、同一份
`Qwen3-0.6B-local-Q8_0.gguf`，SHA-256：
`e93ae7f33194c36ef20c13b9d4492a9888ddf9b1d9b0884f63d789ffcd43c51c`。
本地权重含两份内容相同的 tied embedding/output 张量，固定 converter 均写出，文件约 768 MiB；
此前 ROCm 使用的官方 GGUF 约 610 MiB、哈希不同。因此只能比较本次同机 CPU/CUDA，
不能把 RX 9070 XT 和 RTX 3060 的数字直接算成硬件加速比。

WSL 初始能运行 `nvidia-smi`，但没有 `nvcc`：驱动支持 CUDA 不等于已安装 toolkit。
本次使用用户目录的 NVIDIA 官方组件并逐包验 SHA-256，无需 sudo。
组件解压的 `lib` 与 nvcc 默认寻找的 `lib64` 不一致，补链接后再编译。
没有安装 Linux NVIDIA 驱动，遵循 [NVIDIA WSL 指南](https://docs.nvidia.com/cuda/archive/12.9.2/wsl-user-guide/index.html)。

### 15.4 需要复习的核心原理

**Prefill 与 Decode**：prefill 一次处理输入上下文，decode 一次生成一个新 token。
TTFT 包含等待、prefill 和首 token；吞吐是单位时间所有请求的输出 token 数。
并发 4 不一定表示 4 倍吞吐，多个请求会共享计算和带宽，也可能只是在排队。

**Continuous batching**：`-cb` 允许空闲 slot 在其他请求尚未结束时接新请求，混合短/长输出更有
意义；`-nocb` 是消融对照。Gateway 的队列是服务准入，runtime 的 batch 是算子计算，二者不是同一层。

**Flash Attention**：`-fa on/off` 控制引擎的 attention 实现。通过分块和融合减少中间 attention
矩阵读写，效果受 GPU、上下文长度和 batch 影响；不能照搬 ROCm 的提升倍数到 RTX 3060。

**KV Cache**：保存已处理 token 各层的 K/V，避免 decode 每次重算旧 token。
粗略大小 `2 × 层数 × KV heads × head_dim × 缓存 token 数 × 每元素字节数`；
本模型 28 层、8 个 KV heads、head_dim 128，四个 slot 共 8192 tokens：f16 约 896 MiB。
`KV_CACHE_TYPE=q8_0` / `-ctk q8_0 -ctv q8_0` 压缩缓存；Q8_0 块有 scale 开销，
不能简单认为精确减半。它不改变模型 Q8_0 权重，也不等于 prefix cache。

**Prefix cache**：复用相同前缀的预填充结果，降低重复上下文的 TTFT。
相同 session 不保证缓存命中，必须检查 runtime 的 cached_prompt_tokens。
同一 runtime 的两个 Worker 共享 slot，独立两个 runtime 则复制权重和 KV；多进程不是免费的吞吐。

**显存采样与上下文**：这台 WSL 主机上，旧 `cuMemGetInfo_v2` 在模型运行时仍显示约
11247 MiB 空闲，而 `nvidia-smi` 已显示约 3984 MiB 已用，不能直接把 Driver 读数当整卡容量。
因此优先 `libnvidia-ml.so.1 → nvmlInit_v2 → 按 PCI bus ID 找设备 → nvmlDeviceGetMemoryInfo`。
CUDA visible ordinal 可被 `CUDA_VISIBLE_DEVICES` 重排，不能直接套作 NVML physical index。
NVML 不可用才回退 Driver：retain primary context、保存线程 current context、切换/采样/恢复、析构释放。
`WorkerInfo.memory_probe_source` 标出 `nvml/cuda_driver/hip/unavailable`；失败清零表示未知。
修正后真实测试空闲约 8787.5 MiB，与紧随其后的 `nvidia-smi` 8788 MiB 相符。
NVML 是设备级观测，包含系统/驱动管理影响，不是模型独占或实时 KV 占用；Driver 回退在本机
不保证跨进程准确。调度分数结合 running/waiting/显存压力，不是严格 OOM 预算；12GB 不能全分给模型。

**背压与取消**：Worker 默认最多 4 个生成；Gateway 默认最多排队 64 个、等待 30 秒。
队列满返回 `RESOURCE_EXHAUSTED`，超时返回 `DEADLINE_EXCEEDED`，无健康 Worker 返回 `UNAVAILABLE`。
Cancel、上游断连和 deadline 都要向下关闭模型 HTTP 请求并释放计数；否则会出现“客户端已取消，GPU
仍计算”的资源泄漏。Worker 约 1 秒心跳，5 秒 TTL；恢复后重新注册。

**实时计数与心跳不是同一种真源**：默认 `WorkerRegistry::Snapshot` 使用
`max(心跳 running, Gateway 本地 active)`。RPC 已完成、本地预留已释放时，旧心跳可能仍是满载；
新的 GPU 请求会空等到下一次心跳。基线 FA off gRPC queue_wait 最大 977 ms，TTFT p50 约 1019 ms。
新增 `--gateway-managed-only`，在**单 Gateway 独占所有 Worker 生成流量**时，仅以本地 RPC lease
预留/释放计数准入，健康/TTL/显存仍用心跳，Worker 自身容量检查不变。默认不开启。
共享 Worker、多 Gateway 或直接调用 Worker 时不能开启；否则本地计数不能代表全部负载。
优化是消除服务层陈旧准入，不是让 CUDA 内核变快。源码入口为 `worker_registry.cpp::Snapshot`、
`gateway/main.cpp`；状态指标为 `inference_gateway_managed_only`。

### 15.5 测试方法与结果入口

先运行普通回归和显存 fixture，再执行真实 GPU 算子、真实 gRPC 模型测试、HTTP/gRPC 消融矩阵，
最后走完整 IM 链路。正式压测不与编译/另一组压测并行。
矩阵：每配置预热 2 个，3 轮 × 12 请求；基础输出 128 tokens，温度 0、seed 42、关闭 thinking
与 prefix cache；长前缀实验输出 32。吞吐取三轮中位数，百分位按请求计算。
HTTP/gRPC 使用 ignore_eos 固定输出；IM 自然结束、上限 128，要按实际 usage 数 token，不数字符/SSE。

启动显式指定 `CUDA0 -ngl 99` 并检查 29/29 层 GPU offload，禁止把 CPU 回退标成 CUDA 成绩。

下面均为 **并发 4、4 slots、FA on、f16 KV、关闭 prefix cache**。gRPC/IM 使用上述独占闭环模式；
HTTP 为直接访问运行时。固定引擎、同一 GGUF，完成 QPS 与 token/s 均取三轮中位数。

| 链路      | 设备       | 输出 token/s（min–max）       | 完成 QPS    | TTFT p50 / p95 / p99 ms   | E2E p99 ms |
| ------- | -------- | -------------------------:| ---------:| -------------------------:| ----------:|
| 模型 HTTP | CPU      | 102.50（101.98–102.81）     | 0.801     | 1053.2 / 1094.7 / 1095.2  | 5143.0     |
| 模型 HTTP | RTX 3060 | **602.13（598.45–607.86）** | **4.704** | **52.7 / 61.8 / 67.0**    | **866.6**  |
| gRPC 服务 | CPU      | 102.88（102.63–102.91）     | 0.804     | 910.2 / 1083.5 / 1135.2   | 5096.8     |
| gRPC 服务 | RTX 3060 | **591.61（590.47–598.44）** | **4.622** | **46.8 / 67.0 / 69.3**    | **879.0**  |
| 完整 IM   | CPU      | 80.18（76.88–82.74）        | 0.626     | 1954.5 / 4308.6 / 5112.8  | 8222.8     |
| 完整 IM   | RTX 3060 | **384.01（382.51–389.02）** | **3.000** | **446.5 / 818.4 / 819.8** | **1527.2** |

QPS = 完成请求数 / 整轮时长；token/s = 实际输出 token 数 / 整轮时长，不是单请求 decode 速度。
HTTP/gRPC 每请求 128 tokens；IM 自然结束上限 128，本次样本实际均为 128，但 prompt 与纯 gRPC 不同。
IM TTFT 从 WS 发请求到首增量，E2E 到收到持久化后的最终 Bot 消息；包含消息链路而非仅模型 HTTP。
本次同机加速比：HTTP **5.87 倍**、gRPC **5.75 倍**、完整 IM **4.79 倍**。
这些是指定工作负载的吞吐，不是“最大 QPS”；每配置只有 36 个样本，p99 不能作为生产 SLO 承诺。

优化过程与消融结论：

- 单请求 FA off：CPU 44.01 → CUDA 216.12 token/s，约 4.91 倍；证明 GPU 执行收益。
- CUDA FA off → on，并发 4：507.24 → 602.13 token/s，约 +18.7%。
- 混合输出 32/64/128/256 tokens，continuous batching：248.95 → 323.05 token/s，约 +29.8%。
- 重复长前缀：TTFT p50 162.6 → 20.1 ms，cached/prompt = 32979/33630；不能拿此 QPS 与 128-token 场景直接比。
- f16 → Q8_0 KV：静态 KV 从 896 → 476 MiB，省 46.9%，但吞吐 602.13 → 559.81，下降约 7.0%；本机不缺容量时优先 f16。
- gRPC FA off：默认模式 255.73 → 最终闭环 496.58 token/s，TTFT p50 1018.6 → 59.9 ms；queue_wait sum/max 均为 0 ms（毫秒精度，含预热 38 次）。
- 完整 IM FA on：默认 338.59 → 优化 384.01 token/s，约 +13.4%；E2E p99 2129.3 → 1527.2 ms。
- CUDA 并发 4 → 8（4 slots、FA off）：507.24 → 499.73 token/s，TTFT p50 54.3 → 1058.8 ms；更高并发只是排队。

正式数据为基线 27 配置 + 最终闭环 5 配置，每配置 36 请求，共 **1152 请求、0 失败**，不计预热、
恢复检查和中间复测。只开闭环、尚未改 NVML 的阶段数据独立保留，未挑取“最好一轮”混入最终结果。
真实 CUDA 默认/闭环各 **21/21 检查通过**；选定 CUDA 算子 CPU 数值对照 **252 通过、10 跳过、0 数值失败**，
跳过为部分 RoPE 非连续 view + inplace 组合，不是全算子覆盖或语义质量评估。
第一次真实测试仅因日志 verbosity 不输出 offload 证据而预检失败，已修正并保留失败记录。
最终全项目 CTest **24/24 通过（44.02 秒）**，包含真实 Redis 序列集成测试，未跳过。

完整 IM 测试使用独立 Docker compose 项目 `spark-cuda-bench`：MySQL 3308、Redis 6381/DB11、
Kafka 29092，独立随机 topic/group、私有 0600 密码文件。不清空现有业务库。
断开接收增量后检查历史中只有一条最终 Bot 回复，再重放相同 client_msg_id 检查不重复持久化。
这验证 ai_delta 临时性与 ai_reply 可恢复性，不是 Kafka exactly-once。
基线五配置、闭环两配置的恢复检查均通过。结果、manifest、指标、启动证据及图表位于
`11.2-spark_push/docs/benchmarks/2026-10-07-rtx3060/`，`managed/` 为最终优化版；
详细列表、算子选择和复现命令见 [CUDA 报告](performance-report-2026-10-07-rtx3060.md)。

### 15.6 启动、简历与面试边界

```bash
cd /home/peco/cppcode/fenbushi/11.2-spark_push
export CUDA_TOOLKIT_ROOT=/home/peco/tools/cuda-12.9
MODEL_PATH=/home/peco/models/Qwen3-0.6B-local-Q8_0.gguf \
  RUNTIME_SLOTS=4 FLASH_ATTENTION=on KV_CACHE_TYPE=f16 PREFIX_CACHE=1 \
  bash inference/scripts/serve_runtime.sh cuda
# 另两个终端：
./build-wsl/inference/inference_gateway --listen 127.0.0.1:9300 --metrics-port 9301
# 仅在全部生成流量经这个 Gateway、Worker 不共享时，给上一条命令加 --gateway-managed-only。
./build-wsl/inference/inference_worker --worker-id cuda-0 --listen 127.0.0.1:9400 \
  --gateway 127.0.0.1:9300 --model qwen3-0.6b --backend llamacpp \
  --model-endpoint http://127.0.0.1:8080 --device-type cuda --max-concurrent 4
./build-wsl/inference/inference_cli --model qwen3-0.6b --prompt '解释 KV Cache' --json
```

可以描述为：**基于 C++/gRPC 构建本地推理准入与流式服务，将 Qwen3-0.6B 的 CUDA 后端接入可靠 IM，
实现动态显存采样、取消/deadline 与有界排队，完成真实模型与端到端恢复测试。**
可选量化表述：**在 RTX 3060 12GB 单卡、并发 4、128-token 工作负载下，gRPC 吞吐达到约
592 token/s（4.62 请求/s）、TTFT p99 约 69 ms；相对同机 CPU 吞吐约 5.75 倍，
完整 IM 链路约 384 token/s（3.00 请求/s），完成 1152 请求及断连/幂等恢复验证。**
面试务必带上硬件、模型、精度、并发和输出长度，不把 HTTP 的 602 token/s 写成完整 IM 成绩。

不能写“手写 Flash Attention 内核”“实现 PagedAttention”“生产级多 GPU 平台”或“百万并发 AI 服务”。
被问最难的地方，应讲清模型执行、RPC 生命周期和消息持久化三个边界：取消如何释放 GPU 请求，
流式终止如何保证 usage 完整，断连后如何只恢复最终消息，性能数据如何排除 CPU 回退与输出长度差异。

---

## 16. Logic 三层业务重构：规则复用、可读性与维护

本章对应 **2026-10-09 根目录版本**，不修改教学 subset。目的是把业务从 HTTP/gRPC 入口移出，
使规则只维护一次，并能独立测试真实业务代码。本次没有变更公共协议或做容量压测；第 8、15 章
性能数字仍属于各自日期与工作负载，不是本次重构的性能结果。
独立维护文档见 [Logic 业务层详解](logic-business-layer.md)。

### 16.1 两种“三层”不要混淆

Comet / Logic / Job 是三个部署进程，隔离连接、业务和异步任务；本章是 **Logic 进程内部**
的代码职责：

```text
HTTP / gRPC / Kafka consumer：协议入口
               ↓ 调用
logic/services/：业务规则和用例编排
               ↓ 调用
Repository / DAO / ConversationStore / RedisStore：数据访问
               ↓
MySQL / Redis / Kafka / 附件文件
```

之前已有 DAO，但成员权限、消息编排等也在 handleHistory / HandleUpstreamMessage 中直接执行。
“有 DAO”不等于职责已经分开：两个协议各写一遍权限，就是两个维护点。
现在 HttpApiServer / LogicServiceImpl 不直接依赖 DAO、Redis、KafkaProducer 或 ConversationStore。
main.cpp 统一组装 Service，并让 HTTP、gRPC、Agent consumer 共享实例。

### 16.2 哪一层应该写什么

| 层 | 应负责 | 不应负责 |
|---|---|---|
| 协议适配 | URL/method、JSON 类型、Bearer 提取、Protobuf 转换、响应状态、CORS、流读写 | SQL、Redis key、Kafka 生产、复制成员/附件/账号业务规则 |
| Service | 账号状态、权限、分页边界、ACK 条件、业务调用顺序、订阅与在线状态 | HttpRequest、HttpResponse、ServerContext 等协议对象 |
| Repository/DAO | SQL、文件、Redis/Lua、存储结果与失败 | 决定 HTTP 返回 400 还是 200 |

采用经典三层，不是全项目 DDD/Clean Architecture。核心用户、会话、附件服务通过 Repository
接口隔离存储；其他服务复用具体 DAO、RedisStore、KafkaProducer。Kafka Protobuf 序列化仍在
消息服务实现中，公共业务接口不暴露 RPC 类型。ConversationStore 成熟的热路径/floor/Lua 没重写。

### 16.3 八个业务服务速查

以下路径相对 `11.2-spark_push/`。

| 服务 | 主要规则/方法 | 优先阅读 |
|---|---|---|
| UserService | Register/Login、密码升级、安全 token、Authenticate、管理员鉴权、新鲜状态与 2 秒热缓存 | `logic/services/user_service.cpp` |
| ConversationService | CheckAccess、History、SyncAfter/SyncOffline、MarkDelivered、MarkRead/Unread、会话列表 | `logic/services/conversation_service.cpp` |
| AttachmentService | Upload/Read/AuthorizeMessageImages；归属、ready、会话绑定、共享成员权限 | `logic/services/attachment_service.cpp` |
| MessageService | Submit、持久化确认、实时路由、限流、Agent 请求/增量/最终回复、广播、兼容旧文本接口 | `logic/services/message_service.cpp` |
| RoomService | Join/Leave/Unsubscribe、订阅列表、管理员建房/列表、在线上报与计数 | `logic/services/room_service.cpp` |
| AdminService | 禁用/恢复/软删除、昵称、token 撤销、用户列表、审计；先撤凭证再改状态 | `logic/services/admin_service.cpp` |
| DanmakuService | 旧弹幕 body token、视频时间轴、固定房间会话与 Kafka 推送 | `logic/services/danmaku_service.cpp` |
| KnowledgeService | catalog 稳定 ID 校验和原文读取 | `logic/services/knowledge_service.cpp` |

共用文件：

- `logic/model.h`：User、Session、Message、StoredAttachment、账号状态等，不再分散定义在 DAO 头文件。
- `logic/services/service_status.h`：业务 code/message/原因标记，不是 HTTP 状态或 grpc::Status。
- `logic/services/business_rules.h`：用户中心、房间管理、管理员广播共享管理员规则。
- `logic/services/repositories.h`：User/Conversation/Attachment Repository 窄接口。
- `logic/repositories/dao_repositories.cpp`：生产适配旧 DAO/Store/Redis，不重复 SQL。
- `logic/message_codec.h`：Message → Protobuf 共用字段映射。
- `logic/main.cpp`：连接池 → DAO → Repository → Service → 入口的组装和生命周期。

所有 Service 是进程级实例，DAO/连接池存活更久，入口持非拥有指针；不能注入请求局部对象。
缓存、令牌桶、Agent 最近消息与增量去重状态保持共享和原锁保护，不为每个入口创建一份。
业务实现编译为 spark_push_logic 静态库，服务器与单测链接同一份真实实现。

### 16.4 三个具体复用例子

**会话权限**统一到 ConversationService::CheckAccess：单聊要求调用方为 user1/user2；房间
要求成员查询成功且确实是成员。HTTP History、gRPC SyncMessages、MarkDelivered、图片上传和
非 owner 图片读取复用它，权限拒绝发生在数据读取之前。
图片上传可懒创建规范单聊 `s_<min>_<max>`，先检查规范 ID 与调用方参与关系。本次明确修复
`s_1_8junk` 等非规范拼写可能先创建无关会话再报错的副作用，测试验证拒绝时不调用创建方法。

**鉴权**由入口提取凭证，再统一调用 UserService::Authenticate，共享 Redis token 和 MySQL
最新状态检查。realtime audience 额外拒绝管理员/Bot token。当前每次 WS Submit 都重新验证
连接凭证与 actor；目标状态查询仍有 2 秒缓存。禁用凭证可拒绝后续发送，但不等于立即踢掉所有存量 WS。

**消息用例**：Unary SendUpstreamMessage、双向 MessageStream 转换成 UpstreamCommand，
调用同一 MessageService::Submit，把 UpstreamResult 映射回协议。

```text
凭证/actor/账号状态 → 限流 → 附件 → SQL 原文/序号 → prepare outbox
  → persist Kafka SendAndWait 成功
  → accepted=true → adapter ACK_STAGE_ACCEPTED
  → 实时 Kafka → Job → Comet → delivered_ack
```

accepted 仍只意味着 Kafka 持久化事件得到 delivery report，不是 MySQL 历史 INSERT 完成或接收者
已读。相同 client_msg_id 保留原 msg_id/msg_seq；历史要等 Job 异步落库。
Kafka ai_reply/ai_delta consumer 直接调用 MessageService，不再依赖 RPC 类。delta 不占 seq、
不进历史，最终 reply 才落库；缓存和去重状态必须保持唯一所有者。

### 16.5 兼容边界和后续治理

- 业务分层阶段保留了 URL/Proto/schema 和客户端合同；后续 2026-10-11 工程修复新增协议字段、SQL 表与 RPC 配置，需要按第 19 章同步升级客户端和服务。
- 业务 code 不等于 HTTP status：管理员凭证不符是业务 403 + HTTP 400，禁用用户登录也如此。
  Service 返回业务结果，adapter 保留各入口的旧映射。
- `/api/message/send` 已在后续工程修复中补 Bearer、actor、目标/长度与限流，仍为 SendLegacyText 同步历史/旧响应，不提供 WS Submit 的两阶段 ACK/实时 fanout 合同。
- MarkRead/Unread 仍保留“请求 user_id 等于鉴权身份”规则，本次没额外补齐全部对象级授权。
- 弹幕保留 body token/Redis 解析、固定 room 1001，pushed 不代表 broker 或客户端确认。
- HTTP Leave 不删订阅；Unsubscribe 删持久成员；gRPC Leave 更新在线状态，三者不能混用。
- 内部 gRPC 已加角色密钥与远程 TLS 要求，真实证书互通、mTLS/IAM、高可用和跨节点踢线仍未完成。
- HTTP JSON 类型异常与 history 序列化已修复；在线计数失败回退、MessageService 较多 Agent 编排仍有治理空间。

### 16.6 怎样增加新功能

以“消息撤回”为设计例子：先在业务服务定义发送者、时间窗口和幂等规则；权限调用 CheckAccess；
存储方法写进 DAO/Repository；入口只转换输入输出；依赖在 main 注入。然后先测允许/拒绝/重复/
存储失败，再测真实接口与通知链路。这是维护方法，不代表项目已经实现撤回。
项目 AGENTS.md 已加入长期分层约束，知识登记表和导航也更新，避免后续又向 handler 塞业务。

### 16.7 回归与简历

本节记录 2026-10-09～10 的业务分层回归；2026-10-11 工程修复后的验证以第 19 章为准。
当时新增 `tests/logic_business_service_test.cpp`：内存 Repository 调用真实 Service，**81 项检查通过**，
覆盖密码/token、缓存与新鲜鉴权、成员权限、分页、delivered 离线游标、附件归属/绑定、无副作用
拒绝和管理员业务规则。`logic_layer_contract_test.py` 阻止入口直接依赖数据组件及公共业务头暴露
HTTP/gRPC 类型；它是文本门禁，不是 AST 形式证明。
最终 CTest **26/26 通过，43.27 秒，包含真实 Redis、无跳过**。

真实接口脚本 `tests/logic_business_e2e.py` 显式启用，使用独立 MySQL 3308、Redis 6381/DB11、
Kafka 29092 与随机 topic/group 启动 Logic/Comet/Job，最终 **88 项检查通过（12.17 秒）**，覆盖在线投递、两阶段 ACK、重复、历史、离线
恢复、HTTP/gRPC 权限一致、附件、房间、管理员 token/状态/审计与旧接口。它不默认加入 CTest，
避免普通测试修改开发数据库。详细结果和复现命令位于
`11.2-spark_push/docs/benchmarks/2026-10-09-business-layer/`。

修正异步历史等待后，独立复跑同一真实接口脚本仍 **88 项全部通过（16.65 秒）**，
保存在该目录的 `repeat/business-e2e.json`；主回归和复跑均使用新随机账号/topic，不清空业务库。

另外复用本地 RTX 3060 / Qwen3-0.6B 跑完整 Agent 链路：预热 2 个、正式 4 个请求均成功，
断开增量后历史只有一条最终回复，重放 client_msg_id 后历史仍只有输入与最终回复两条。
这是功能回归，不是性能 A/B，也不是 Kafka exactly-once 证明。

可写简历：**将 C++ IM 逻辑服务重构为协议适配、业务 Service、数据访问三层，集中复用鉴权、
会话成员与附件规则，统一 Unary/Streaming/Agent 消费入口的消息编排，通过业务单测及真实
HTTP/gRPC/WebSocket 回归验证 ACK、幂等、历史和离线恢复语义。**

面试应讲清：规则唯一入口在哪，怎样用测试替身验证真实 Service，依赖为何由 main 组装，
为什么重构不能改变 ACK 边界，以及旧 API 的兼容与安全限制。不要写“重构使 QPS 提升 X%”。

---

## 17. GitHub 发布同步与版本复现（2026-10-10 历史发布）

### 17.1 本次发布包含什么

本章记录 2026-10-10 发布，当前 2026-10-11 工程修复与验证见第 19 章。
历史发布目标为 [specetator/grpc_aiagent](https://github.com/specetator/grpc_aiagent) 的 `main`，
代码与测试提交为 `5c2738a7256d69b423644ccf487ba7fb586dd00b`。它包含两组此前已实现、尚未推送的改动：

| 内容 | 代码/文档入口 | 适用边界 |
|---|---|---|
| 本地 Qwen3-0.6B CUDA 推理与显存观测 | `inference/worker/gpu_memory_probe.*`、`inference/scripts/`、`docs/performance-report-2026-10-07-rtx3060.md` | RTX 3060 12GB 实测；不上传模型或 CUDA Toolkit |
| Gateway 独占闭环准入 | `inference/gateway/worker_registry.*`、`gateway/main.cpp` | 默认关闭；仅单 Gateway 独占全部 Worker 流量时启用 |
| Logic 三层业务重构 | `logic/services/`、`logic/repositories/`、`docs/logic-business-layer.md` | 保留正常客户端协议、ACK/幂等/恢复语义及明确的旧接口限制 |
| 测试与可复核证据 | `tests/logic_business_*`、`inference/tests/`、`docs/benchmarks/2026-10-07-rtx3060/`、`2026-10-09-business-layer/` | 原始 JSON、manifest、指标与图表，测试日期和工作负载分别记录 |

根目录是被发布的运行版本；`05_persist_subset` 等是教学快照，本次没有据其回退或替换主线。
当前文档也进入仓库 `docs/project-guide-and-resume.md`，README 提供入口。
本机手册保留在原路径；GitHub 版仅按其所在目录转换相对链接，正文和验证口径同步。

### 17.2 发布前验证与日期口径

2026-10-10 本次实际复核：

- `cmake --build build-wsl -j2`：通过；
- 业务 Service 单测：81 项检查通过；分层依赖门禁通过；
- 启用专用 Redis 6381 后，全项目 CTest **26/26 通过、无跳过，44.10 秒**；
- 知识登记表严格校验、Pi 引用检查通过；代码与待发布文件进行凭证和生成产物检查；
- 使用普通 Git 提交与快进推送，不强制覆盖远端历史，不改用户其他容器或开发数据库。

第 15 章 CUDA 吞吐与算子数据是 **2026-10-07** 的实验，不是今天重新测出的性能；
第 16 章真实接口两次各 88 项检查和本地 Qwen 全链路恢复验证是 **2026-10-09** 的回归。
今天复跑了构建与普通/Redis 回归，未重新进行完整 GPU 性能矩阵，不能据此更新 QPS 或加速比。
GitHub Actions 是独立的远端验证，以仓库 Actions 页的实际状态为准，不用本机成功代替云端结果。

### 17.3 怎样复现和维护

```bash
git clone https://github.com/specetator/grpc_aiagent.git
cd grpc_aiagent
# 需要复现本次代码/测试版本时：
git switch --detach 5c2738a7256d69b423644ccf487ba7fb586dd00b
# 使用 README 安装构建依赖，再运行：
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release -DBUILD_TESTING=ON
cmake --build build -j2
ctest --test-dir build --output-on-failure
# 普通 CTest 的 Redis 集成默认跳过；启用前确认是专用测试 Redis。
SPARK_PUSH_RUN_REDIS_TESTS=1 SPARK_PUSH_REDIS_PORT=6381 \
  ctest --test-dir build --output-on-failure
```

真实 HTTP/gRPC/WS 和 CUDA 测试按各报告的专用依赖、模型和启动参数执行，不直接指向开发业务库。
机器路径如 `/home/peco/models/...` 是本机记录，换机器要提供自己的模型、Toolkit 和私有凭证。
测试 secret 文件、API key、token、附件、构建目录和模型二进制不上传；`*.log` 由 gitignore 忽略，
详细运行日志保留本机，GitHub 主要提供结构化结果/manifest/指标和复现脚本。

之后维护时，本机手册与仓库在线版同时更新，并检查相对链接。技术/架构改变才更新 canonical
说明；每次性能实验另建带日期的 snapshot，保留原有参数和失败记录，不覆盖旧成绩。

---

## 18. 多人聊天室共享 CANN Agent：协作开发与进度交接

### 18.1 这次解决什么问题

2026-10-11 新增：一个聊天室绑定一个算子任务、一个原生 Pi 会话和一个独立源码工作区。
多人在同一房间用 `@CANN 问题` 与公共 CANN Agent 交流；换一个人继续提问，不再重新创建私人上下文。
普通讨论仍按原聊天室发送，不会每条消息都调用模型。

“无缝接手”落实为四类可以找回的事实：

| 交接内容 | 保存在哪里 | 新成员如何使用 |
|---|---|---|
| 聊天与最终 Agent 答复 | MySQL 消息历史，沿用房间 msg_seq | 重连补历史；临时 delta 不当作历史 |
| 目标、当前结论、下一步 | room_agent_task 与 room_agent_checkpoint | 打开任务面板查看；人工摘要修改带 revision |
| 当前源码与版本副本 | 独立 workspace 与内容寻址 checkpoints | Agent 读取同一个工作区；按 code_revision 定位源码 |
| 谁执行了什么、测试是否真通过 | room_agent_request、Gateway turn 账本与测试 receipts | 查看 actor、request_id、源码 hash、runner hash 和退出码 |

这不是把所有成员伪装成房主，也不是把群聊全文无上限地塞进提示词。
每轮都保留真实发起者；任务身份稳定，执行权限分别校验。人工 summary/next_steps 需主动保存，
不会因 Agent 声称“已经完成”就自动改成已验收状态。

详细规范见 [多人共享 Agent 文档](room-agent-collaboration.md)；
这章是便于不看代码复习的导览。根目录主线已经实现，教学 subset 不随之改写。

### 18.2 三层架构与核心技术栈

```text
Web 房间面板 / WebSocket
  → Comet（凭证与连接）→ Logic HTTP/gRPC adapter
  → MessageService + RoomAgentService / OperatorTaskService
  → Repository / MySQL（任务、版本、固定输入、结果）
  → Kafka ai_request → Bridge HTTP/SSE → Gateway / AgentRouter
  → 单会话 FIFO → Pi + CANN RAG → 任务工作区 / 固定测试执行器
  → ai_delta 临时广播；ai_reply 持久化后走 Job / Comet 房间投递
```

| 技术 / 规则 | 为什么用在这里 | 核心落点 |
|---|---|---|
| C++ 业务 Service | HTTP/gRPC 复用身份、成员、授权和任务规则；不让协议函数各写一份 SQL | [room_agent_service.cpp](../logic/services/room_agent_service.cpp) |
| Repository + MySQL 事务 | 任务和交接版本是可恢复事实；最新版本与历史记录必须一起提交 | [room_agent_repository.cpp](../logic/repositories/room_agent_repository.cpp) |
| revision / CAS | 两个人同时保存摘要时，拒绝旧版本覆盖，而非最后写入者悄悄获胜 | OperatorTaskService::Checkpoint / UPDATE WHERE revision |
| Redis | 提供 token、在线路由/计数与缓存；当前消息序号和原文幂等已迁到 SQL | RedisStore；SQL MessageReservations |
| Kafka | 延续已有异步 Agent 请求及最终消息投递链路；accepted 不等于推理已完成 | [message_service.cpp](../logic/services/message_service.cpp)、Bridge |
| HTTP/SSE | Bridge 与 Gateway 的推理请求和流式增量；房间 delta 不分配消息序号 | [hermes_client.cpp](../hermes_bridge/hermes_client.cpp) |
| SQLite turn ledger | Gateway 保存原生会话路由、请求幂等与已完成输出；与 MySQL 职责不同 | [agent_router.py](../cannbot/scripts/agent_router.py) |
| FIFO + 执行互斥 | 同一任务工作区不能被两个用户的 turn 同时改写；不同发言仍有顺序 | [agent_scheduler.py](../cannbot/scripts/agent_scheduler.py) |
| Pi 工具白名单 + CANN RAG | 共享知识检索，但读成员不能写源码；不开放通用 shell | [room-workspace.ts](../cannbot/pi/extensions/room-workspace.ts) |
| SHA-256 / 内容寻址 | 用真实文件内容标识版本，不用模型摘要冒充源码检查点 | [room_agent.py](../cannbot/scripts/room_agent.py) |
| bubblewrap + prlimit | 固定测试子进程不接收任意命令，隔离网络/挂载并限制资源 | room_agent.py 的 run_tests |

### 18.3 一轮多人协作具体怎么走

1. 房主创建/绑定任务，明确 title/spec，并授权需要修改源码的 developer_user_ids。
   同一房间不能悄悄切换到另一个 task_id；修改授权和交接内容使用版本号。
2. 成员 A 发 `@CANN ...`。新版 Comet 从已鉴权连接注入 auth_token，
   Logic 重新核验发起者与房间成员资格，不信任客户端填写的 from_user_id。
3. Logic 给人类消息分配序号、确认持久化；保存这次 AI 请求的固定输入快照，再投递 Kafka。
   快照包含任务说明、交接摘要、触发前最近讨论及当前问题，不包含 bearer。
4. Bridge 用部署侧 Bearer 访问 Gateway；可信 collaboration scope 带
   room_id/task_id/actor_user_id/bot_user_id/can_develop。
   群聊正文中的“我是管理员”不改变这些字段。
5. Router 使用 `room_<room_id>_<task_id>` 作为共享原生会话身份，而非 actor 的私聊身份。
   actor 只表示这一轮是谁发起；FIFO 保证同工作区单 turn 执行。
6. Pi 按成员能力读取/修改工作区，检索 CANN 知识，必要时调用固定测试工具。
   流式预览广播给实际数据库成员，不落历史、不推进 delivered cursor。
7. 最终答复及源码/测试证据回到 Logic，沿既有 persist_message 与 push_group 路径进入历史。
   B 接手时继续使用同一任务、同一 Pi 会话和同一份源码，不需要从 A 的个人工作目录拷贝。

单 Logic 的提交互斥覆盖序号分配、持久化确认和请求入队；目前是进程内锁，不是跨实例租约。
上下文背景最多取最近 20 条并安全裁剪，长期信息靠原生会话、人工摘要和源码版本共同维护。
它能恢复协作事实，不保证长时间运行中的设备作业能迁移到另一台机器继续执行。

### 18.4 权限、幂等和可靠性：面试重点

权限分三次验证：

- Logic：有效用户凭证、房间持久成员、房主/显式开发授权；裸 gRPC 房间请求也必须带 auth_token。
- Gateway：可信 Bridge Bearer 和固定绑定的房间 scope；仅公共 Pi 可入房间，私人 Hermes technical 不开放。
- Pi 工具：只允许工作区内的非隐藏、非符号链接路径；普通成员只能 read 和 CANN 只读知识工具，
  开发成员再开放 write/edit/room_run_tests。共享房间禁用 /reset、/model 等控制命令。

请求幂等不是“收到相同文字就不执行”：它基于固定 request_id 与输入快照。
重复已完成请求回放原结果，不重复改文件或测试；actor 也是输入身份的一部分。
Gateway 重启发现执行状态不明时标记 unknown，不盲目重新运行可能有副作用的工具。

现有 accepted_ack 仍只确认人类消息 durable。MySQL 请求快照与 Kafka AI 入队尚非事务 outbox，
失败窗口需要原请求重试/人工核对；不能把本次实现写成 exactly-once。
退出订阅后不能继续查询任务/历史或收到新答复，但不能追回已看到的内容。
房间可自行加入，开发权限仍须明确授权；保密协作还需要邀请制和独立 OS 身份。

### 18.5 源码版本和“真的测过”怎么区分

工作区之外保存 `checkpoints/<room_task>/<code_revision>` 的不可变内容副本。
同一源码 hash 复用副本，每版最多 512 个文件 / 16 MiB；尚无总量配额和自动清理。
备份必须覆盖 MySQL、Gateway SQLite、Pi session 与工作区/版本副本，不能只备份聊天文本。

测试工具没有任意 command/path 参数，只执行管理员配置的 Python runner。
bubblewrap 仅暴露工作区、系统运行库和只读 runner，不暴露 home、模型凭据或网络；
缺少 runner/隔离能力就失败，不自动降级为直接执行。prlimit 与超时约束资源。

同一轮 Pi 可能并行调用多个工具，因此除了 turn 之间串行，还对 write/edit 与测试做互斥。
执行器记录测试前后源码 hash、runner hash、exit_code、timed_out、日志和 passed；
若源码在测试中变化，即使退出码为 0 也不算通过。
status=done、Agent 自述“通过”与实际执行回执是三个不同概念。

当前固定执行器验证的是 Linux 主机 C++ Add 示例，不是昇腾 NPU 实机精度或性能。
CANN 知识检索实际测过，但设备透传/远程 NPU runner 尚未实现。
Pi 文件工具 guard 不等于整个模型进程已经得到完整 OS 沙箱隔离。

### 18.6 这次测试的结果与过程

2026-10-11，使用独立 MySQL 3308 / Redis 6381 DB11 / Kafka 29092，
不在开发业务库压测或清空数据。发布前实测结果：

| 验证 | 结果 / 覆盖 |
|---|---|
| 协作功能发布时的构建与 CTest | 当时 29/29 通过，无跳过；44.56 秒，含专用 Redis；后续工程修复新增测试见第 19 章 |
| C++ 房间业务 / Python 房间测试 | 36 条检查 / 10 个测试通过 |
| 真实 Pi TS guard / UI 合同测试 | 文件路径、符号链接、工具权限、写测试互斥；流式乱序/重复、跨房间与晚到 delta 均通过 |
| 原有 HTTP/gRPC/WS 业务回归 | 88 条检查通过，11.10 秒 |
| 受控模型完整房间 E2E | 44 条检查通过，12.13 秒；7 次执行，同工作区峰值执行数 1 |
| 真实 Pi 完整房间 E2E | 16 条检查通过，18.84 秒；两个真实用户 A 写代码、B 接手读源码并执行测试 |
| 真实 CANN 知识工具 | 四页完整拼接 931 个 codepoint；引用版本检查通过 |

受控 E2E 的模型决策是确定性适配器，但测试确实启动 bubblewrap、g++ 和 Add 精度可执行程序，
同时覆盖失败与成功回执。真实 Pi E2E 使用本机已配置的 deepseek/deepseek-flash；
未改变个人模型/凭据配置，不把它当作本地 Qwen 的推理成绩。
UI 验证是实际模块的 DOM 合同测试，不是完整浏览器人工验收。

本轮审查与真实测试发现并修复：
排队者超时错误释放正在执行者的锁；Pi --tools 漏列自定义工具；
裸 RPC 可伪造房间 sender；同一 turn 并行写/测试缺乏互斥。
加入源码前后版本检查，避免旧代码结果被记成新代码通过。
结果与复现脚本见 [结构化测试证据](benchmarks/2026-10-11-room-agent/)。
本次是功能、安全与交接回归，不是吞吐压测，不能据运行秒数推算 QPS 或开发效率提升比例。

### 18.7 怎样启用和使用

默认关闭，未擅自启动常驻协作服务或替换私人 Agent 配置。管理员在原有部署配置中补充：

```dotenv
SPARK_PUSH_ROOM_AGENT_ENABLED=true
SPARK_PUSH_ROOM_WORKSPACE_ROOT=/admin-owned/private/room-workspaces
SPARK_PUSH_ROOM_TEST_RUNNER=/admin-owned/runner.py
```

安装 bubblewrap、util-linux 与对应编译依赖；工作区根目录为部署账户独享的 0700 目录。
公共 Pi 的 bot_user_id 必须对应 Logic 主机器人，owner_user_ids 为空。
重新启动新版 Logic、Comet、Bridge、Pi gateway；runner 可不配置，但测试会明确不可用。

打开 Web 聊天室的“CANN 协作任务”：房主创建任务并授权，成员 `@CANN` 提问，
有权限者保存 summary/next_steps/status 交接内容。Android 暂未新增任务面板。

新接口是 POST /api/chatroom/agent/configure、/task、/checkpoint，
均带 Bearer；业务失败查看 JSON code，版本冲突 code=409。
旧裸 gRPC 房间发送脚本需补 auth_token，新版 Comet 下的 WS 客户端不需自行填这个字段。
SQL 新增三张 room_agent_* 表，初始化/增量迁移仅 CREATE IF NOT EXISTS，不删旧会话。

### 18.8 简历写法与可追问内容

可作为独立项目要点：

> 在 C++ 分布式 IM 系统中实现多人共享 CANN Agent 协作入口，抽离 RoomAgentService /
> OperatorTaskService 与 Repository；以共享 Pi 会话、独立工作区、MySQL CAS 交接版本和请求账本
> 支持跨用户接手；实现请求级身份核验、开发权限、FIFO 执行互斥、源码内容寻址检查点和隔离测试回执，
> 经 44 项协作 E2E、16 项真实 Pi E2E 及 88 项原业务回归验证。

面试应能解释：为什么 task/session 与 actor 分开；为什么 WS presence 不代表授权；
为什么 turn 幂等与消息 ACK 不同；为什么进程内锁还不支持多实例；
为什么测试通过必须绑定源码版本；摘要交接与真实源码/执行账本各自解决什么问题。
不要写“生产级多租户沙箱”“多人效率提升 xx%”“已完成 NPU 算子性能验证”或“绝对 exactly-once”。


---

## 19. 工程审查修复、验证与发布（2026-10-11）

### 19.1 本轮解决的问题与实施顺序

对根目录主线按调用链、数据流、线程模型与生命周期审查，在基线
`2f285f89146db4a7b9c65cdbc263c4a521973ee2` 上按正确性、并发/生命周期、安全和容量依次修复。
工程修复提交为 [a90aea0](https://github.com/specetator/grpc_aiagent/commit/a90aea045a2c02abeb6d0d6b8a28808d1944a2be)；
该提交的 98 个受验证源码/配置/测试文件已逐一与回执 SHA-256 核对一致。手册随后独立提交。
不改教学快照、不修改存量业务数据；详细映射、兼容合同与操作步骤见
[工程审查修复](review-remediation.md)。

| 顺序 | 修复内容 | 可验证触发条件 |
|---|---|---|
| 1. 网络正确性与权限 | HTTP 半包解析、长度/JSON 异常；旧 send Bearer/actor；RPC 角色密钥；广播范围 | 每个 TCP 切分点、异常 JSON、伪造 sender、匿名/错误角色 RPC、空房间 scope |
| 2. 持久身份与恢复 | SQL 统一序号、canonical 原文/outbox、Kafka 命名日志卷、safe_seq 与多会话分页 | 首次发布确认失败后同 ID 重试、缓存不可用、16 并发取号、低 seq 晚落库、容器重建 |
| 3. 并发与生命周期 | owning 连接闭包、异步握手、稳定 stub/context、池自愈、路由 compare-delete、Ping/Close、退出 | 慢鉴权不阻塞已有连接、30 次异常断开、旧下线晚到、计数器丢失、SIGTERM |
| 4. 容量与运维 | 有界队列/字节预算、慢连接输出、ACK watchdog、ready、DLQ CLI、per-session 锁、日志轮转 | 满队列拒绝、stalled reply 超时、ready=503、重放目标/格式校验、并发轮转 |

### 19.2 必须记住的新消息合同

```text
0 reserved → 1 prepared → 2 Kafka published → 3 history persisted
       └─ 从未 prepare 且超过 10 分钟 → 4 sealed
```

SQL message_sequence 是唯一取号权威。reservation 保存原正文/时间戳/序号；prepare 保存不可变
持久化 protobuf。accepted 仍只在 Kafka delivery report 成功后返回，不以 SQL 预留代替。
prepared 恢复每秒最多 10 条，未完成记录不因一天过期删除；已结算 stage=3/4 创建满一天后分批清理，
不能声称永久幂等。sealed ID 重试明确失败，应使用新 ID。

实时 fanout 允许相同消息身份重试，下游仍需去重。目标 IO 线程接受发送才计 delivered，
不是终端收到；逐条 receipt 和安全前缀共同推进连续 delivered_seq。
分页位置 next_seq 与结算证明 safe_seq 分开；Offline 使用会话/序号 continuation，
Sync/Offline 同时限制条数和字节。ai_delta 保持临时显示，最终 ai_reply 才走持久链。

### 19.3 实际验证与证据边界

下表为工程修复候选的本地回执，区别于第 18 章协作功能发布时的 29 项测试。
源码候选指纹、命令/结果在 [validation.json](benchmarks/2026-10-11-review/validation.json)，
日志原始样本在 [logging.json](benchmarks/2026-10-11-review/logging.json)。
回执绑定的是发布前源码指纹与基线，不把未来提交号冒充测试时已存在的版本。

| 验证 | 实际结果 | 覆盖与限制 |
|---|---|---|
| C++ 全量构建 | cmake --build build-wsl -j2 通过 | 当前受影响目标重新编译 |
| CTest | 43 注册、40 通过、3 opt-in 跳过、0 失败，28.84 秒 | 3 个环境依赖项另行补跑，不写成默认 43/43 无跳过 |
| 隔离 MySQL | 通过 | 原文/时间戳重试、二进制 outbox、并发序号、混合入口、实际 Service 安全页、乱序 receipt |
| 隔离 Redis | 2 项通过 | 保留的历史 allocator；50 次新旧路由并发与计数器丢失 nonce |
| 真实 Kafka | 通过 | 自建 broker/命名卷；消息与已提交 offset 在容器替换后保留；CLI 发布获确认 |
| 真实 HTTP/Comet 网络回归 | CTest 中通过 | 分包、类型异常、慢鉴权、coalesced 首帧、Ping/Pong/Close、异常断开与 SIGTERM |
| 页预算与客户端游标 | 通过 | 1003 条跨会话、80×64 KiB 分页、实际 Web 游标函数、旧文本元数据 |
| Android | Kotlin 编译通过 | unit test task 为 NO-SOURCE；没有设备/行为测试通过结论 |
| 知识源/引用 | 70 来源严格校验、citation 通过 | 已安装 Pi 包/个人 settings 有既有漂移，未覆盖个人配置 |

容器测试只创建并清理自己的随机命名容器/卷，没有迁移或重启业务依赖。
本轮按项目 Skill 在同一会话顺序实施/审查/测试，不声称有隔离审查 Agent。
CI 已加入 SQL、Redis 路由、真实 Kafka 重建及知识校验；本地回执不代替 GitHub Actions 结果。

未执行：全 IM 多进程混沌、真实 Redis 停启/网络黑洞、实际 TLS 证书互通、ASan/TSan 压力、
多机/磁盘故障、长期慢客户端、Android 真机与本轮 GPU runtime。
因此不能把回归通过写成已达到生产 SLA、绝不丢消息或全量故障自愈。

### 19.4 复现验证

从仓库根目录执行；Docker 测试使用隔离资源，Android 需要已配置 SDK 和缓存依赖：

```bash
cmake --build build-wsl -j2
ctest --test-dir build-wsl --output-on-failure -j2
python3 tests/run_mysql_review_tests.py build-wsl
python3 tests/run_redis_review_tests.py build-wsl
python3 tests/run_kafka_volume_test.py build-wsl
python3 cannbot/scripts/validate_knowledge.py --strict
python3 cannbot/scripts/test_pi_citations.py
cd android-app
./gradlew --offline :app:compileDebugKotlin :app:testDebugUnitTest
```

已有 Pi 安装同步检查 sync_pi_agent.py --check 会报告个人安装与模板漂移，需单独处理；
它不修改当前包。不要为消除检查差异直接覆盖已有个人配置。

### 19.5 部署前存量升级顺序

1. 停旧发送入口，排空旧持久化/广播 Kafka 事件，备份 SQL、broker 日志与 offsets。
   旧消息已取号但未入历史时，仅查询历史 MAX 不够；禁止旧 Redis/new SQL writer 混跑。
2. 核对旧 Kafka 实际 log.dirs，把完整日志、KRaft metadata 与 offsets 迁入新命名卷。
   现在固定 /var/lib/kafka/data；不能直接 compose down 后期待旧数据自动进入新卷。
3. 执行 [一次性 SQL 迁移](../sql/migrations/20261011_message_reservations.sql)：三张新表，
   旧 delivered_seq 重置为 0 重扫，read_seq 不变。重复执行会再次重扫，不当作每次启动操作。
4. 配置 COMET/JOB 与可选 ADMIN RPC 角色密钥；脚本在不跟踪的 .env.rpc 中生成，权限 0600，
   不打印密钥。跨机启用服务器证书/私钥和客户端 CA。同步升级 Logic/Comet/Job 与 Web/Android。
5. 小流量核对 readiness、assignment/offsets、outbox stage、历史与跨会话补拉，再开放入口和重测容量。

新 schema 已含新表，Logic/Job 启动检查缺失时失败，不自动迁移现有库。
单 Kafka broker/RF=1 仍不提供机器故障容错。此处仅记录操作步骤，本次没有执行生产部署或数据迁移。

### 19.6 值班排障与性能结论

关键 consumer 的 DLQ/offset 确认失败时，ready 归零并受控非零退出；Logic HTTP/Job metrics
提供 /ready，未就绪 503。DLQ 成功确认后仍可继续消费，ready=200 不证明 DLQ 为空，
需对 dead_letters、consumer_failed、lag 和 outbox 恢复失败告警。

DLQ 工具默认 dry-run，只解析一条 sparkpush.dead_letter.v1 JSON；显式 topic 要与记录匹配，
发布需 --apply 和 brokers。它不删除 DLQ、不改原 offset，也不打印正文/key。先修复根因，
再导出到 0600 临时文件、核对目标并重放，最后验证历史和安全水位；不要清空 topic 或跳 offset 代替恢复。

日志默认 50 MiB 轮转、保留 5 个历史文件，SPARK_LOG_CONSOLE=1 可恢复控制台输出。
WSL2 单线程 10 万条×128 字节三次测量：旧 40.24/36.60/39.59 ms，新 23.36/26.23/25.87 ms，
stdout 重复输出从 18.2 MB 降至 0；这是日志成本实验，不是 IM TPS/p99。日志仍同步磁盘 IO。
当前 SQL reservation 增加了受理往返，完整吞吐与尾延迟尚未重测；第 8 章旧数据保留为历史证据。

下一阶段的生产验收顺序见第 11 章：升级演练 → 真实故障/生命周期压力 → 安全部署与容灾 →
告警和恢复闭环 → 当前版本容量测量 → 依证据调整架构。
