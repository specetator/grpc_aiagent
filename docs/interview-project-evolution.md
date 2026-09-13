# Spark Push：从教学 Demo 到工程化实时消息系统

> 用途：简历项目描述、面试自我介绍和技术追问复盘。
> 对比基线：原始语雀学习文档与原仓库远端 `source/master`；本地快照 commit 为 `addc175`。
> 当前版本：上述基线叠加本仓库工作区中的工程化改造。
> 结论先行：这是一个“基于原始 Demo 做深度二次开发”的项目，不应包装成全部从零原创；真正有价值的是发现问题、定义语义、完成改造并用测试证明结果的过程。

原始资料：[语雀学习文档](https://www.yuque.com/linuxer/vhnzb2/dg193t3kaac4mvey)；[教学项目仓库](http://gitlab.0voice.com/2510_vip/11.2-spark_push.git)。

## 1. 面试时如何给项目定性

推荐项目名：

**Spark Push 分布式实时消息系统（二次工程化）**

一句话版本：

> 我基于一个 C++ 实时推送教学 Demo 做了系统性的二次工程化。原项目已经有 Comet、Logic、Job 三进程和 WebSocket、gRPC、Kafka、Redis、MySQL 基础链路，我重点补齐了双阶段 ACK、会话序号和幂等、缺口补偿、离线补推、持久化事件、Job 到 Comet 的双向流、场景隔离限流、可观测性、工业化用户中心，以及 Hermes AI Bot 接入，并用 E2E 和故障注入验证了消息一致性。

这个说法有三个优点：

1. 诚实说明原始 Demo 的贡献，不冒充全部从零实现；
2. 把自己的工作落到协议、状态机、存储和故障恢复，而不是只说“加了几个功能”；
3. 面试官可以继续追问 ACK、Kafka offset、幂等、游标和背压，正好进入项目最有深度的部分。

## 2. 原始 Demo 和当前版本的核心差异

| 维度 | 原始 Demo | 当前版本 | 工程价值 |
|---|---|---|---|
| 基础架构 | Client → Comet → Logic → Kafka → Job → Comet | 保留三进程架构并强化每段语义 | 没有为“重构”而推翻可用架构 |
| 上行 RPC | 每条消息 Unary gRPC | Comet → Logic 复用 `MessageStream` | 减少逐条 RPC 对象和状态机开销 |
| 下行 RPC | Job 每条消息调用 Unary `PushToComet` | 每个 Comet 一条双向 `PushStream`，reader/writer 分离 | 支持逐条响应、重连、超时和背压 |
| ACK | 一个笼统 `ack` | `accepted_ack` / `delivered_ack` | 区分“可靠受理”和“在线投递” |
| 序号 | MySQL `UPDATE` 后再 `SELECT` 取号 | Redis Lua 原子完成 floor 校准、去重和 `INCR` | 消除并发取号竞态，降低数据库热行压力 |
| 幂等 | 主要依赖 `(session_id,msg_seq)` 唯一键 | `client_msg_id` 热路径去重 + `request_id` 投递去重 + MySQL 字段比对 | 覆盖客户端重试、Kafka 重放和流重连 |
| 持久化 | Logic 同步落 MySQL 后发送实时 Kafka 消息 | `persist_message` 持久化事件，Job 幂等落库 | 将数据库写移出热路径，同时保留可重放边界 |
| Kafka 位点 | 业务回调只负责把 RPC 任务丢进线程池，随后异步提交 offset | 回调返回处理结果，RPC/落库完成后同步提交 | 不把“任务已入线程池”误当成“业务已完成” |
| 断线恢复 | 主要依赖手动查历史 | `msg_seq` 缺口检测、`SyncMessages`、`delivered_seq`、`SyncOffline` | 从“在线能收到”提升到“断线可恢复” |
| 场景隔离 | 推送共用主 topic 和资源 | single/group/broadcast/persist 分 topic、消费组和令牌桶 | 防止广播流量挤占单聊 |
| 可观测性 | 以日志为主 | Logic/Job/Comet metrics，统计 lag、延迟、重连、重复、丢失、限流 | 能回答“慢在哪里、丢在哪里” |
| 用户系统 | 账号、密码、Token 的教学实现 | PBKDF2、随机 Token、状态机、软删除、Token 批量撤销、审计日志、管理端 | 从登录 Demo 变成可运营的用户中心雏形 |
| 前端 | 基础收发和历史列表 | 乐观消息、状态气泡、去重、历史/实时屏障、重连、缺口补偿、安全 DOM | 可靠性语义真正体现在用户体验上 |
| AI 扩展 | 无 | Hermes Bot、异步 Kafka 编排、SSE 增量、最终消息回归普通单聊 | 复用 IM 可靠链路，不为 AI 另造一套消息系统 |
| 工程化 | 手工准备依赖和逐进程启动 | Docker Compose、一键启停、ready 检查、CTest、CI | 降低演示和回归成本 |

## 3. 五段式项目演进主线

这一节是整份文档的主干。无论写简历、做项目复盘还是回答面试题，都按下面的因果链展开：

```text
原始实现
   ↓ 先肯定已有架构和能力
发现问题
   ↓ 用故障窗口、源码和测试说明问题真实存在
技术优化
   ↓ 解释协议、状态、存储和性能上的取舍
功能扩展
   ↓ 说明可靠底座如何支撑用户中心、前端和 AI
面试讲法
   ↓ 用结果、证据和边界收口
```

### 3.1 原始实现：先承认 Demo 已经解决了什么

原始项目不是一个只有页面的简单聊天室。它已经完成了分布式实时消息系统的基本骨架：

```text
Client
  ├─ HTTP 登录/历史查询 ───────────────► Logic
  └─ WebSocket 长连接 ─► Comet ─gRPC─► Logic
                                           │
                                           ├─ MySQL：用户、会话、消息、已读状态
                                           ├─ Redis：Token、用户路由、房间路由
                                           └─ Kafka：push_to_comet / broadcast_task
                                                          │
                                                          ▼
                                                         Job
                                                          │ Unary gRPC
                                                          ▼
                                                        Comet
                                                          │ WebSocket
                                                          ▼
                                                        Client
```

原始实现中各模块的价值是：

| 模块 | 原始职责 | 设计价值 |
|---|---|---|
| Comet | Muduo WebSocket、鉴权、心跳、连接表、房间本机 fanout | 把高连接数和业务计算解耦 |
| Logic | 登录、会话、消息落库、路由查询、Kafka 生产 | 统一业务规则和消息生成 |
| Job | 消费 Kafka，通过 gRPC 下发 Comet | 削峰并隔离 Logic 与慢下游 |
| Redis | Token、`user -> comet`、`room -> comet` | 支撑多端在线和跨节点路由 |
| MySQL | 用户、会话、消息、已读游标、群组和弹幕 | 保存长期业务事实 |
| Kafka | 实时推送任务与广播任务 | 解耦生产、消费和故障恢复 |

原始 Demo 已支持单聊、聊天室、广播、弹幕、历史消息和未读水位。尤其是聊天室的两级 fanout 值得保留：Logic 只把消息发到房间有用户在线的 Comet，Comet 再在本机连接表中展开，避免 Logic 对大房间逐用户 fanout。

因此二次开发的起点不是推翻架构，而是追问：这条链路在并发、崩溃、重试、断线和慢依赖出现时，消息语义是否仍然成立？

### 3.2 发现问题：从“功能可演示”走向“故障后可解释”

我先沿一条消息逐段检查完成条件，而不是直接增加中间件。最终归纳出下面十类问题。

| 问题 | 原始实现/现象 | 真实风险 | 发现依据 |
|---|---|---|---|
| ACK 语义模糊 | 上行只有一次笼统回复 | 无法区分“系统已受理”和“目标已投递”，失败后不知道该不该重试 | 对照上行回复、Kafka 生产和 Comet 下发三个时间点 |
| 会话序号竞态 | MySQL 先 `UPDATE last_msg_seq+1`，再独立 `SELECT` | 并发请求可能读到相同的新值，产生重复 `msg_seq` | 检查原版 `SessionDao::AllocateMessageSeq` 的两个独立语句 |
| Kafka 位点过早 | Job callback 只把 Unary RPC 放入线程池，callback 返回后 consumer `commitAsync` | Job 崩溃时 offset 已前移、内存任务尚未完成，消息可能丢失 | 对照原版 `JobRunner::HandleMessage` 与 `KafkaConsumer::HandleMessage` |
| Logic 崩溃窗口 | 同步落 MySQL、发送实时 Kafka、返回客户端分散在多个步骤 | 任一步骤失败都可能出现“已落库未推送”或“已回复但缺少可重放事件” | 枚举每一步之后进程崩溃的结果 |
| 断线不可自愈 | 只有历史查询和 `read_seq`，没有缺口协议与目标投递游标 | 客户端短暂断网可能漏消息；用 read 水位补推又会重复发送未读消息 | 检查原版 proto 中没有 `SyncMessages/SyncOffline/MarkDelivered` |
| 下行缺少背压 | 每条 Kafka 消息创建一次 Unary 调用，线程池异步掩盖下游变慢 | 慢 Comet 会积压内存任务；任务入队不等于投递成功 | 分析 Job 线程池、RPC 完成和 offset 的先后关系 |
| 场景互相干扰 | 单聊、群聊共用推送 topic 和资源，广播也会占用 Job 能力 | 广播或大群峰值可能拖高单聊尾延迟 | 对照 topic、消费组和 worker 的资源边界 |
| 安全与运营不足 | 密码直接写入 `password_hash` 字段，Token/状态/审计为教学实现 | 数据泄露后无法抵抗离线破解，也无法封禁、撤销和追责 | 检查原版用户 DAO 与 HTTP 登录逻辑 |
| 前端状态竞态 | 历史 HTTP、实时 WebSocket、乐观消息分别渲染 | 重复气泡、顺序跳动、切换会话后旧请求污染当前页面 | 实际复现 Hermes 对话中历史和实时内容交错 |
| 可运维性不足 | 手工启多个服务，主要依赖日志观察 | 环境难复现，无法量化 lag、延迟、重复和丢失 | 实际搭建和排障过程 |

问题定位中最关键的认识是：

```text
任务进入线程池 ≠ RPC 完成
Kafka produce 入本地队列 ≠ broker 已确认
Comet 调用 conn->send() ≠ 浏览器已处理
浏览器收到消息 ≠ 用户已读
实时投递成功 ≠ MySQL 已完成最终落库
```

如果不先区分这些事实，后续所有“消息不丢”“已送达”和“高性能”都只是模糊口号。

### 3.3 技术优化：围绕消息状态机逐层补齐

技术改造不是若干互不相关的功能，而是由同一个消息状态机推导出来的。

#### 第一步：重新定义消息完成语义

我把消息状态拆成：

```text
accepted：消息已经越过可重放的持久化边界
delivered：目标 Comet 已把消息交给在线 WebSocket 连接发送队列
read：客户端上报用户已读水位
stored：持久化消费者已将消息幂等写入 MySQL
```

协议增加 `accepted_ack` 和 `delivered_ack`。`accepted_ack` 以 `persist_message` 获得 Kafka delivery report 为边界；`delivered_ack` 以 Job 收到目标 Comet 对同一 `request_id` 的成功 reply 为边界。当前 delivered 是服务端投递，不冒充客户端确认或已读。

#### 第二步：修复序号与端到端幂等

把 MySQL 热行上的两步取号改为 Redis Lua，单次原子完成：

1. 用 MySQL `MAX(msg_seq)` 校准 Redis floor；
2. 按 `session_id + sender_id + client_msg_id` 检查客户端重试；
3. 新消息执行 `INCR` 分配会话内序号；
4. 单调推进 `session:last_seq` 并设置去重 TTL。

不同故障层使用不同幂等键：客户端重试使用 `client_msg_id`，Job/Comet 流重放使用 `request_id`，最终 MySQL 使用 `(session_id,msg_seq)` 唯一约束并比较完整字段。这里追求的是至少一次链路上的业务幂等，不宣称分布式 exactly-once。

#### 第三步：把可靠事件和 MySQL 写入解耦

Logic 不再把本地线程池任务当成可靠持久化，而是先把 `PersistMessageRequest` 写入持久化 Kafka topic，并等待 broker delivery report。Job 的独立 persist consumer 幂等写 MySQL，业务完成后才同步提交 offset。

```text
Kafka 至少一次事件
  + Redis/client_msg_id 热路径幂等
  + MySQL 唯一键与字段比对
  = 可重放的最终一致持久化
```

当前事实源选择更接近“Kafka durable event”。如果未来要求 MySQL 事务是唯一事实源，则应切换为业务行与 outbox 行同事务提交，再由 relay 发布 Kafka，而不是把两种方案混为一谈。

#### 第四步：长连接流、背压和正确提交位点

Comet→Logic 增加 `MessageStream`，Job→每个 Comet 复用一条双向 `PushStream`。下行流包含：

- 有界队列，满时拒绝而不是无限占内存；
- writer 串行写、reader 按 `request_id` 匹配 reply；
- deadline、指数退避重连和 Unary fallback；
- Comet 最近请求 ID 去重；
- 收到对应 reply 后，本条 Kafka 消息才允许提交位点。

这里不错误宣称 Unary 每条都新建 TCP；gRPC Channel 本身会复用 HTTP/2 连接。Streaming 的价值是减少逐调用状态开销，并把应用层排队、逐条应答、重连和背压统一到一条可观测的流中。

#### 第五步：缺口补偿和离线补推

客户端为每个 session 维护连续 `msg_seq`：小于等于当前水位时去重，等于 `last+1` 时推进，大于 `last+1` 时调用 `SyncMessages` 分页补洞。服务端另存每用户、每会话的 `delivered_seq`；重新鉴权后调用 `SyncOffline`，补推大于投递水位的消息。

`delivered_seq` 与 `read_seq` 必须分离。用户可以收到但没有阅读；如果用 `read_seq` 做重连补推，每次重连都会重复发送全部未读消息。

#### 第六步：场景隔离、可观测性和验证闭环

单聊、群聊、广播、持久化拆分 topic 和 consumer group；入口按 sender、room、scope 使用独立令牌桶，避免广播制造单聊队头阻塞。Logic、Job、Comet 输出 Prometheus text metrics，记录 Kafka lag、投递延迟、重连、重复、丢失、限流和持久化结果。

验证口径从“发送端 socket write 成功”升级为独立发送端与接收端的 E2E：

```text
sent == accepted_ack == delivered_ack == receiver_delivered
send_failed == 0
ack_errors == 0
```

本机 8 连接 × 100 条回归得到 800/800/800/800，实际接收 QPS 535.5 msg/s；p50 753.8ms、p95 1411.1ms、p99 1469.0ms。这个数字只代表本机回归，并暴露队列平台区的尾延迟，不能外推为生产集群容量。

### 3.4 功能扩展：让可靠底座产生业务价值

功能扩展建立在可靠消息链路之上，并明确哪些是原 Demo 已有能力、哪些是二次开发。

| 能力 | 归属 | 当前实现重点 |
|---|---|---|
| 单聊、聊天室、广播、弹幕、历史 | 原 Demo 已有，当前继续完善 | 不把课程原有功能包装成个人原创 |
| 用户中心与管理员端 | 二次开发 | PBKDF2、CSPRNG Token、active/disabled/deleted、软删除、批量撤销、审计日志和管理 API/UI |
| 前端可靠状态 | 二次开发 | 乐观消息、发送中/已接收/已送达、统一去重、历史屏障、重连、缺口同步、安全 DOM |
| Hermes AI Bot | 二次开发 | 固定 Bot、独立 Bridge、Kafka 异步编排、多轮历史、SSE delta、final reply 回归普通单聊 |
| 工程化交付 | 二次开发 | Docker Compose、一键启停、ready 检查、CTest、CI、Windows 主机访问 |

Hermes 接入没有直接放进 Logic 同步调用。用户输入先作为普通消息越过 accepted 边界，再写 `ai_request`；Bridge 调用 Windows Hermes `/v1/chat/completions`，增量经 `ai_delta` 提升首字体验，最终 `ai_reply` 重新进入普通单聊的序号、持久化、推送和离线恢复链路。

临时 delta 不落库，因为它只是展示过程；最终回答才是可靠事实。这样即使生成时断线，用户重连后仍能通过普通历史和游标恢复完整回答，也不会把半截文本污染消息表。

用户中心同样体现了数据分层：MySQL 是用户主数据，Redis 只保存 Token 和在线态；删除采用软删除，避免历史消息中的 sender 引用失效；禁用和删除伴随 Token 批量撤销，管理员操作写追加式审计日志。

### 3.5 面试讲法：用问题、决策、证据和边界形成闭环

推荐回答顺序不是罗列技术栈，而是四句话一个闭环：

1. **背景**：原 Demo 已有 Comet/Logic/Job 和完整实时链路，我负责二次工程化；
2. **问题**：ACK、offset、序号和断线恢复缺少严格语义，异步线程池还扩大了崩溃窗口；
3. **决策**：定义 accepted/delivered/read，增加 Redis Lua 幂等序号、持久化 topic、PushStream、游标补偿和场景隔离；
4. **证据与边界**：用 E2E、数据库复核和 Redis 故障注入证明结果，同时说明 delivered 不是已读、本机数据不能外推生产。

一句话讲法：

> 我基于 C++ 实时推送教学 Demo 做深度二次工程化，重点不是增加页面，而是重新定义消息系统在重试、崩溃和断线场景下的完成语义；我实现了双阶段 ACK、幂等序号、Kafka 可重放持久化、gRPC 双向流、缺口与离线补偿，再以 E2E 和故障注入闭环，最后在这套可靠底座上补了用户中心和 Hermes AI Bot。

面试中应主动区分三类贡献：

- **继承并理解**：三进程架构、WebSocket/gRPC/Kafka/Redis/MySQL、单聊/房间/广播/弹幕；
- **分析并改造**：ACK、offset、幂等、序号、流式下发、背压、游标、限流、指标和测试；
- **独立扩展**：用户中心、管理员端、前端状态机、Hermes Bridge、SSE 和工程化脚本。

这种表达既不会掩盖教学项目基线，也能把个人工作落实到可以继续追问的源码、故障窗口和测试结果。

## 4. 从最开始构建项目的完整演进顺序

下面的顺序适合面试讲述。前五阶段描述原 Demo 已经具备的基础，后续阶段是二次工程化工作的重点。

### 阶段 0：先定义问题，而不是先堆中间件

目标不是简单做一个网页聊天室，而是拆解实时消息系统的三个职责：

- Comet 管连接：维护大量 WebSocket、鉴权绑定用户、管理本机房间成员、向客户端收发帧；
- Logic 管业务：校验用户和场景、生成会话消息、查询路由、决定投递和持久化；
- Job 管异步任务：消费 Kafka，把消息路由到对应 Comet，隔离业务入口与下行推送。

Redis 保存 Token、用户到 Comet 的在线路由和热游标；MySQL 保存用户、会话、消息和用户会话状态；Kafka 解耦 Logic 和 Job。

面试表达：

> 一开始最重要的决定是按连接、业务、异步投递拆进程。Comet 是有状态的连接网关，Logic 尽量保持可横向扩展，Job 承接 Kafka 消费和 fanout。这个边界后面增加可靠性时基本没有推翻，说明原始分层是合理的。

### 阶段 1：搭出可编译、可运行的 C++ 服务骨架

原项目使用 C++17、CMake、Muduo、gRPC/Protobuf、librdkafka、Hiredis 和 MySQL Client，先完成公共配置、日志、连接池、线程池和协议生成，再分别启动 Logic、Comet、Job 和 Web Demo。

这一阶段真正需要考虑的是依赖边界和线程模型：

- Muduo EventLoop 负责网络事件，不能在事件循环线程中长时间阻塞；
- gRPC 用于服务间强类型通信，Protobuf 作为 Kafka payload 的一部分；
- MySQL/Redis 使用连接池，避免每次请求重连；
- Kafka producer 复用，并配置 `acks=all`、幂等 producer、重试和 batching。

面试表达：

> 我没有把所有逻辑塞进一个服务。网络 I/O、业务判断、异步投递和持久化分别有明确边界，后面做压测时才能判断瓶颈属于事件循环、数据库、Kafka 还是 gRPC。

### 阶段 2：完成认证和 WebSocket 长连接

原始链路是：浏览器先经 HTTP 登录拿 Token，再通过 WebSocket query 参数连 Comet；Comet 调 Logic 的 `VerifyToken`，Logic 从 Redis 查 `token:<token>`，并登记 `route:user:<uid> -> comet_id`。

这一步解决“连接属于谁”和“消息应该发到哪个 Comet”。用户可能多端登录，因此路由是集合而不是单值；单聊时 Logic 按目标用户查询全部 Comet，再按 Comet 聚合目标用户。

原始实现仍有明显教学性质：密码字段直接保存传入值，Token 由用户 ID 和秒级时间戳拼接，登录日志还可能输出 Token。后来用户中心阶段会统一修复。

### 阶段 3：跑通单聊实时消息链路

原始单聊路径：

```text
浏览器
  → WebSocket single_chat
  → Comet
  → Logic.SendUpstreamMessage
  → MySQL 创建会话、分配 msg_seq、插入 message
  → Kafka push_to_comet
  → Job 消费
  → Unary gRPC PushToComet
  → 目标 Comet 本机连接集合
  → WebSocket
```

这一版已经能演示分布式路由：Logic 不直接持有客户端连接，Job 也不理解浏览器协议，Comet 只向本机连接发送。

但是“能收到”不等于“可靠”：ACK 没有精确定义；Kafka send 入本地队列不等于 broker 已确认；Job 把任务提交线程池后 consumer 就可能推进 offset；客户端断线期间主要靠手工拉历史恢复。

### 阶段 4：扩展聊天室、广播、弹幕和历史消息

原 Demo 继续增加了：

- 聊天室：Redis 维护 `room -> comet`，Logic 只 fanout 到有该房间在线用户的 Comet，再由 Comet 做本机二次 fanout；
- 广播：任务写入 `broadcast_task`，Job 投递所有配置的 Comet；
- 弹幕：按 `video_id + timeline_ms` 存储并按时间轴查询；
- 历史、未读和会话列表：MySQL 保存消息，`read_seq` 表示已读水位。

这里最值得讲的是两级 fanout：如果 Logic 展开房间内所有用户，房间人数越大，业务层 CPU、Redis 查询和 Kafka payload 都会膨胀；按 `room -> comet` 路由后，跨服务只传到相关节点，用户级展开留在 Comet 本机内存完成。

### 阶段 5：原 Demo 的第一次异步性能优化

原版本给 Job 增加 RPC 线程池，Kafka consumer 收到消息后把 Unary gRPC 调用提交到线程池，避免一个慢 Comet 完全阻塞消费线程。这提升了吞吐，但引出了一个更隐蔽的语义问题：

```text
Kafka callback 返回
    ≠ RPC 已完成
    ≠ Comet 已接收
    ≠ WebSocket 已发送
```

原 Kafka callback 是 `void`，`HandleMessage` 提交线程池后立即返回，consumer 随后 `commitAsync`。如果 Job 此时崩溃，offset 可能已经前移，而 RPC 任务尚未完成。

面试表达：

> 这是项目从“性能 Demo”转向“可靠性工程”的转折点。异步化不能只看 QPS，还必须重新定义完成条件。否则只是把等待藏到后台，同时丢掉了故障恢复依据。

### 阶段 6：先画消息状态机，再改代码

我先把消息状态拆成三个不同事实：

1. accepted：系统已经获得可重放的消息事实；
2. delivered：服务端已经把消息交给目标在线 WebSocket 连接的发送队列；
3. read：用户在业务层确认阅读。

当前只实现前两个阶段和已有 `read_seq` 接口，没有把 delivered 说成 read。状态机如下：

```text
client_msg_id
    │
    ▼
Redis Lua：幂等检查 + 分配 msg_seq
    │
    ├─ persist_message 得到 Kafka delivery report ──► accepted_ack
    │
    └─ push_single / push_group
             │
             ▼
       Job PushStream → Comet → WebSocket send
             │
             ├─ 推进目标用户 delivered_seq
             └─ 回来源 Comet delivered_ack
```

这个状态机决定了后面的协议、Kafka 提交点、游标和测试口径。

### 阶段 7：修复序号和幂等热路径

原版 `msg_seq` 由 MySQL 执行 `UPDATE last_msg_seq = last_msg_seq + 1`，随后再 `SELECT last_msg_seq`。两个语句不在同一事务内，并发时可能出现：A 更新到 1，B 更新到 2，A 和 B 随后都读到 2，从而产生重复序号。

当前版本改成 Redis Lua，一次原子脚本完成：

- 将 Redis 当前值提升到不低于 MySQL `MAX(msg_seq)` 的 floor；
- 检查 `session_id + sender_id + client_msg_id` 去重键；
- 新消息执行 `INCR` 分配序号；
- 单调更新 `session:last_seq`；
- 给去重键设置 TTL。

首次看到某会话时回源 MySQL 最大序号，解决 Redis 丢数据或水位落后后序号回退的问题。持久化时 MySQL 继续用 `(session_id,msg_seq)` 唯一键兜底；遇到重复键时比较完整消息字段，相同才视为幂等重放，不同则报告真实冲突。

验证不是只跑 happy path。我把 MySQL 最大序号保持在 100，人为把 Redis 水位降到 1，重启 Logic 后继续发送，实际分配 101～120，没有回退或唯一键冲突。

### 阶段 8：把 ACK 拆成 accepted 和 delivered

协议增加 `AckStage`、`accepted_at_ms`、`request_id`、`delivered_count`、`ack_comet_id` 和 `ack_user_id`。

`accepted_ack` 只有在 `persist_message` 收到 Kafka delivery report 后才返回。它回答的是：“如果 Logic 此时重启，是否还有一个可重放事件？”

`delivered_ack` 在 Job 收到目标 Comet 对指定 `request_id` 的成功回复后，再回到发送方所在 Comet。它回答的是：“目标在线连接是否已经进入服务端发送阶段？”

需要主动说明边界：Muduo `conn->send()` 成功不等于远端浏览器已经处理，更不等于用户已读。生产级强 delivered 还可增加客户端协议 ACK；read receipt 则是第三种业务事件。

### 阶段 9：将持久化移出热路径，但不用不可靠的本地异步队列

原版在 Logic 同步操作 MySQL，时延和连接池压力直接进入消息热路径。简单改成本地线程池虽然快，但 Logic 在任务尚未落库时崩溃会丢任务。

当前选择“持久化 Kafka topic”方案：Logic 构造 `PersistMessageRequest` 写入 `persist_message`，Job 的独立消费组负责补建 session、插入 message，并用 `GREATEST` 推进 `last_msg_seq`。只有业务处理完成后才提交 offset。

一致性模型是：

```text
至少一次 Kafka 事件
    + Redis/client_msg_id 幂等
    + MySQL 唯一键和字段比对
    = 可重放的最终一致持久化
```

没有宣称分布式 exactly-once。当前 retry 耗尽会写不含正文的 `[DLQ]` 审计日志，但尚未实现独立可重放 DLQ topic。

面试追问“为什么不是事务 Outbox”时可以回答：

> 当前消息先以 Kafka 作为 accepted 的 durable boundary，因此选择持久化事件更贴合现有链路。如果业务必须以 MySQL 事务为事实源，我会把业务行和 outbox 行放在同一事务，再由 relay 发布 Kafka。两者解决的是不同事实源下的双写问题。

### 阶段 10：增加游标、缺口补偿和离线补推

实时链路可能乱序、重复，也可能在用户断线时无法投递。客户端不依赖“我大概收到了”，而是维护每个 session 的连续 `msg_seq`：

- 正常收到 `last_seq + 1`：推进连续游标；
- 收到 `seq <= last_seq`：按消息身份去重；
- 收到 `seq > last_seq + 1`：发送 `sync`，从 `after_seq` 拉缺口；
- `SyncMessages` 在 Logic 校验用户确实属于单聊或聊天室，再查询 MySQL；
- `sync_end.has_more` 为真时继续分页，直到补齐。

服务端为每个用户、每个会话增加 `delivered_seq`。WebSocket 鉴权成功后调用 `SyncOffline`，查询大于该水位的消息；只有 Comet 确实找到连接并调用发送后才单调推进游标。

开发中真实发现过一个问题：在线实时下发成功后没有推进接收方 `delivered_seq`，导致重连时把已收到的消息再次补推。修复方式是 Comet 汇总多个目标的 `UserDeliveredCursor`，批量调用 `MarkDelivered`；前端仍按 `msg_id/client_msg_id` 去重，形成服务端游标和客户端幂等两层保护。

### 阶段 11：Job → Comet 改为双向长连接，并补齐背压

原版虽然缓存了 gRPC Channel，HTTP/2 传输连接通常可以复用，但每条消息仍是一个独立 Unary RPC，需要创建 ClientContext、发送 metadata、维护调用状态并逐条等待结果。

当前 Job 为每个 Comet 维护一条 `PushStream`：

- writer 线程从有界队列串行写请求；
- reader 线程按 `request_id` 匹配每条 reply；
- Kafka 消息只有收到对应 reply 后才算本次业务处理完成；
- 流断开后指数退避重连；
- `request_id=msg_id@comet_id`，Comet 保存有界最近 ID 集合，重放不重复下发；
- stream 超时或不可用时回退到有 deadline 的 Unary；
- 队列达到 `push_stream_queue_max` 后拒绝，而不是无限占用内存。

这里的重点不只是“长连接更快”，而是把请求、回复、Kafka offset 和重连幂等串成一个完整完成协议。

### 阶段 12：按业务场景隔离限流、排队和消费

单聊、群聊、广播的流量形态不同：单聊强调低延迟，群聊会放大 fanout，广播可能瞬间覆盖所有 Comet。如果共用队列，广播峰值会制造 head-of-line blocking。

当前按场景拆分：

| 场景 | 限流 key | 默认速率 / burst | topic | consumer group |
|---|---|---:|---|---|
| single | sender user | 1000/s，200 | `push_single` | `spark_push_group_single` |
| group/chatroom | room id | 300/s，60 | `push_group` | `spark_push_group_group` |
| broadcast | scope | 20/s，5 | `broadcast_task` | `spark_push_group_broadcast` |
| persist | session id | 由 Kafka 和消费能力约束 | `persist_message` | `spark_push_group_persist` |

入口采用 `scene + key` 的进程内令牌桶；Kafka topic 和消费组进一步做资源隔离；Job 到不同 Comet 的 stream 队列也互相隔离。

需要承认：当前令牌桶是 Logic 单实例内存状态，多 Logic 部署时不是全局限流。生产环境可改成网关级限流、Redis Lua 分布式桶或按用户一致性路由到固定 Logic。

### 阶段 13：用可观测性和 E2E 定义“完成”

只打印日志不能回答是 Kafka 堵、Comet 重连，还是客户端没收到。因此给 Logic、Job、Comet 增加轻量 Prometheus text metrics，重点包括：

- `reconnect_total`：Job → Comet 重连；
- `kafka_lag_ms`：最近 record age；
- `delivery_latency_ms`：投递延迟 count/sum/max；
- `delivery_attempt/success/lost/duplicate`：投递结果；
- `delivery_cursor_update_failed`：游标更新失败；
- `rate_limited_total_<scene>`：场景限流；
- `persist_success/failed`：持久化结果；
- `comet_push_stream_ready`：流真正进入 Comet 服务端。

原压测工具只统计发送端 `socket send`，不能证明接收端收到。我新增 E2E 工具，发送账号和接收账号使用独立 WebSocket，同时统计：

```text
sent == accepted_ack == delivered_ack == receiver_delivered
send_failed == 0
ack_errors == 0
```

本机 8 连接 × 100 条回归结果是 800/800/800/800，实际投递 QPS 535.5 msg/s，p50 753.8ms、p95 1411.1ms、p99 1469.0ms。这个结果不是为了宣称高性能，反而说明压力进入队列平台区后尾延迟明显上升，因此必须结合 lag、队列长度和 p99 调参。

另一个真实问题是：E2E 已经实时 100/100，但测试后立即停止 Job，数据库暂时没有完整落库；继续运行 Job 后 `persist_message` 重放成功。由此把实时 delivered 和 persistent stored 明确成两个阶段，测试也增加数据库与游标复核。

### 阶段 14：把账号系统升级为用户中心雏形

原 Demo 的认证只适合教学。当前做了以下改造：

- 密码使用 PBKDF2-HMAC-SHA256、随机 16 字节 salt、120000 次迭代；
- 兼容旧 MD5，登录校验成功后标记升级，避免一次迁移让旧账号全部失效；
- Token 使用 OpenSSL CSPRNG 生成 32 随机字节，不再拼接用户 ID 和时间；
- Redis 除 `token:<token>` 外增加 `user:tokens:<uid>` 反向索引，可以批量撤销；
- 用户增加 active、disabled、deleted 状态和 `deleted_at`，采用软删除保留消息和审计引用；
- 增加用户列表、禁用、恢复、软删除、修改资料、Token 撤销和审计查询接口；
- 审计日志只追加，记录 actor、target、action、reason、metadata 和时间；
- 关键 HTTP 接口要求 Bearer Token，并校验 body 中的 user_id 与认证身份一致；
- WebSocket 握手和消息上行都检查账号状态，禁用账号不能重新鉴权或继续发送。

这部分的设计思考是：用户表是 MySQL 主数据，Redis 只存凭证和在线态；删除用户不能直接物理删行，否则历史消息会出现发送者孤儿引用。

当前边界是已建立连接不会被跨节点立即踢断，只是后续上行会被拒绝。生产版需要踢线事件、统一 IAM、强一致审计和权限模型。

### 阶段 15：让前端行为与后端可靠性语义一致

后端有 ACK 和游标，如果页面仍然重复、跳动或断线后无反馈，用户不会认为系统可靠。当前单聊和聊天室前端增加：

- 发送后立即显示乐观气泡；
- `发送中 → 已接收 → 已送达` 状态更新，失败能按 `client_msg_id` 定位气泡；
- 历史、实时和乐观消息使用统一身份映射去重；
- 首屏历史加载期间暂存实时消息，完成后按序合并；
- 切换会话时用 generation + AbortController 丢弃旧请求；
- WebSocket 指数退避重连，重连后重新 join 并触发游标同步；
- 只在用户接近底部时自动滚动，避免阅读历史时被新消息拉走；
- 消息正文使用 DOM `textContent`，避免内容通过 `innerHTML` 造成 XSS 或破坏布局；
- Comet 开启 `TCP_NODELAY`，降低小消息受 Nagle 算法影响的等待。

曾经出现过“像是把后台缓存内容也发出来”的现象。定位后发现主要是历史 HTTP 返回、WebSocket 实时消息和临时 AI 气泡并发合并的竞态，不是把 Hermes 内部缓存泄漏给客户端。通过视图 generation、历史屏障和消息身份去重修复了展示顺序和重复问题。

### 阶段 16：补齐本地交付、测试和 CI

为了让项目不是“只能在作者机器运行”，增加了：

- Docker Compose 启动 Redis、MySQL、Kafka，并使用数据卷保留数据；
- `start_demo.sh` 自动检查配置、依赖健康、topic、编译产物、端口和 PushStream ready；
- `stop_demo.sh` 只停止当前项目的精确可执行文件，可选择停止依赖但不删除卷；
- `.env.example/.env.local` 隔离真实口令，SQL 初始化默认不再 DROP 数据库；
- CTest 覆盖密码与 Token、令牌桶、配置、WebSocket frame 和 Redis 并发序号；
- CI 执行构建、单元测试和 Redis 集成测试。

这里一个细节很能体现真实运维思考：只检测 9105 端口监听还不代表 Job 的 PushStream 已进入服务端，所以启动脚本等待 `spark_push_comet_push_stream_ready 1`，避免冷启动时页面能打开但第一批消息丢进未就绪链路。

### 阶段 17：接入 Hermes Bot，但不阻塞 IM 热路径

第一阶段先增加固定 Hermes Bot 用户和独立 `hermes_bridge`。用户给 Bot 发消息后：

```text
用户输入
  ├─ persist_message：先保存普通单聊消息
  └─ ai_request：异步交给 hermes_bridge
          └─ HTTP /v1/chat/completions → Windows Hermes
                  └─ ai_reply → Logic
                         ├─ 分配正式 msg_seq
                         ├─ persist_message
                         └─ push_single → Job → Comet
```

关键设计是外部 AI 请求不占用 Comet/Logic 普通消息热路径。Hermes 慢或失败不会阻塞其他用户聊天，最终回答仍是普通单聊消息，因此历史、游标、离线补推和 E2E 语义都可复用。

多轮上下文由 Spark Push 从 MySQL 最近 50 条历史恢复，并加约 12 KiB prompt 预算；进程内保留最多 100 条短期热上下文，覆盖上一轮已推送但尚未完成 MySQL 落盘的短窗口。最终事实仍以 MySQL 为准。

### 阶段 18：从“完整回答”优化到 SSE 增量，并拆解 AI 延迟

非流式调用必须等 Hermes 生成完整回答，体感明显慢。随后启用 `stream=true`：Bridge 读取 SSE delta，发布 `ai_delta`；Logic 只把它作为不分配 `msg_seq` 的临时体验事件推送；最终 `ai_reply` 再生成唯一正式消息并替换临时气泡。

为什么 delta 不持久化：半截文本不是可靠业务事实，断线补半截内容既复杂又容易污染历史。最终完整回答才进入 `persist_message`，重连后恢复完整消息即可。

为了分析“本机为什么比 Telegram 还慢”，把延迟拆为：

```text
用户发送 → accepted_ack          ：Spark Push 受理延迟
请求开始 → 首个 hermes_delta     ：Hermes TTFT
请求开始 → final ai_reply         ：完整生成延迟
```

并记录 TTFT、总生成时延、prompt 字符数和消息数。若 accepted 很快而首个 delta 很慢，瓶颈通常在模型推理、工具调用或上下文，而不是 Kafka/Comet/WebSocket。

当前 Bridge 仍是单进程同步消费，一次长回答可能阻塞同 partition 后续 AI 请求；下一步应增加有界并发 worker、每会话保序、取消请求、持久化请求状态和 AI 专用 DLQ。

## 5. 一条消息现在到底怎样走

面试时可以沿下面这条路径讲源码：

1. 浏览器生成唯一 `client_msg_id`，乐观插入“发送中”气泡；
2. Comet 从已鉴权连接取得 user_id，通过 `MessageStream` 发 Logic，不相信客户端伪造的发送者；
3. Logic 检查账号状态、参数、场景限流和目标权限；
4. Redis Lua 以 `session + sender + client_msg_id` 去重并分配连续 `msg_seq`；
5. Logic 把 `PersistMessageRequest` 写入 `persist_message` 并等待 delivery report；
6. 成功后向客户端返回 `accepted_ack`；
7. 在线目标按路由聚合后写入 `push_single` 或 `push_group`；
8. Job 消费消息，通过对应 Comet 的 `PushStream` 发送，并等待同一 `request_id` 回复；
9. Comet 按目标用户或本机房间成员下发 WebSocket，批量上报目标 `delivered_seq`；
10. Job 回来源 Comet `delivered_ack`，发送方气泡变成“已送达”；
11. 独立 persist consumer 幂等写 MySQL，再推进 `session.last_msg_seq`；
12. 如果客户端发现序号缺口或重连，调用 `SyncMessages/SyncOffline` 从 MySQL 补齐。

这条链路中 Kafka 实时投递和持久化是两个 topic，因此顺序不保证完全同步；协议通过 accepted、delivered、stored 的不同事实和幂等来容纳这种异步性。

## 6. 两分钟面试介绍稿

> 这个项目是一个 C++17 的分布式实时消息系统。我不是把原始教学 Demo 直接写进简历，而是在它的 Comet、Logic、Job 三进程架构上做了完整二次工程化。Comet 基于 Muduo 管 WebSocket 长连接，Logic 负责鉴权、会话序号、路由和 Kafka 生产，Job 消费 Kafka 并通过 gRPC 把消息投递到对应 Comet；Redis 保存 Token、在线路由和热游标，MySQL 保存用户、会话和消息。
>
> 我改造时先发现原系统的“ack”语义不清，而且 Job 把 RPC 任务提交线程池后就可能提交 Kafka offset，所以异步性能和可靠性是冲突的。我先定义 accepted、delivered、read 三个阶段，然后把协议拆成 accepted_ack 和 delivered_ack。消息用 Redis Lua 原子完成 client_msg_id 去重和 msg_seq 分配，持久化改成独立 Kafka topic，由 Job 至少一次消费、MySQL 唯一键幂等落库。客户端维护每会话连续游标，发现缺口调用 SyncMessages，重连后按 delivered_seq 自动补离线消息。
>
> 性能上，我把 Job 到每个 Comet 的逐条 Unary RPC 改成双向 PushStream，做了有界队列、request_id 匹配、超时、指数退避重连、去重和 Unary fallback；单聊、群聊和广播分 topic、消费组和令牌桶，避免广播挤占单聊。为了证明改造有效，我写了独立收发账号的 E2E 工具，本机 8×100 条测试达到 sent、accepted、delivered_ack、receiver delivered 全部 800，并通过 Redis 水位故障注入验证序号不会回退。
>
> 后面我又补了 PBKDF2、随机 Token、用户禁用和软删除、Token 批量撤销、审计日志与管理端，并把 Windows 上的 Hermes 作为 Bot 通过独立 Bridge 接入。AI 增量只负责在线体验，最终回答仍回到普通单聊持久化链路，所以不会破坏历史和离线恢复语义。

## 7. 简历写法

### 项目标题

**Spark Push 分布式实时消息系统｜C++ 后端 / 二次工程化**

### 项目描述

基于 C++17、Muduo、gRPC/Protobuf、Kafka、Redis、MySQL 构建的分布式实时消息系统，采用 Comet/Logic/Job 分层，支持单聊、聊天室、广播、弹幕、历史消息、离线补推和 AI Bot。

### 推荐写成 4 条

- 基于教学 Demo 完成消息可靠性重构，设计 `accepted_ack/delivered_ack` 状态机；以 Redis Lua 原子实现 `client_msg_id` 幂等和会话 `msg_seq` 分配，并以 MySQL 唯一键及字段比对兜底 Kafka 至少一次重放。
- 将消息持久化改为独立 Kafka durable topic，修正消费完成与 offset 提交边界；实现 `msg_seq` 缺口补偿、`delivered_seq` 离线补推和客户端去重，解决断线丢消息及重连重复投递问题。
- 将 Job→Comet 下行改为按节点复用的 gRPC 双向流，加入有界队列、逐请求应答、超时、指数退避重连、流内幂等和 Unary fallback；按单聊/群聊/广播拆分 topic、消费组与限流策略。
- 建立 metrics、CTest、Redis 故障注入和独立收发端 E2E 回归；本机 8 连接×100 条测试实现 sent/accepted/delivered_ack/receiver-delivered 均为 800，并据 p99 与 Kafka lag 识别队列平台区，而非仅以 socket send 作为吞吐指标。

如果岗位偏业务后端，可把第四条替换为：

- 将教学账号模块升级为用户中心雏形：PBKDF2 加盐哈希、CSPRNG Token、active/disabled/deleted 状态机、软删除、Token 反向索引批量撤销、管理员 API 和追加式审计日志，并接入 Hermes Bot 异步消息链路与 SSE 增量回复。

## 8. 高频追问与回答

### 8.1 为什么需要两个 ACK？

因为一个 ACK 无法同时表达可靠受理和在线投递。accepted 的恢复依据是 Kafka 持久化事件；delivered 的依据是目标 Comet 已把消息交给在线连接发送队列。两者失败处理不同，read 又是更上层的用户行为，不能混为一谈。

### 8.2 delivered_ack 会不会说得太满？

会，所以要主动限定：当前 delivered 是 server-side delivered，不是 TCP 对端确认或用户已读。更严格的实现应由客户端收到消息后回协议 ACK，再单独产生 read receipt。

### 8.3 为什么 Redis 分配序号，MySQL 还保留 last_msg_seq？

Redis 是低延迟热路径，MySQL 是长期事实源。首次访问会话时用 MySQL `MAX(msg_seq)` 校准 Redis floor，防止 Redis 恢复后回退；持久化再用 `GREATEST` 单调推进 MySQL 水位。两者不是双主，而是热路径与最终事实分工。

### 8.4 Redis Lua 就能保证全局有序吗？

它保证同一个 Redis 主节点上、同一 session key 的原子递增，不保证跨 session 全局顺序。项目需要的是会话内顺序。Redis Cluster 部署时相关 key 需要使用相同 hash tag，主从切换仍需结合持久化和故障策略评估是否丢最近写。

### 8.5 为什么不用 Kafka exactly-once？

Kafka EOS 主要覆盖 Kafka 内 consume-transform-produce 事务，不能自动把外部 MySQL 写入同一事务。这里采用至少一次消费加业务幂等；若 MySQL 是主事实源，则进一步使用事务 Outbox 和 relay。

### 8.6 Job 为什么不能提交任务后就 commit offset？

因为线程池接受任务只说明任务在进程内存里。进程崩溃后任务消失，但 offset 可能已经前移。当前回调必须等 PushStream reply 或持久化成功后返回，再同步提交；重试耗尽后记录 DLQ 审计，这是当前明确边界。

### 8.7 gRPC 本来就是 HTTP/2 长连接，为什么 Streaming 还有价值？

Unary Channel 确实会复用 HTTP/2 连接，不能错误地说每条消息都新建 TCP。Streaming 的收益是减少逐调用 ClientContext、metadata 和调用状态机开销，并让应用层能在一条流内做有界排队、request/reply 关联、重连和背压。

### 8.8 如何防止广播拖慢单聊？

入口按 scene 令牌桶，Kafka 按 topic 和 consumer group 隔离，Job 消费路径独立。这样广播堆积不会直接占用单聊 partition 和消费位点。生产环境还应配置不同 worker 配额、partition 数和优先级队列。

### 8.9 离线补推为什么用 delivered_seq，不用 read_seq？

delivered 表示服务端是否已经把消息交给该用户连接，read 表示用户是否读过。用户可以收到但未读；如果用 read_seq 做补推，会在每次重连重复发送所有未读消息。两种水位必须分开。

### 8.10 Hermes 为什么不直接在 Logic 里调 HTTP？

模型响应可能是秒级，放在 Logic 热路径会占用工作线程并放大超时。独立 Bridge 和 `ai_request` topic 隔离了慢依赖；最终回答重新进入普通单聊链路，复用持久化、游标和离线恢复。

### 8.11 AI delta 为什么不落库？

delta 是展示过程，不是最终事实。落库会制造大量碎片消息，还会让重连恢复半截回答。当前只持久化 final reply；delta 丢失最多影响在线动画，不影响最终历史。

### 8.12 你遇到过哪些真实问题？

至少可以讲三个：

1. E2E 实时 100% 后立即停 Job，数据库暂未完整落库，暴露 realtime delivered 与 persistent stored 是不同阶段；
2. 在线投递后没有推进目标 delivered cursor，重连发生重复补推，修复为 Comet 批量上报目标用户游标；
3. 页面看似泄漏“缓存回答”，实际是历史请求、实时帧和临时 AI 气泡竞态，使用 generation、历史屏障和统一消息身份去重解决。

## 9. 不应夸大的边界

面试中主动说明这些边界，可信度会更高：

- 本地 Kafka 是单 broker，不能宣称完成 Kafka 副本容灾；
- E2E 的 535.5 msg/s 是单机本地回归数字，不能外推生产集群容量；
- `delivered_ack` 不是客户端 ACK，更不是已读；
- 当前 DLQ 只是审计日志，没有独立可重放 topic 和管理工具；
- metrics 是进程内 text exporter，进程重启清零，没有完整告警规则；
- 令牌桶是单 Logic 实例状态，不是分布式全局限流；
- Hermes Bridge 仍缺有界并发、取消、分布式请求状态和持久化 DLQ；
- HTTP/WS 公开部署前还需要 TLS、统一入口鉴权、严格 CORS、网关限流和安全审计；
- 当前工程化改动需要形成清晰 Git commit 历史并推到个人仓库，代码演进证据本身也是面试材料的一部分。

## 10. 建议的源码讲解顺序

1. [`proto/spark_push.proto`](../proto/spark_push.proto)：先讲 ACK、Sync、MarkDelivered、MessageStream 和 PushStream；
2. [`logic/grpc_service.cpp`](../logic/grpc_service.cpp)：讲热路径、幂等、持久化事件、场景路由和 Hermes；
3. [`logic/redis_store.cpp`](../logic/redis_store.cpp)：讲 Lua 序号与 client_msg_id 去重；
4. [`job/service.cpp`](../job/service.cpp)：讲 Kafka callback、PushStream、重连、reply 匹配和 offset 边界；
5. [`comet/comet_server.cpp`](../comet/comet_server.cpp)：讲 WebSocket、连接映射、在线下发、游标上报和离线补推；
6. [`logic/conversation_store.cpp`](../logic/conversation_store.cpp)：讲 MySQL floor、幂等持久化和读/投递水位；
7. [`common/security.cpp`](../common/security.cpp) 与 [`logic/http_server.cpp`](../logic/http_server.cpp)：讲用户中心安全和对象级授权；
8. [`load_test/e2e_bench.cpp`](../load_test/e2e_bench.cpp) 与 [`performance-report-2026-08-09.md`](performance-report-2026-08-09.md)：最后用测试闭环。

## 11. 项目后续最值得做的三件事

按求职展示价值排序：

1. **整理 Git 历史**：按“基线导入、可靠性协议、游标恢复、PushStream、用户中心、Hermes、前端与测试”拆成可审查提交，并打一个可运行 tag；
2. **补故障矩阵**：测试 Logic/Job/Comet/Kafka/Redis 在不同 ACK 窗口崩溃后的行为，增加独立 DLQ topic 和重放命令；
3. **做多实例压测**：至少两个 Logic、两个 Comet、多个 Kafka partition，采集 Prometheus p50/p95/p99、lag、队列深度和 CPU/内存，验证单机结论在分布式部署下是否成立。

做到这三点后，这个项目的亮点就不再是“用了很多技术栈”，而是：能够定义消息系统的正确性，能从故障窗口推导协议，能用指标和测试证明改造，也知道哪些结论还不能下。
