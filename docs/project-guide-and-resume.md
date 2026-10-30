# Spark Push 项目指引与简历手册

> 更新：2026-10-30；以 `sparkpush_project/` 根目录当前源码为准。
> Hermes Native 基于 `0c4f1f2c4204481ce77a4a16162a360bbf2bf2fc` 增量实现；测试回执保存源码 SHA-256。2026-10-12 启动验收基线仍为 `496f3e1`。
> 仓库：[specetator/grpc_aiagent](https://github.com/specetator/grpc_aiagent)。

本文保留当前架构、核心链路、算法细节、运行方法、最新测试和简历材料。
已移除旧 Redis 取号算法、重复面试稿、历史发布流水和过时性能表；完整实验留在带日期的专题报告中。
快速复习顺序：第 1～3 节理解项目，第 4 节准备算法追问，第 6～7 节查看证据和简历。不认识的词先查第 9 节。

## 1. 项目定位与边界

Spark Push 是一个基于 C++17 的实时消息/IM 与 Agent 协作项目。
Comet 接入 WebSocket，Logic 管鉴权、会话、序号、幂等和路由，Job 消费 Kafka 做投递与历史持久化。
可选 Bridge 经 AgentRouter 接入 Kernel Agent（基于 Pi）、Hermes Legacy/Native，或使用 Inference Gateway，最终答案复用消息恢复链路。
Hermes Native 通过官方 Runs API 复用 Runtime；默认关闭，尚未完成真实 Hermes 与完整 IM 端到端验收。

| 能力 | 当前实现 |
|---|---|
| 用户/权限 | 注册登录、PBKDF2 密码、随机 token、撤销索引、管理员审计、会话/附件/房间成员检查 |
| 消息 | 单聊、聊天室/群 fanout、管理广播、弹幕；WebDemo 与 Kotlin/Compose Android |
| 可靠性 | SQL 持久序号、原文 reservation/outbox、两阶段 ACK、Kafka 业务完成后提交 offset |
| 恢复 | 历史游标分页、逐条 delivery receipt、safe_seq、跨会话离线 continuation、客户端缺口补偿 |
| 容量/运维 | 有界队列、流 deadline、慢连接输出上限、路由代次、readiness、DLQ CLI、日志轮转 |
| Agent | Kernel Agent 子任务编排、CANN RAG；与 Hermes 隔离路由、HTTP/SSE、执行账本；任务交接、CAS 与测试回执 |
| 可选推理 | gRPC Gateway/Worker、容量准入、会话路由、CPU/CUDA/ROCm 后端与显存观测 |

这是可复现的工程实践项目，尚未完成多机高可用、长期压力、全 IM 混沌和生产 SLA 验证。
最新测试还显示单会话大突发存在超时边界，不能声称百万并发、消息绝不丢或端到端 exactly-once。

## 2. 架构、数据与线程模型

```text
Client ── HTTP 登录/历史 ───────────────────► Logic
  │                                           │
  └── WebSocket ─► Comet ── MessageStream ──────┤
                                              ├─ Redis：token、路由、在线计数、缓存
                                              ├─ MySQL：序号、原文 reservation/outbox
                                              ├─ persist_message ─► Job ─► MySQL 历史
                                              └─ push_single/group ─► Job
Client ◄── WebSocket ◄── Comet ◄── PushStream ───┘

Agent：Logic ─► ai_request ─► Bridge ─► AgentRouter ─► Kernel Agent / Hermes Legacy / Hermes Runs
       Logic ◄── ai_delta / ai_reply ◄────────────┘
       Bridge 也可按配置选择 Inference Gateway
```

| 模块 | 职责/并发单元 | 默认入口 |
|---|---|---|
| Comet | Muduo main loop + 8 IO worker；有界 Unary 池；8 条上行流各有 reader/writer 与 pending | WS 9000、内部 RPC 9105、metrics 9203 |
| Logic | HTTP 默认 4 个 IO worker；gRPC 线程；共享 Service/Repository；后台 outbox 恢复 | HTTP 9101、内部 RPC 9100 |
| Job | 按 topic 的 consumer；每 Comet 一组 PushStream reader/writer；ACK 工作池 | metrics 9202 |
| Bridge | Kafka AI consumer + 模型 HTTP/SSE；隔离秒级模型请求 | 默认无外部监听 |
| Kernel Agent Gateway / AgentRouter | session/turn/event 账本、FIFO 调度、Runtime 路由；工具由对应 Runtime 执行 | 本机 8643 |
| WebDemo | 用户与管理 UI；客户端排序、去重、补拉 | HTTP 9010 |

拆分让连接密集、业务密集与消费任务分别治理；CPU、broker、网络仍可共享，拆进程不等于绝对隔离。
Logic 内部另有一层：`HTTP/gRPC adapter → Service → Repository/DAO`。
adapter 提取凭证和映射协议；Service 统一规则；Repository 封装 SQL/Redis/Kafka 等实现。

### 2.1 存储与标识

| 存储/表 | 权威内容 |
|---|---|
| Redis | token/撤销索引、user/room→Comet、generation+nonce、在线计数、未读缓存 |
| Kafka | 持久化、单聊、群聊、广播和 AI 事件；topic/group 分隔故障与积压 |
| message_sequence | 当前唯一 `msg_seq` 分配权威；按 session 锁行 |
| message_reservation | canonical 原文、client ID 唯一键、二进制 outbox、阶段 |
| message / session | 消息历史与会话最新序号；`UNIQUE(session_id,msg_seq)` |
| message_delivery_receipt / user_session_state | 逐条投递证明、连续 delivered_seq 与 read_seq |
| room_agent_* / Gateway SQLite | 协作任务/交接版本；Agent route/turn/event 账本 |

```text
单聊 session_id = s_<min(uid1,uid2)>_<max(uid1,uid2)>
房间 session_id = r_<room_id>
msg_id = session_id + "-" + msg_seq
```

client_msg_id 标识客户端的一次业务发送，唯一范围为会话+发送者；request_id 匹配内部流请求/重放。
msg_seq 仅在会话内递增，可能有封存缺号；不保证跨会话顺序或跨 Logic 的 Kafka 发布顺序。
当前 WS、旧 HTTP send、弹幕共用 SQL 取号；保留的 Redis allocator 仅供历史 benchmark。

### 2.2 消息状态

| 状态 | 已证明什么 | 没有证明什么 |
|---|---|---|
| reserved/prepared | SQL 已保存身份/原文，prepared 另有不可变 outbox | Kafka 已确认 |
| accepted_ack | persist Kafka delivery report 成功 | 历史已入 SQL、接收者在线 |
| delivered_ack | 目标连接 owner IO 线程检查容量并接受发送操作 | 终端已收到/显示/阅读 |
| history persisted | Job 完成幂等历史写入并标记 stage=3 | 用户已读 |
| read_seq | 客户端主动上报已读 | 发送方一定收到实时已读回执 |

delivered_seq 是连续安全水位，不能直接 MAX(已投递 seq)。高序号先到时，低序号可能还在 Kafka/SQL 中。
ai_delta 是临时显示，不落历史、不取号、不推进任何消息游标；最终 ai_reply 才走持久链。

## 3. 核心调用链

### 3.1 登录与握手

1. HTTP 登录读取 MySQL 用户、验证 active 和 PBKDF2；签发随机 token，写 Redis 正向/撤销索引。
2. Comet 收到带 token 的 WS 请求，将 VerifyToken 放入有界 RPC 池，3 秒 deadline。
3. 鉴权完成切回 owner loop；检查连接仍存在、仍在握手状态且服务未退出。
4. 注册 generation+nonce 路由，保存用户连接；握手总等待 5 秒、header 最多 16 KiB。
5. 跨会话翻页 SyncOffline；每条发送被 owner loop 接受后上报 receipt。

### 3.2 一条单聊消息

1. Comet 从连接上下文取得 actor/token，经 MessageStream 发送场景、目标、正文和 client ID。
2. Logic 重新验证凭证/actor、状态和对象权限，限流后构造 session。
3. per-session 提交锁与 SQL 事务统一序号，返回新消息或已有 canonical 原文。
4. prepare 二进制 persist outbox，SendAndWait 等 Kafka 确认，再回 accepted_ack。
5. 按 Redis 目标路由产生实时 push 任务；重试可再次 fanout，消息身份保持不变。
6. Job 经 PushStream 投递并等待 request_id 对应 reply，Comet 在 IO 线程接受发送后计数。
7. Job 向来源连接回 delivered_ack；Comet 逐条 MarkDelivered；persist consumer 独立写历史。
8. 客户端按 ID 去重、按 seq 排序，缺口用 safe_seq/分页补齐。

SQL 确认与实时投递是独立阶段。发布确认超时后，prepared 事件仍可能后台完成；同 ID 重试消除不确定性。
旧 `/api/message/send` 已要求 Bearer 并校验 actor/目标/限流，但仍同步写历史，保留旧响应，无 WS 两阶段合同。

### 3.3 房间、广播与 Agent

房间使用 room→Comet 路由缩小 fanout，缺路由时回源持久成员；成员授权不能由 WS presence 替代。
广播的房间成员快照由 Logic 查询，Job 遇空/异常 scoped 范围拒绝，绝不扩大成全局广播。
Agent 人类输入先走普通持久消息，再发 ai_request；Bridge 调模型，delta 只预览，reply 保存完整答案。
消息 outbox 不等于模型执行事务：Agent 入队/工具副作用/消息最终持久化之间仍需独立账本和恢复。

## 4. 算法实现与面试追问

下面按数据结构、步骤、并发/失败边界说明当前实现。伪代码用于解释，函数与源码链接是实际实现。

### 4.1 SQL 会话取号与 canonical 幂等

入口：[MySqlMessageReservations::Reserve](../logic/message_reservation.cpp)。
两类唯一约束：session/seq，以及 session/sender/client ID；原文按字节保留。

```text
BEGIN
INSERT message_sequence(session,0)
  ON DUPLICATE KEY UPDATE last_seq=last_seq
SELECT last_seq FOR UPDATE
if last_seq==0: floor=max(历史 MAX(seq),session.last_msg_seq)
find reservation(session,sender,client_id)
if found:
    sealed → 拒绝
    type/body 不同 → 拒绝
    COMMIT，返回原 seq/body/timestamp
else:
    检查 int64 上限
    更新 last_seq=floor+1
    INSERT canonical 原文、stage=0
    COMMIT
```

先 UPSERT 再 FOR UPDATE，是为了取得同一会话的排他锁；对该行使用 INSERT IGNORE 可能让并发事务
先取得共享锁、随后互相等待锁升级。会话锁覆盖查幂等、递增和保存原文，避免两个请求都判定“新消息”。

旧 HTTP 空 client ID 使用安全随机内部标识，每次提交独立；WS 要求 ID 1～128 字节。
重试返回原 timestamp 而非重新取当前时间，避免 Job 完整字段比较把合法重试判为冲突。
参数用 prepared statement；client ID/outbox 使用二进制语义，避免字符集排序规则掩盖差异。

成本：索引定位和事务往返；同会话必须串行，不同会话可竞争不同 SQL 行。
热点会话瓶颈在锁持有时间和 IO，不是简单哈希复杂度。升级前必须排空旧事件，不能混跑两个取号权威。

**追问**：只用 message 唯一键为什么不够？它只能拒绝碰撞，无法找回未落历史消息的原文和序号。

### 4.2 Outbox 状态、重试与清理

入口：[PrepareOutbox/Published/Pending](../logic/message_reservation.cpp)、[ConversationStore](../logic/conversation_store.cpp)。

```text
0 reserved → 1 prepared → 2 Kafka published → 3 history persisted
0 超过 10 分钟、从未 prepare → 4 sealed
```

1. canonical 消息构造 persist protobuf，prepare 把字节写 SQL，已有 payload 必须完全相同。
2. SendAndWait 返回成功后才 accepted；确认超时可能已入 broker，因此不能换 ID 盲重发。
3. 后台每秒扫描最多 10 条 stage=1，用原始二进制重发，确认后推进 published。
4. Job 插入/核对历史后标 stage=3；晚到 delivery report 使用 GREATEST(stage,2)，不得回退 stage=3。
5. 只有 stage=0 能封存，封存后旧 ID 重试拒绝；stage=1/2 未完成记录不能按一天过期删除。
6. stage=3/4 创建满一天后分批清理，单批最多 1000；这里是有限窗口幂等。

业务历史、outbox 和 Kafka 不在同一分布式事务，重发仍可能重复；下游需保持幂等。
sealed 缺号可结算，但不能把仍 prepared/published 的缺号当不存在。

**追问**：Logic 在 Kafka 成功、更新 stage 前崩溃？stage=1 会重发原事件，由 Job 幂等收敛。

### 4.3 会话提交锁：跨会话并行

入口：[KeyedMutex](../common/keyed_mutex.h)、[MessageService::Submit](../logic/services/message_service.cpp)。
map<session-key, weak_ptr<mutex>> 只保存弱引用；调用者拿 shared_ptr 保证等待/持锁期间 mutex 存活。

1. 短暂锁住 map，查 weak_ptr；已失效时创建新 mutex。
2. 每 128 次请求扫描并删除失效 weak entry，避免永远保存历史会话锁。
3. 释放 map 锁后 try_lock 对应会话；每 5 ms 重试，总等待最多 8 秒。
4. 取得锁后执行该提交，包括认证、预留、prepare、Kafka 确认与任务生成。

map 平均查找 O(1)，周期清扫 O(S)；同会话 mutex 不承诺 FIFO 公平性。
锁覆盖 Kafka 等待会限制热点会话吞吐，最新数据已显示这一边界。跨实例仅 SQL 保证唯一序号，
进程内锁不能保证多 Logic 的 Kafka 发布顺序。

### 4.4 delivered 连续水位：receipt + 安全前缀

入口：[UserSessionStateDao::AdvanceDelivered](../logic/user_session_state_dao.cpp)。
数据结构：用户/会话状态行 + 每条 seq 的唯一 receipt；SQL join 查第一个缺失证明。

```text
safe = min(持久计数 last_seq, 最早 stage<3 的 seq - 1)
BEGIN，锁 user_session_state(user,session)
previous = delivered_seq
INSERT 当前 seq receipt（重复忽略）
first_missing = (previous,safe] 中最小的历史消息且没有 receipt
candidate = first_missing 存在 ? first_missing-1 : safe
delivered_seq = max(previous,candidate)
删除 <= delivered_seq 的 receipt，COMMIT
```

safe 的求值在 SQL 事务内；不存在未结算 reservation 时，以持久计数/兼容历史 MAX 作边界。
GetDelivered 也会重算，所以历史落库后能利用已有高序号 receipt 收敛。

例：seq=1 prepared 尚未落历史，seq=2 已投递；safe=0，receipt(2) 不能推进 delivered。
等 1 持久化但尚未投递时，first_missing=1；只有补投 1 后才能一起推进到 2。
同用户/会话状态行锁避免两个 receipt 更新互相覆盖。这里必须扫描/连接相关索引范围，
不能把它描述为单条 GREATEST 的 O(1) 更新。

**追问**：为什么 read_seq 不代替 delivered_seq？阅读晚于接纳，按已读补推会重复全部未读消息。

### 4.5 历史、Sync 与跨会话 Offline 分页

入口：[ConversationService](../logic/services/conversation_service.cpp)、[MessageDao](../logic/message_dao.cpp)。
历史倒序使用 WHERE session_id=? AND seq<anchor ORDER BY seq DESC LIMIT L，复合索引定位后读一页。
Sync 正序使用 seq>after，先取得 safe，再读取 L+1 条，用多一条判断 has_more。

1. 校验 actor 是会话参与者/房间持久成员。
2. Sync **先读安全前缀、后读历史**，避免查询之间低序号晚插入被错误跳过。
3. 按条数与 2 MiB 估算字节预算截页；单行超过预算返回 413，不推进游标，需 HTTP history。
4. next_seq 是本页最后 seq；safe_seq=min(safe,next_seq)，空页可返回完整 safe。
5. Offline 收集已授权单聊/房间，按 session ID 排序，从 after_session_id/after_seq 继续。
6. 每会话以 max(delivered_seq, continuation) 查询，共用页预算；每页最多 200，继续直到无下一页。

索引分页约 O(log N + L)，Offline 另有会话排序与逐会话查询成本。
next_seq 不能证明此前缺号已结算；一次 Offline 查询也不是全库快照，晚落库需再次扫描/缺口补拉。

### 4.6 客户端消息游标与流式 TreeMap

入口：[Web 游标](../web_demo/static/index.html)、[SparkViewModel](../android-app/app/src/main/java/com/peco/sparkim/SparkViewModel.kt)。
正式消息使用每会话 cursor + seen set，界面消息按 ID 去重：

```text
seen.add(seq)
while seen 包含 cursor+1: 删除它，cursor++
seq > cursor+1 → 请求 Sync(cursor)
sync_end:
  请求 after<=当前连续 cursor 时，才允许 cursor=max(cursor,safe_seq)
  删除 seen 中 <=cursor，再消费连续部分
  has_more → 用 next_seq 翻页；仍有缺口 → 延迟重拉
```

seen 超过 4096 时清空并从连续游标补拉；不能直接设 cursor=收到的最大 seq。
ai_delta 不参与这个算法。Android 连接回调带 generation，旧 socket/晚到响应不能污染新连接状态。

Agent 增量另用 TreeMap<delta_index,delta>：

1. index 小于 expected 或 key 已存在，丢弃重复/旧帧。
2. 插入有序树，反复 remove(expected) 并追加 StringBuilder，然后 expected++。
3. 达 256 帧/512 KiB 上限或增量积压时停止预览，靠最终 reply/历史恢复。
4. UI 约 40 ms 合并刷新；Web 使用 requestAnimationFrame 与 256 帧缓冲。

树的插入/删除 O(log W)，正文追加摊销 O(正文长度)；避免每次重建整个字符串造成 O(n²)。
正式 seq 排序、delta_index 重排、连接 generation 是三个不同状态空间。

### 4.7 双向 gRPC 流与 deadline

入口：[Comet MessageStream](../comet/comet_server.cpp)、[Job PushStream](../job/service.cpp)。
结构：FIFO send_queue、request_id→pending 哈希表、writer、reader、mutex/cv 与可取消 context。

1. enqueue 生成/携带 request_id，同时记入队时刻的绝对 deadline。
2. writer 串行 Write；reader 查 pending、填结果/唤醒 completion，不能假设 reply 按发送顺序到达。
3. Write 成功只说明 gRPC 接受数据；Job 收同 ID 的成功 reply 后才认可 callback 完成。
4. 读写失败标 broken，旧 pending 失败后重连；保留业务 ID，让 Kafka/上游安全重试。
5. context 创建、替换与取消共用锁，锁内复核 running；channel/stub 在组件生命周期内稳定。
6. Stop 取消 context、唤醒队列、join reader/writer；不能让 callback 抓住已析构的 raw this。

Comet 每流 queue+pending 最多 512 项，排队数据 8 MiB；8 秒 ACK 期限从入队开始。
独立 100 ms watchdog 过期排队请求并清理 pending，pending 超时还取消 stalled 流。
Job 每 Comet 队列默认 10000，reply 等待默认 2 秒，失败可 Unary fallback。
按 ID 查找平均 O(1)，pending watchdog 扫描 O(P)；网络 Write/Finish 等待不是 CPU 复杂度。

**追问**：writer 堵住还怎么超时？超时由独立 watchdog 执行，不依赖 writer 能继续循环。

### 4.8 request_id 去重：deque + unordered_set

入口：[CometServer::AcceptPushRequest](../comet/comet_server.cpp)。
set 平均 O(1) 查 ID；deque 保存插入顺序，容量 100000，超出就 erase(front)+pop_front。
命中不刷新位置，因此是 FIFO 窗口，不是 LRU；空 ID 不记录。
Job 默认业务投递 ID 是 msg_id@comet_id，重复调用返回成功但 delivered_count=0。
窗口有容量边界，不能作为永久事实幂等；客户端和 SQL 仍需去重。

### 4.9 Kafka producer、consumer 与 offset

入口：[KafkaProducer](../common/kafka_producer.cpp)、[KafkaConsumer](../common/kafka_consumer.cpp)。

- producer acks=all、idempotence；本地单 broker/RF=1 不等于多副本容灾。
- Send 异步入 librdkafka；SendAndWait 用 opaque→DeliveryState map、condition_variable 等 DR。
- 等待超时的状态要保留到实际 DR，不能提前释放 opaque 产生悬空指针/地址复用。
- consumer 禁 auto commit，callback 完成后 commitSync；失败最多三次并退避。
- persist/配置了持久死信的 Agent 路径确认 DLQ 后才提交原 offset；DLQ/commit 失败停消费。
- 实时 push/broadcast 仍是 best-effort 重试与死信日志，不等于全链路可重放 DLQ。

进程在处理完成、commit 前崩溃会重放，所以 callback 必须幂等。
同 partition 记录顺序不保证异步 fanout 的完成顺序，也不保证跨 partition 全局顺序。
producer 队列默认 10000 条/32 MiB；积压不应只看 lag，还要看拒绝、队列年龄与下游容量。

### 4.10 Job 幂等落库与长字段读取

入口：[MessageDao](../logic/message_dao.cpp)、[Job 持久化](../job/service.cpp)。
INSERT 遇唯一键冲突，逐字段比较 session、seq、sender、type、content、client ID、timestamp；
完全相同才当幂等成功，不同则 sequence collision，不能静默覆盖。
session.last_msg_seq 使用 GREATEST 避免乱序回退，reservation 标 persisted。

MEDIUMTEXT 读取先绑定 64 KiB 缓冲，reported_length 超容量或 MYSQL_DATA_TRUNCATED 时，
按真实长度分配 string，再 mysql_stmt_fetch_column。成本 O(正文长度)。
动态读取只防截断，不扩大模型上下文；Sync 字节页预算仍可能拒绝很大的单行。

### 4.11 ThreadPool：有界 producer/consumer

入口：[ThreadPool](../common/thread_pool.cpp)。queue<Task> 含 function 与字节权重，mutex/cv 保护状态。

```text
Submit:
  锁内检查 started/stopping、项数、bytes<=max_bytes-queued_bytes
  push，更新计数，notify_one；拒绝返回 false
Worker:
  cv.wait(stopping || !queue.empty)
  pop/decrement，解锁执行，捕获异常
Stop:
  标 stopping，选择 drain 或丢未执行项
  notify_all，join 已运行 worker，清队列
```

默认排队 1024 项/提交权重 32 MiB，硬件线程数为 0 时回退至少 1。
入/出队 O(1)，执行时间由任务决定；字节权重是调用方估算，不是进程总内存硬上限。
Stop(false) 也需等待已运行任务；阻塞 RPC 必须有自己的 deadline。

### 4.12 MySQL/Redis 池与 RAII

入口：[MySQL pool](../common/mysql_pool.cpp)、[Redis pool](../common/redis_pool.cpp)。
deque<ConnEntry> 记录空闲连接/last_used，mutex/cv 管总数、min/max 和等待。

1. 锁内清过期超额空闲、借出连接，或预留创建名额。
2. 解锁后 connect/健康检查，重新加锁核对停止状态；网络不能占用全池锁。
3. 失败减计数、关闭坏连接并通知等待者；每 250 ms 尝试恢复 min。
4. 即使关闭自动增长，也允许补回 min，避免所有连接失效后永远没人 Release 来唤醒。
5. 归还健康连接到 deque 尾部；RAII guard 自动归还，Redis guard 禁复制、允许 move。

默认 Acquire 等待 3 秒；绝对期限防多次唤醒重复延长等待。
这不取消底层 connect/PING/SQL，总耗时还受原生超时约束。Redis 热连接仅空闲超过健康间隔才 PING。
平均借还 O(1)，回收可扫描过期项；池越大不一定越快，需看 DB 能力和锁竞争。

### 4.13 Redis 路由防迟到删除与短缓存

入口：[RedisStore](../logic/redis_store.cpp)。

```text
上线 Lua：n=INCR generation；SET lease=n:nonce；SADD route comet
下线 Lua：仅当 GET lease == 自己的 n:nonce 才 DEL lease / SREM route
```

generation 处理旧下线晚到；安全随机 nonce 处理 Redis 计数器丢失后同数字复用。
Lua 原子执行保证比较与删除不可插入新注册；它没有 TTL 心跳租约，Comet 崩溃仍需路由治理。
Redis 全量丢失后也不会自动重注册所有存量连接。

本地 uid→{comets,expire} 缓存用 shared_mutex；命中共享锁复制，未命中读 Redis 后独占写入。
默认 TTL 1 秒，注册/删除主动失效；多 Logic 无广播失效，因此 TTL 是陈旧路由边界。
缓存不是 LRU、没有容量上限；当前 SQL 取号不依赖 Redis 缓存。

### 4.14 令牌桶与背压

入口：[SceneRateLimiter](../common/rate_limiter.cpp)。map<scene:key,Bucket>，一把 mutex 管所有桶。

```text
tokens = min(burst, tokens + elapsed_steady_seconds * rate)
tokens < 1 → 429
否则 tokens -= 1
```

首次填 burst，single 默认 1000/s、burst=200；group 300/60；broadcast 20/5。
时间使用 steady_clock，避免系统时钟跳变；桶是进程级，不是全局分布式限流。
平均定位 O(1)，桶表当前无自动淘汰。令牌桶限制入口，不能替代内部队列/消息字节/输出缓冲上限。
观察整条链：入口→SQL/Kafka→Job queue→gRPC flow control→Comet output→慢客户端。

### 4.15 HTTP/WS 增量解析与握手算法

入口：[HttpContext](../muduo/net/http/HttpContext.cc)、[CometServer](../comet/comet_server.cpp)。
HTTP request-line→headers→body→complete 状态机仅消费已完整确认的数据。
半包退出本次解析等待下次 onMessage；循环必须消费字节/推进状态或退出，不能原地空转。
Content-Length 按十进制累积，检查 `length > (maxBody-digit)/10` 防溢出；重复、符号、异常 header 拒绝。
不支持 Transfer-Encoding/chunked，提前拒绝。限制 line 8 KiB、headers 32 KiB、body 8 MiB、未完成 30 秒。

WS 握手 Accept=Base64(SHA1(client_key+GUID))，仅用于升级协议，不是密码安全算法。
源码中 SHA1：追加 0x80、补零到块内 56 字节、追加大端 64-bit bit length；512-bit 块展开 80 个 word，
四阶段函数/常数做 80 轮 32-bit 加法与循环左移；最终输出 20 字节。Base64 每 3 字节拆 4 个 6-bit，
余 1/2 字节分别补 ==/=。两者均为 O(输入长度)，只在握手执行。

WS 帧：先读 2 字节，按 7/16/64-bit 取网络序长度；数据未齐不 retrieve；校验 FIN、mask、RSV/opcode，
再 `payload[i] ^= mask[i%4]`。文本最多 64 KiB；不支持分片/二进制数据帧。
控制帧最多 125 字节，Ping 回同负载 Pong，Close 回应并 1 秒后强关；Pong 不作业务消息。
mask/序列化 O(P)。握手 header 上限只计算 header 部分，不能把 coalesced 首帧误当超长 header。

### 4.16 连接表、fanout 与跨线程所有权

入口：[TcpConnection](../muduo/net/TcpConnection.cc)、[CometServer](../comet/comet_server.cpp)。
user_conns 是 unordered_map<uid,set<shared_ptr<connection>>>，room_users 保存本机订阅用户。
锁内复制目标指针、锁外排队到各 owner loop；fanout O(目标连接数)，不是 O(消息数)。

跨线程 send/startRead/stopRead 闭包持有 shared_ptr 与消息字节；状态读 atomic，转换归 owner loop。
不能捕获临时 string.data 或裸连接指针；延迟 close 用 weak_ptr，回调执行前 lock。
owner loop 检查连接仍在、frame+output 不超过 4 MiB 后才计 delivered；慢连接关闭，不无限积压。
高水位是每连接限制，10000 连接不能直接当任何机器都安全的总内存预算。

析构顺序也属于算法合同：传输对象先停止/join IO worker，连接表和 callback 依赖之后释放。
RPC 池、流、timer、恢复任务分别停止；取消并不意味着回调已退出，必须 join/等待实际执行收敛。

### 4.17 密码、token、内部 RPC 与附件

入口：[Security](../common/security.cpp)、[Internal RPC](../common/internal_rpc.h)。
PBKDF2-HMAC-SHA256：随机 16-byte salt、120000 轮、32-byte digest，常量时间比较。
旧 MD5 成功验证后可 rehash；新 token 用 CSPRNG 32 字节再 hex，Redis TTL+反向索引支持撤销。
兼容旧 token 低频 SCAN 兜底，不能高频 KEYS；随机 token 不靠时间戳/自增 ID。

内部 metadata 必须恰好一个 x-spark-rpc-key，角色 COMET/JOB/可选 ADMIN 不互换。
默认只允许本机明文；跨机要求 TLS；密钥来自不跟踪环境，至少 32 字节。
工作负载认证不代替用户 actor/会话权限检查，WS Submit 仍重新验证连接 token。

图片先 Bearer HTTP 上传：成员校验→base64/魔数/尺寸→临时文件+rename→attachment 元数据。
每条最多 4 张、每张最多 4 MiB；WS/Kafka 只带 ID。绑定时检查 owner/session，Bridge 才读文件转模型输入。
文件和 SQL 不在同一事务，应按失败分支理解清理；客户端不能提交任意磁盘路径。

### 4.18 SSE、增量合并与最终 envelope

入口：[SSE parser](../hermes_bridge/sse_parser.cpp)、[Bridge](../hermes_bridge/main.cpp)。

1. Feed 缓冲半行，按换行取 event/data；空行结束一条 event，data 支持跨行拼接。
2. 第一段立即发 delta，随后累计约 48 字节或 50 ms 才 flush，降低 Kafka/UI 每 token 开销。
3. DONE 需要已有合法终态；缺 DONE、非 stop、空答案均不能写成成功 reply。
4. 重复 final 要求文本/metadata 一致；最终 envelope.sequence 必须等于 delta_count。
5. delta 异步发送，final SendAndWait；进程内 reply cache 可重发已完成输出，但非永久持久幂等。

parser 预算 16 MiB，扫描 O(输入字节)；不把 JSON chunk 边界当 SSE event 边界。
完整回复是恢复事实，增量丢失只能影响预览，不能让游标跳过最终消息。

### 4.19 Agent 路由、FIFO 与版本化交接

入口：[AgentRouter](../cannbot/scripts/agent_router.py)、[Scheduler](../cannbot/scripts/agent_scheduler.py)、
[Room workspace](../cannbot/scripts/room_agent.py)、[Room Repository](../logic/repositories/room_agent_repository.cpp)。

route_key 由 tenant/channel/conversation/thread 的 canonical JSON 哈希生成，避免串用户/渠道；
request_id 对应固定 input_hash，completed 回放、状态未知拒绝盲执行；SQLite turn/event 账本可恢复。
event 使用 request/sequence 与唯一 event_id，冲突拒绝；只保留有界回放窗口。

FIFO 调度：每 session ticket 列表；只有队首、session 未运行、全局/per-agent 配额满足才能 acquire。
队列计时用 monotonic；finally 只释放自己真正 acquired 的执行权，超时排队者不得解锁正在执行者。
它是单 Gateway 进程约束，不是多实例分布式锁。

协作交接：房间绑定 task/session/workspace；成员可读，房主/显式授权者可开发。
checkpoint 使用 UPDATE WHERE revision 做 CAS；过时版本 409，不“最后写覆盖”。
源码按相对路径/真实字节 SHA-256 做内容寻址副本；测试前后版本、runner hash、exit_code 和 timeout
共同定义 passed，代码在执行中改变时退出 0 也不算通过。

同 turn 的 write/edit/test 另有互斥；固定 runner 经 bubblewrap/prlimit，不接受任意 shell 命令。
task=done、模型说通过、真实执行回执不同。当前 runner 为 Linux 主机示例，不能写成 NPU 实机验证。
版本副本有限制，尚无全局配额清理；Pi 工具 guard 不等于整个模型进程已完整 OS 多租户隔离。

### 4.20 推理服务准入、KV 与显存观测

入口：[Inference serving](inference-serving.md)、[WorkerRegistry](../inference/gateway/worker_registry.cpp)。
Gateway 管排队/选择/取消，Worker 管 runtime 和自身并发；模型批处理由 runtime 执行。
Worker 约 1 秒心跳、5 秒 TTL；选择前过滤模型/健康，再按运行/等待/显存状态决策。

默认准入 running=max(心跳 running,Gateway active RPC lease)，避免漏算直连/共享流量。
RPC 完成释放 lease，旧心跳仍可能满载；单 Gateway 独占全部 Worker 流量时可显式 managed-only，
用本地 lease 准入，健康/显存仍来自心跳。多 Gateway/共享 Worker 不满足这个前提。
取消、断连、deadline 要向下关闭模型请求并释放计数，否则会留下无主计算。

Prefill 处理输入，Decode 逐 token；TTFT 含排队/预填充/首 token，TPOT 是后续单 token 成本。
continuous batching 把新请求补进空 slot，服务队列与计算 batch 是两层。
Flash Attention 通过分块/融合减少中间矩阵 IO，不保证所有 GPU/短上下文都更快。

```text
KV bytes ≈ 2 × layers × kv_heads × head_dim × cached_tokens × bytes_per_element
```

KV 量化与模型权重量化不同；prefix cache 重用预填充，是否命中要看 cached_prompt_tokens。
多个 Worker 共用 runtime 时共享 slot；多个 runtime 复制权重/KV，不能直接把多进程当吞吐收益。
GPU 使用 NVML 按 PCI bus ID 采样，避免 CUDA visible ordinal 重排造成读错卡；回退 Driver 时保存/恢复
线程 current context、retain/release primary context。观测值不是严格 OOM 预算。
本轮运行的是 adapter/探针合同测试，没有重跑真实 CUDA/ROCm/NPU 性能。

### 4.21 Hermes Native：执行账本、原生终态和控制抢占

入口：[Native Adapter](../cannbot/scripts/hermes_native_adapter.py)、[Kafka 控制循环](../common/kafka_consumer.cpp)、
[完整设计与回滚](hermes-native-runtime-integration.md)。本机核对的是 Hermes 0.21.1，而非假设线上最新文档全部可用。

1. Logic 已验证的 tenant/channel/conversation/thread/user/agent 路由形成 Native Session 身份，使用独立 API Session，不借用 CLI Session。
2. AgentStore 在唯一一次 `POST /v1/runs` 前持久化准入意图，稳定派生 Idempotency-Key；同 request_id 改输入拒绝。
3. SSE 只投影过滤后的文本、工具与审批进度；仅原生 completed 且输出有效才落成功结果，再进入 `ai_reply`。
4. 中断或重投查询原 Run；已落盘结果直接复用。提交丢响应且无 run_id 时保留 unknown，不能在幂等窗口过期后盲目 POST。
5. `/stop` 与审批通过独立控制池；可选 Kafka 控制窗口越过长任务执行控制，但 offset 只提交连续完成前缀。
6. 审批令牌绑定用户会话、Run、请求、代次及时效，只支持 once/deny；默认关闭，超时或缺少交互支持时拒绝。
7. `/new` 先确认原生 Session 创建，再切本地指针；失败保留旧上下文。终态输出与已确认发布状态不可被迟到恢复覆盖。

这是执行幂等与消息可靠性分工，不是跨 Hermes/SQLite/Kafka 的分布式事务。没有端到端 exactly-once；
无 run_id 的 unknown 需人工核对，有界控制窗口外不保证低延迟。Runs 没有 compact/clarify 端点时明确返回不支持。
PiRpcClient、Legacy Adapter、Scheduler 和原 WindowsRelayPool 保持原实现；Native 不复制 Memory 或重造 Agent Loop。

**追问**：已经有 Idempotency-Key，为什么不重发？服务端有保留期；执行过工具但幂等记录过期后重发可能再次执行。

### 4.22 Kernel Agent：复用运行时与子 Agent 编排

入口：[Kernel 子 Agent 扩展](../cannbot/pi/extensions/kernel-subagents.ts)、
[产品与权限边界](../docs/kernel-agent.md)、[Codex 源码对照](../docs/codex-subagent-reference.md)。

Kernel Agent 是本项目的产品名称，Pi coding agent 是底层运行时。项目复用 Pi SDK 的
`createAgentSession`、会话和工具实现，不另写模型请求、流式协议或 Agent Loop。
子任务生命周期参考开源 Codex CLI 的 spawn / send / wait / close 机制，并按当前 Pi 环境适配。
保留 `pi` / `pi_rpc`、bot ID、旧状态目录与 `pi_gateway.py`，产品改名不迁移聊天历史。

```text
主 Agent 接收任务
  → 划分可并行研究 / 有依赖的设计与实现
  → spawn_agent：独立 Session、角色、任务上下文、写入范围
  → wait_agent / list_agents：查看状态并收集结果
  → send_input：补充真实测试证据或安排修正
  → 主 Agent 核对代码与测试，close_agent，汇总最终答案
```

1. 子任务按主 turn 归属管理，用独立内存 Session 隔离模型上下文；父模型和思考等级沿用，
   必要事实由任务提示显式传递。隔离上下文不等于隔离操作系统或复制工作区。
2. 工具按“父会话已有能力 ∩ 角色允许能力”收敛。researcher / architect / reviewer / tester 只读；
   developer 只能写 `allowed_writes` 中的文件，并拒绝并发开发者持有重叠范围。
3. 不给子 Agent shell 和继续派生能力。tester 负责检查测试设计与已有证据，主 Agent 执行命令；
   房间子 Agent 仅只读工作区/CANN，房间写入和 runner 继续由可信授权的主 Agent 使用。
4. 子任务数量、交互轮数、执行时间、wait 时长和返回文本均有上限。等待超时只表示本次没有等到，
   不能直接认定子任务失败，更不能自动重跑产生副作用的修改。
5. 父 turn 结束、取消、切换会话或关闭时回收子任务，避免 worker 重用时跨会话泄漏。
   子会话不跨 turn 持久化或自动恢复；主 turn 的 SQLite 账本与子会话生命周期分别管理。

| 并发层次 | 输入对象 | 解决的问题 |
|---|---|---|
| Gateway worker 池 | 不同会话的请求 | 同会话串行、跨会话有界并发 |
| Kernel 子 Agent | 同一个任务的子任务 | 独立上下文、分工与结果收集 |
| 房间协作 | 多人的同一算子任务 | 权限、交接与版本化证据 |

**可讲的算法与工程点**：任务注册表与所有权；状态机和迟到结果处理；有界资源配额；
取消与释放顺序；工具能力集合求交；并行写入冲突检查。子 Agent 报告“通过”后，
主 Agent 仍要检查实际命令、退出码和源码版本，不能把模型自报当成测试回执。
子任务的 CANN 引用也需由主 Agent 重新读取来源，才能进入父 turn 的引用校验账本。

**追问**：为什么不直接把 Codex 的 Rust AgentControl 复制过来？它依赖 Codex 的线程、配置和协议栈；
本项目借鉴并注明生命周期语义，执行部分复用已接入的 Pi SDK，避免再引入一套模型与会话运行时。
这不等于已经实现完整 Codex multi-agent，也不代表 NPU 算子自动优化已完成实机闭环。

**本轮验收（2026-10-30）**：子任务生命周期/权限 53 项、真实 Pi SDK + 受控模型集成 32 项、
名称浏览器检查 6 项通过；独立分支 CTest 为 46 通过 / 3 跳过。
[验收记录](benchmarks/2026-10-30-kernel-agent/validation.json)同时登记已有命令卡片浏览器失败，
不将受控模型验证表述为真实模型或 NPU 性能验收。

## 5. 构建、启动、升级与排障

### 5.1 最小运行路径

依赖包括 CMake/C++17、OpenSSL、gRPC/Protobuf、librdkafka、hiredis、MySQL client、Docker。
基础 IM 不需要 Pi/CANN/GPU；首次使用先按 [README](../README.md) 安装依赖并创建 .env.local。
从项目根目录执行；已有环境先完成下节迁移：

```bash
cmake -S . -B build-wsl -DCMAKE_BUILD_TYPE=Release -DBUILD_TESTING=ON
cmake --build build-wsl -j2
ctest --test-dir build-wsl --output-on-failure -j2
./scripts/sparkctl.sh doctor
./scripts/sparkctl.sh up
./scripts/sparkctl.sh health
./scripts/sparkctl.sh logs
./scripts/sparkctl.sh down
```

.env.local 保存个人密码/管理员/模型配置，.env.rpc 保存角色密钥，均不跟踪。
start_demo 生成/载入 .env.rpc，手动启动要给进程注入对应角色；不要把密钥写进命令示例或日志。
公网默认 HTTP/WS 明文，需要部署 HTTPS/WSS；远程内部 RPC 需要服务器 cert/key 与客户端 CA。

### 5.2 存量迁移

1. 停旧消息入口、排空旧持久化/广播事件，备份 SQL 和完整 broker 日志/offsets。
2. 将旧 Kafka 实际 log.dirs（含 KRaft metadata）迁入新命名卷 /var/lib/kafka/data；不能直接 down 后期待自动迁移。
3. 执行 [一次性 SQL 迁移](../sql/migrations/20261011_message_reservations.sql)：三张表，delivered_seq 重置重扫，read_seq 保留。
4. 配置 RPC 角色/远程 TLS，同步升级 Logic/Comet/Job 和 Web/Android。
5. 小流量核对 readiness、offset、outbox stage、分页/重试，再重测容量并开放入口。

Logic/Job 新表缺失时启动失败，不自动迁移；重复迁移会再次重置 delivered，不作为每次启动步骤。
完整兼容、回退和 DLQ 操作见 [工程修复与迁移](review-remediation.md)。

Kernel Agent 产品入口使用 `bash cannbot/scripts/setup_kernel_agent.sh` 和 `kernel-agent gateway`。
存量机器人显示名可执行 [名称迁移](../sql/migrations/20261030_kernel_agent_identity.sql)，
它保留自定义名称和既有会话 ID。安装与重启是部署步骤，源码更新不表示正在运行的服务已迁移。

### 5.3 运维信号

| 现象 | 优先检查 |
|---|---|
| 消息发送失败 | Comet ACK timeout/queue reject、单会话持锁时间、SQL pool/outbox、Kafka DR |
| accepted 无 delivered | Redis 路由、push_single lag、Job stream ready、目标连接/输出高水位 |
| delivered 无历史 | persist consumer、DLQ、唯一键冲突与 stage=1/2 |
| 重连漏补/重复 | receipt 连续水位、safe_seq/next_seq、Offline continuation、同用户多连接 |
| 内存/延迟上涨 | queue/pending 数与字节、连接输出、Kafka backlog、热点 fanout/补拉 |
| 进程在却不可用 | /ready、consumer_failed、两组 stream_ready；不能只看 PID/监听端口 |

关键消费者 DLQ/offset 确认失败会 ready=0 并非零退出；成功写 DLQ 后仍可 ready，因此另需 DLQ 告警。
日志默认 50 MiB 轮转、5 个历史文件，仍同步 IO；sparkctl 同时看 .log/.out。
dlq_replay 默认 dry-run，--apply 才发布；校验 schema/目标/hex，不删 DLQ、不改原 offset、不打印正文/key。
现有 WS 错误帧未透传全部内部 code，定位需结合服务端指标，不能只凭“send failed”判断根因。

## 6. 最新测试与性能数据

第 6.1～6.4 节保留 2026-10-11 的 IM 实测；第 6.6 节记录 2026-10-28 Native 验证，两者不混作一次测试。
历史 IM 测试环境为 WSL2、12 logical CPU、约 15.52 GiB 内存。
客户端、三个服务及隔离 Docker MySQL 8.0/Redis 7.2/Kafka 3.7 同机；其他主机负载未停。
SQL 接受路径使用当前实现，MySQL 为临时容器磁盘层，单 broker、单 partition、RF=1，AI 关闭。
配置和二进制指纹、各轮原始结果见 [本轮测试报告](benchmarks/2026-10-11-current/README.md)。

### 6.1 功能验证

| 验证 | 最新实际结果 |
|---|---|
| C++ 构建 | 通过；E2E 工具另增加错误计数观测并重编 |
| 默认 CTest | 43 注册，40 通过、3 opt-in 跳过、0 失败；29.76 秒 |
| 隔离 MySQL/Redis 补跑 | 1+2 项通过；原文/outbox、并发取号、receipt/安全页与路由代次 |
| 真实 Kafka 替换/重放 | 消息与已提交 offset 保留、CLI 获 broker 确认，通过 |
| Android | 离线 Gradle 编译检查成功，目标 UP-TO-DATE；单测任务 NO-SOURCE |
| 知识/引用 | 严格知识与引用校验通过；Pi 个人安装仍有已知漂移，不覆盖个人配置 |
| GitHub CI | `340d5b7` 对应 [CI 38117916580](https://github.com/specetator/grpc_aiagent/actions/runs/38117916580) 全部通过；2026-10-12 核对 |

CTest 含 HTTP/WS 分包/慢鉴权/异常断开/Ping/Close/退出、游标和 Agent 合同回归。
没有把 CUDA 探针 mock 当真实 GPU 性能，也没有把 Android 编译当设备行为测试。
2026-10-12 同步时本地与远端实现一致；CI 结果对应上列基线，压测仍为 2026-10-11 的实测，不改日期或外推容量。
同日移出旧教学快照后，主线重新构建通过；CTest 40 通过、3 opt-in 跳过、0 失败，29.84 秒。
本次目录整理未重跑隔离 SQL/Redis/Kafka 或压测，上表集成验证仍对应原测试记录与已通过的 CI。

### 6.2 三轮成功负载

E2E 每轮新建一对账号，同会话 8 个发送连接×20 条，1 个独立接收连接；另有独立账号预热 20 条。
历史每轮 8×50 次、页长 50；建连使用无历史账号，200 连接/8 线程；Kafka 下推每轮 1000 条。

| 测试 | 三轮吞吐范围 | 吞吐中位数 | 各轮 p99 的中位数 | 成功证明 |
|---|---:|---:|---:|---|
| 单会话 E2E | 30.90～39.78 msg/s | 33.52 msg/s | 4723.21 ms | 每轮 sent/accepted/delivered ACK/实际唯一接收=160；SQL 行/唯一 seq=160 |
| 历史查询 | 1153.88～1268.06 QPS | 1158.62 QPS | 11.62 ms | 每轮 400/400，无失败 |
| 认证建连 | 1247.21～1651.43 conn/s | 1290.94 conn/s | — | 每轮 200/200；不含长期持连/带历史重连 |
| Kafka→Job→Comet | 996.92～1212.31 msg/s | 1197.01 msg/s | 811.46 ms | 每轮 1000 条唯一接收、无缺失/解析错误 |

这些是三轮短负载测量，不是稳态峰值或生产容量。p99 为**各轮分位数的中位数**，没有冒充合并样本 p99。
E2E 吞吐以独立接收完成计，延迟从发送到接收；全部 ACK 和 SQL 行另行核对。
接收工具按 client ID 去重，结果中的“无缺失”不表示线上从未重复发帧。
Kafka 分层绕过 Logic 受理/SQL 历史，不能拿它代替完整消息吞吐。

### 6.3 大突发失败与改进重点

800 条同会话一次性突发没有通过：首次测得 accepted=325、delivered ACK/实际接收=333、ACK 错误=475。
失败记录保留在 [initial-burst-800.json](benchmarks/2026-10-11-current/initial-burst-800.json)，
诊断复测 accepted=330、delivered ACK/实际接收/SQL 行=337、ACK 错误=470；
Comet 指标记录 468 次 ACK 超时和 7 次流重连。错误帧缺少内部 code，不能把全部错误归为同一原因。
两次失败均保留；不能把失败样本的 received/duration 当容量 QPS。

当前 Submit 的 per-session 锁覆盖 SQL 原文预留/prepare 与 Kafka SendAndWait，积压从入队计 8 秒 deadline。
这提供顺序和有界等待，但限制单会话突发处理。后续应先分解锁等待、SQL 往返、broker 确认耗时，
再评估缩短临界区、可靠异步确认/批处理和客户端发送窗口；不能直接抬高 timeout 或改成无确认 ACK。
失败/超时消息可能已 prepare/入 Kafka，需要同 ID 重试并通过历史/游标核对，不等于全部永久丢失。

### 6.4 复现

Docker harness 只创建并删除自己随机命名的容器/卷，不碰现有业务数据。

```bash
python3 tests/run_mysql_review_tests.py build-wsl
python3 tests/run_redis_review_tests.py build-wsl
python3 tests/run_kafka_volume_test.py build-wsl
python3 tests/run_current_benchmarks.py --build build-wsl \
  --out /tmp/spark-bench-160 --repeats 3 --messages-per-conn 20
# 诊断大突发：预期可能退出 1；失败结果仍落盘，保留它。
python3 tests/run_current_benchmarks.py --build build-wsl \
  --out /tmp/spark-bench-800 --repeats 1 --messages-per-conn 100 --allow-incomplete
python3 cannbot/scripts/validate_knowledge.py --strict
python3 cannbot/scripts/test_pi_citations.py
```

尚未执行多机/磁盘损坏、真实 TLS 证书互通、Redis 黑洞、ASan/TSan 长压、Android 真机与本轮 GPU 运行。
旧 CUDA/ROCm 实验另见专题报告，本文不沿用它们作为本轮最新数据。

### 6.5 全新克隆启动验收（2026-10-12）

在无个人环境文件、无 Agent/CANN/GPU 的临时克隆中，从零编译并运行 README 的启动路径。
[验收回执](benchmarks/2026-10-12-release-smoke.json) 对应实现/测试工具基线 `496f3e1d4d132342e7482388e72052eb451d9a13`：

- doctor、CMake 构建、sparkctl up/health 均通过；默认 IM 不强制依赖 CANN 知识库。
- CTest 40 通过、3 数据库 opt-in 跳过、0 失败；该次执行 31.85 秒，未另补跑数据库 opt-in。
- 80 条消息 sent/accepted/delivered ACK/独立接收均为 80，SQL 行数/唯一 seq 为 80，无发送或 ACK 错误。
- 弹幕页 HTTP 可访问，已改用本地视频；本次未做浏览器视频播放和 Android 真机验证。

临时服务和数据卷清理完成；原本已停止的依赖容器仅临时更名，验收后恢复原名。
这是启动与正确性 smoke test，不覆盖多机/长压，也不替代第 6.2 节的三轮容量测量。

### 6.6 Hermes Native 增量验证（2026-10-28）

证据：[技术报告](hermes-native-runtime-integration.md)、[机器可读回执](benchmarks/2026-10-28-hermes-native.json)。

| 验证 | 结果与范围 |
|---|---|
| Native 单元 | 38 通过；fake Hermes、临时 SQLite，含准入丢响应、取消竞态、终态不可变与 `/new` 失败恢复 |
| Native Gateway | 6 通过；真实本机 HTTP/SSE + fake Hermes，含审批、停止、回放与发布回执 |
| Pi / Router | 40 / 31 通过；PiRpcClient AST 与基线一致，原运行配置和 Session 未迁移 |
| 构建 / 默认 CTest | 构建通过；47 项注册，44 通过、3 数据库 opt-in 跳过、0 失败 |
| 隔离数据库补跑 | 自建临时 MySQL/Redis 容器，1 + 2 项通过；默认跳过的 3 项全部补验 |
| 真实 Kafka 恢复 | 独立临时容器/卷，替换后消息与已提交 offset 保留、重放获 broker 确认 |
| 知识 / 引用 | 严格知识与引用校验通过；`sync_pi_agent --check` 保留既有快照/settings 差异，不覆盖个人配置 |
| 真实 Hermes | 仅只读 capabilities 探测；未执行真实模型、工具、Skills/Memory 或完整 Native IM E2E |

自动化测试证明合同与恢复逻辑，不能替代真实部署或性能验收。Native 默认关闭，原四个 Pi/Hermes 联系人保留。
真实灰度需独立测试联系人、Session 与工具沙箱，并验证离线历史、原生审批及长任务停止。

## 7. 简历与项目介绍

### 7.1 30 秒介绍

> 我在 C++17 教学 IM 基础上完成了 Comet/Logic/Job 分层和可靠性改造。重点是 SQL 原文幂等/outbox、
> Kafka accepted 与连接 delivered 两阶段、receipt 连续游标，以及异步连接生命周期、流背压和异常回归。
> 我用独立接收端和 SQL 核对结果；最新测量也保留单会话大突发失败，明确容量和部署边界。

### 7.2 可选简历条目

按岗位选 3～4 条，并准备第 4 节对应追问；避免把全部方向堆成一条：

- 基于 Muduo/WebSocket、gRPC 双向流与 Kafka 拆分连接、业务和异步任务，抽离共享 Service/Repository，统一身份、会话、成员和附件规则。
- 以 MySQL 会话事务、canonical 原文和二进制 outbox 统一当前发送入口的序号/幂等，支持确认失败重试和 prepared 恢复；保留原时间戳，拒绝同 ID 改正文。
- 分离 accepted/delivered/read，以逐条 receipt、safe_seq 和跨会话分页处理晚落库、乱序及重连，避免 MAX delivered 导致永久跳过缺口。
- 修复 HTTP 半包与异常输入、跨线程连接闭包、慢鉴权与流 context 取消；引入有界队列、字节预算、慢连接输出上限、可 join RPC 池及受控退出。
- 建立角色 RPC 鉴权、generation+nonce 路由防迟到删除、readiness/DLQ 重放及日志轮转；以自动化回归及隔离 SQL/Redis/Kafka 验证对应边界，结果按日期分别记录。
- 基于 Pi SDK 定制 Kernel Agent，参考 Codex CLI 子 Agent 生命周期实现独立子会话的派发、等待、补充输入与回收；用角色工具约束、显式写入范围和有界资源管理控制同任务并发。
- 在 IM 中实现共享 CANN Agent，使用任务/原生会话/工作区绑定、CAS 交接、内容寻址版本和固定 runner 测试回执支持跨用户接手；设备/NPU 性能另行验证。
- 在保留 Pi 与 Legacy 通路的前提下，通过 Hermes Runs API 实现独立 Session、准入/结果账本、原生终态投影与一次性审批；对未知提交禁止盲目重跑，完成 38 项 Native 单元与 6 项 HTTP/SSE 合同测试，真实 E2E 待验收。
- 为长任务增加可选 Kafka 控制抢占，保持普通任务顺序并仅提交连续完成的 offset 前缀；用本地 mock broker 验证停止抢占、失败重放与位点安全，保留有界窗口限制。

数字只有附带日期/环境/负载才使用：本轮可写“三轮各 160 条消息 ACK/唯一接收/SQL 完整”，
不把旧 1100 msg/s 写成当前吞吐，不把 Kafka 分层速率写成整链路 TPS。

### 7.3 高频追问索引

| 问题 | 应答核心 / 细节入口 |
|---|---|
| 为什么 SQL 留在受理路径？ | 原文/序号/恢复必须持久；历史仍异步，4.1～4.2 |
| Redis 清空怎么不重号？ | 当前 SQL 是唯一权威；token/路由仍需重建，4.1/4.13 |
| 为什么不能 MAX delivered？ | 低 seq 可能晚落库；receipt+safe 前缀，4.4 |
| 空洞如何推进游标？ | 仅有 safe_seq 证明的已结算缺号，4.5～4.6 |
| Write 成功为何不提交 offset？ | 必须收到同 ID 业务 reply；崩溃前后允许重放，4.7/4.9 |
| FIFO/LRU/有序树分别用在哪里？ | queue/deque、近期 set、delta TreeMap，4.6/4.8/4.11 |
| 回调如何防 UAF？ | owning 闭包、weak timer、owner loop 与析构/join，4.16 |
| 为什么大突发失败？ | 持锁范围、排队 deadline 与 SQL/Kafka 时延，4.3/6.3 |
| 子 Agent 与 worker 池有何区别？ | 同一任务拆分与跨会话准入是两层；子会话不跨 turn 持久化，4.22 |
| Agent 多人接手如何可恢复？ | task/session/actor 分离，账本、CAS、源码与测试 hash，4.19 |
| 推理缓存/批处理如何区分？ | 服务准入、runtime batch、KV/prefix cache，4.20 |
| Hermes 工具会不会重复执行？ | 准入预写、稳定身份、原 Run 查询；无 run_id 的 unknown 不自动重发，4.21 |
| 长任务没结束，stop 怎样送达？ | 独立控制池 + Kafka 可选控制窗口；完成前缀提交位点，4.21 |
| Native 是否已经上线验收？ | 默认关闭；区分 fake、只读实机探针和未完成的完整 E2E，6.6 |

不要声称：exactly-once、百万连接、生产级多租户沙箱、全链路 DLQ、全量故障自愈、NPU 实机通过。
回答按“结论→结构/步骤→真实证据→边界”展开，先解释当前实现，再谈改进方案。

## 8. 源码与专题入口

| 主题 | 首选入口 |
|---|---|
| 运行/配置 | [README](../README.md)、[启动说明](../scripts/00_prepare_and_run.md)、conf/ |
| 业务层与扩展 | [Logic 业务层](logic-business-layer.md)、logic/services/、logic/repositories/ |
| 工程修复/迁移 | [review-remediation](review-remediation.md)、sql/migrations/ |
| 最新测试 | [本轮报告](benchmarks/2026-10-11-current/README.md)、tests/、load_test/ |
| Kernel Agent/多 Agent/协作 | [产品与子任务](kernel-agent.md)、[运行时](pi-agent-integration.md)、[多 Agent](multi-agent-integration.md)、[房间协作](room-agent-collaboration.md) |
| Hermes Native | [架构/部署/回滚](hermes-native-runtime-integration.md)、[测试回执](benchmarks/2026-10-28-hermes-native.json) |
| 推理架构 | [Inference](inference-serving.md)、inference/gateway/、inference/worker/ |
| GPU 历史实验 | [CUDA 2026-10-07](performance-report-2026-10-07-rtx3060.md)、[ROCm 2026-09-30](performance-report-2026-09-30-rx9070xt.md) |

旧教学快照、课件、Android 迁移备份与旧 CMake/预编译库已移出工作树，演进对照可查 Git 历史。
弹幕演示改为浏览器选择本地视频，不上传视频；旧专题里的 Redis 取号描述不是当前运行真源。
有冲突时回到根目录代码与本手册。
后续顺序：真实故障/迁移演练→热点会话性能分解→安全与路由租约→告警恢复→多机与长期容量验证。

## 9. 术语索引

本节解释正文里的模块名、协议、状态词和缩写。每条先说这个词本身是什么，再说它在 Spark Push 里落在哪。步骤、数字和证据仍以第 1～8 节为准。

同一个英文词会指不同的东西，要连着它的宾语看：

| 看到的词 | 实际是什么 |
|---|---|
| 登录 token | 证明“你是这个用户”的随机字符串，存在 Redis |
| 令牌桶 token | 限流用的许可，花掉 1 个才能发一次 |
| 模型 token | 大模型吐出的一小段文本，和登录无关 |
| Redis generation | 这次上线的路由代次，用来挡住迟到的下线 |
| 连接 generation | 客户端这次连接的代号，旧连接的回调不能改新连接 |

查词顺序：9.1 看一条消息会留下哪些证据，9.2～9.4 看 Kafka、canonical 原文和 gRPC，后面的表按主题补其余词。

### 9.1 一条消息会留下哪些证据

用户发出一条单聊后，系统分几步记账。后一步的成功，只证明后一步自己的事。

| 状态 | 这时已经发生的事 | 这时还不能当作已经发生 |
|---|---|---|
| reserved | MySQL 分配了 `msg_seq`，并冻结 canonical 原文 | 发给 Kafka 的字节还没写好 |
| prepared | 同一行写入了不可变的二进制 outbox | Kafka 还没确认 |
| accepted_ack | `persist_message` 的 Kafka 收条已回来 | 历史表可能还没写入；对方可能不在线 |
| delivered_ack | 目标连接所属的 IO 线程检查容量后，接受了这次发送 | 对方客户端未必已经显示，也未必已读 |
| history persisted | Job 把历史行幂等写入 MySQL，阶段变成 3 | 用户未必已读 |
| read_seq | 客户端主动说“我读到这里” | 发送方当时未必收到这条已读 |

现场例子：1 号还停在 prepared，2 号已经 delivered。连续水位停在 0。客户端要等 1 号落库并补投之后，才能把水位推进到 2。直接取“已投递序号的最大值”会把 1 号永久跳过。

`ai_delta` 是模型回答的临时预览。它不取号、不写历史、不推动上面任何一个水位。完整答案 `ai_reply` 才重新走这条持久链路。

### 9.2 Kafka 是什么

Kafka 是一个按主题保存记录的日志服务。发送方把记录追加到主题末尾；消费方按自己记住的位置往下读。这个位置叫 offset。

普通消息由 Logic 写入、Job 执行。AI 主题的方向不同，见下表。主题分开，积压和故障也分开：

| 主题 | 谁写入 | 谁消费 | 消费完做什么 |
|---|---|---|---|
| `persist_message` | Logic | Job | 幂等写入 MySQL 历史 |
| `push_single`、`push_group`、`broadcast_task` | Logic | Job | 经 gRPC 推给对应 Comet |
| `ai_request` | Logic | Bridge | 调用 Pi 或推理网关 |
| `ai_delta` | Bridge | Logic | 只把预览推给在线客户端 |
| `ai_reply` | Bridge | Logic | 再走 `persist_message` 和推送，成为可恢复的答案 |

几个跟着 Kafka 一起出现的词：

| 术语 | 含义 |
|---|---|
| broker | 一台 Kafka 服务进程。本轮测试是单 broker |
| topic | 一类记录的名字，例如持久化和单聊推送各用各的主题 |
| partition | 主题内部的一段有序日志。顺序只在同一个 partition 内有效 |
| producer | 往 Kafka 写的一方。Logic 写持久化和推送，Bridge 写 `ai_delta`、`ai_reply` |
| consumer | 从 Kafka 读的一方。Job 读持久化和推送，Bridge 读 `ai_request`，Logic 读 `ai_delta`、`ai_reply` |
| consumer group | 共用一个组名的消费者。组内分 partition，并共用 offset |
| offset | 消费进度。处理成功后才提交；提交前崩溃，这条会再被读到 |
| commit / commitSync | 把 offset 正式记到 Kafka。`commitSync` 会等这次提交完成 |
| auto commit | 客户端按时间自动提交。本项目关掉了它，避免业务没做完就记成完成 |
| acks=all | 生产者等到同步副本都确认。副本数只有 1 时，确认的就是那一台 |
| RF | replication factor，一个 partition 有几份副本。本轮 RF=1 |
| idempotence | 这里指 Kafka 生产者自己的重试去重。它管的是“同一次生产不要写成两条” |
| DR | delivery report，broker 对这一次写入的收条。`SendAndWait` 等的就是它 |
| opaque | 随这条写入交给 Kafka、收条回来时原样带回的指针。收条到达前不能释放 |
| librdkafka | 本项目使用的 C 语言 Kafka 客户端库 |
| lag | 消费者落后日志末尾多少条。队列被拒绝、排队年龄也要一起看 |
| DLQ | dead letter queue，死信队列。处理失败的记录可以改存到这里 |
| KRaft | Kafka 自己保存集群元数据的方式。这些文件在数据目录里，迁移时要一起搬 |
| log.dirs | Kafka 存放日志和 KRaft 元数据的目录 |
| best-effort | 实时推送失败后有限重试并记日志。这条路径本身没有完整的可重放死信 |
| exactly-once | 崩溃和重试之后，效果仍只出现一次。本项目的模型是“提交前可能重放，下游按同一身份合并” |

因此简历里可以把实现说成：Kafka 收条和连接接受发送分成两段确认，提交 offset 前允许重放，历史写入和客户端会合并重复。第 6 节给出的是三轮短负载，以及一次没有通过的同会话大突发。

### 9.3 canonical 原文具体是什么

canonical 的意思是“被认定的那一份正式版本”。这里的原文，就是这条消息第一次受理时冻结下来的内容。

它的物理位置是表 `message_reservation` 的 `original_message` 列，类型是二进制大对象。第一次 `Reserve` 成功时，Logic 写入一份 JSON，字段固定为：

| JSON 字段 | 内容 |
|---|---|
| session | 会话号 |
| seq | 刚分配的 `msg_seq` |
| sender | 发送者 |
| type | 消息类型 |
| content | 正文 |
| time | 当时的时间戳，单位毫秒 |
| client | 客户端带来的 `client_msg_id` |

代码里的输出参数就叫 `canonical`。三种结果：

1. 这个会话、这个发送者、这个 `client_msg_id` 第一次出现：分配新序号，把上面这份 JSON 存进去，并把它当作结果返回。
2. 同一个身份再来，类型和正文的字节也相同：读出旧 JSON，返回原来的序号、正文和时间戳。
3. 同一个身份再来，但类型或正文有差异：拒绝。同一个 ID 不能改口。

时间戳必须沿用旧值。若重试时改成“现在”，Job 逐字段比较会把这次合法重试看成另一条消息。

它和 outbox 是相邻的两份数据。原文回答“这条消息是什么”；outbox 是依据这份原文编出来、准备发给 Kafka 的 protobuf 字节，放在同一行的 `persist_payload`。prepare 时写入 outbox，已有字节必须完全相同。

正文还有第二处 canonical。Agent 的 `route_key` 把 tenant、channel、conversation、thread 收成字段顺序固定的 JSON，再做哈希。那是“同一身份永远得到同一个键”的规范写法，和消息原文无关。

### 9.4 gRPC 是什么

gRPC 是两个进程之间的调用方式。调用方按函数的形式发出请求，参数和返回值用 Protobuf 编成二进制。双方靠同一份 `.proto` 知道每个字段的编号和类型。

用户侧用 HTTP 和 WebSocket。gRPC 用在服务进程之间：Comet、Logic、Job，以及可选的推理 Gateway 与 Worker。

| 形态 | 样子 | 本项目中的例子 |
|---|---|---|
| Unary | 一次请求，一次响应 | Comet 拿 token 请 Logic 做 `VerifyToken`；流失败时的 Unary fallback |
| 双向流 | 一条调用上两边可以连续发送 | 上行 `MessageStream`：Comet → Logic；下行 `PushStream`：Job → Comet |

流里的回复按 `request_id` 找回那一次请求。先发出的请求，可以后收到回复。`Write` 成功只说明 gRPC 库收下了字节；Job 收到同一个 ID 的成功 reply，才认为这次推送调用完成。

旁边几个协议词：

| 术语 | 含义 |
|---|---|
| HTTP | 一问一答的文本/二进制协议。登录、拉历史、上传图片用它 |
| WebSocket / WS | 先用 HTTP 握手，再在同一条连接上双向发送帧。客户端和 Comet 的长连接 |
| WSS / HTTPS | 跑在 TLS 上的 WebSocket / HTTP。公网部署要用加密这层 |
| TLS | 传输加密，并靠证书确认对面是哪台服务器 |
| CA | 证书签发者。客户端用它判断服务器证书是否可信 |
| SSE | Server-Sent Events。服务器沿一条 HTTP 响应持续推文本事件。Bridge 用它读模型输出 |
| Protobuf | Protocol Buffers。带字段编号的二进制格式。outbox 和 gRPC 参数都用它 |
| RPC | remote procedure call，远程过程调用。gRPC 是其中一种实现 |
| metadata | 随一次 RPC 附带的键值。内部调用要恰好带一个 `x-spark-rpc-key` |
| channel | 到某个 gRPC 服务的连接 |
| stub | 由 `.proto` 生成的客户端对象，上面是一个个可调用的方法 |
| context | 一次调用的取消和期限句柄。取消表示“请停”，线程仍要等它真正退出 |
| deadline | 绝对截止时间。Comet 的 ACK 从入队算起 8 秒；池子借连接的 3 秒也不会因为多次醒来而延长 |
| flow control | 接收方声明自己还能收多少字节。发送方必须遵守，避免把慢的一方撑爆 |
| Bearer | HTTP 头 `Authorization: Bearer` 后面放登录 token，用来表明这次请求带着访问凭证 |

内部密钥只区分 Comet、Job、管理员这几个进程角色。用户是谁、能不能进这个会话，仍要拿连接上的登录 token 再查一次。

### 9.5 模块和角色

| 术语 | 含义 |
|---|---|
| IM | instant messaging，即时通讯。单聊、房间、已读、离线补拉都属于这一层 |
| Spark Push | 这个 C++17 项目的名字。连接、业务、异步任务分进程 |
| Comet | 守着用户长连接的接入进程。WebSocket 9000，内部 RPC 9105，指标 9203 |
| Logic | 业务进程。管登录、会话、序号、幂等、路由和历史查询。HTTP 9101，内部 RPC 9100 |
| Job | 消费 Kafka 的进程。负责推送到 Comet，以及把历史写入 MySQL。指标 9202 |
| Bridge | 可选进程。消费 AI 请求，用 HTTP/SSE 调模型，再把预览和最终答案发回 |
| Kernel Agent / Gateway | 基于 Pi 的算子研发助手及本地网关，默认端口 8643。管会话、轮次、事件、工具与子任务 |
| Inference Gateway | 可选推理入口。负责排队、挑选 Worker、取消 |
| Inference Worker | 可选推理进程。管模型运行时、心跳和本进程并发 |
| WebDemo | 浏览器演示页，HTTP 9010。自己做排序、去重和补拉 |
| Kotlin / Compose | 写 Android 客户端的语言和界面框架 |
| Muduo | 本项目使用的 C++ 网络库。一条连接固定归某一个事件循环线程 |
| adapter | HTTP 或 gRPC 的外壳。它取出凭证、翻译协议，再调用 Service |
| Service | 业务规则所在的层。鉴权、序号、成员、附件都在这里统一 |
| Repository | Service 下面的存储封装，挡住 SQL、Redis、Kafka 的具体写法 |
| DAO | data access object。针对某张表或一类查询执行 SQL 的对象 |
| actor | 这次请求声称的操作者。提交时用连接上的 token 重新核对 |
| 路由 | “这个用户或房间当前连在哪台 Comet”。在线路由放 Redis |
| presence | 某条 WebSocket 还连着。房间能不能发言，以数据库中的成员为准 |
| fanout | 把一条消息复制给多个连接或多个 Comet。群聊和广播都要做这件事 |
| 弹幕 | 叠在画面上的短消息。它和单聊、旧 HTTP 发送共用同一套 SQL 取号。演示时由浏览器选择本地视频，视频文件不上传 |

拆成 Comet、Logic、Job 是为了分别限制连接数、业务复杂度和消费速度。机器的 CPU、Kafka 和网卡仍然可以是同一套。

### 9.6 消息身份和表

| 术语 | 含义 |
|---|---|
| session_id | 会话号。单聊是 `s_` 加两个用户号，小号在前；房间是 `r_` 加房间号 |
| msg_seq | 会话内部的递增序号。只保证这个会话内的顺序，跨会话没有总顺序 |
| msg_id | `session_id` 加 `-` 再加 `msg_seq`。服务端给这条消息的身份 |
| client_msg_id | 客户端给“一次发送”起的 ID。唯一范围是同一个会话加同一个发送者。WebSocket 上长度为 1～128 字节 |
| request_id | 一次内部流请求的配对号。Job 默认用 `msg_id@comet_id`。业务发送身份看 `client_msg_id`，流上的配对看 `request_id` |
| reservation | `message_reservation` 中的一行。保存序号、canonical 原文、outbox 和阶段 |
| outbox | 发件箱模式：先把即将送出的事件写进数据库，确认失败后再用同一份字节重发。本项目放在 `persist_payload` |
| stage | 这一行走到哪一步。0 已预留，1 已准备，2 Kafka 已发布，3 历史已落库，4 已封存 |
| sealed | stage=4。只有超过 10 分钟且从未 prepare 的行能封存。封存会留下一个可以跳过的缺号 |
| protobuf | 见 9.4。persist outbox 是 protobuf 字节 |
| message_sequence | 每个会话一行，锁住它来分配下一个 `msg_seq` |
| message | 历史消息表。和 reservation 分开；Job 幂等插入后，用户才能从历史里读到 |
| receipt | `message_delivery_receipt` 的一行，证明这个用户的这个序号曾经被接受发送 |
| delivered_seq | 连续已投递水位。它以下的历史消息都已有投递证明；封存缺号可以越过，尚未落库的预约不能越过 |
| read_seq | 客户端上报的已读水位。它晚于投递，不能用来推断哪些消息已经送到 |
| safe_seq | 服务端先算 safe：持久序号，与最早未落库预约的前一位，取较小者。返回本页时再和本页最后一条取较小值；空页可以返回完整 safe。客户端按 4.6 的条件用它补齐游标 |
| 水位 | 连续确认到的最大位置。更高的序号可以已经出现，但中间有洞时不能跳过去 |
| continuation | 跨会话离线补拉的书签，记住下一个会话和序号从哪里接着查 |
| 游标 cursor | 客户端已经连续收齐的序号。比它大、中间还有洞的序号放进 seen，先不把游标跳到最大值 |
| seen | 客户端已经见过、但还连不上游标的序号集合 |
| TreeMap | 按键排序的树形映射。Android 用它把乱序的 `ai_delta` 按 `delta_index` 排好 |
| envelope | 最终答案的包裹。它的 sequence 要等于 delta 的条数 |

阶段 0 超过 10 分钟会被封存。阶段 1 和 2 还没走完，不能按“创建满一天”删掉。阶段 3 和 4 满一天后才分批清理，所以幂等只保证留在这个窗口内。

### 9.7 可靠性、游标和投递

| 术语 | 含义 |
|---|---|
| ACK | acknowledgement，确认。本项目把 accepted 和 delivered 分成两次 ACK |
| 两阶段 ACK | 第一次确认 Kafka 已收持久化事件，第二次确认目标连接已接受发送 |
| 幂等 | 同一操作做两次，结果和做一次一样。相同 `client_msg_id` 返回同一条；相同历史事件字段全同才算重复成功 |
| sequence collision | 序号相同，但内容字段不同。这是冲突，历史写入不能悄悄覆盖 |
| Sync | 在一个会话里，按“序号大于 after”拉取安全前缀内的一页 |
| Offline / SyncOffline | 握手后按会话逐个补拉离线消息，用 continuation 记住读到哪 |
| has_more | 这次多读了一条，说明后面还有下一页 |
| next_seq | 本页最后一条的序号。它本身不证明前面的缺号都已结算 |
| 413 | HTTP“内容过大”。单条历史超过本页字节预算时返回它，并且不推进游标 |
| 半包 | TCP 这次只送到了请求或帧的一部分。解析停下来等下一次数据 |
| coalesced | 一次读里同时到了握手头和后面的第一帧。计算头的长度时不能把帧算进去 |
| FIN / opcode / mask / RSV | WebSocket 帧头。FIN 表示这一帧结束，opcode 表示文本、关闭或心跳，客户端数据必须带 mask，RSV 在本项目中为 0 |
| Ping / Pong / Close | 控制帧。Ping 回一份相同内容的 Pong；Close 先回应再关闭。Pong 不是聊天消息 |
| 高水位 | 单条连接的待发送字节上限。这里是帧加输出缓冲 4 MiB，超过就关慢连接 |
| Unary fallback | 流上的回复没在期限内回来时，Job 可以改用一次一问一答再推 |
| watchdog | 独立计时器。写线程堵住时，仍由它把超过 deadline 的排队请求判失败 |
| 背压 | 下游满了以后，上游改为拒绝、限速或断开，避免内存无限增长 |
| 令牌桶 | 按固定速率补充许可、最多存 burst 个。许可不足时拒绝，HTTP 状态是 429 |
| burst | 桶里允许积攒的许可上限，用来放过一小段突发 |
| 429 | HTTP Too Many Requests。这次被限流拒绝 |
| steady_clock | 单调时钟。系统时间被改掉时，它不会跟着回拨，适合算速率和等待 |
| readiness | `/ready` 表示这个进程现在能正常服务。端口还在听，只能说明进程还活着 |
| metrics | 一段可抓取的运行计数，例如队列长度、ACK 超时、流是否就绪 |
| dry-run | 只检查并打印将要做什么。`dlq_replay` 默认如此，加上 `--apply` 才真正发布 |
| schema | 数据的结构。重放死信时要先核对结构、目标和十六进制内容 |

入口限流、SQL、Kafka、Job 队列、gRPC 流量控制、Comet 输出缓冲、慢客户端，是一整条链。只放宽其中一处的超时，不会让整条链变快。

### 9.8 MySQL、Redis 和 SQL 写法

| 术语 | 含义 |
|---|---|
| MySQL | 关系型数据库。序号、原文、历史、投递收据以它为准 |
| Redis | 内存键值库。放登录 token、在线路由、在线计数和未读缓存 |
| SQL | 操作 MySQL 的语句。正文里的“SQL 取号”就是用事务在 MySQL 里分配序号 |
| 事务 | 一组 SQL 要么一起提交，要么一起撤销。取号、查重、保存原文在同一个事务里 |
| 唯一键 | 数据库拒绝重复的键。同一会话的序号不能重复；同一发送者的 `client_msg_id` 也不能重复 |
| UPSERT | `INSERT ... ON DUPLICATE KEY UPDATE`。行不存在就插入，存在就走更新。这里用它保证会话序号行存在 |
| FOR UPDATE | 读的同时给这行加排他锁，别的事务要等本次提交。同一个会话的取号因此串行 |
| INSERT IGNORE | 键冲突时静默跳过。用在这条序号行上时，两个事务可能先拿共享锁，再互相等待升级，所以当前实现不用它取号 |
| 共享锁 / 锁升级 | 共享锁允许多人读；之后若要改，必须升级成排他锁。两个人同时升级就会互相等 |
| prepared statement | SQL 文本里用 `?` 占位，数值另送。比较按传入的字节进行 |
| 字符集排序规则 | MySQL 比较字符串的规则。二进制列按原始字节比较，避免看起来相近的 ID 被当成同一个 |
| GREATEST | 取两个数里较大的一个。用来避免乱序更新把阶段或 `last_msg_seq` 改小 |
| MEDIUMTEXT / MEDIUMBLOB | 存放较长文本或二进制的列类型。历史正文、原文和 outbox 用这类大字段 |
| 连接池 | 预先维持有限个数据库连接，请求来了借出，用完归还 |
| RAII | 用 C++ 对象的生命周期管理资源。guard 析构时自动还连接，中途返回也会还 |
| SQLite | 单个文件里的数据库。Kernel Agent Gateway 用它记主轮次和事件 |
| TTL | time to live，存活时间。到期后 Redis 键或本地路由缓存失效。路由缓存默认 1 秒 |
| Lua | Redis 执行的一小段脚本。比较租约和删除在脚本里一次做完，中间插不进别的命令 |
| generation | 这个用户在这台 Comet 上的上线次数。旧的下线请求必须带当时的次数 |
| nonce | 配在 generation 后面的随机数。Redis 计数器丢失后重新从小数开始，旧请求仍然对不上 |
| lease | 租约，这里是 Redis 里的 `generation:nonce`。只有持有当前值的下线可以删路由 |
| 路由代次 | 就是上面的 generation。新连接先注册，旧连接后下线，也不能把新路由删掉 |
| INCR / SET / GET | Redis 命令：整数加一、写入、读取 |
| SADD / SREM | 向集合加入成员、从集合移除成员。路由集合用它们记录 Comet |
| SCAN / KEYS | 两种遍历键的方式。SCAN 分段进行；KEYS 会一次卡住服务器。旧 token 只偶尔用 SCAN |
| 未读缓存 | Redis 里的未读计数缓存。投递事实以 MySQL 收据和 `delivered_seq` 为准 |

当前序号权威是 MySQL。Redis 整库清空后，已分配的 `msg_seq` 不会因此重号；token 和在线路由需要重新建立。

### 9.9 线程、锁、队列和复杂度

| 术语 | 含义 |
|---|---|
| main loop | Muduo 的主事件循环线程，负责接受新连接并分发给 IO 线程 |
| IO worker | 跑事件循环的工作线程。Comet 默认 8 个 |
| owner loop | 拥有某条连接的那个事件循环。这条连接的状态变更都回到它上面做 |
| mutex | 互斥锁。同一时刻只让一个线程进入被保护的代码 |
| 临界区 | 拿着锁执行的那段代码。这段里如果还要等 Kafka，整个会话的后续提交都要排队 |
| try_lock | 试着拿锁，拿不到就先返回。会话锁每 5 毫秒试一次，最多等 8 秒 |
| condition_variable / cv | 条件变量。线程睡在上面，等别人通知，避免空转查队列 |
| shared_ptr | 共享所有权的指针。还有副本在，对象就不会销毁 |
| weak_ptr | 不增加所有权的指针。使用前先 `lock()`；对象已经没了就拿不到 |
| atomic | 可以安全地被多个线程同时读写的变量 |
| UAF | use after free。对象已经析构，回调仍使用它的地址。退出时要等线程和回调真正结束 |
| join | 等到那个线程的函数返回。取消 context 之后仍然需要 join |
| callback | 稍后由网络库、Kafka 或定时器调用的函数 |
| ThreadPool | 固定数量的工作线程，加一个有上限的任务队列 |
| 有界队列 | 限制条数和字节数的队列。满了就拒绝本次提交 |
| deque | 两端都能进出的队列。本项目用它按插入顺序忘掉最早的 ID，也用它放空闲连接 |
| unordered_set / 哈希表 | 按键平均一次定位。`request_id` 去重和 pending 表都用它 |
| FIFO | first in, first out，先进入的先出来。ID 窗口满了就丢掉最早的那个 |
| LRU | least recently used，最近用过的留到最后。本项目的 ID 窗口不因为再次命中而刷新位置 |
| 哈希 | 把一段数据变成固定长度的摘要。路由键和源码副本用哈希当身份 |
| O(1) | 数据变多时，这项操作的代价基本不变。平均查找哈希表就是这样 |
| O(log N) | 代价随数量缓慢增长。数据库索引定位、树的插入属于这一类 |
| O(n²) | 代价随长度的平方增长。每次都重新拼接整段文本就会这样，所以增量用树加追加 |
| map | 有序映射。会话锁表用它保存键到锁的弱引用 |
| shared_mutex | 允许多人同时读，写的时候只留一个写者。本地路由缓存用它 |
| 字节权重 | 入队时由调用方估计的任务大小。它是队列上限，不是整个进程的内存上限 |
| KiB / MiB | 1 KiB = 1024 字节，1 MiB = 1024 KiB。正文里的 16 KiB、8 MiB 都是字节上限 |

`request_id` 的内存窗口容量是 100000。满了就忘掉最早的 ID，之后的重复要靠客户端和 MySQL 再挡一次。

### 9.10 密码、token 和附件

| 术语 | 含义 |
|---|---|
| PBKDF2 | 把密码反复哈希很多轮，让猜密码变慢。这里用 HMAC-SHA256，120000 轮 |
| HMAC | 带密钥的哈希。PBKDF2 内部使用它 |
| salt | 每个密码旁边的随机字节。相同密码因此得到不同摘要 |
| digest | 哈希结果。这里是 32 字节 |
| 常量时间比较 | 比较摘要时，耗时不随第一个不同字节的位置变化 |
| CSPRNG | 适合安全用途的随机数生成器。登录 token 的 32 字节从这里来 |
| hex | 十六进制文本。32 字节随机数会变成一串十六进制再交给客户端 |
| MD5 | 旧的快速哈希。只有旧账号登录成功时才用来验证，随后改写成 PBKDF2 |
| SHA-1 | WebSocket 握手用来计算 Accept 值的哈希。这里不用它保管密码 |
| SHA-256 | 256 位哈希。源码副本和 Agent 路由键用它 |
| Base64 | 把二进制写成可放进文本协议的字符。握手计算里用到 |
| GUID | WebSocket 标准规定的固定字符串，和客户端的 key 拼在一起再做 SHA-1 |
| 魔数 | 文件开头用来识别 JPEG、PNG 等类型的固定字节。上传检查看它，不看文件名 |
| rehash | 旧算法验证通过后，用新算法重新保存这份密码 |

图片先经 HTTP 上传，检查成员、类型和尺寸，再用临时文件改名落到附件目录。聊天帧和 Kafka 只携带附件 ID。Bridge 需要把图送给模型时，才按 ID 读文件。

### 9.11 Agent 与推理

| 术语 | 含义 |
|---|---|
| Agent | 这里指能接工具、按任务改源码并给出最终回答的程序。人类的话先作为普通消息保存 |
| ai_request | 请模型处理的事件。发生在对应的人类消息已经走持久链路之后 |
| ai_delta | 模型输出的一段预览。只用于临时显示 |
| ai_reply | 完整答案。它重新进入持久化、推送和历史 |
| delta_index | 预览片段的序号。客户端按它重排，和 `msg_seq` 不是同一个空间 |
| flush | 把积攒的片段发出去。Bridge 大约凑满 48 字节或 50 毫秒发一次 |
| DONE | 模型流结束的标记。没有合法结束标记时，不能写成成功的 reply |
| tenant / channel | 租户和渠道。路由键把它们和会话、线程编在一起，避免把不同用户的上下文串起来 |
| turn / event | Agent 的一轮，以及这一轮里按序号记下的事件。Gateway 把主 turn 和事件记在 SQLite |
| input_hash | 这次请求输入的哈希。同一个 `request_id` 重放时必须还是这份输入 |
| route_key | 由规范 JSON 哈希得到的路由身份 |
| FIFO 调度 | 每个会话一张票队列。只有队首、会话空闲、配额还够，才能开始执行 |
| monotonic | 只向前走的时钟。排队超时用它，避免改系统时间影响谁该执行 |
| CAS | compare-and-swap。更新时带上读到的版本，版本仍匹配才写成功。冲突返回 409 |
| 409 | HTTP Conflict。交接的版本已经旧了，这次写入不会覆盖新版本 |
| 内容寻址 | 用内容的 SHA-256 当作这份源码副本的身份。内容相同，身份相同 |
| checkpoint | 协作过程中的检查点，带版本号，供 CAS 更新 |
| bubblewrap | Linux 上收紧进程可见范围的沙箱。固定测试器跑在里面 |
| prlimit | 限制进程可用的 CPU、内存等资源 |
| runner | 执行测试的固定程序。调用者不能改成任意 shell 命令 |
| CANN | 华为昇腾的神经网络软件栈。简历中的 CANN Agent 指这套协作能力 |
| NPU | neural processing unit，神经网络处理器。当前测试器是 Linux 主机示例 |
| runtime | 真正执行模型的运行时。批处理和 KV 由它负责，不由 Gateway 代替 |
| 心跳 / heartbeat | Worker 大约每秒报告自己还活着、负载和显存。超过约 5 秒没有心跳就视为过期 |
| lease（推理） | Gateway 记下的进行中调用。用来估算这个 Worker 上已经有多少请求 |
| Prefill | 模型先读完整段输入，并填好缓存 |
| Decode | 之后一个模型 token 一个模型 token 地生成 |
| TTFT | time to first token。从请求到第一个输出 token 的时间，含排队和预填充 |
| TPOT | time per output token。第一个 token 之后，每个后续 token 的耗时 |
| KV cache | 为已处理 token 保存的 Key/Value。解码时复用它，显存随缓存 token 数增长 |
| continuous batching | 运行中的批次有空位时补进新请求。网关队列和计算批次是两层 |
| slot | 批次里的一个并发位置 |
| Flash Attention | 一种分块、融合的注意力实现，用来减少中间矩阵的读写 |
| prefix cache | 复用相同提示词前缀的预填充。是否命中看 `cached_prompt_tokens` |
| CUDA | NVIDIA 的 GPU 计算平台 |
| ROCm | AMD 的 GPU 计算平台 |
| NVML | NVIDIA 的管理库。本项目按 PCI 总线号读取对应显卡的显存 |
| PCI bus ID | 显卡在主板上的总线地址。用它避免“第 0 号 GPU”被环境变量重新编号后读错卡 |
| OOM | out of memory，内存或显存耗尽。监控读数不是一份严格的 OOM 预算 |
| 量化 | 用更少的位数存 KV 或模型权重。两种量化是不同的开关 |

本轮推理测试核对的是适配器和探针合同，没有重跑 CUDA、ROCm 或 NPU 的真实性能。

### 9.12 构建、测试和性能词

| 术语 | 含义 |
|---|---|
| C++17 | 2017 年发布的 C++ 语言标准。本项目按这一版编写 |
| CMake | 读取 `CMakeLists.txt` 并生成编译命令的工具。`cmake --build` 才开始编译 |
| ctest / CTest | CMake 自带的测试运行器。默认注册 43 项，其中 3 项要另选才跑 |
| opt-in | 默认不跑、需要明确打开的测试 |
| OpenSSL | 提供哈希、随机数和 TLS 的库 |
| hiredis | 访问 Redis 的 C 库 |
| Docker | 用容器运行 MySQL、Redis、Kafka。测试脚本只清理自己创建的容器和卷 |
| CI | 持续集成。这里指 GitHub Actions 在远端编译并跑测试 |
| E2E | end to end，从发送端到另一个独立接收端，并核对 ACK 和 SQL |
| QPS | queries per second，每秒查询次数。历史查询的吞吐用它 |
| msg/s | 每秒消息数。端到端数字按独立接收端完成的条数计算 |
| TPS | transactions per second。Kafka 到 Comet 这一段的速度不能写成整条链路的 TPS |
| p99 | 把耗时从小到大排列后，99% 的样本都比它快。表中是各轮 p99 的中位数 |
| 中位数 | 排序后位于中间的那个数。三轮结果用它代表集中位置 |
| WSL2 | 跑在 Windows 上的 Linux 环境。第 6 节的机器是一台 WSL2 |
| ASan | AddressSanitizer。编译期加入的内存错误检查 |
| TSan | ThreadSanitizer。编译期加入的数据竞争检查 |
| mock | 替身。CUDA 探针的 mock 结果不是真实显卡性能 |
| Gradle | Android 的构建工具。编译成功只说明工程能编过 |
| 指纹 | 用来辨认这次二进制和配置的摘要。压测报告用它对应到具体构建 |
| 预热 | 正式计时前先发送一批消息，让连接和缓存进入工作状态 |
| 并发 | 同一时段有多份工作在进行。同一会话的取号仍是串行的 |

1 KiB = 1024 字节，1 MiB = 1024 KiB。第 5、6 节里的命令可以直接复制；个人密码和角色密钥放在不提交的环境文件中。

### 9.13 缩写对照

| 缩写 | 展开 | 在本文中的位置 |
|---|---|---|
| ACK | acknowledgement | 9.1、9.7 |
| CA | certificate authority | 9.4 |
| CAS | compare-and-swap | 9.11 |
| CI | continuous integration | 9.12 |
| CLI | command line interface，命令行工具 | 5.3 的 DLQ 重放 |
| CSPRNG | cryptographically secure pseudo-random number generator | 9.10 |
| CUDA | Compute Unified Device Architecture | 9.11 |
| DAO | data access object | 9.5 |
| DLQ | dead letter queue | 9.2 |
| DR | delivery report | 9.2 |
| E2E | end to end | 9.12 |
| FIFO | first in, first out | 9.9 |
| gRPC | 远程过程调用框架的名字 | 9.4 |
| GUID | globally unique identifier；这里是协议固定字符串 | 9.10 |
| HMAC | hash-based message authentication code | 9.10 |
| HTTP | Hypertext Transfer Protocol | 9.4 |
| IM | instant messaging | 9.5 |
| IO | input/output | 9.9 |
| KV | key-value，推理里指注意力缓存 | 9.11 |
| LRU | least recently used | 9.9 |
| NVML | NVIDIA Management Library | 9.11 |
| NPU | neural processing unit | 9.11 |
| OOM | out of memory | 9.11 |
| PBKDF2 | Password-Based Key Derivation Function 2 | 9.10 |
| PCI | Peripheral Component Interconnect | 9.11 |
| QPS | queries per second | 9.12 |
| RAII | Resource Acquisition Is Initialization | 9.8 |
| RF | replication factor | 9.2 |
| RPC | remote procedure call | 9.4 |
| ROCm | Radeon Open Compute | 9.11 |
| SHA | Secure Hash Algorithm | 9.10 |
| SSE | Server-Sent Events | 9.4 |
| SQL | Structured Query Language | 9.8 |
| TLS | Transport Layer Security | 9.4 |
| TPS | transactions per second | 9.12 |
| TTFT | time to first token | 9.11 |
| TPOT | time per output token | 9.11 |
| TTL | time to live | 9.8 |
| UAF | use after free | 9.9 |
| WS | WebSocket | 9.4 |
