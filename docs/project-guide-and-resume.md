# Spark Push 项目指引与简历手册

> 更新：2026-10-12；以 `11.2-spark_push/` 根目录当前源码为准。
> 实现与测试工具基线：`340d5b7fda1252cc5a06f892c35e4626a4b4780c`；旧教学快照已移出根目录，主线实现不变。
> 仓库：[specetator/grpc_aiagent](https://github.com/specetator/grpc_aiagent)。

本文保留当前架构、核心链路、算法细节、运行方法、最新测试和简历材料。
已移除旧 Redis 取号算法、重复面试稿、历史发布流水和过时性能表；完整实验留在带日期的专题报告中。
快速复习顺序：第 1～3 节理解项目，第 4 节准备算法追问，第 6～7 节查看证据和简历。

## 1. 项目定位与边界

Spark Push 是一个基于 C++17 的实时消息/IM 与 Agent 协作项目。
Comet 接入 WebSocket，Logic 管鉴权、会话、序号、幂等和路由，Job 消费 Kafka 做投递与历史持久化。
可选 Bridge 把消息接入 Pi Agent 或 Inference Gateway，最终答案复用消息恢复链路。

| 能力 | 当前实现 |
|---|---|
| 用户/权限 | 注册登录、PBKDF2 密码、随机 token、撤销索引、管理员审计、会话/附件/房间成员检查 |
| 消息 | 单聊、聊天室/群 fanout、管理广播、弹幕；WebDemo 与 Kotlin/Compose Android |
| 可靠性 | SQL 持久序号、原文 reservation/outbox、两阶段 ACK、Kafka 业务完成后提交 offset |
| 恢复 | 历史游标分页、逐条 delivery receipt、safe_seq、跨会话离线 continuation、客户端缺口补偿 |
| 容量/运维 | 有界队列、流 deadline、慢连接输出上限、路由代次、readiness、DLQ CLI、日志轮转 |
| Agent | HTTP/SSE 流式输出、任务/源码交接、工具权限、CAS 检查点与版本化测试回执 |
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

Agent：Logic ─► ai_request ─► Bridge ─► Pi / Inference Gateway
       Logic ◄── ai_delta / ai_reply ◄───────────┘
```

| 模块 | 职责/并发单元 | 默认入口 |
|---|---|---|
| Comet | Muduo main loop + 8 IO worker；有界 Unary 池；8 条上行流各有 reader/writer 与 pending | WS 9000、内部 RPC 9105、metrics 9203 |
| Logic | HTTP 默认 4 个 IO worker；gRPC 线程；共享 Service/Repository；后台 outbox 恢复 | HTTP 9101、内部 RPC 9100 |
| Job | 按 topic 的 consumer；每 Comet 一组 PushStream reader/writer；ACK 工作池 | metrics 9202 |
| Bridge | Kafka AI consumer + 模型 HTTP/SSE；隔离秒级模型请求 | 默认无外部监听 |
| Pi Gateway | session/turn/event 账本、FIFO 调度与工具执行 | 本机 8643 |
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

## 5. 构建、启动、升级与排障

### 5.1 最小运行路径

依赖包括 CMake/C++17、OpenSSL、gRPC/Protobuf、librdkafka、hiredis、MySQL client、Docker。
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

测试日期 2026-10-11，环境为 WSL2、12 logical CPU、约 15.52 GiB 内存。
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
- 建立角色 RPC 鉴权、generation+nonce 路由防迟到删除、readiness/DLQ 重放及日志轮转；43 项注册回归及隔离 SQL/Redis/Kafka 验证对应边界。
- 在 IM 中实现共享 CANN Agent，使用任务/原生会话/工作区绑定、CAS 交接、内容寻址版本和固定 runner 测试回执支持跨用户接手；设备/NPU 性能另行验证。

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
| Agent 多人接手如何可恢复？ | task/session/actor 分离，账本、CAS、源码与测试 hash，4.19 |
| 推理缓存/批处理如何区分？ | 服务准入、runtime batch、KV/prefix cache，4.20 |

不要声称：exactly-once、百万连接、生产级多租户沙箱、全链路 DLQ、全量故障自愈、NPU 实机通过。
回答按“结论→结构/步骤→真实证据→边界”展开，先解释当前实现，再谈改进方案。

## 8. 源码与专题入口

| 主题 | 首选入口 |
|---|---|
| 运行/配置 | [README](../README.md)、[启动说明](../scripts/00_prepare_and_run.md)、conf/ |
| 业务层与扩展 | [Logic 业务层](logic-business-layer.md)、logic/services/、logic/repositories/ |
| 工程修复/迁移 | [review-remediation](review-remediation.md)、sql/migrations/ |
| 最新测试 | [本轮报告](benchmarks/2026-10-11-current/README.md)、tests/、load_test/ |
| Pi/多 Agent/协作 | [Pi](pi-agent-integration.md)、[多 Agent](multi-agent-integration.md)、[房间协作](room-agent-collaboration.md) |
| 推理架构 | [Inference](inference-serving.md)、inference/gateway/、inference/worker/ |
| GPU 历史实验 | [CUDA 2026-10-07](performance-report-2026-10-07-rtx3060.md)、[ROCm 2026-09-30](performance-report-2026-09-30-rx9070xt.md) |

旧教学快照已移出根目录，演进对照可查 Git 历史；旧专题里的 Redis 取号描述不是当前运行真源。
有冲突时回到根目录代码与本手册。
后续顺序：真实故障/迁移演练→热点会话性能分解→安全与路由租约→告警恢复→多机与长期容量验证。
