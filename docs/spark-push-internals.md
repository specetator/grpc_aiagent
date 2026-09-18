# Spark Push 小模块技术细节

这份文档按**具体数据结构、逐步算法、容量和失败路径**描述实现。大框架见
[`spark-push-architecture.md`](spark-push-architecture.md)。

约定：

- 当前实现**没有**自定义链表节点或手写 `LinkedList`。FIFO 用 `std::queue`，按时间淘汰用 `std::deque` + `unordered_set`，按 ID 匹配用哈希表，按序号重排用有序树。
- 行号会随代码变动；以函数名和容器字段为准。
- 若文档与根目录源码冲突，以源码为准。

---

## 1. 容器选择原则

| 场景 | 实际容器 | 为什么是它 | 不是什么 |
| --- | --- | --- | --- |
| 线程池待执行任务 | `std::queue<function<void()>>` | 严格 FIFO，配合 `mutex` + `condition_variable` | 优先级队列、无锁环 |
| MySQL / Redis 空闲连接 | `std::deque<ConnEntry>` | 从 front 弹出最旧空闲连接，back 归还 | 手写双向链表 |
| Comet 用户连接 | `unordered_map<int64_t, set<TcpConnectionPtr>>` | 一个用户多连接；`set` 去重指针 | 单连接覆盖 |
| 投递 `request_id` 去重 | `unordered_set` + `deque` | set O(1) 查重，deque 记录淘汰顺序 | LRU（没有按访问刷新） |
| Job PushStream 待写请求 | `queue<PendingRequest>` | 单 writer 串行 Write | 无界内存队列（有 10000 上限） |
| Job 已写出未回复 | `unordered_map<string, PendingRequest>` | reply 可能乱序，按 `request_id` 匹配 | 按下标等待 |
| Android 流式乱序帧 | `TreeMap<Int, PendingStreamDelta>` | 需要最小缺口序号，O(log n) 插入 | 链表、数组线性扫描 |
| Agent 短期热上下文 | `unordered_map → deque<Message>` | 每会话 FIFO 100 条 | 严格 LRU（满 1024 会话时删 `begin()`） |

选择依据始终是访问模式：FIFO 发送、按时间淘汰、按 ID 回调、按整数序号重排。容量、锁和唤醒条件必须和生命周期一起看。

---

## 2. 会话 ID

单聊必须在 Logic、Job 落库、WebDemo、Android 得到同一值。

```text
if (user1 > user2) swap(user1, user2)
session_id = "s_" + user1 + "_" + user2
msg_id     = session_id + "-" + msg_seq
```

落点：

- Logic 热路径：`logic/grpc_service.cpp` `HandleUpstreamMessage`
- MySQL 补 session：`SessionDao::GetOrCreateSingleSession`（内部再次 swap）
- Android：`SparkModels.kt` `singleSessionId()`
- WebDemo：`buildSingleSessionId()`

聊天室：`session_id = "r_" + room_id`。Agent 的 `conversation_id` 必须等于这个 Spark session_id，否则 Logic 拒绝最终 envelope。

---

## 3. Redis 键空间

`logic/redis_store.cpp` 使用的键：

| 键 | 类型 | TTL | 作用 |
| --- | --- | --- | --- |
| `token:{token}` | STRING user_id | 登录时 24h | 正向凭证 |
| `user:tokens:{uid}` | SET | `max(ttl+3600, 86400)` | 撤销时的反向索引 |
| `route:user:{uid}` | SET comet_id | 无 | 用户在哪些 Comet |
| `session:msg_seq:{sid}` | STRING int | 无 | 下一个序号的计数器 |
| `session:last_seq:{sid}` | STRING int | 无 | 未读计算的高水位 |
| `message:dedup:{sid}:{sender}:{client_msg_id}` | STRING seq | 24h | 幂等 |
| `user_session:read_seq:{uid}:{sid}` | STRING int | 无 | 已读缓存 |
| `room:comets:{room}` | SET comet_id | 无 | 房间 fanout |
| `room:online_count:{room}` | STRING int | 无 | 在线人数 |
| `room:comet_count:{room}:{comet}` | STRING int | 无 | 该 Comet 上的房间人数 |

没有 Redis Cluster hash tag，没有 `EVALSHA`，没有 `WATCH`/`MULTI`。序号的原子性完全靠一段 Lua 在 Redis 单线程里执行。

---

## 4. Redis Lua：校准、去重、取号

函数：`RedisStore::AllocateSessionMsgSeq`。

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

EVAL 参数：

- `KEYS[1]` = `session:msg_seq:{session_id}`
- `KEYS[2]` = `session:last_seq:{session_id}`
- `KEYS[3]` = dedup 键；`client_msg_id` 为空则传空字符串，跳过去重
- `ARGV[1]` = MySQL `MAX(msg_seq)` 校准值
- `ARGV[2]` = dedup TTL，热路径 86400

逐步行为：

1. 若 Redis 计数器落后于 MySQL，先抬到 floor，避免重启后从 0 再发号。
2. 再查幂等键。命中则返回旧 seq，`is_new=0`，**仍然可能已经抬过 floor**。
3. 未命中则 `INCR`，只有新值更大才写 `last_seq`，防止水位回退。
4. 写入 dedup 键并设置 EX。

返回值必须是两个 INTEGER 的数组，否则 C++ 视为失败。TTL 过期后同一 `client_msg_id` 会拿到新序号——这是 24 小时窗口幂等，不是永久唯一。

---

## 5. 热路径取号：64 分片校准

`ConversationStore::AppendMessageHotPath` 在 Lua 之外还有一层进程内校准。

数据结构：

```text
array<mutex, 64> seq_seed_mutexes_
array<unordered_set<string>, 64> seq_seeded_sessions_
shard = hash(session_id) % 64
```

步骤：

1. 锁对应 shard。不同会话可并行校准，同一会话串行。
2. 若该 session 尚未出现在本进程的 set 中：
   - `SELECT COALESCE(MAX(msg_seq),0) FROM message WHERE session_id=?`
   - `AllocateSessionMsgSeq(..., floor=max_seq)`
   - 插入 set（即使这次是重复 client_msg_id）
3. 已校准的会话后续调用 `floor=0`，只靠 Lua INCR/去重。
4. 构造内存中的 `Message`：`msg_id = session_id + "-" + seq`。**这里不写 MySQL。**

限制：

- seed set 从不淘汰。Logic 进程长期运行会累积 session_id。
- 若 Redis 被 FLUSH 而本进程还活着，该 session 被当成“已校准”，下一次会用 `floor=0`，可能与 MySQL 已有序号冲突。Job 落库时靠唯一键兜底。
- 这不是分布式锁。真正串行化发生在 Redis Lua。

HTTP `/api/message/send` 走另一条冷路径 `AppendMessage`：直接 `UPDATE session SET last_msg_seq=last_msg_seq+1` 再 INSERT，不经过 Lua。

---

## 6. 令牌桶限流

`common/rate_limiter.cpp` `SceneRateLimiter::Allow`。

配置（`conf/logic.conf`）：

| scene | key | rate | burst |
| --- | --- | ---: | ---: |
| single | sender user_id | 1000/s | 200 |
| chatroom/group | room_id | 300/s | 60 |
| broadcast | scope | 20/s | 5 |

算法（所有桶共用一把 mutex）：

1. `bucket_key = scene + ":" + key`
2. 首次见到该桶：`tokens = burst`，`last_refill = now`
3. `elapsed = now - last_refill`（steady_clock，秒）
4. `tokens = min(burst, tokens + elapsed * rate)`
5. `tokens < 1` → 拒绝；否则 `tokens -= 1` 放行

超限返回 429，**不进入 Kafka**。短时峰值由 Kafka 有界队列和 Job 队列吸收，不把无限内存队列伪装成可靠性。头文件里的默认值（300/60 等）会被配置覆盖。

---

## 7. 线程池

`common/thread_pool.cpp`。

```text
workers_ : vector<thread>
tasks_   : queue<function<void()>>     // 无界
mutex_ + cv_
stopping_
```

`Submit`：锁内 `push`，`notify_one`。池已停止或空函数返回 false。

`WorkerLoop`：

1. `cv.wait` 直到 `stopping_ || !tasks_.empty()`
2. 若停止且队列空，线程退出
3. `move(front)` + `pop`，锁外执行
4. 捕获所有异常，只打日志，线程继续活着

`Stop`：置 `stopping_`，`notify_all`，join 全部 worker，再 swap 空队列丢掉剩余任务。析构调用 Stop。

用途：Comet 的 `grpc_pool_`（离线同步、MarkDelivered），Job 的 `delivery_ack_pool_`（回传 delivered_ack）。任务队列无界，调用方必须自己限制提交速率。

---

## 8. MySQL 连接池

`common/mysql_pool.cpp`。空闲连接是 `deque<ConnEntry>`，每项带 `last_used`。

获取 `Acquire(timeout_ms)`：

1. 先 `CleanupIdleUnlocked`：从 front 关掉超过 `idle_timeout_ms` 且总数仍大于 `min_pool_size` 的连接。
2. 有空闲则 `pop_front`。
3. 否则若 `enable_auto_grow && total < max`，当场 `mysql_real_connect`。
4. 否则按 timeout：`<0` 一直 `cv.wait`，`=0` 立即失败，`>0` 则 `wait_for`。
5. 借出前若 `ping_on_borrow`，`mysql_ping` 失败则丢弃并重试。

归还：停止中则 `mysql_close` 并减少计数；否则 `push_back` 并 `notify_one`。

RAII：`MySqlConnGuard` 移动语义转移归还责任，析构调用 `Release`。Job 配置示例：min 2 / size 8 / max 16。

不是：连接级事务池、预处理语句缓存、读写分离。

---

## 9. Redis 连接池

`common/redis_pool.cpp` 结构与 MySQL 池同构：`deque` 空闲、mutex、cv、自动扩容、空闲回收。

创建连接时额外做：

1. `redisConnectWithTimeout`
2. 若配置了密码：`AUTH`
3. 若 `db != 0`：`SELECT`
4. 设置读写超时

`RedisConnGuard` 析构归还。Logic 热路径每次 Lua EVAL 都 `Acquire()` 一次，用完归还。

---

## 10. Kafka 生产者：异步 Send 与 SendAndWait

`common/kafka_producer.cpp`。

公共配置：`acks=all`、`enable.idempotence=true`、`retries=5`、`linger.ms=5`、`batch.num.messages=1000`、`queue.buffering.max.messages=200000`。压缩依次尝试 lz4 / snappy / gzip / none。后台线程 `poll(5)` 驱动 delivery report。

两种发送：

**`Send`（实时 push topic）**

- `produce(..., msg_opaque=nullptr)` + `poll(0)`
- 只表示进入 librdkafka 队列，不等 delivery report

**`SendAndWait`（persist / ai_request / DLQ）**

1. 分配 `shared_ptr<DeliveryState>`，用裸指针当 opaque 放进 `unordered_map<void*, ...>`
2. `produce(..., opaque)`
3. `cv.wait_for(timeout_ms)` 直到 `dr_cb` 设置 `done`
4. `ok = (err == ERR_NO_ERROR)`

`accepted_ack` 的边界就是 persist 这条 `SendAndWait` 成功。超时 5s（`persist_kafka_timeout_ms`）。超时后 map 里可能残留条目，直到迟到的 DR 到来被丢弃。

析构：停 poll 线程，`flush(3000)`。

---

## 11. Kafka 消费者：重试、提交、DLQ

`common/kafka_consumer.cpp`。Job 四个 consumer 都关闭 auto commit。

主循环：独立线程 `consume(1000)`。成功记录：

1. 用 record timestamp 更新 `spark_push_kafka_lag_ms`
2. 业务回调最多 3 次，失败退避 `100 * attempt` ms
3. 异常当失败，日志不打印消息正文

提交策略分两类：

| consumer | 回调失败后 | 原因 |
| --- | --- | --- |
| persist（有 DLQ topic） | 先 `SendAndWait` 死信 JSON；成功才 `commitSync`；DLQ 或 commit 失败则停消费并离组 | 不能跳过落库事件 |
| push / broadcast（无 DLQ） | 仍 `commitSync`（best-effort） | 避免毒消息卡住分区；靠指标暴露丢失 |

persist 死信 schema：`sparkpush.dead_letter.v1`，key/payload 以 hex 保存，避免非法 UTF-8。非法 protobuf：persist 返回 false（走重试/DLQ）；push 返回 true（跳过并提交）。

---

## 12. 口令哈希

`common/security.cpp`。

新口令：

```text
pbkdf2_sha256$120000$<16字节盐的hex>$<32字节摘要的hex>
```

- 盐：`RAND_bytes` 16 字节
- 派生：`PKCS5_PBKDF2_HMAC` + SHA-256，120000 次
- 比较：`CRYPTO_memcmp` 定长时间比较

登录时若库中仍是 32 位 hex MD5，校验成功后标记 `needs_rehash`，由 HTTP 层升级。管理员账号和 Agent bot 不允许用用户 Token 建立 WebSocket。

Token：32 字节随机 → 64 hex，`SETEX token:{t} 86400 uid`，同时 `SADD user:tokens:{uid}`。撤销时先读反向索引，再 `SCAN MATCH token:* COUNT 256` 兜底旧数据，最后 `DEL` 路由。

---

## 13. 用户路由缓存

`RedisStore::GetUserRoutes`：

```text
unordered_map<int64_t, {comets, expire_time}>
shared_mutex
TTL 默认 1000 ms
```

读：shared lock，未过期直接拷贝。
未命中：`SMEMBERS route:user:{uid}`，unique lock 写回。
`AddRoute` / `RemoveRoute` / 撤销会 `erase` 该 uid。

不是 LRU，没有容量上限，没有负缓存。热路径用它避免每条消息都打 Redis。

---

## 14. WebSocket 握手：自实现 SHA1 + Base64

`comet/comet_server.cpp` 匿名命名空间。不依赖 OpenSSL 做握手。

**Base64Encode**

- 3 字节一组拼成 24 bit，切 4 个 6 bit 查表 `A-Za-z0-9+/`
- 剩余 1 字节补 `==`，剩余 2 字节补 `=`

**SHA1**（FIPS 180-1）

1. 初值 `0x67452301, 0xEFCDAB89, 0x98BADCFE, 0x10325476, 0xC3D2E1F0`
2. 消息后追加 `0x80`，再补 0 直到 `size % 64 == 56`
3. 追加 64 bit 大端 bit 长度
4. 每 64 字节一块：前 16 个 word 大端载入，后 64 个 `RotL(w[i-3]^w[i-8]^w[i-14]^w[i-16], 1)`
5. 80 轮，四段常数 `0x5A827999 / 0x6ED9EBA1 / 0x8F1BBCDC / 0xCA62C1D6`

**Accept**

```text
Sec-WebSocket-Accept = Base64( SHA1( client_key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11" ) )
```

Header 查找大小写敏感，只认 `Sec-WebSocket-Key:`。Token 从 query `token=` 取，**不做 URL decode**。握手阶段 `VerifyToken` 在 muduo IO 线程同步调用。

---

## 15. WebSocket 帧解析与发送

入站 `HandleWebSocketFrame`：

1. 至少 2 字节。`FIN` 必须为 1，客户端必须 mask；否则 `shutdown`
2. payload 长度：7 bit / 16 bit / 64 bit
3. 上限 `4 MiB`，超过断开
4. 数据不够则 return，等下次 `OnMessage`
5. 4 字节 mask，逐字节 `payload[i] ^= mask[i % 4]`
6. opcode `0x8` 关闭；`0x1` 当文本；其它 opcode 消费后丢弃（无 ping/pong）

出站 `BuildWebSocketTextFrame`：

- `0x81`（FIN + text），服务端帧不 mask
- 长度编码与 RFC6455 相同
- 不分片

上行 JSON 轻量解析（`websocket_utils.cpp`）拒绝转义字符和嵌套数组；`type` 仅允许 `single_chat | chatroom | chatroom_join | chatroom_leave`。`sync` 控制帧在 `OnTextMessage` 用 nlohmann 解析。

---

## 16. 连接表与房间表

```text
user_conns_ : unordered_map<int64_t, set<TcpConnectionPtr>>
room_users_ : unordered_map<int64_t, set<int64_t>>
conns_mu_   : mutex   // 同时保护 recent_push_*
```

握手成功：`user_conns_[uid].insert(conn)`，**不踢掉**旧连接。
断开：从 set 擦除；set 空了才 `NotifyUserOffline`（detached 线程调 Logic，Logic 再 `SREM` 路由）。
`PushToUsers`：锁内把指针拷到局部 `vector`，锁外 `send`。muduo `send` 线程安全，会切回所属 EventLoop。

房间：`chatroom_join` 把 uid 放入 `room_users_`；本 Comet 上该房间从 0 变 1 时才通知 Logic `SADD room:comets`。TCP 断开**不会**自动清理房间成员，只靠显式 leave。

---

## 17. request_id 定长去重窗口

`CometServer::AcceptPushRequest`，容量 100000。

```text
recent_push_ids_   : unordered_set<string>   // O(1) 判断
recent_push_order_ : deque<string>           // 进入顺序
```

步骤：

1. 空 `request_id` → 放行且不记录
2. set 已有 → 指标 `delivery_duplicate_total`，返回 false
3. `insert` + `push_back`
4. 超过 100000：`erase(front)` + `pop_front`

这是 FIFO 窗口，不是 LRU：重复命中不会把 ID 挪到队尾。Job 的 `request_id` 默认 `msg_id@comet_id`。重复请求仍回 `error.code=0, delivered_count=0`，避免 Kafka 死循环。

Logic 侧同类结构：

- `hermes_stream_completed_`：set + deque，上限 100000
- `hermes_delta_seen_`：上限 200000
- `hermes_history_cache_`：最多 1024 个 session，每个 deque 100 条；满时 `erase(begin())`，按哈希桶顺序删，不是 LRU

---

## 18. Comet → Logic MessageStream

配置：`use_grpc_stream=true`，`grpc_stream_count=8`。

每条流：

```text
send_queue : queue<PendingRequest>   // 无界
send_queue_mutex + send_queue_cv
writer_thread / reader_thread
```

全局：

```text
pending_callbacks_ : unordered_map<string, callback>
request_id_counter_ : atomic<uint64_t>
```

`SendToStream`：`fetch_add` 得到 id，`stream_idx = id % stream_count`，push 到对应队列，`notify_one`。

Writer：wait → pop → 把 callback 放入 `pending_callbacks_` → `Write`。Write 失败只 log 并退出循环，**Comet 侧不重连**。

Reader：`Read` → 按 `request_id` 取出 callback → 锁外调用。

Logic `MessageStream` 顺序 Read，处理完用同一 `request_id` Write 回去。这消除了逐条 Unary 的 ClientContext 开销，但 Comet 发送队列无界。

---

## 19. Job → Comet PushStream

每个 `comet_id` 一个 `StreamState`：

```text
queue            : queue<PendingRequest>          // 上限 10000
pending          : unordered_map<string, PendingRequest>
PromiseState     : mutex + cv + done + ok + reply
stream_broken    : atomic<bool>
```

**SendToStream**

1. 队列已满 → `stream_queue_rejected_total`，返回 false（Kafka 将重试）
2. push + notify
3. 等待 `push_rpc_deadline_ms`（2000）
4. 超时：若 pending 里仍是同一个 completion 指针则擦除，记 timeout 指标

**Writer** 最多 4 次尝试：

- 流坏了先 `ReconnectStream`
- 退避：从 `max(10, 200ms)` 倍增到 5000ms
- Write 前把请求放进 `pending[request_id]`
- Write 失败：从 pending 拿掉自己，sleep，重连

**Reader**

- 按 `reply.request_id` 匹配；找不到记 unmatched
- `error==0 && delivered_count>0` 才异步 `SendDeliveryAck`
- 读循环退出：把 remaining pending 全部 `ok=false` 唤醒，避免永远阻塞

**ReconnectStream**

1. `TryCancel` + `WritesDone` + join reader + `Finish`
2. **先失败所有 pending**，让 Kafka 重试，绝不在无 reply 时提交 offset
3. 新建 channel / stub / stream，拉起新 reader，`reconnect_total++`

Unary 兜底：3 次，间隔 100/200ms。两条路径都失败才 `delivery_lost_total`，回调返回 false。

---

## 20. 落库幂等与单调水位

Job `PersistMessage` 最多 4 次，退避 50/100/200ms。

`MessageDao::InsertMessage`：

1. 预处理 INSERT 七个字段
2. errno 1062（`uk_session_seq`）时，按全部字段比对已有行
3. 完全相同 → 视为幂等重放，返回成功
4. 序号相同但内容不同 → `"message sequence collision"`，**停止重试**

水位：

```sql
UPDATE session SET last_msg_seq=GREATEST(last_msg_seq, ?) WHERE session_id=?
UPDATE user_session_state
SET delivered_seq=GREATEST(delivered_seq, VALUES(delivered_seq)),
    read_seq=GREATEST(read_seq, VALUES(read_seq))
```

乱序回调不会把游标打回去。`read_seq` 的 Redis 缓存是覆盖写，可能短暂落后于 MySQL GREATEST；未读数以 `max(last_seq - read_seq, 0)` 计算，不含 delivered。

---

## 21. MEDIUMTEXT 动态读取

历史原因：`TEXT` 约 64 KiB，长 Agent 回复会被截断。迁移 `sql/migrations/002_message_content_mediumtext.sql` 把 `content_json` 改为 `MEDIUMTEXT`。

读取仍先绑定 64 KiB 缓冲（快路径）。`AssignTextColumn`：

1. 若 `reported_length > capacity`，或 `fetch_ret == MYSQL_DATA_TRUNCATED` 且长度顶满缓冲
2. 则 `FetchFullTextColumn`：按 `reported_length` 分配 `string`，`mysql_stmt_fetch_column` 再取一次
3. 否则直接 `assign(buffer, reported_length)`

这解决的是**读侧静默截断**。模型上下文窗口和 Android 输入框高度都不变。

---

## 22. 离线补推与客户端缺口

握手后 `RequestOfflineSync`（grpc 线程池，3s deadline）：

1. `SyncOffline{user_id, limit=200}`；Logic 把 limit clamp 到 `[1,1000]`
2. 列出该用户全部单聊 + 存在的聊天室 session
3. 每个 session 从 `delivered_seq` 之后取消息，共用一个 remaining 预算
4. Comet 对每条 `PushToUsers`；send 成功才记录该 session 的 max seq
5. 至少一条成功才 `MarkDelivered`

客户端缺口（Web/Android 相同语义）：

```text
seenSequences[session].add(seq)
while seen.contains(cursor+1): cursor++
if seq > cursor+1: send sync(after_seq=cursor)
```

补偿消息带原始 `msg_id` / `client_msg_id` / `msg_seq`，客户端按 ID 去重。`ai_delta` 没有 `msg_seq`，不参与游标。

---

## 23. SSE 解析状态机

`hermes_bridge/sse_parser.cpp`。上限 16 MiB。

状态：`pending_` 未拆行缓冲、`event_name_`、`event_data_`、`failure_`（粘性错误）、`final_` / `stopped_` / `done_`。

`Feed`：

1. 追加 chunk，超限失败
2. 按 `\n` 切行，去掉 `\r`
3. 空行 → `ProcessEvent`
4. `event:` / `data:` 去掉一个可选前导空格；其它行（`id:`、注释）忽略
5. 无换行的尾巴留在 `pending_`

`ProcessEvent`：

- `[DONE]` 必须已经有 final 或 `finish_reason=stop`
- `agent.event`：校验 envelope；`error` 失败；`assistant_progress` 禁止出现在终态之后；`assistant_delta` 改写成 OpenAI `choices[0].delta.content`
- `pi.final` / `hermes.final` / `agent.final`：非空文本，重复终态必须文本和 metadata 一致
- 普通 `choices`：只取第一项；`finish_reason` 必须是 `"stop"`；delta 在终态之后到达则失败

`Finish` 要求 `done && (final || stopped)` 且文本非空。缺 DONE、非 stop、空回答都不能写成成功 `ai_reply`。

---

## 24. Bridge 增量合并与 request_id 缓存

`hermes_bridge/main.cpp` `HandleRequest`：

- 进程内 `reply_cache_[request_id]`；命中则跳过模型，只重发 `ai_reply`
- 缓存达到 10000 条时**整表清空**（不是 LRU）
- Kafka 失败恢复：若缓存有最终回答，recovery 只重发 reply

增量 flush 规则：

1. 第一帧立即发送（TTFT 指标）
2. 之后 `delta_buffer.size() >= 48` 字节，或距上次 ≥ 50 ms
3. progress 先 flush 文本，再单独发 `progress:true`，`delta_index` 仍递增
4. delta 用异步 `Send`；最终 reply 用 `SendAndWait`

`delta_index` 从 0 递增。最终 envelope 的 `sequence` 必须等于 `delta_count`，不能和最后一条 delta 重复。

---

## 25. Agent envelope 与 Router 账本

`cannbot/scripts/agent_events.py`：

```text
route_key  = "rt_" + sha256(canonical_json(tenant,channel,conversation,thread))[:32]
session_key = "agent:{agent_id}:{channel_id}:{route_key 去掉 rt_}"
event_id    = "evt_" + sha256(request_id + "\0" + sequence)[:32]
```

slug：`[a-z][a-z0-9_-]{0,47}`。`request_id` ≤ 121 字节，禁止控制字符。`sequence ∈ [0, 2^31)`。

`AgentStore`（SQLite WAL，文件 0600）：

- `begin_turn`：`BEGIN IMMEDIATE`；同一 `request_id` 必须同一 `input_hash`，否则拒绝；已 completed 则回放，不再调模型；`running`/`failed` 拒绝重复执行
- `append_event`：`(request_id, sequence)` 主键 + `event_id` 唯一；payload 冲突抛错；终态或每 256 个序号修剪非终态事件，只留最近 4096
- `replay_events`：`sequence > after` 最多 4096，第 4097 条表示截断

contact 模式：session 必须是 `s_{lo}_{hi}` 且恰好一个已知 bot。sticky 模式：首次按 keyword 长度加权选 Agent，之后粘滞；`/agent` 只通过受控 action + revision 切换。

---

## 26. WebDemo：乐观消息、缺口、rAF

`web_demo/static/index.html`：

- `client_msg_id = Date.now() + '-' + random(36).slice(2,8)`
- `ws.bufferedAmount > 1MiB` 拒绝再发
- 渲染去重表 `renderedMessageRows` 上限 20000，同时索引 `msg_id` 和 `client_msg_id`
- 历史未就绪时实时帧进 `pendingLiveMessages`，完成后按 `msg_seq` 刷入
- 流式：`eventBuffer` Map + `seenEventIds`；缓冲超过 256 则放弃缺口（最终消息仍是权威）
- `requestAnimationFrame`（退化 `setTimeout(16)`）合并 `pendingText`；距底部 < 96px 才自动滚
- 重连：`wsGeneration` 丢弃旧 socket；延迟 `min(1000*2^n, 10000)`

Agent 卡片：只有发送方是受信 bot，且 `agent_command_result.client_msg_id` 匹配原请求时才更新。模型名按文本节点写入，不作为 HTML 执行。

---

## 27. Android：连接代数与 TreeMap 流式合并

`SparkClient.connect`：

```text
generation = connectionGeneration.incrementAndGet()
旧 socket.cancel()
回调仅当 generation 匹配且 socket === 当前 WebSocket 才生效
单帧上限 16 MiB
```

`StreamAccumulator`：

```text
pending        : TreeMap<Int, PendingStreamDelta>
text           : StringBuilder          // 只追加，不整串相加
nextDeltaIndex : Int?  从 0 开始
dirty / publishedText / publishedProgress
```

`acceptDelta(index, delta)`：

1. `index < 0`：无序号，立即 apply（progress 类）
2. 否则 `nextDeltaIndex` 初始化为 0
3. `index < expected` 或 key 已存在 → 丢弃（不回退）
4. `pending[index] = delta`
5. `while pending.remove(cursor) != null`: apply 并 `cursor++`
6. `nextDeltaIndex = cursor`

apply：progress 替换状态文本；正文 `StringBuilder.append`。
刷新：第一帧强制；之后 40 ms 一批。150 秒无事件把临时气泡标失败，但正式 `ai_reply` 到达仍替换预览。
事件线程：`Channel.UNLIMITED` + `Dispatchers.Default.limitedParallelism(1)`，避免每个 delta 一个协程抢 Compose 状态。

重连延迟 1800 ms。恢复后按每个 session 的 cursor 发 `sync`。断开时不把最终回答伪造为成功。

---

## 28. 长度审计

`common/text_metrics.h`：

- UTF-8 字节 = `size()`
- Unicode 字符 = 非 continuation 字节数（`(b & 0xC0) != 0x80`）
- 开关：`SPARK_PUSH_LENGTH_AUDIT=1|true|TRUE`，静态初始化一次

链路比较顺序：

```text
provider raw
  → Bridge chunk_count / provider_output_bytes
  → Logic ai_reply / db_before_write
  → MySQL db_after_read
  → Web/Android final_bytes / display_bytes
```

只修**第一个**下降点。12 KiB 是 prompt 输入预算，不是输出上限；Compose `maxLines=4` 只限制可见高度。

---

## 29. 房间在线计数

`ReportRoomJoin` / `Leave` 是两次独立 `INCRBY`，不是 Lua 事务：

1. `room:online_count:{room} ±1`（不钳到 0）
2. `room:comet_count:{room}:{comet} ±1`
3. join 后该 Comet 计数从 0 到 1 → `SADD room:comets`
4. leave 后 ≤0 → `SREM`（计数键不 DEL）

断线不自动 leave 时会偏高。这只影响 fanout 集合，不影响消息序号。

---

## 30. 把这些细节连回大框架

一次单聊成功，实际走过的小模块是：

1. Android/Web 生成 UUID `client_msg_id`，乐观插入
2. Comet SHA1 握手（仅首次）+ 帧解码
3. Logic 令牌桶 → 64 分片校准 → Redis Lua 取号
4. Kafka `SendAndWait` persist → `accepted_ack`
5. Kafka 异步 push_single
6. Job persist：INSERT + GREATEST；唯一键冲突则字段比对
7. Job PushStream：queue → pending map → Comet FIFO 去重窗口 → WS 明文帧
8. Comet MarkDelivered；Job 回传来源 `delivered_ack`
9. 客户端推进 cursor；发现缺口再 `sync`

Agent 路径在第 4 步之后分叉：`ai_request` → SSE 状态机 → 48B/50ms 合并 → TreeMap/rAF 预览；终态再从第 4 步重走普通单聊。
