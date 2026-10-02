# 02：可靠消息管道，从消息预留到设备确认

这篇文档沿着当前主线源码解释消息可靠性。
阅读目标是理解每个成功响应背后的持久化事实。
代码位置均指向仓库主线，不使用教学 subset 的旧实现。
测试数字应以当次性能报告为准，本文讲算法与验证方法。

## 1. 先区分五种不同的进度

“发送成功”不能只用一个布尔值表达。
本项目有五个相关但不同的事实：

| 状态 | 证明的事实 | 尚未证明的事实 |
| --- | --- | --- |
| reservation | SQL 已保留原消息与序号 | Kafka 尚未确认消息 |
| accepted ACK | persist Kafka delivery report 成功 | Job 尚未必写入历史 |
| history/outbox COMMIT | 历史与投递任务已同时持久化 | 客户端尚未必在线 |
| delivered ACK | 接收目标 Comet 接受连接发送队列 | 客户端尚未必保存 |
| device receipt | 指定设备已确认本地持久处理 | 用户尚未必阅读 |

已读状态还有独立的 `read_seq`。
读过一条消息是用户行为，不能从 TCP 发送成功推导。
客户端向服务器确认本地保存，也不等于已读。

协议定义见 [spark_push.proto](../../proto/spark_push.proto)。
Logic 接口见 [grpc_service.cpp](../../logic/grpc_service.cpp)。

## 2. 会话名称为什么必须稳定

单聊使用规范化名称 `s_<较小 UID>_<较大 UID>`。
例如用户 12 与用户 7 的会话始终是 `s_7_12`。
两个人从不同方向发消息，也会进入同一个序号空间。
聊天室使用 `r_<room_id>`。

名称会作为 SQL 查询条件、Kafka key 和客户端去重分组。
若发送方向决定不同名称，同一次对话就会分裂成两份历史。
稳定名称同时让客户端能用 `(session_id, msg_seq)` 定位消息。

单聊 SQL `session.type` 为 0。
聊天室 SQL `session.type` 为 2。
1 是普通群组类型，不能把聊天室写成 1。

对应实现见 [session_dao.cpp](../../logic/session_dao.cpp)。
新的原子持久化保持相同的类型与成员字段定义。

## 3. 为什么只在 Redis 中分配序号不够

考虑这个时间顺序：

1. Logic 在 Redis 中给消息分配序号 100。
2. Kafka 已确认 persist 事件，客户端拿到 accepted ACK。
3. Job 尚未把事件写入 MySQL。
4. Redis 重启，未恢复原消息缓存。
5. 客户端重试相同 `client_msg_id`。

只查询 MySQL 历史，仍然找不到这条消息。
如果再次在 Redis 分配，新序号可能与未落库事件碰撞。
即使分配到新序号，也会产生一条逻辑重复消息。
保留 Redis 去重键 24 小时只能缩小窗口，不能消除窗口。

因此，新消息分配的权威放在 SQL reservation ledger。
Redis 在新链路中主要服务在线路由和进度展示缓存。
消息预留不依赖 Redis 成功才能成立。

## 4. reservation ledger 的两张表

实现见 [message_reservation.h](../../logic/message_reservation.h) 与
[message_reservation.cpp](../../logic/message_reservation.cpp)。

`session_sequence_reservation` 保存每个会话已分配的最高序号。
它的主键是二进制 `session_id`。
一条会话对应一行，作为跨进程序号分配的锁定对象。

`message_reservation` 保存首次完整消息。
主键由 `session_id + sender_id + client_msg_id` 组成。
另外有 `(session_id, msg_seq)` 唯一键。
客户端 ID 使用 `VARBINARY(128)`，按字节区分。

为什么不能沿用不区分大小写的默认文本排序规则？
因为 Redis key 区分 `Case` 与 `case`。
SQL 若把两者当成同一个 ID，两个系统的幂等定义就会冲突。
同样要防止不同重音字符被某些文本排序规则视作相等。

payload 使用 `MEDIUMBLOB` 保存完整原 Message 的编码。
内容字段仍是原字符串，不是重新生成的一段用户文本。
首次时间戳、发送者、类型、内容与客户端 ID 全部保留。

## 5. Reserve 的事务步骤

`MessageReservation::Reserve` 使用同一条 MySQL 连接：

1. 检查会话、发送者、类型、客户端 ID 等输入范围。
2. `START TRANSACTION`。
3. 插入序号行，存在时用 `GREATEST` 提升到传入 floor。
4. `SELECT highest_seq ... FOR UPDATE` 锁住会话行。
5. 查找相同发送者与客户端 ID 的原预留。
6. 命中时返回完整原 Message，不接受重试内容覆盖。
7. 未命中时分配 `highest_seq + 1`。
8. 更新最高序号并插入完整原消息。
9. `COMMIT`。
10. 返回预留结果，之后才由 Logic 写 Kafka。

真实核心 SQL：

```sql
INSERT INTO session_sequence_reservation(session_id, highest_seq)
VALUES (?, ?)
ON DUPLICATE KEY UPDATE
highest_seq=GREATEST(highest_seq, VALUES(highest_seq));

SELECT highest_seq
FROM session_sequence_reservation
WHERE session_id=? FOR UPDATE;
```

插入与 `FOR UPDATE` 都服务于同一个会话序号锁。
不同 Logic 进程对同一会话调用，也会被 MySQL 序列化。
不同会话使用不同行，可以并行提交。

重试内容可以与原请求不同，但返回值仍是首次原消息。
幂等的对象是请求 ID 对应的原结果，不能只返回原序号。
否则重试的新时间戳会导致历史不可变字段比较失败。

## 6. 空 ID、失败与分配洞

空 `client_msg_id` 是旧客户端兼容路径。
它每次分配一个新序号，不做请求幂等。
新客户端发送队列必须生成并重复使用稳定 ID。

预留成功后 Kafka 发送失败，序号可能暂时没有历史消息。
如果请求永远不再重试，这个数值洞可能一直存在。
服务器不能据“目前查不到消息”宣布该序号永久不存在。
这也是设备确认需要 sparse receipt 的原因。

事务中任何 SQL 或编码失败都回滚。
数据库连接归还连接池前，未完成的事务必须结束。
事务 guard 在未提交时执行 `ROLLBACK`。

SQL COMMIT 响应丢失也是不确定结果。
后续相同 ID 重试，会从 ledger 得到已提交的原消息。
客户端不应根据一次网络超时换一个新 ID。

## 7. 升级前历史与旧 Kafka 积压

`ConversationStore::AppendMessageHotPath` 先查询旧历史的客户端 ID。
这让升级前已落库消息继续返回原结果。
查询客户端 ID 使用二进制比较。

新预留的 floor 至少包含 MySQL 当前最大历史序号。
升级时还尝试读取旧 Redis 的最新分配进度。
这样可以避免新 ledger 从较小序号重新开始。

旧 Redis floor 只能表达“分配到哪里”。
它不能重建升级前尚未落库的每一个请求原文。
因此迁移时应先停止旧 Logic 的新流量，并排空旧 persist 积压。
再启用新的 ledger 分配链路。

对应接入见 [conversation_store.cpp](../../logic/conversation_store.cpp)。

## 8. accepted ACK 的真正边界

Logic 的 `PersistToTopic` 构造 `PersistMessageRequest`。
事件中包含完整消息、场景、会话成员及 ACK 来源。
还包含发送时确定的收件人 UID 列表。
单聊快照包含双方，支持发送者其他设备同步。
聊天室先验证发送者成员资格，再获取成员快照。

Kafka key 使用 `message.session_id`。
同会话事件会进入相同 key 的处理顺序。
Logic 使用 `KafkaProducer::SendAndWait` 等待 delivery report。
只有该等待成功，才能返回 accepted ACK。

单纯 `Send` 返回 true 只表示本地生产者接受入队。
它不能替代 durable accepted 的边界。

## 9. Kafka delivery report 的对象所有权

实现见 [kafka_producer.cpp](../../common/kafka_producer.cpp)。
`SendAndWait` 创建 `shared_ptr<DeliveryState>`。
把 `state.get()` 作为 librdkafka 的 opaque 地址。
该地址不是交给回调自行 delete 的独占对象。

真正的所有权保存在生产者 `deliveries_` 表中：

```cpp
auto state = std::make_shared<DeliveryState>();
void* opaque = state.get();
deliveries_[opaque] = state;
```

回调按 `msg_opaque()` 查表，取出 shared_ptr，再删除表项。
回调随后设置 `done` 和 `ok` 并通知等待者。
等待者自身也持有 shared_ptr。
两条线程通过 state 的 mutex 与 condition_variable 协调。

等待超时不能立刻销毁仍可能收到 delivery report 的对象。
当前超时返回 false，表项由迟到回调清理。
生产者析构时停止 poll、尝试 flush，最后清理剩余表项。
因此迟到 callback 不会引用调用栈已经失效的对象。

超时也不证明 Kafka 没写入。
相同稳定 ID 重试依赖 reservation 与历史幂等消除业务重复。
Kafka producer idempotence 与业务幂等解决不同层次的问题。

## 10. Job 将一次事件交给 SQL

入口是 [JobRunner::HandlePersistMessage](../../job/service.cpp)。
格式错误、非法身份或不可变序号冲突走毒事件处理。
有效消息遭遇临时 MySQL 故障则持续退避重试。
不能在三次临时失败后永久跳过 accepted 消息。

停止 Job 时，先清除运行标志并唤醒等待。
然后停止 persist consumer，再停止 dispatcher 和推送流。
Kafka callback 的取消标志保证停止不是毒事件。
未完成事件既不提交消费位点，也不误写入 DLQ。

毒事件依然保存在 durable DLQ，供诊断与明确修复。
本项目没有声称自动修复非法内容或真实序号碰撞。

## 11. PersistAtomically：一个事务完成持久化交接

实现见 [DeliveryOutbox::PersistAtomically](../../logic/delivery_outbox.cpp)。
同一条连接上的步骤是：

1. 校验持久化事件的字段范围。
2. `START TRANSACTION`。
3. 插入或保留规范会话元数据。
4. 验证已存在会话的类型与成员字段。
5. 插入 outbox，重复时保留首次 payload。
6. 验证首次 outbox 的消息内容与本次消息完全一致。
7. 插入历史 message。
8. 重复键时逐字段验证历史不可变内容。
9. 用 `GREATEST` 更新会话 `last_msg_seq`。
10. `COMMIT`。

message 表不单独存储字符串 `msg_id`。
它可由 `session_id + '-' + msg_seq` 重建。
唯一键 `(session_id, msg_seq)` 保护序号身份。

真实消息重复比较包括发送者、类型、JSON 原文、时间戳与客户端 ID。
字符串以取回的字节内容比较，不用 SQL 文本排序规则判断相等。
不能使用“重复键就成功”来掩盖不同消息抢占相同序号。

任意一步失败都会回滚新 session、history 与 task。
不会出现历史已经提交但 outbox 没有提交的双写窗口。
Kafka callback 仅在这个事务完成后返回成功。

## 12. 为什么保留首次 outbox payload

客户端重试时，来源 Comet 可能已经换了节点。
因此新 persist 事件的 ACK 来源字段可能不同。
历史消息必须相同，首次投递任务信息仍保持原值。
重试不应把一个已经完成的 task 重新设置成待处理。

outbox 主键是 `msg_id`。
任务保存完整 persist 事件，而不是一个易失内存指针。
Job 重启后可从 SQL 重新读取完整投递计划。
旧事件没有收件人快照时，通过单聊双方或房间成员兼容推导。

## 13. Kafka batch 并行如何保持同会话顺序

实现见 [KafkaConsumer::Loop/ProcessBatch](../../common/kafka_consumer.cpp)。
Job 的 persist consumer 使用有界并行：

- worker 数使用 `delivery_workers`，限制在 1 到 32。
- 最多收集 64 条记录。
- 收集时间窗口是 5 毫秒。
- 同一 Kafka key 在一个 batch 中固定分配到同一 lane。

lane 不是固定的 `hash(key) % workers`。
算法首次遇到一个 key 时，找当前记录数最少的 lane。
将 key 到 lane 的映射记录在本 batch 的 `assigned` 表中。
后续相同 key 直接复用该 lane。
每条 lane 按原记录次序串行处理。

下一 batch 要等当前 batch 的 worker 全部 join 完成。
所以同 key 不会跨两个同时运行的 batch 乱序执行。
不同 key 可以并行执行自己的 SQL 事务。
并行事务可共享数据库的 group commit 效果。

## 14. 消费位点不能越过未完成记录

batch 的每条回调使用 `commit=false`。
worker 只报告业务处理是否已经 durable。
所有 lane 完成后，主消费者再检查 batch 的 durable 标志。

有未完成事件时，不提交这个 batch。
成功事件也可能随后重放，因此 SQL 幂等是必需条件。
只有整个 batch durable，才聚合并提交位点。
聚合键是 `(topic, partition)`，值是该分区本 batch 最大 `offset + 1`。
Kafka 保存的是下一条应消费的位置，不能提交最大 offset 本身。
一个 batch 使用一次 `commitSync(vector<TopicPartition*>)`。
这同时提交所有涉及分区，避免每条记录一次 broker 往返。
提交前再次检查运行标志与业务取消标志；停止中的 batch 留给重放。
整体返回错误或任何分区返回错误，都不能继续处理后续 batch。
位点提交失败会停止消费者，让替代实例重放。

这不是跨 Kafka 与 MySQL 的分布式事务。
COMMIT 成功但位点未提交时，重复消费是正常恢复路径。
幂等原消息比较与 outbox 主键使其安全。
不能把这种行为描述成网络层“恰好一次发送”。

## 15. dispatcher 与 ClaimReady 的候选查询

outbox 和 message 的 JOIN 是最后一道投递可见性门槛。
新链路两者已经同事务提交，JOIN 仍兼容旧任务。
没有历史行的任务不能被发送。

候选必须同时满足：

- `completed_at IS NULL`。
- `next_attempt_at <= NOW(3)`。
- 没有租约，或者租约已过期。
- 同会话不存在更早且已落历史的未完成任务。

候选按计划时间与创建时间排序，最多查询 64 个。
这只是候选列表，不是同时持有 64 个任务租约。
实际每次调用只 claim 一个任务，避免后排任务在本地队列过期。

同会话 head 判断使用 `NOT EXISTS`。
内部也 JOIN message，未落库的毒任务不会永久堵住有效后续消息。
这种旧兼容窗口中的迟到低序号由客户端缺口恢复处理。

## 16. 原子 claim 与 lease token

两个 dispatcher 可以同时读到相同候选。
读到候选不等于取得所有权。
真正的 claim 是有条件的 UPDATE：

```sql
UPDATE delivery_outbox
SET lease_token=?, lease_until=?, attempts=attempts+1
WHERE msg_id=?
  AND completed_at IS NULL
  AND next_attempt_at<=NOW(3)
  AND (lease_until IS NULL OR lease_until<=NOW(3));
```

只有 affected rows 为 1 的 worker 获得任务。
token 包含进程号、单调时钟值与本进程递增计数。
数据库中的 token 才是判断 owner 的依据。

Complete 与 Retry 都要求 token 相同且租约尚未过期。
旧 worker 即使继续运行，也不能修改新 owner 的状态。
租约 fencing 约束 SQL 结果，客户端仍要对重复下行去重。

## 17. 按时间续租，而不是每路由一次事务

默认 lease 至少 60 秒。
配置的 RPC 总预算较大时，lease 也相应提升。
单任务用 `steady_clock` 测量距上次续租的时间。
不到租期三分之一，不执行 SQL Renew。

查成员路由时每 64 个收件人检查一次时间。
每次发往 Comet 前也检查时间。
超过阈值后续租成功，重新记录时间。
常见单聊任务只有 Claim 与 Complete 两次 outbox 提交。

租约到期后更新不允许伪装成成功。
即使当前线程认为它仍是 worker，也必须由 SQL fencing 验证。
数据库时间用于租约，单调时钟仅控制本地检查频率。

## 18. 自动重试与离线完成

当前路线查询失败、未知 Comet 或推送失败都保留 SQL 任务。
失败延迟按 `100 * 2^(attempts-1)` 毫秒增长。
指数限制到 8，当前实际最大延迟为 25.6 秒。
外围另有 30 秒上限，当前参数不会触及该值。

下一次尝试重新查询当前 Redis 路由。
任务不绑定第一次发送时已经失效的 WebSocket 连接。
如果所有目标当前无有效路由，可以完成在线任务。
未来恢复交给每个设备的 SQL receipt 与历史同步。

“离线完成”表示没有继续在线扇出任务。
它不把任何设备的 received cursor 向前推进。

## 19. sender sync 与 delivered ACK

单聊收件人快照包含发送者本人。
这是为了把手机发出的消息同步到电脑。
但是发送者自己的连接不能证明接收者收到下行。

Job 每个 Comet 将目标分成两组。
接收者组保留 `ack_user_id/ack_comet_id`。
发送者组使用 `@sender_sync` request ID 后缀，清空 ACK 来源。
即使发送者与接收者在同一个 Comet，也分别请求。

接收者离线、发送者在线时：

1. 发送者拿到 accepted ACK。
2. 发送者其他设备收到自身消息。
3. 不产生接收目标的 delivered ACK。

客户端自身消息同步同样不能直接显示“接收者已投递”。
本地状态需要保持 accepted 与 delivered 的区别。
ACK 乱序时，还应避免较晚 accepted ACK 回退已知 delivered 状态。

## 20. 设备 prefix 与 sparse receipt

设备 ID 是安装或浏览器实例的稳定标识。
服务器从已鉴权连接上下文取得 UID 和 device ID。
客户端 ACK 不能自行声明另一用户的身份。
格式限制为 ASCII 字母、数字、点、下划线、连字符，最长 64 字节。

prefix 是从开始连续保存的序号前缀。
sparse receipt 表达前缀之后已经保存的离散消息。
一帧最多确认 256 个 sparse 序号。
Logic 检查会话成员授权及序号不超过数据库历史最大值。

用消息到达顺序 `1, 3, 2` 说明：

1. 保存 1：prefix=1，可确认 1。
2. 保存 3：prefix 仍为 1，sparse 中保存 3。
3. 服务器恢复查询可以排除已确认的 3，但仍会找 2。
4. 保存 2 后，客户端连续前缀变成 3。
5. 确认 prefix=3 后，服务端压缩不大于 3 的 sparse 行。

如果 2 是尚未发布的 reservation，不能直接把 prefix 改成 3。
数值洞不会阻止用 sparse 确认其他已保存消息。
晚到的 2 仍然能恢复，防止“取最大值”造成静默丢失。

设备 SQL 见 [user_session_state_dao.cpp](../../logic/user_session_state_dao.cpp)。
查询过滤见 [message_dao.cpp](../../logic/message_dao.cpp)。

## 21. 三类游标与旧客户端

`read_seq` 是用户阅读状态。
`delivered_seq` 是旧 server queue 语义，按用户与会话保存。
`device_session_state.received_seq` 是设备的连续持久接收前缀。
`device_receipt` 存储对应的 sparse receipt。

新 device 模式的离线恢复只向该连接发送。
Comet 排队成功不会写设备 received cursor。
客户端 SQLite 事务成功之后，才发送 received ACK。

旧连接没有 device ID，则走旧 delivered cursor 恢复路径。
兼容路径继续保留原语义，不伪装成设备级可靠确认。
AI delta 没有普通持久化消息身份，不写这些游标。
最终 AI reply 才经过 reservation、persist 与 outbox。

## 22. 路由代次、租约与注销

Redis 路由使用带截止时间的 ZSET。
epoch hash 保存 `Comet 启动代次:连接计数`。
同启动代次的旧计数不能覆盖新计数。
下线删除仅在 generation 与当前值相同时执行。

假设 gen1 关闭后异步 UserOffline 迟到。
新 gen2 已经鉴权时，gen1 的条件删除不会移除 gen2。
Comet 本地用户桶同样保留最大的连接 epoch。

心跳每 10 秒分批续租，一批最多 256 个用户。
租约有效期为 60 秒；代次与租约 key 有额外保留时间。
缺失注册不能仅凭心跳重新建立。
否则注销删除路由后，旧连接的心跳会复活旧路由。

当前缺失注册返回 409。
Comet 关闭仍符合本代次的连接，客户端重新走 VerifyToken。
有效 token 才能重新登记，已注销 token 被拒绝。
Redis 重启的恢复因此也经过鉴权。
如果 epoch 仍存在、只有 ZSET 租约过期，匹配 generation 的心跳可以恢复租约。
这与 epoch 注册已经删除的注销窗口不同。

实现见 [redis_store.cpp](../../logic/redis_store.cpp) 与
[comet_server.cpp](../../comet/comet_server.cpp)。

## 23. 如何验证故障窗口

| 场景 | 必须观察的结果 | 相关测试 |
| --- | --- | --- |
| pending 时 Redis 丢失 | 相同 ID 原结果、新 ID 不复用序号 | reservation integration |
| 同 ID 并发 | 只有一个首次预留、全部返回同消息 | reservation integration |
| 历史冲突 | SQL 全回滚、不暴露新 task | outbox integration |
| Job 临时 SQL 故障 | 未提交位点，恢复后继续 | E2E fault suite |
| Job 停止中断 callback | 无错误 DLQ，重启可重放 | Kafka integration/E2E |
| Comet 暂时不可达 | outbox 自动退避后恢复 | E2E fault suite |
| 两设备接收不同进度 | 每设备独立恢复 | receipt/E2E suite |
| 慢连接 | 受限缓冲、关闭、设备未确认可重放 | budget/E2E suite |
| sender 在线 receiver 离线 | 自身同步但无 delivered ACK | E2E ACK contract |

表中是故障窗口的验证合同，是否通过以该次测试报告为准。
实际故障注入限定在独立 lab 和本轮注册的会话，不能破坏其他实例的数据。
例如 `accepted_identity_survives_redis_cache_loss` 先暂停 lab Job，
再删除该会话的 allocator、last-seq、original、dedup Redis key。
它没有执行全局 `FLUSHALL`，也没有用重启共享 Redis 影响其他项目。

`comet_503_durable_automatic_retry` 启动临时本地 gRPC 代理。
只将 lab Job 的 Comet 目标指向代理，代理先返回 503，再恢复正常转发。
测试检查 retry metric、故障时没有下行、恢复时 legacy 接收者无需重连。
`finally` 停止代理并还原目标，避免测试故障留在后续性能运行中。

`job_outage_accepted_event_recovery` 与 `comet_restart_device_recovery`
只停止测试脚本拥有的 lab 进程，再由相同脚本启动。
MySQL 临时故障持续重试由代码与消费取消合同保证；
不能把上述代理故障测试冒充已经验证整个数据库断电恢复。

批量位点测试见 [kafka_parallel_integration_test.cpp](../../tests/kafka_parallel_integration_test.cpp)。
本地 librdkafka mock broker 接收真实协议请求，验证不同会话并行、同会话保序。
测试让第二次 OffsetCommit 返回拒绝，确认三条记录没有逐条提交。
另验证两个分区各自提交下一位点、取消零提交、提交拒绝后整批重放。
mock 测试不能替代真实 broker 集群的复制、磁盘和故障切换验证。

测试入口见 [run_im_checks.py](../../tests/run_im_checks.py)、
[im_e2e_performance.py](../../tests/im_e2e_performance.py)、
[delivery_outbox_integration_test.cpp](../../tests/delivery_outbox_integration_test.cpp)、
[message_reservation_integration_test.cpp](../../tests/message_reservation_integration_test.cpp)、
[device_receipt_integration_test.cpp](../../tests/device_receipt_integration_test.cpp)。

DAO 测试必须使用独立数据库，不能清空真实业务库来制造测试条件。
测试环境未明确启用时返回 77，并由 CTest 标记为 skipped。
Skipped 不等于行为已经通过验证。
真实性能报告同时保留环境、参数、样本与失败场景结果。

## 24. 索引、复杂度与 fsync 瓶颈

reservation 查找依赖三字段主键，序号依赖会话主键。
一次 Reserve 在一个会话行锁下做有界 SQL 往返。
同会话分配是串行的，这是序号一致性的成本。

history 按 `(session_id, msg_seq)` 索引分页。
device receipt 主键包含用户、设备、会话、序号。
恢复查询利用 prefix 与 sparse 过滤已确认消息。

outbox ready 索引包含完成状态与下一次尝试时间。
会话 head 检查索引包含会话、完成状态与序号。
Claim 一次最多扫描返回 64 个候选，但不能等同于固定 64 次成本。
数据分布、积压量与查询计划都会影响 SQL 工作量。

优化期间真实观察到多个小 autocommit 写入被 fsync 延迟限制。
仅增加 dispatcher worker 不能消除单 persist callback 的串行提交。
因此将 session、message、outbox 合并为一次事务。
再让独立 Kafka key 的 SQL 事务并行，为 group commit 提供机会。

正常任务取消每 route 强制 Renew，减少不必要的提交次数。
这些优化保持数据库持久化设置，不以关闭 fsync 换取漂亮吞吐。
最终吞吐仍应以同环境的新运行数据判断，不能用理论次数直接宣称容量。

## 25. 这条链路提供什么保证

Kafka 到 SQL、SQL 到 Comet 都允许重复尝试。
稳定请求 ID、原消息不可变比较、outbox owner fencing 与设备去重共同处理重复。
客户端需要先保存消息，再确认设备进度。
服务端需要先获得 durable 事实，再返回对应 ACK。

系统没有跨所有组件的全局“恰好一次”事务。
它提供可重放的持久交接和有身份的幂等恢复。
broker/数据库自身永久丢失、非法毒事件与部署迁移仍有明确边界。
阅读性能报告时，要把本地实测容量与这些可靠性合同一起理解。
