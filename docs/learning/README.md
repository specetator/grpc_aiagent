# Spark Push 源码学习指南

这套文档以 **2026-10-02 根目录实际源码**为依据，讲解已经实现的系统、算法、数据结构、线程协作和故障恢复。建议打开源码对照阅读：每章都链接到具体文件，说明关键函数与不变量。历史 `02_auth_subset/` 到 `06/` 只是演进快照，不代表这次最终实现。

## 1. 推荐阅读顺序

| 顺序 | 章节 | 读完应能回答的问题 |
|---|---|---|
| 1 | [底层运行时、连接与数据结构](01-core-runtime.md) | Buffer 如何移动读写指针？EventLoop 怎样跨线程收任务？队列、定时器和连接池怎样加锁？64 个连接桶如何定位？ |
| 2 | [可靠消息与设备游标](02-reliable-message-pipeline.md) | 同一个 client_msg_id 为什么不会分配新序号？Kafka offset 何时能提交？Job 崩溃后谁接手？收到 1、3、2 时游标怎样变化？ |
| 3 | [Android 本地库与发送队列](03-android-local-store.md) | 四张表如何关联？消息落库与 ACK 谁先发生？如何避免 SQL OR 查询退化？重启后图片与发送队列怎么恢复？ |
| 4 | [AI、Agent 与推理调度](04-agent-and-inference.md) | SSE 怎样拼帧？delta 为什么不能推进游标？同会话任务怎样串行？推理 worker 怎么选、怎么取消？ |
| 5 | [成熟开源 IM 的可复用设计](05-open-source-designs.md) | 哪些设计适合本项目？为什么借鉴合同与分层，比直接替换协议更合适？ |

实施合同见 [本次可靠性升级说明](../im-reliability-2026-10-02.md)，实测数据和复现方法见 [本地性能报告](../performance-report-2026-10-02-im.md)。旧的 [技术细节文档](../spark-push-internals.md) 补充用户中心、命令系统等内容；涉及消息分配、设备接收和 Job 投递时优先看本套新文档。

## 2. 从一条消息看整个项目

```text
Android / Web
  │ WebSocket：token 认证、聊天消息、接收回执
  ▼
Comet ── gRPC ──► Logic ── Kafka persist topic ──► Job
  ▲                  │                             │
  │                  ├─ MySQL：身份预留、用户/群     ├─ MySQL：历史 + delivery task
  │                  └─ Redis：路由租约、缓存        └─ 当前路由查找、任务重试
  └────────────── Job → Comet → WebSocket ────────────┘
  │
  └─ Android SQLite 提交 → received_ack → Logic 设备游标

AI 分支：Logic → Hermes 请求 topic → bridge → Pi gateway / inference
                                      ↓
       临时 delta → 在线展示；最终 ai_reply → 普通可靠消息链路
```

`accepted_ack` 表示 Kafka 持久化事件得到 delivery report。它不代表消息已经被接收设备写入本地库。`delivered_ack` 表示目标 WebSocket 发送队列接纳了消息。设备 `received_ack` 才报告本地提交完成。用户已读状态另有自己的含义。这三个边界是阅读所有优化前必须记住的合同。

## 3. 源码目录与职责

| 目录 / 文件 | 实际职责 | 阅读入口 |
|---|---|---|
| `common/` | 配置、日志、MySQL/Redis 池、线程池、Kafka、限流、预算、指标 | [thread_pool.cpp](../../common/thread_pool.cpp)、[kafka_consumer.cpp](../../common/kafka_consumer.cpp) |
| `comet/` | TCP/WebSocket、认证、连接索引、流式 RPC、在线推送、设备补偿 | [comet_server.cpp](../../comet/comet_server.cpp) |
| `logic/` | 用户/群权限、消息分配、幂等、历史、回执、HTTP、AI 编排 | [grpc_service.cpp](../../logic/grpc_service.cpp)、[conversation_store.cpp](../../logic/conversation_store.cpp) |
| `job/` | Kafka 消费、SQL 原子落库、持久投递任务、重试与租约 | [service.cpp](../../job/service.cpp) |
| `proto/` | 组件共同使用的 protobuf/gRPC 协议 | [spark_push.proto](../../proto/spark_push.proto) |
| `sql/` | 服务端表结构、增量迁移 | [schema.sql](../../sql/schema.sql)、[migrations](../../sql/migrations) |
| `android-app/` | Compose 界面、Kotlin 网络层、SQLite、队列与流式文本 | [SparkMessageStore.kt](../../android-app/app/src/main/java/com/peco/sparkim/SparkMessageStore.kt) |
| `web_demo/` | Web 客户端、Agent 通道交互 | [agent_channel.js](../../web_demo/static/agent_channel.js) |
| `hermes_bridge/` | Kafka 到 HTTP/SSE 或推理 gRPC 的适配 | [main.cpp](../../hermes_bridge/main.cpp) |
| `cannbot/` | Pi gateway、Agent 会话、工具/知识包、Python 调度 | [pi_gateway.py](../../cannbot/scripts/pi_gateway.py) |
| `inference/` | C++ Gateway、Worker、调度、外部推理 runtime 适配 | [推理部署说明](../inference-serving.md) |
| `tests/` | 单元、真实依赖集成、端到端故障与性能测试 | [测试数据目录](../../tests/performance/results) |

HTTP 登录与 WebSocket 聊天是两个入口。用户中心通过 Logic HTTP 完成注册、密码验证、token 签发等；Comet 握手拿 token 调 Logic 验证，并登记带 generation 的在线路由。消息不得绕过 Logic 的身份和群成员检查。附件由 HTTP 上传，消息内携带受校验的引用，二进制图片不塞进 Kafka 消息正文。

## 4. 数据结构速查：按用途理解选择

| 使用点 | 实际结构 | 关键操作与代价 | 学习章节 |
|---|---|---|---|
| 网络 Buffer | `vector<char>` + 读写下标 | 尾部追加；不足时搬移或扩容，搬移 O(n) | 01 |
| EventLoop 跨线程任务 | `vector<Functor>` | mutex 下追加、交换；锁外批量执行 | 01 |
| 通用线程池 | `queue<Task>`，默认基于 deque | FIFO O(1)；条件变量等任务 | 01 |
| 空闲数据库连接 | deque，连接池计数与条件变量 | front 取最老连接，RAII 归还 | 01 |
| 定时器 | 按时间排序的 set + active set | 插入/删除 O(log n)，到期批量取出 | 01 |
| 在线用户连接 | 64 桶，每桶哈希映射和 mutex | uid 模 64；桶内平均 O(1) 找用户，fanout O(k) | 01 |
| 消息身份 | MySQL 复合主键 / 唯一索引 | TX 内锁 session 行；同身份返回首条不可变内容 | 02 |
| Kafka 消费 batch | vector + key 分组执行 | 同 key 顺序、跨 key 并行；耐久后按 partition 提交 | 02 |
| 投递任务 | SQL 表、状态、过期时间、ownership token | 条件 claim 与 token fencing，头部任务限制会话顺序 | 02 |
| 在线路由 | Redis ZSET + HASH + Lua | 到期分数删除、generation 条件更新 | 02 |
| 设备已接收 | 连续前缀 + SQL 稀疏 receipt 表 | 明确前缀单调，稀疏序号排除补偿重复 | 02 / 03 |
| Android 消息表 | SQLite 复合索引 | 分别按 server/client/seq 精确定位；避免宽 OR 扫描 | 03 |
| AI delta 重排 | Kotlin TreeMap | 插入 O(log n)，只消费 nextIndex 连续片段 | 03 |
| AI 上下文缓存 | unordered_map + 每会话 deque | 小窗口内去重；缓存整体淘汰不是 LRU | 04 |
| 推理等待 | deque + active 哈希映射 | FIFO 准入，取消/清理 O(n) 删除等待项 | 04 |

项目当前业务代码没有手写 `Node { prev, next }` 的双向链表。标准库 queue/deque、哈希表、树和 SQLite B-tree 的实现归标准库/数据库负责。文档会讲清楚本项目怎样调用、保护并约束这些结构；涉及底层库时说明边界，避免把库内部结构误写成这里的源码。

## 5. 三个练习，把阅读变成可验证理解

### 练习 A：追踪一个 client_msg_id

从 Android durable enqueue 开始，把 `client_msg_id`、临时本地消息、服务器 `msg_id` 和 `msg_seq` 的变化写下来。发送后断网、改变重试正文再发送同 ID，找出 SQL 预留身份为何返回原始正文。对应测试包括 message reservation DAO 和端到端 immutable retry。再说明为什么 SQL 预留提交成功、Kafka 失败时不能返回 accepted。

### 练习 B：画出 1、3、2 的游标变化

初始 prefix=0。提交 1 后 prefix=1；提交 3 后 prefix 仍为 1，同时记录 sparse=3；补到 2 后本地扫描得到 prefix=3。分别指出每一步 Android TX 改哪张表、网络 ACK 带什么、服务端 SQL 怎样避免把 2 隐藏掉。再考虑序号 2 永远只有预留身份、没有持久消息的情况：prefix 不能假装跨过它，稀疏回执仍可避免反复下载后续已落库消息。

### 练习 C：分析一次进程崩溃

列出 SQL message/task 同事务提交前后、RPC 发送前后、设备 SQLite 提交前后、received_ack 前后四类崩溃点。判断重启后由 Kafka、outbox 租约还是设备 sync 恢复，哪一步可能重复，重复由哪个唯一身份去重。不要用“exactly once”笼统替代这些具体边界。

## 6. 怎么读性能证据

先读报告里的测试边界、原始样本数和失败数，再看吞吐数字。端到端延迟是发送 socket 前到接收端完整帧解析；它不是服务端函数耗时。Android SQLite 的消息每秒数字仅代表 Robolectric 本地存储实验，不是手机端完整聊天吞吐。

旧版在线推送可能早于 MySQL 持久化，新版等待 SQL message/outbox 提交才投递，故不能把两者的 recipient latency 直接解释成相同可靠性下的性能进步。失败的突发压测会一并保留。源码优化、负载窗口变化和超时放宽必须在报告里分开写清楚。

## 7. 拓展阅读

- [用户中心、密码和审计](../user-center.md)
- [系统组件与附件边界](../spark-push-architecture.md)
- [Pi Agent 接入](../pi-agent-integration.md)
- [多 Agent 路由与事件合同](../multi-agent-integration.md)
- [Inference Gateway / Worker 部署](../inference-serving.md)
- [本次 schema 迁移 005](../../sql/migrations/005_device_delivery.sql) 与 [006](../../sql/migrations/006_message_reservation.sql)

每次修改后，以根目录当前源码为准同步文档，再重新生成知识包。测试数据只覆盖所记录的机器、版本与负载；它为理解实现提供证据，也保留尚未解决的容量边界。
