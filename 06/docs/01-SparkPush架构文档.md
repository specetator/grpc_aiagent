# Spark Push 架构文档

> 更新日期: 2026-02-07

## 一、服务架构概览

```
┌─────────────┐     HTTP API      ┌─────────────┐
│   Web/App   │ ─────────────────▶│    Logic    │
│   Client    │                   │   Server    │
└─────────────┘                   └──────┬──────┘
       │                                 │
       │ WebSocket                       │ Kafka (push_to_comet / broadcast_task)
       ▼                                 ▼
┌─────────────┐     gRPC          ┌─────────────┐
│    Comet    │◀──────────────────│     Job     │──── MySQL（异步落盘）
│   Server    │                   │   Server    │
└──────┬──────┘                   └─────────────┘
       │
       │ gRPC 双向流 / Unary
       ▼
┌─────────────┐
│    Logic    │
│   Server    │
└─────────────┘
```

**三层架构**（参考 B 站 goim 设计）：
- **Comet（接入层）**：管理 WebSocket 长连接、协议解析、gRPC 双向流转发
- **Logic（逻辑层）**：业务逻辑、鉴权、路由、消息序号分配、Kafka 投递
- **Job（任务层）**：Kafka 消费、gRPC 推送到 Comet、MySQL 异步持久化

**设计原则**：
- Logic 无状态：状态存储在 Redis，可水平扩展
- Comet 有状态 + Logic 做路由：Comet 维护本机连接，Logic 通过 Redis 维护全局路由
- Kafka 解耦：Logic 不直接调用 Comet，推送与持久化分离

## 二、端口配置

| 服务 | 端口 | 协议 | 用途 |
|------|------|------|------|
| Logic Server | 9100 | gRPC | Comet 调用 Logic 的 RPC 接口 |
| Logic Server | 9101 | HTTP | 前端 REST API（登录/注册/发消息/历史等） |
| Comet Server | 9200 | WebSocket | 客户端长连接 |
| Comet Server | 9205 | gRPC | Job 推送消息到 Comet（CometService） |
| Web Demo | 9080（单独启动默认是9001， 一键启动时设置的9080） | HTTP | 静态页面服务 |
| Kafka | 9092 | TCP | 消息队列 |
| Zookeeper | 2181 | TCP | Kafka 依赖 |
| MySQL | 3306 | TCP | 数据存储 |
| Redis | 6379 | TCP | 缓存/路由/会话序号/限流 |

## 三、消息推送链路

### 3.1 单聊消息完整链路

```
Client A ──WebSocket──▶ Comet
    ──gRPC双向流──▶ Logic（限流 → 构造session_id → Redis INCR msg_seq → 查路由 → Kafka投递）
    ──Kafka──▶ Job
       ├── PushHandler（消费组_push）──gRPC──▶ Comet ──WebSocket──▶ Client B
       └── PersistHandler（消费组_persist）──▶ MySQL（INSERT ON DUPLICATE KEY UPDATE）
```

### 3.2 HTTP 消息流程

```
Client ──HTTP──▶ Logic ──Kafka──▶ Job ──gRPC──▶ Comet ──WebSocket──▶ Client
```

### 3.3 Kafka Topic 与消费组设计

| Topic | 生产者 | 消费者 | 用途 |
|-------|--------|--------|------|
| push_to_comet | Logic | Job | 单聊/聊天室/弹幕消息推送 |
| broadcast_task | Logic | Job | 系统广播消息 |

同一 `push_to_comet` Topic 被两个独立消费组消费（Kafka 消费组隔离机制）：

| 消费组 | 处理器 | 职责 |
|--------|--------|------|
| `{group}_push` | PushHandler | 实时推送到 Comet（低延迟优先） |
| `{group}_persist` | PersistHandler | 异步写入 MySQL（吞吐优先） |

两个消费组各自维护 offset，互不影响。推送失败不影响持久化，持久化慢不影响推送。

### 3.4 session_id 构造规则

| 场景 | session_id 格式 | 说明 |
|------|----------------|------|
| 单聊 | `single:{min_uid}:{max_uid}` | 确保 A→B 和 B→A 使用同一会话 |
| 聊天室 | `room:{room_id}` | 按房间划分 |
| 弹幕 | `danmaku:{video_id}` | 按视频划分 |
| 广播 | `broadcast` | 全局 |

## 四、各组件详细设计

### 4.1 Comet（接入层）

**核心文件**：
| 文件 | 职责 |
|------|------|
| `comet_server.h/cpp` | WebSocket 服务端主类：连接管理、帧解析、gRPC 调用 |
| `comet_grpc_service.h/cpp` | gRPC 服务端（CometService）：接收 Job 下行推送 |
| `websocket_utils.h/cpp` | WebSocket 帧构建和 JSON 解析（手写实现 RFC 6455） |
| `app.h/cpp` | 启动入口 `RunComet()` |

**核心类 `CometServer`**：
```cpp
class CometServer {
  TcpServer server_;                    // Muduo TCP 服务器
  std::unordered_map<int64_t, std::set<TcpConnectionPtr>> user_conns_;  // 用户连接映射
  std::unordered_map<int64_t, std::set<int64_t>> room_users_;            // 本机房间成员
  ThreadPool grpc_pool_;                // gRPC 异步线程池

  // 多流支持
  bool use_stream_;                     // 是否启用双向流模式
  int stream_count_;                    // 流数量（默认4）
  std::vector<std::unique_ptr<StreamState>> streams_;   // 每个流独立的上下文
  std::unordered_map<std::string, callback> pending_callbacks_;  // request_id → 回调
};
```

**线程模型**：
- Muduo EventLoop 线程：处理 WebSocket 连接事件（IO 线程数可配置，默认 8）
- gRPC 线程池（4 线程）：异步执行 VerifyToken 等阻塞 RPC
- 双向流线程：每个流 2 个线程（WriterLoop + ReaderLoop），共 `stream_count × 2` 个线程

**连接上下文**：
```cpp
struct ConnContext {
  enum State { kHandshake, kOpen } state;
  int64_t user_id;
  std::string conn_id;     // 格式: comet_id:序号
  int64_t last_pong_ms;    // 心跳检测
};
```

**关键流程**：
1. **握手**：从 WebSocket URL 提取 token → gRPC 异步调用 `VerifyToken` → `runInLoop` 回调更新连接状态
2. **上行消息**：解析 JSON → 构造 `UpstreamMessageRequest` → 通过双向流发送到 Logic
3. **下行推送**：`PushToUsers()` 先拷贝连接集合再发送，缩小临界区
4. **心跳**：周期性 Ping/Pong 检测，超时断开

### 4.2 Logic（逻辑层）

**核心文件**：
| 文件 | 职责 |
|------|------|
| `grpc_service.h/cpp` | LogicService gRPC 实现（鉴权/上行消息/房间管理/双向流） |
| `http_server.h/cpp` | HTTP REST API 服务器 |
| `redis_store.h/cpp` | Redis 封装（token/路由/序号/限流/本地缓存） |
| `conversation_store.h/cpp` | 统一封装会话+消息+未读数据访问 |
| `*_dao.h/cpp` | 数据访问对象（6个 DAO） |

**gRPC 服务接口**：
| 接口 | 模式 | 调用方 | 用途 |
|------|------|--------|------|
| `VerifyToken` | Unary | Comet | Token 鉴权 + 路由注册 |
| `SendUpstreamMessage` | Unary | Comet（传统模式） | 上行消息处理 |
| `MessageStream` | 双向流 | Comet（高性能模式） | 持久连接消息通道 |
| `UserOffline` | Unary | Comet | 用户下线，清理路由 |
| `ReportRoomJoin/Leave` | Unary | Comet | 房间路由管理 |
| `Broadcast` | Unary | HTTP API | 系统广播 |

**HTTP API 端点**：
| 端点 | 方法 | 鉴权 | 用途 |
|------|------|------|------|
| `/api/login` | POST | 无 | 用户登录 |
| `/api/register` | POST | 无 | 用户注册 |
| `/api/message/send` | POST | Bearer Token | 发送消息 |
| `/api/session/history` | GET/POST | Bearer Token | 历史消息查询 |
| `/api/session/mark_read` | POST | Bearer Token | 标记已读 |
| `/api/session/unread` | GET/POST | Bearer Token | 未读数查询 |
| `/api/session/single_list` | GET/POST | Bearer Token | 单聊会话列表 |
| `/api/chatroom/list` | GET | Bearer Token | 聊天室列表 |
| `/api/chatroom/join` | POST | Bearer Token | 加入聊天室 |
| `/api/chatroom/leave` | POST | Bearer Token | 离开聊天室 |
| `/api/chatroom/unsubscribe` | POST | Bearer Token | 取消订阅聊天室 |
| `/api/chatroom/online_count` | GET | Bearer Token | 聊天室在线人数 |
| `/api/danmaku/send` | POST | Bearer Token | 发送弹幕 |
| `/api/danmaku/list` | GET | Bearer Token | 拉取视频弹幕 |
| `/api/admin/chatroom/create` | POST | 管理员 | 创建聊天室 |
| `/api/admin/chatroom/list` | GET | 管理员 | 聊天室管理列表 |
| `/api/admin/broadcast` | POST | 管理员 | 发送系统广播 |

**核心方法 `HandleUpstreamMessage` 流程**：
1. 滑动窗口限流检查（`CheckRateLimit`）
2. 本地构造 `session_id`（无 MySQL 查询）
3. Redis `INCR session_seq:{session_id}` 获取 `msg_seq`
4. 查询路由（本地缓存 → Redis HVALS）
5. 构造 `PushToCometRequest`，发送到 Kafka

**DAO 层**：
| DAO | 对应表 | 职责 |
|-----|--------|------|
| `UserDao` | `user` | 用户注册/登录/查询 |
| `SessionDao` | `session` | 会话创建/查询 |
| `MessageDao` | `message` | 消息写入/历史查询 |
| `UserSessionStateDao` | `user_session_state` | 已读游标管理 |
| `GroupDao` | `im_group` | 群组/聊天室管理 |
| `GroupMemberDao` | `group_member` | 成员管理 |
| `DanmakuDao` | `video_danmaku` | 弹幕数据访问 |

### 4.3 Job（任务层）

**核心文件**：
| 文件 | 职责 |
|------|------|
| `push_handler.h/cpp` | 推送处理器：Kafka 消费 → gRPC 推送到 Comet |
| `persist_handler.h/cpp` | 持久化处理器：Kafka 消费 → MySQL 写入 |
| `service.h/cpp` | JobRunner 组合入口 |

**PushHandler 推送模式**：

| 模式 | 配置 | 特点 |
|------|------|------|
| 传统模式（默认） | `use_push_stream=false` | 线程池 + 同步 Unary RPC，带重试，可靠性高 |
| 流模式 | `use_push_stream=true` | 每个 Comet 一条 gRPC 客户端流（`PushStream`），支持自动重连 |

**可靠性保障**：
- 重试机制：指数退避（100ms → 400ms → 1600ms），最多 3 次
- 死信处理：重试用尽后记录 `[DLQ]` 日志，便于离线排查和补偿
- Kafka 手动提交 offset（`enable_auto_commit=false`），at-least-once 语义
- MySQL 幂等写入：`INSERT ... ON DUPLICATE KEY UPDATE`（唯一键 `session_id + msg_seq`）

**PersistHandler 持久化**：
- 独立消费组（`{group}_persist`），异步写入 MySQL
- 线程池并发写入（8 线程）
- 支持 `need_persist` 字段控制是否落盘
- 特殊标记 `comet_id="persist_only"`：仅落盘不推送（离线用户场景）

### 4.4 Common（公共库）

| 文件 | 职责 |
|------|------|
| `thread_pool.h/cpp` | 固定大小线程池（生产者-消费者模型） |
| `redis_pool.h/cpp` | Redis 连接池（RAII Guard + 自动扩容 + 空闲回收） |
| `mysql_pool.h/cpp` | MySQL 连接池（RAII Guard + ping-on-borrow + 自动重连） |
| `kafka_producer.h/cpp` | Kafka 生产者封装（低延迟：`acks=1`, `linger.ms=0`） |
| `kafka_consumer.h/cpp` | Kafka 消费者封装（回调模式 + 手动 offset 提交） |
| `config.h/cpp` | 配置文件加载（key=value 格式） |
| `logging.h/cpp` | 日志系统 |
| `signal_handler.h` | 信号处理（SIGTERM/SIGINT 优雅关闭） |

**连接池设计**：
```
┌─────────────────────────────────────────────────────┐
│  连接池 (Redis / MySQL)                               │
│                                                      │
│  Acquire()  ──────▶  空闲队列有连接?                   │
│                          │ 是 → 弹出 (+ ping 健康检查) │
│                          │ 否 → 未达上限? → 创建新连接  │
│                          │      已达上限? → 超时等待     │
│                                                      │
│  RAII Guard  ──析构──▶  Release() 归还到空闲队列        │
│                                                      │
│  后台清理   ──────▶  idle_timeout_ms 超时回收空闲连接   │
└─────────────────────────────────────────────────────┘
```

**优雅关闭**：
```cpp
// common/signal_handler.h
InstallSignalHandler([&loop]() { loop->quit(); });
// 收到 SIGTERM/SIGINT → ShutdownRequested() = true → 自定义回调
// Comet/Logic/Job 三个服务均使用此机制
```

## 五、异步架构设计

### 5.1 Comet gRPC 异步化

Comet 中的 gRPC 调用（VerifyToken、SendUpstreamMessage）已改为异步模式：

```cpp
// comet_server.cpp - HandleHandshake
grpc_pool_.Submit([...]() {
  // 异步执行 VerifyToken gRPC（阻塞操作在线程池中执行）
  auto status = stub->VerifyToken(&ctx_rpc, vreq, &vrep);

  // 回调到 EventLoop 线程更新连接状态（线程安全）
  conn->getLoop()->runInLoop([...]() {
    conn->send(resp);
    conn->setContext(ctx);
  });
});
```

**关键点**：
- 使用 `ThreadPool` 提交阻塞的 gRPC 调用，避免阻塞 Muduo EventLoop
- 涉及 muduo 连接上下文的操作必须用 `runInLoop()` 调度回 EventLoop 线程
- `TcpConnection::send()` 本身线程安全，可直接调用

### 5.2 gRPC 双向流（Comet ↔ Logic）

**原理**：Comet 与 Logic 之间建立多条持久的双向流连接，消除每次 RPC 调用的开销

```
传统模式：每条消息 = 1 次 gRPC 调用（ClientContext + HEADERS + 请求-响应）
多流模式：N 个持久连接，消息以流的形式收发，通过 request_id 匹配回调
```

**架构**：
```
┌─────────────────────────────────────────────────────────────┐
│  Comet                                                       │
│  ┌────────────┐    ┌────────────────┐                        │
│  │ WebSocket  │───▶│  SendToStream  │──┐                     │
│  │ EventLoop  │    │ (轮询选择流)    │  │                     │
│  └────────────┘    └────────────────┘  │                     │
│                                        ▼                     │
│  ┌──────────────────────────────────────────────────────┐   │
│  │  Stream 0          Stream 1          Stream N-1       │   │
│  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐│   │
│  │  │WriterLoop    │  │WriterLoop    │  │WriterLoop    ││   │
│  │  │ReaderLoop    │  │ReaderLoop    │  │ReaderLoop    ││   │
│  │  │send_queue    │  │send_queue    │  │send_queue    ││   │
│  │  └──────────────┘  └──────────────┘  └──────────────┘│   │
│  └──────────────────────────────────────────────────────┘   │
│                              │                               │
│  pending_callbacks_ ◀────────┘ (共享 request_id → callback)  │
└─────────────────────────────────────────────────────────────┘
                              │ N 条双向流
                              ▼
                        ┌─────────────┐
                        │   Logic     │
                        │MessageStream│
                        └─────────────┘
```

**Proto 定义**：
```protobuf
message StreamMessage {
  string request_id = 1;  // 用于匹配请求和响应
  oneof payload {
    UpstreamMessageRequest upstream = 2;
    UserOfflineRequest offline = 3;
    RoomReportRequest room_join = 4;
    RoomReportRequest room_leave = 5;
  }
}

message StreamResponse {
  string request_id = 1;
  ErrorInfo error = 2;
  oneof payload {
    UpstreamMessageReply upstream_reply = 3;
    SimpleReply simple_reply = 4;
  }
}

service LogicService {
  rpc MessageStream(stream StreamMessage) returns (stream StreamResponse);
}
```

**多流配置**：
```ini
# comet.conf
use_grpc_stream=true    # 启用双向流模式
grpc_stream_count=4     # 并行流数量
```

### 5.3 gRPC 客户端流（Job → Comet）

Job 到 Comet 支持可选的客户端流模式：

```protobuf
service CometService {
  rpc PushToComet(PushToCometRequest) returns (PushToCometReply);       // Unary（默认）
  rpc PushStream(stream PushToCometRequest) returns (PushStreamReply);  // 客户端流
}
```

```ini
# job.conf
use_push_stream=false   # 默认关闭（unary 更可靠）
```

## 六、存储设计

### 6.1 Redis 数据结构

| Key 模式 | 类型 | 用途 | 操作 |
|---------|------|------|------|
| `token:{token}` | STRING | Token → user_id | SETEX（24h TTL） |
| `route:user:{uid}` | HASH | 用户路由 {conn_id → comet_id} | HSET/HDEL/HVALS |
| `session_seq:{session_id}` | STRING(INT) | 消息序号 | INCR 原子自增 |
| `room:comets:{room_id}` | SET | 房间所在 Comet 列表 | SADD/SREM/SMEMBERS |
| `room:online_count:{room_id}` | STRING(INT) | 房间在线人数 | INCRBY |
| `room:comet_count:{room_id}:{cid}` | STRING(INT) | 单 Comet 房间计数 | INCRBY |
| `rate_limit:{key}` | ZSET | 滑动窗口限流 | ZADD/ZRANGEBYSCORE/ZCARD |

### 6.2 MySQL 表设计

| 表名 | 用途 | 关键索引 |
|------|------|---------|
| `user` | 用户账号信息 | `uk_account` |
| `session` | 会话元信息（单聊/群/聊天室） | `uk_session_id` |
| `message` | 消息存储 | `uk_session_seq(session_id, msg_seq)` — 幂等保证 |
| `user_session_state` | 用户已读游标 | `uk_user_session(user_id, session_id)` |
| `im_group` | 群组/聊天室 | `idx_owner` |
| `group_member` | 群成员 | `uk_group_user(group_id, user_id)` |
| `video_danmaku` | 弹幕时间轴 | `idx_video_timeline(video_id, timeline_ms)` |
| `broadcast` | 广播任务记录 | `uk_task_id` |

### 6.3 存储访问模式

```
热路径（Logic）：
  1-2 次 Redis 操作 → 写 Kafka（无 MySQL 查询）
    ├─ Redis INCR：获取 msg_seq
    └─ Redis HVALS：查询用户路由（本地缓存命中时跳过）

冷路径（Job PersistHandler）：
  MySQL INSERT ... ON DUPLICATE KEY UPDATE（异步、幂等）

读路径（HTTP API）：
  历史消息 → MySQL 查询（按 session_id + msg_seq 排序）
  路由查询 → 本地缓存（TTL 1s）→ Redis
```

## 七、配置文件说明

### 7.1 comet.conf
```ini
listen_addr=0.0.0.0
listen_port=9200            # WebSocket 端口
comet_grpc_port=9205        # gRPC 端口（Job 调用）
logic_grpc_target=127.0.0.1:9100
comet_id=comet-1
comet_io_threads=8          # Muduo IO 线程数
comet_grpc_pool_size=4      # gRPC 异步线程池大小
use_grpc_stream=true        # 启用双向流模式
grpc_stream_count=4         # 并行双向流数量
```

### 7.2 logic.conf
```ini
listen_addr=0.0.0.0
listen_port=9100            # gRPC 端口
http_port=9101              # HTTP API 端口
kafka_brokers=127.0.0.1:9092
kafka_push_topic=push_to_comet
kafka_broadcast_topic=broadcast_task
redis_host=127.0.0.1
redis_port=6379
redis_password=
redis_db=0
redis_pool_size=16          # Redis 连接池大小
mysql_host=localhost
mysql_port=3306
mysql_user=root
mysql_password=123456
mysql_db=spark_push
mysql_pool_size=16          # MySQL 连接池大小
http_threads=4              # HTTP 服务器 IO 线程数
```

### 7.3 job.conf
```ini
kafka_brokers=127.0.0.1:9092
kafka_consumer_group=spark_push_group
kafka_push_topic=push_to_comet
kafka_broadcast_topic=broadcast_task
comet_targets=comet-1=127.0.0.1:9205
use_push_stream=false       # 推送模式：false=可靠 unary RPC, true=流模式
mysql_host=localhost
mysql_port=3306
mysql_user=root
mysql_password=123456
mysql_db=spark_push
mysql_pool_size=8           # 持久化连接池大小
```

## 八、可靠性保障

### 8.1 全链路消息不丢失

| 段 | 链路 | 保障措施 |
|----|------|---------|
| 1 | Client → Comet | TCP + WebSocket 协议保证 |
| 2 | Comet → Logic | gRPC HTTP/2 + request_id 匹配（双向流） |
| 3 | Logic → Kafka | `acks=1` + 大发送队列 |
| 4 | Kafka → Job | 手动 offset 提交（at-least-once） |
| 5 | Job → Comet | 3 次指数退避重试 + DLQ 日志 |
| 6 | Job → MySQL | 3 次重试 + 唯一键幂等写入 |
| 7 | 离线消息 | `comet_id="persist_only"` 确保持久化 |

### 8.2 消息去重

- **存储层**：MySQL 唯一键 `(session_id, msg_seq)` + `ON DUPLICATE KEY UPDATE`
- **客户端层**：通过 `msg_id` 和 `msg_seq` 去重

### 8.3 消息有序性

- `msg_seq` 由 Redis `INCR` 原子生成，保证会话内严格递增
- Kafka 按 `comet_id` 作为分区 Key，保证同一 Comet 的消息有序

### 8.4 限流

- 滑动窗口算法（`RedisStore::CheckRateLimit`）
- 基于 Redis ZSET，支持按用户/连接维度限流

## 九、性能优化

### 9.1 优化历程

| 阶段 | 单聊 QPS | 累计提升 | 关键优化 |
|------|---------|---------|---------|
| 基线 | 8,330 | — | 同步 gRPC 调用，每条消息查 Redis |
| + 本地缓存(mutex) | 14,281 | +71% | 用户路由本地缓存，TTL 1 秒 |
| + 读写锁(shared_mutex) | 16,656 | +100% | 读多写少场景优化 |
| + gRPC 双向流 | 33,322 | +300% | 持久流连接，消除 RPC 开销 |
| + 200 并发连接 | ~50,000 | +500% | 压测并发提升 |

### 9.2 关键优化点

**① MySQL 异步化**：Logic 热路径去掉 5-6 次 MySQL 查询，改为 Job 异步持久化

**② 本地路由缓存**：
```cpp
// redis_store.cpp - GetUserRoutes
bool RedisStore::GetUserRoutes(int64_t user_id, std::vector<std::string>* comets) {
  // 1. 先查本地缓存（读锁，多线程可并发读）
  {
    std::shared_lock<std::shared_mutex> lock(route_cache_mutex_);
    auto it = route_cache_.find(user_id);
    if (it != route_cache_.end() && it->second.expire_time > now) {
      *comets = it->second.comets;
      return true;
    }
  }
  // 2. 缓存 miss，查 Redis（HVALS route:user:{uid}）
  // 3. 更新缓存（写锁，TTL 1 秒）
}
```

**③ gRPC 多流**：`grpc_stream_count=4`，请求轮询分配到不同流，提升并行度

**④ 热路径日志移除**：减少文件 I/O 开销

**⑤ HTTP Server 多线程**：4 个 IO 线程并行处理 HTTP 请求

### 9.3 性能测试结果

**测试环境**：单机（Comet + Logic + Job + Redis + MySQL + Kafka）

| 场景 | QPS | 持久化 | 说明 |
|------|-----|--------|------|
| 单聊（双向流 + 200连接） | ~50,000 | 100% ✓ | 4 条双向流 |
| 单聊（双向流 + 50连接） | 33,322 | 100% ✓ | 基准测试 |
| 房间聊天（100用户） | 20,030 | 100% ✓ | 同房间广播 |
| HTTP API（4线程200连接） | ~5,710 | — | REST 接口 |

### 9.4 当前瓶颈与优化方向

**瓶颈**：Redis `INCR`（每条消息必须调用一次获取 `msg_seq`，单实例约 5-7 万/秒）

| 方案 | 预期效果 |
|------|---------|
| Redis 分片 | 按 session_id hash 到不同 Redis 实例 |
| 批量申请序号 | 一次申请 N 个序号，本地分配 |
| Logic 横向扩展 | 多 Logic 实例，Comet 负载均衡 |
| 多流连接 | 增加 `grpc_stream_count` 进一步提升吞吐 |

## 十、Proto 完整定义

### 10.1 核心消息
```protobuf
message ChatMessage {
  string msg_id = 1;          // 服务端唯一 ID
  string session_id = 2;      // 会话 ID
  int64 msg_seq = 3;          // 会话内严格递增序号
  int64 sender_id = 4;        // 发送方用户 ID
  int64 timestamp_ms = 5;     // 消息时间戳（毫秒）
  string msg_type = 6;        // 消息类型: text / system / danmaku
  string content_json = 7;    // 消息内容 JSON
  string client_msg_id = 8;   // 客户端 ID（ACK 匹配）
}
```

### 10.2 服务定义
```protobuf
service LogicService {
  rpc VerifyToken(VerifyTokenRequest) returns (VerifyTokenReply);
  rpc SendUpstreamMessage(UpstreamMessageRequest) returns (UpstreamMessageReply);
  rpc UserOffline(UserOfflineRequest) returns (SimpleReply);
  rpc ReportRoomJoin(RoomReportRequest) returns (SimpleReply);
  rpc ReportRoomLeave(RoomReportRequest) returns (SimpleReply);
  rpc Broadcast(BroadcastRequest) returns (BroadcastReply);
  rpc MessageStream(stream StreamMessage) returns (stream StreamResponse);
}

service CometService {
  rpc PushToComet(PushToCometRequest) returns (PushToCometReply);
  rpc PushStream(stream PushToCometRequest) returns (PushStreamReply);
}

service JobService {
  rpc EnqueuePushTask(PushToCometRequest) returns (SimpleReply);
  rpc EnqueueBroadcastTask(BroadcastTaskRequest) returns (SimpleReply);
}
```

## 十一、依赖服务

| 服务 | 路径 | 数据目录 |
|------|------|----------|
| Kafka | ~/3rd/kafka_2.13-3.7.0 | ~/kafka_data/kafka-logs |
| Zookeeper | ~/3rd/kafka_2.13-3.7.0 | ~/kafka_data/zookeeper |
| MySQL | 系统服务 | /var/lib/mysql |
| Redis | 系统服务 | /var/lib/redis |

**构建依赖**：
| 库 | 用途 |
|----|------|
| Muduo | TCP/HTTP 网络库（内置精简版） |
| gRPC + Protobuf | RPC 框架 + 序列化 |
| librdkafka | Kafka 客户端 |
| hiredis | Redis 客户端 |
| libmysqlclient | MySQL 客户端 |
| nlohmann/json | JSON 解析（Comet） |
| Boost | muduo 依赖（boost::any） |

## 十二、Web Demo 与测试

### 12.1 Web Demo 页面

| 页面 | URL | 功能 |
|------|-----|------|
| 主页（单聊） | `http://<IP>:9080/index.html` | 单聊消息收发 |
| 聊天室 | `http://<IP>:9080/chatroom.html` | 多人聊天室 |
| 弹幕 | `http://<IP>:9080/danmaku.html` | 视频弹幕演示 |
| 管理后台 | `http://<IP>:9080/admin_chatroom.html` | 创建聊天室/发送广播 |
| 测试页 | `http://<IP>:9080/test.html` | 功能测试 |

### 12.2 性能测试工具

`load_test/http_bench.cpp`：高性能 HTTP 压测工具
- 多线程 + 多连接 + HTTP 流水线（pipelining）
- epoll 事件驱动
- 支持 Bearer Token 鉴权
- 可配置连接数、每连接消息数、线程数、流水线深度

### 12.3 支持场景

| 场景 | 协议 | 说明 |
|------|------|------|
| 单聊 | WebSocket / HTTP | 一对一实时消息 |
| 聊天室 | WebSocket / HTTP | 多人房间消息 |
| 弹幕 | WebSocket / HTTP | 视频时间轴弹幕 |
| 系统广播 | HTTP → Kafka | 全量/分组广播 |
