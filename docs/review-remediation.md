# 工程审查修复与迁移

基线：`2f285f89146db4a7b9c65cdbc263c4a521973ee2`。对应工作区父目录的
`Project_Review_2026-10-11.md`，按正确性、并发、生命周期、安全、容量顺序实施。

实施边界：修改当前主线与必要的 Muduo 修复；不修改教学快照、生成代码、现有密钥文件，
不重启业务依赖或迁移正在运行的数据。保持 accepted/delivered 的两阶段含义。

## 顺序与验收

1. C01/C04 HTTP 分包与异常输入、C02 旧接口鉴权、C03 RPC 工作负载身份、C10 广播范围。
   验收：真实 HTTP/RPC 边界拒绝异常与越权请求，分包不会阻塞其他请求。
2. C05/I01 消息原文幂等、唯一序号、失败重试，C11 Kafka 卷，I03 离线分页与安全游标。
   验收：重复请求返回相同消息；缓存丢失、并行发送、Kafka 失败后重试不产生冲突或跳过消息。
3. C08/C09 连接所有权与异步鉴权、C07 连接池、I02 路由代次、C06 Ping/Pong、A01 退出。
   验收：慢鉴权不阻塞已有连接；数据库恢复后池自愈；旧下线不删除新路由；停止可收敛。
4. I04 队列与背压、I05 消费失败就绪状态、F01 房间顺序、A02 故障注入端口、F02 性能证据。
   验收：积压有上限，关键消费者失败能被探测，房间之间可并行。性能结论以实测为准。

## 部署兼容性

- `/api/message/send` 现在要求用户 Bearer；body `from_user_id` 必须等于认证用户。
  原响应字段保留，旧 HTTP 路径仍不宣称 WS accepted/delivered ACK。
- Logic 与 Comet 的内部 gRPC 使用 `internal_grpc_listen_addr`，默认 `127.0.0.1`。
  Comet 请求使用 `SPARK_RPC_COMET_KEY`，Job 推送使用 `SPARK_RPC_JOB_KEY`。
  内部 Broadcast 单独要求 `SPARK_RPC_ADMIN_KEY`；未配置时禁用该入口。
  `start_demo.sh` 通过 `init_rpc_keys.sh` 生成并载入不跟踪的 `.env.rpc`，绝不输出密钥。
  手动启动需先 `set -a; source .env.rpc; set +a`。生产环境只给每个工作负载所需的密钥。
- 远程内部 RPC 必须配置服务器 `SPARK_RPC_TLS_CERT`/`SPARK_RPC_TLS_KEY` 和客户端
  `SPARK_RPC_TLS_CA`，校验证书的服务名；未启用 TLS 时禁止非本机监听或连接。
  改动需要同时升级 Logic、Comet、Job。此前已打开的旧流在滚动升级前应排空。
- 房间广播携带 Logic 查询的成员 ID；旧的无成员 scoped task 会被拒绝，绝不扩大为全局广播。
- Kafka 日志现在固定在 `/var/lib/kafka/data` 的命名卷。现有容器旧日志**不会自动迁入**：
  部署前停止入口、排空并备份旧 broker 数据和 offsets，确认实际旧 `log.dirs`，再将整个
  KRaft 日志目录（含 metadata）迁入命名卷并核对 topic/offsets。不要直接 `compose down`。
  单 broker/RF=1 仍不提供机器故障容错；生产集群需独立配置复制与备份。

## 当前消息契约

`msg_seq` 的唯一权威改为 MySQL `message_sequence`。所有当前发送入口使用同一个事务，
以 `(session_id, sender_id, client_msg_id)` 找到原始消息；重试保持原时间戳、正文和序号，
复用 ID 修改正文会被拒绝。Redis 不再承担当前生产发送链的序号分配；旧 Lua API 保留给历史
benchmark，不能把该 benchmark 的吞吐当成当前消息接受吞吐。

发送状态为：`0 reserved → 1 outbox prepared → 2 Kafka published → 3 history persisted`。
原文与二进制持久化事件在 Kafka 发布前写入 SQL；每秒恢复最多 10 条 prepared 事件。
Job 重复处理时逐字段核对已有消息，阶段更新不会被晚到的 Kafka delivery report 降级。
仅预留但从未 prepare 的记录在 10 分钟后可封存为 `4 sealed`；封存后旧 ID 重试会失败，
客户端需使用新 ID。已结算记录在创建满一天后分批清理，因此不能声称无限期去重；
未完成持久化的记录不会因一天过期而删除。

- `accepted_ack` 仍只在 Kafka delivery report 成功后返回。SQL 预留成功不等于 accepted。
  发布确认失败/超时后，后台仍可能完成已 prepare 的事件；客户端用同一 ID 重试来消除不确定性。
- 实时 fanout 可用相同 request ID 重发，包括首次持久化发布失败后重试。它是至少一次尝试，
  下游按消息 ID 去重；实时队列失败可由离线/游标恢复覆盖。
- `delivered_ack` 表示目标连接的所属 IO 线程接受发送操作，不是客户端收讫或阅读。
  推送与游标补拉均在这个边界以后上报 receipt；断连/队列拒绝不会直接推进 MAX 游标。
- `message_delivery_receipt` 保存逐条投递证明。delivered 只能推进到“所有已持久化低序号已投递，
  或缺号已封存”的连续水位；高序号先送达、低序号尚未落 SQL 时不会跳过低序号。
- Sync 的 `next_seq` 仅用于取下一页，`safe_seq` 才是该页覆盖的已结算前缀证明。Web/Android
  只在当前连续游标覆盖请求起点后使用证明；SQL 暂时缺口保留并再次补拉。
- Offline 每页最多 200 条，Sync/Offline 同时受 2 MiB 估算字节预算约束，避免超过默认 gRPC 4 MiB 接收限制；携带会话与序号 continuation，遍历多个会话；不存在固定只补第一页的限制。
  单条存储消息超过页预算会显式失败，不推进游标，需用 HTTP history 获取。旧 HTTP 原文通过存储元数据补齐 WS envelope；history 使用 JSON 序列化，兼容旧文本与带引号的 client ID。
  新的更低消息在补拉时尚未落库，会由后续缺口补拉或再次离线扫描覆盖，不能宣称一次扫描就是快照。
- `ai_delta` 仍不落 SQL、不分配序号、不推进游标；最终 `ai_reply` 保持普通持久化链。

## 连接、退出与容量

Comet 鉴权移到有界工作池，单次 RPC deadline 3 秒，握手总等待 5 秒，头部最大 16 KiB；
握手后收到的首帧不会被误当成头部丢弃。Ping 返回相同负载的 Pong；控制帧校验 FIN/mask/RSV/
125 字节限制，并实现有截止时间的 Close。

TcpConnection 跨线程闭包持有连接和数据，状态读取使用 atomic，状态转换归所属 EventLoop。
Comet 和 HTTP 传输对象先销毁并等待 IO 线程，回调依赖之后释放；原 detached RPC 全部纳入
可等待工作池，channel/stub 在组件存活期间保持稳定。Logic/Comet 处理 SIGINT/SIGTERM，
先停止入口与 gRPC 等待，再等待后台恢复/消费任务；Comet 取消流后 join reader/writer。
流 context 的创建与取消用同一把锁，并再次检查退出状态。

MySQL/Redis 默认 Acquire 等待 3 秒，连接丢失后即使关闭自动扩容，也会补回最小连接数；
每 250 ms 重试，不依赖某个借出连接 Release。网络创建与健康检查移出池的全局锁。
Acquire 的等待期限不取消底层 connect/ping；总调用耗时还受原生连接/读写超时约束，
这里没有实现跨所有 SQL 语句的请求取消。

路由删除采用 generation + 随机 nonce 的 compare-delete：迟到下线只能删除自己注册的路由。
nonce 防止 Redis 计数器丢失后复用数字造成错误删除。这里没有实现故障 Comet 的 TTL 心跳租约，
也没有自动为 Redis 全量丢失前已在线的所有连接重新注册路由；运维仍需重建连接/路由。

默认容量（配置与运行环境应按实际内存预算收紧）：

| 资源 | 默认限制 / 拒绝行为 |
| --- | --- |
| Comet 连接 | `comet_max_connections=10000`；超限关闭 |
| WS 文本消息 | 64 KiB；超限关闭 |
| 每条 Comet→Logic 流 | 队列+等待回调 512 项，排队数据 8 MiB；超限返回错误 |
| 通用 ThreadPool | 排队 1024 项 / 按提交权重 32 MiB；返回 false |
| Comet 连接输出 | 4 MiB；在 loop 内检查容量，高水位关闭慢连接 |
| Kafka producer 队列 | 10000 条 / 32 MiB；发送失败向调用者返回 |
| HTTP | request line 8 KiB、headers 32 KiB、body 8 MiB、不完整请求 30 秒 |
| Logic HTTP 工作线程 | `logic_http_threads=4`；同步业务调用仍占用工作线程 |

流 ACK deadline 从入队时计时，独立 100 ms watchdog 清理超时并取消流，不依赖 writer 能继续
执行。房间提交锁改成 per-session：同一房间保序，其他房间可并行；最长等待锁 8 秒。
进程内锁不能宣称对多 Logic 实例提供全局 Kafka 入队顺序；跨实例序号仍由 SQL 事务保证唯一。
这些限制提供有界积压，并不保证默认 10000 个慢连接在任何机器上都符合内存预算。

## Readiness 与 DLQ 恢复

关键 consumer 的 DLQ 或 offset 确认失败时，Logic/Job 的 ready 归零并受控退出（非零状态），
由 supervisor 重启。Logic HTTP 和 Job metrics 服务提供 `/ready`，未就绪返回 HTTP 503；
`sparkctl health` 同时检查 ready 和流指标。确认写入 DLQ 后 consumer 可以继续：这仍然需要
对 `spark_push_kafka_dead_letters_total_<topic>`、consumer_failed、lag 和 SQL/outbox 恢复失败报警。
不能把 `/ready=200` 理解为 DLQ 为空、存储无积压或每条消息都已进入历史。

值班重放步骤：先处理数据库/broker/内容冲突原因，再从对应 `.dlq` 导出**一条 value JSON**到
权限为 0600 的临时文件。DLQ 包含二进制原文的 hex，属于敏感数据，勿贴到公开日志/工单。
不改变原始 offset、不删除原 DLQ；工具默认只验证，不联网、不输出正文或 key。

```bash
# 根据实际构建目录调整路径；显式目标必须与 DLQ 记录 topic 一致。
./build-wsl/tools/dlq_replay --file /private/record.json --topic persist_message
# 核对后由运维显式发布；超时可能已经被 broker 接受，重试同一记录。
./build-wsl/tools/dlq_replay --file /private/record.json --topic persist_message --apply --brokers 127.0.0.1:9092
# 带有 recovery 的 Agent 记录可选择恢复消息，目标必须匹配 recovery.topic。
./build-wsl/tools/dlq_replay --file /private/record.json --topic ai_reply --recovery
```

记录中 null key/payload 会被拒绝，避免悄悄改成空字符串。修复后核对历史行、reservation stage、
SQL 安全水位和 DLQ 计数。不要用清空 topic 或跳 offset 代替恢复。

## 存量升级顺序

1. 在旧版本停止新消息入口，排空 Kafka 的旧持久化/广播事件，备份 SQL 和 broker 数据。
   必须涵盖旧已取号、尚未落历史的事件；仅查询 `MAX(message.msg_seq)` 不足以支持混跑升级。
2. 按前述要求保存旧 Kafka 完整日志/metadata/offsets，迁入显式命名卷；核对实际配置路径和权限。
   本仓改动没有执行任何现有依赖的 down/up。
3. 在当前基线 schema 上执行 `sql/migrations/20261011_message_reservations.sql`。
   它创建三张表并将旧 delivered_seq 重置为 0，以重新扫描历史；read_seq 不重置。
   这是一次升级操作，重复执行会再次重扫历史，不能当成每次启动的自动迁移。
4. 配置每个工作负载所需的内部 RPC 密钥和远程 TLS；同步升级 Logic/Comet/Job，重新连接客户端。
   旧客户端不能把 next_seq 当安全水位，因此 Web/Android 也应升级。
5. 检查 readiness、consumer assignment/offsets、prepared outbox 清理、跨会话补拉和重复发送。
   迁移后再开放入口；先小流量验证，再测当前 SQL 预留链路容量。

Logic/Job 启动会检查新表，缺失时失败并给出迁移路径，不自动修改运行中数据库。
新建环境的 schema.sql 已含新表。回退旧二进制前再次停止写入、排空事件并备份新 SQL 序号，
不能直接让旧 Redis 分配器与新 SQL 分配器混跑。

## 对照审查项

| 审查项 | 本次落实 | 验证证据 |
| --- | --- | --- |
| C01/C04 | HTTP 分包推进、输入边界、JSON 异常响应 | parser 每个切分点、真实 HTTP 错误类型/半包请求 |
| C02/C03 | legacy Bearer/actor、工作负载 RPC 授权、loopback/TLS 配置 | 真实 HTTP 拒绝、真实 gRPC 匿名/错误角色/流拒绝 |
| C05/I01 | canonical 原文、SQL 统一取号、prepared outbox 恢复 | 真实 MySQL 并发、重试原文、两种写入口；发布失败后重试相同 protobuf |
| C06/C09 | Pong、异步鉴权、握手状态与 deadline | 真实 WS 控制帧、单 IO 线程慢鉴权、合并首帧 |
| C07 | pool 周期自愈、默认有限等待、锁外连接 | 实际 MySQL pool + 原生 connect/ping 故障包装；Redis 集成访问 |
| C08/A01 | owning callback、loop 检查、context 取消协调、受控退出 | 断连循环、流 ACK 超时、真实 Comet SIGTERM 收敛 |
| C10 | scoped broadcast 带权威成员、空范围 fail closed | Job 实际 fanout helper 的群内/空范围/全局测试 |
| C11 | Kafka 显式日志目录+命名卷 | 隔离 broker 删除重建后消息与已提交 offset 保留 |
| I02 | generation+nonce compare-delete | 50 次并发旧离线/新上线、计数器丢失后的 stale token |
| I03 | 多会话分页、逐条 receipt、safe_seq | 1003 条跨会话分页、真实 SQL 乱序/晚落库、实际 Web 游标函数 |
| I04 | 有界队列、字节权重、输出高水位、独立 watchdog | 线程池容量拒绝、真实 Comet ACK watchdog 取消 stalled 流 |
| I05 | failed → ready=0 → 非零退出；安全重放工具 | 已有真实 librdkafka mock 的 DLQ/offset 失败用例、HTTP 503、broker 重放确认 |
| F01/A02 | per-session 锁、预留与发布可替换端口 | 不同 room 锁独立；实际 Submit 注入首次发布失败 |
| F02 | 默认只写文件、同步轮转、IO 错误提示 | 并发轮转保留上限、可复现日志测量 |

## 验证回执与限制

源基线为上述 commit，验证候选为发布前的工作区变更；验证日期 2026-10-11。
后续发布提交保留该候选的源码指纹，提交记录不改变测试执行时的版本口径。
CMake 已重新编译受影响目标后执行。最终 CTest 注册 43 项：40 通过、3 项 opt-in 跳过；
这 3 项已在隔离 MySQL/Redis 中补跑通过。Kafka 真实 broker 重建与重放验证通过，knowledge
严格校验 70 个来源、citation 校验、diff/脚本语法检查通过。Android Kotlin 编译通过。
本地构建后的最终复核为 PASS WITH NOTES，按项目 Skill 在同一会话顺序切换 Reviewer/Tester，
没有声称派出独立子 Agent。源码指纹与命令结果见
[验证回执](benchmarks/2026-10-11-review/validation.json)。

SQL 持久预留增加了接受路径的存储往返，是正确性优先的选择；当前链路吞吐和尾延迟需重新压测。

```bash
cmake --build build-wsl -j2
ctest --test-dir build-wsl --output-on-failure -j2
python3 tests/run_mysql_review_tests.py build-wsl
python3 tests/run_redis_review_tests.py build-wsl
python3 tests/run_kafka_volume_test.py build-wsl
python3 cannbot/scripts/validate_knowledge.py --strict
python3 cannbot/scripts/test_pi_citations.py
# Android SDK 和依赖已缓存的环境
cd android-app
./gradlew --offline :app:compileDebugKotlin :app:testDebugUnitTest
```

容器 harness 只创建自己随机命名的临时容器/卷，结束后清理；没有操作现有业务依赖。
CI 新增 MySQL、路由与 broker 重建回归。Android 编译通过；unit test task 为 NO-SOURCE，
不能宣称存在 Android 行为单测或设备端全链路回归。

日志比较：WSL2，本地缓冲文件，单线程 10 万条、正文 128 字节、三次进程 wall time，
旧版 40.24/36.60/39.59 ms，新版 23.36/26.23/25.87 ms；stdout 重复写入从 18.2 MB 降至 0。
默认每文件 50 MiB，保留 5 个历史文件；`SPARK_LOG_CONSOLE=1` 可恢复控制台输出。
原生库 stderr 仍在 `.out`，`sparkctl logs` 同时读取 `.log` 与 `.out`。测量脚本为
`tests/benchmark_logging.py`，原始数据和源码 SHA-256 见 `docs/benchmarks/2026-10-11-review/logging.json`。
这只是日志成本测量，不是消息吞吐/生产 P99；同步磁盘 IO 仍可能阻塞线程，未盲目引入异步日志。
大房间路由批量化仍应在当前链路实测后决定。

本机已安装 Pi knowledge/settings 与仓库模板的漂移是审查前已有状态；
`sync_pi_agent.py --check` 会报告不一致，本次没有覆盖用户安装配置。
仍未执行多机故障、磁盘损坏、真实 TLS 证书互通、ASan/TSan 断连压力、Android 设备、真实 GPU
、Redis 真实停启/网络黑洞恢复和持续高并发/慢客户端压力实验；这些限制不因单元与隔离集成通过而消失。
