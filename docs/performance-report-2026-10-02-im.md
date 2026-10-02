# 2026-10-02 IM 可靠性、本地性能与验证报告

本次完成 SQL 自动投递恢复、设备接收游标、Android 本地库/发送队列和连接层容量约束。本地端到端最终窗口测试的 **8,480 条消息全部获得 accepted ACK，接收端唯一消息数完全一致，错误数为 0**；14 项故障/功能场景通过。Android APK 构建成功，8 项本地存储测试通过。

端到端性能存在明显回退：新版约 45–59 条/秒，旧版约 718–1,133 条/秒。新版的在线投递等待 SQL message/outbox 耐久提交，旧版可先在线推送再写 SQL；两者的可靠性边界不同。该结果不能称为相同可靠性下的吞吐提升，也不能作为生产容量证明。高突发导致 ACK 超时和测试窗口内未完全投递的失败数据保留在仓库。

## 1. 环境、隔离与数据边界

- Windows 主机中的 Ubuntu 24.04 / WSL2，Linux `6.18.33.2-microsoft-standard-WSL2`，AMD Ryzen 5 9600X，12 个逻辑 CPU，约 15.2 GiB WSL 总内存。
- C++ Release 构建；MySQL 8.0.46、Redis localhost、Kafka 3.7.0；Python 3.12.3。MySQL buffer pool 128 MiB，doublewrite 开启；[环境原始记录](../tests/performance/results/final/environment.json)。
- 实测保留 `innodb_flush_log_at_trx_commit=1`、`sync_binlog=1`，没有降低 SQL 耐久性换吞吐。
- Kafka 19092/19093，HTTP 19101，WebSocket 19000，Logic gRPC 19100，Comet gRPC 19105；Redis 测试 DB 14。
- 各轮使用独立 `spark_im_*_20261002` 测试数据库、topic 前缀、consumer group。测试只操作自己的 synthetic 用户、会话和 Redis 键。
- 私有 lab manifest 保存测试凭据与进程配置，位于源码仓外；上传的数据只含延迟、计数、测试断言和构建哈希。
- 中途 WSL 重启后重新准备隔离环境并重跑；最终结果均来自恢复后的最新二进制。

基础提交为 `8579f0e54ece87c9038c29cfcf7fa83cc45e957a`。最终二进制 SHA-256：

| 服务 | SHA-256 |
|---|---|
| Logic | `8a2d7ebc1a58574b39ca18bcfdb25a77376f605436b142dd1f540a862b7cb7f0` |
| Comet | `5ed44f2d67b8c3750bb2f5f05ec942435e97d84f4339c436c47a34b644bc81d9` |
| Job | `b1268b0e856c8c1b34d4290a53e372b5b6ef7afde3c310ce6188036254b35f72` |

完整记录见 [最终性能 JSON](../tests/performance/results/final/report.json)、[最终功能 JSON](../tests/performance/results/functional/report.json)。

## 2. 端到端负载与计时方法

每个发送连接使用不同账号，各发送 20 条消息，全部发给一个公共接收账号的一条接收连接。消息负载、发送连接数和计时边界在 baseline/new 两轮相同；顺序运行。注册、建立连接与一条接收确认的预热消息不计入计时。

计时从发送 socket 写入前，到接收端完整 WebSocket 帧解析。每个 client_msg_id 只计一次接收。accepted ACK 延迟另记，不将 HTTP 注册时间或服务端函数耗时混入 recipient latency。

最终采用 **每个发送连接最多一个未收到 accepted ACK 的请求**，完成窗口为 180 秒。它约束客户端向服务器注入负载的窗口，不约束已 accepted、尚未 delivered 的服务端 backlog。于是新版在 256 连接下虽然 ACK 延迟较低，接收 p95 仍高达 87.6 秒。整个测试不是单条消息往返 ping。

早期突发模式每连接连续提交 20 条，产生更大排队；它与最终窗口模式不混在同一个比较表。没有修改产品 RPC timeout 让突发测试表面通过。

## 3. 最终同负载结果

| 发送连接 | 消息数 | 旧版接收条/秒 | 新版接收条/秒 | 旧版接收 p95 ms | 新版接收 p95 ms | 新版错误 |
|---:|---:|---:|---:|---:|---:|---:|
| 8 | 160 | 718.187 | 59.037 | 31.714 | 1,622.627 | 0 |
| 32 | 640 | 897.824 | 48.508 | 185.912 | 10,149.194 | 0 |
| 128 | 2,560 | 1,036.238 | 45.866 | 467.606 | 43,320.562 | 0 |
| 256 | 5,120 | 1,132.856 | 45.083 | 591.806 | 87,624.923 | 0 |

双方四档全部满足 accepted 与 recipient unique 数等于 expected；新版共 8,480 条。256 档新版完成耗时 113.568 秒，accepted p50/p95 为 1,141.206 / 1,321.924 ms。接收延迟远大于 accepted，直接体现 Job 耐久投递积压。

原始证据：[旧版 JSON](../tests/performance/results/baseline-steady/report.json)、[新版 JSON](../tests/performance/results/final/report.json)、[新版原始样本目录](../tests/performance/results/final)、[旧版原始样本目录](../tests/performance/results/baseline-steady)。CSV 保存逐条 accepted/received 延迟，JSON 保存配置、计数、分位数和二进制哈希。

## 4. 优化过程与失败数据

### 4.1 修复了哪些实际开销

最初新增 outbox 路径把一条 persist 事件拆成多个 SQL autocommit，在本机 fsync 成本下只有约 14 条/秒。现在 `PersistAtomically` 将 session metadata、消息、delivery task 放入一笔事务；Kafka batch 按 key 保序、跨 key 有界并行执行。这减少提交次数，也建立了 SQL message/task 的原子可见性。

还修正了 Kafka 批处理末尾逐条 `commitSync` 的开销：全 batch 业务成功后，按 `(topic, partition)` 聚合 `max(offset)+1`，一次提交 offsets vector。取消或业务失败时不跨过未完成 batch。多分区 offset commit 本身不是全球事务；SQL 幂等承担部分 offset 提交后的安全重放。真实本地 librdkafka mock 网络测试覆盖提交失败、取消和分区进度。

这些优化后的本机结果仍约 45–59 条/秒；[批量位点优化前窗口数据](../tests/performance/results/updated-steady/report.json)与最终数据接近，不能声称 offset 合并解决了当前主瓶颈。

### 4.2 瓶颈证据与尚未实施的建议

MySQL performance schema 观测过 63,648 次 Claim UPDATE 对 10,614 次 Complete，约六次候选竞争 UPDATE 对一次完成；相关 UPDATE 平均约 19.92 ms，session ensure INSERT 平均约 25.66 ms。这个计数来自多轮共用测试进程的诊断窗口，**不是最终四档逐条采样指标**，只能说明候选争抢和 SQL 写入值得继续剖析。

下一步可以在保持 SQL token fencing 的前提下研究进程内候选避争、真正批量 claim 与减少无变化写入；不能简单用一个锁串行所有含 fsync 的 claim，也不能通过关闭持久化来掩盖问题。这些优化未在本次实现，报告不宣称已完成。

### 4.3 保留的突发失败

[原子落库后的突发实验](../tests/performance/results/atomic-final/report.json)中，128 档接收端最终收到 2,560 条，但只收到 1,696 个 accepted ACK，864 个客户端错误；256 档在 60 秒窗口内收到 3,006/5,120 条，3,400 个错误。产品 stream 请求有超时边界，高排队下客户端可收到错误，而先前持久接受的事件仍可能继续投递。因此重试必须保持 client_msg_id，错误不能被解读成“肯定没提交”。

[最早失败版本](../tests/performance/results/pre-optimization/report.json)、[中间批次](../tests/performance/results/updated)、[原子落库后的突发批次](../tests/performance/results/atomic-final)与[位点优化前窗口批次](../tests/performance/results/updated-steady)均保存。`final/report.json` 只把完成的最终四档作为性能验收，最终功能验收另指向已修正 ACK 等待辅助函数的 `functional/report.json`。

## 5. 14 项端到端功能 / 故障验证

| 场景 | 关键断言 |
|---|---|
| 两设备独立接收回执 | A 已确认消息不重放；未确认 B 仍收到补偿 |
| 新设备恢复历史 | 其他设备确认过的历史仍能在新设备恢复 |
| 稀疏回执与分配空洞 | 高序号 ACK 不隐藏随后持久化的低序号消息 |
| 多页恢复 | 205 条跨过 200 条分页边界，回执后继续下一页 |
| 相同 ID、不同正文重试 | 等待本次重试新 ACK，身份/正文仍是原始值 |
| 群成员与 fanout | 成员收到消息，非成员请求 403 且不收到 |
| Job 停机恢复 | accepted 事件在 Job 重启后自动投递 |
| Redis 缓存丢失 | Job 停机时删除自己会话缓存，再次发送同 ID 得原始身份 |
| 路由 generation fencing | 旧验证/旧断线不能覆盖或移除新版路由，租约可恢复 |
| Comet 503 自动重试 | durable task 保留，在线旧客户端无需重连也能收到 |
| 最后连接关闭竞态 | 20 次关闭/重连保留最新路由 |
| 慢读者背压 | 200 条 × 64 KiB 负载触发预算拒绝，连接容量受约束 |
| Comet 重启与设备补偿 | 未确认消息在进程重启后恢复 |
| AI 临时 / 最终合同 | synthetic Kafka delta 不写历史/receipt；最终 reply 只存一次并跨设备恢复 |

全部最终断言和耗时见 [功能测试报告](../tests/performance/results/functional/report.json)。AI 场景注入 synthetic Kafka 事件，没有调用外部模型；它验证 IM 分支合同，不验证模型生成质量或 GPU 吞吐。慢读者测试观测的是预算拒绝，不能由一次命中推断 RSS 的精确上限。

## 6. Android 构建与 SQLite 实测

命令 `./gradlew assembleDebug testDebugUnitTest --no-daemon --console=plain` 成功，8 项测试，0 failure/error/skipped。使用 SDK 36 编译、私有 JDK 17、Robolectric Android 28 native SQLite。没有真机 UI/网络测试。

| 同 1 KiB 负载存储实验 | 写入条/秒 | 说明 |
|---|---:|---|
| 原 alias OR 查询 | 约 973.4 | 可能退化成较大范围扫描 |
| 只加 msg_id 索引 | 约 932.5 | OR 谓词仍未解决查询计划问题 |
| 最终分别执行带索引等值查询 | 12,212.73 | 5,000 条，100 个事务，每事务 50 条 |

最终 transaction p50/p95 为 2.334 / 9.294 ms，读取最近 200 条平均 12.495 ms。这个结果是同机存储消融，不是 Android 手机的完整消息吞吐；首次较慢样本也保留，不删掉暖机成本。

APK 大小 12,195,955 字节，SHA-256 `0ae2bac2562fa582371f2f799e6c7f3876e197e1c557e7aa3966dd9c39c8c03a`。上传源码、构建日志、JUnit XML、原始 benchmark JSON 和源文件哈希；依赖缓存与生成 APK 不作为源码文件上传。

证据：[构建摘要](../android-app/test-results/android-build-summary.json)、[最终原始 benchmark](../android-app/test-results/android-sqlite-benchmark.json)、[JUnit](../android-app/test-results/android-final-junit.xml)、[测试资料说明](../android-app/test-results/README.md)。

## 7. 复现与必需检查

需要 C++ 编译依赖、MySQL、Redis、Kafka 与 Python grpc/protobuf。先查看脚本帮助并准备隔离配置；manifest 含测试密码，不得提交 Git。基础版与更新版路径由 prepare 脚本生成的 manifest 配置；原版二进制必须来自上述基础提交。

```bash
python3 tests/prepare_im_lab.py --help
python3 tests/run_im_checks.py \
  --build /home/peco/spark-im-test-20261002/build \
  --manifest /home/peco/spark-im-test-20261002/lab/manifest.json

python3 tests/im_e2e_performance.py \
  --manifest /path/to/private-manifest.json \
  --output tests/performance/results/reproduction \
  --variants baseline,updated --connections 8,32,128,256 \
  --messages-per-connection 20 --serial-accepted-ack \
  --completion-timeout 180 --skip-functional

python3 tests/im_e2e_performance.py \
  --manifest /path/to/private-functional-manifest.json \
  --output tests/performance/results/reproduction-functional \
  --variants updated --functional-only --ai-live
```

端到端显式 gRPC 场景要求当前 Python 环境装有 grpcio/protobuf。启用 synthetic AI 的 lab 配置需 `hermes_enabled=true`，而不启动外部模型。脚本退出码为失败时，应保存原始记录并检查 expected/accepted/received 三组计数，不将延长超时当成运行时优化。

必需检查含 Release build、严格知识源校验、citation 测试、临时 Pi home 的知识包 install/check、24 项 CTest；原始完整输出见 [checks.txt](../tests/performance/results/checks.txt)。真实 MySQL DAO 测试显式 opt-in 并限制独立测试数据库。Kafka 故障测试使用本地 librdkafka mock 协议服务，端到端消息路径使用真实 Kafka broker，两者覆盖范围不同。

此外 AI 学习文档审查发现同会话 waiter 超时会错误释放其他 owner。最小修复加入本次调用的 acquired/queued 标记，新增真实线程同步回归；完整 Agent router 测试共 33 项通过。原始结果见 [Agent scheduler 验证](../tests/performance/results/agent-scheduler-validation.txt)。

## 8. 已验证的边界

本机证明了所列恢复行为、故障断言、负载完成与 Android 本地事务算法。尚未证明真机 UI、跨机/跨地域网络、长期百万任务留存、大群生产负载或整站灾备。完成 delivery task 目前保留供重放幂等，生产归档/保留政策需单独确定。升级时先停止旧 Logic 接受并排空旧 Kafka persist backlog，再同时切换新 Logic/Job/Comet，避免把旧 Redis-only 身份当成新 SQL reservation 保证。
