# 2026-10-11 当前消息链路测试

代码基线 `5ade21e8fc52c6917c0ae8cff4d65a09a5530961`；本轮服务代码未改。
E2E 工具仅增加错误帧计数；新的 Python 标准库 harness 使用隔离依赖运行当前二进制。
结果是短负载快照，不代表生产容量。失败样本一并保留。

## 环境与口径

- WSL2 `6.18.40.1-microsoft-standard-WSL2`，12 logical CPU，MemTotal 16272868 KiB（约 15.52 GiB）。
- 客户端、Logic/Comet/Job、MySQL 8.0、Redis 7.2、Kafka 3.7 同机；其他主机负载未停。
- MySQL 为临时容器磁盘层，无 tmpfs；Kafka 单 broker、单 partition、RF=1，不验证多副本容灾。
- Logic HTTP 4 worker、Comet IO 8 worker/8 条 MessageStream，默认入队 ACK deadline 8 秒；AI 关闭。
- 每轮新账号/会话；不同性能项目串行运行，先用独立会话 E2E 预热，项目间短暂等待，不并行争抢 CPU。
- harness 只操作自身随机命名容器与卷；不加载个人凭证文件，私有配置/日志最后清除。

JSON 包含 UTC 时间、源码 HEAD、执行时 harness SHA-256、四个 benchmark 二进制 SHA-256、参数和原始输出。
成功与诊断运行的 harness 指纹不同：成功后增加了服务端计数抓取；服务代码和四个 benchmark 二进制未变。
执行时 benchmark 工具包含未提交的错误计数观测，所以不能仅凭 HEAD 推断工具内容。

## 功能回归

| 命令 | 实际结果 |
|---|---|
| `cmake --build build-wsl -j2` | 成功；E2E 增加观测后另重编该 target |
| `ctest --test-dir build-wsl --output-on-failure -j2` | 43 注册，40 通过、3 opt-in 跳过、0 失败；29.76 秒 |
| `python3 tests/run_mysql_review_tests.py build-wsl` | fresh Docker MySQL，message_reservation_integration_test 通过；0.19 秒 |
| `python3 tests/run_redis_review_tests.py build-wsl` | fresh Docker Redis，route_generation/redis_sequence 两项通过；0.15 秒 |
| `python3 tests/run_kafka_volume_test.py build-wsl` | 实际 broker 确认重放，替换容器后消息/已提交 offset 保留，通过 |
| Android 离线 Gradle | `:app:compileDebugKotlin :app:testDebugUnitTest` 成功；编译 UP-TO-DATE，单测 NO-SOURCE |

默认跳过的三项已由隔离 SQL/Redis harness 补跑。redis_sequence 检查兼容 Lua，不表示当前发送使用 Redis 取号。
CTest 覆盖 HTTP/WS 异常、流 deadline、客户端游标、Agent/Inference 合同；不把探针 mock 当真实 GPU。
结构化命令回执见 [validation.json](validation.json)。没有再次运行旧的无 RPC 凭证业务 E2E 脚本。

## 三轮成功负载

数据：[success-160/results.json](success-160/results.json)。每轮 E2E 为 8 个发送连接 × 20 条、独立接收者；另有新会话预热 20 条。
历史 8×50 查询、页长 50、160 条历史；建连 200/8 线程、持连 1 秒、无历史账号；Kafka 下推 1000 条。

| 项目 | 三轮吞吐范围 | 中位数 | 各轮 p99 中位数 |
|---|---:|---:|---:|
| E2E 唯一接收 | 30.9035～39.7826 msg/s | 33.5228 msg/s | 4723.21 ms |
| 历史查询 | 1153.88～1268.06 QPS | 1158.62 QPS | 11.6194 ms |
| 认证建连 | 1247.21～1651.43 conn/s | 1290.94 conn/s | — |
| Kafka→Job→Comet | 996.923～1212.31 msg/s | 1197.01 msg/s | 811.457 ms |

每轮 E2E sent/accepted ACK/delivered ACK/唯一接收均为 160，SQL COUNT 和 DISTINCT seq 均为 160。
历史每轮 400/400、建连 200/200、下推唯一接收 1000/1000。ACK 与最终 SQL 行另行核对，不能把其中一个代替其他证明。
p99 是每轮分位数的中位数，未合并原始样本。接收工具按 ID 去重，“无缺失”不证明网络帧从未重复。
Kafka 分层直接写 push_single，绕过 Logic 接受与 SQL 历史；建连不证明长期持连/历史补拉能力。

## 800 条突发失败

1. [首次失败](initial-burst-800.json)：800 sent、325 accepted、333 delivered ACK/唯一接收、475 ACK errors，工具退出 1。
2. [带指标复测](burst-800/results.json)：800 sent、330 accepted、337 delivered ACK/唯一接收、470 ACK errors；最终 SQL 行/唯一 seq=337。

复测服务端 Comet 计数显示 468 次 ACK timeout、7 次 read failed/reconnect，warmup 没有这些计数。
其他服务计数是包含 warmup 的累积值，各组件抓取非原子快照，不能要求与客户端瞬时计数完全相等。
WS 错误帧没有内部 code，工具将缺省字段记为 0；**不是成功错误码，也不能把 470 个错误全部分类为同一根因**。

复测 `measurements_completed=true` 表示四类测量完成；`ok=false`、进程退出 1 明确表示负载失败。
其余三类诊断测量成功不抵消 E2E 失败。失败样本 received/duration 不能用作有效容量 QPS。
首次 harness 的字段解析正则遗漏数字，p50/p95/p99 错写到 `_ms`；其 raw_result_lines 完整，首次分位数仅以原始行为准。
三轮成功与诊断复测已使用修复后的解析器。首次文件原样保留，不用修正后的新数据覆盖旧结果。

当前 Submit 会话锁覆盖认证、SQL Reserve/Prepare、Kafka SendAndWait/任务生成，ACK 从 Comet 入队开始计 8 秒。
代码路径和超时指标共同指向热点会话串行积压，尚未用分段 tracing 量化 SQL/锁/Kafka 各自耗时。
超时不证明未受理；部分请求可能已 prepare/入 Kafka，需同 ID 重试、核对原文/历史。没有验证这个失败负载的完整恢复。
下一步应测分段耗时、可靠异步确认/批处理与客户端发送窗口，不能只调大 deadline。

## 复现与边界

在仓库根目录构建后运行：

```bash
python3 tests/run_current_benchmarks.py --build build-wsl \
  --out /tmp/spark-bench-160 --repeats 3 --messages-per-conn 20
python3 tests/run_current_benchmarks.py --build build-wsl \
  --out /tmp/spark-bench-800 --repeats 1 --messages-per-conn 100 --allow-incomplete
```

三次运行均记录 cleanup_succeeded=true，最后检查没有遗留 spark-current-test 标记的容器。
没有重跑 GPU/设备、Android 真机、多机、真实 TLS 互通、磁盘损坏、黑洞网络、ASan/TSan 长压。
个人 Pi 安装同步检查仍有已知漂移，未覆盖个人配置。当前运行算法见 [项目手册](../../project-guide-and-resume.md)。
