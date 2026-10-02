# 2026-10-02 本地 IM 测试数据

实现合同见 [升级说明](../../docs/im-reliability-2026-10-02.md)，完整方法、负载差异和性能回退见 [性能报告](../../docs/performance-report-2026-10-02-im.md)。测试脚本为 [im_e2e_performance.py](../im_e2e_performance.py)，私有配置准备脚本为 [prepare_im_lab.py](../prepare_im_lab.py)。

| 数据目录 | 含义 |
|---|---|
| `results/final` | 最终二进制，8/32/128/256 发送连接，每连接 20 条、一个待 accepted ACK 请求；四档全部完成，共 8,480 条。JSON、summary CSV、逐条 latency CSV 与 environment/hash 记录 |
| `results/functional` | 最终 14 项故障/功能断言；相同 ID 重试明确等待本次请求的新 ACK |
| `results/baseline-steady` | 基础提交、相同最终窗口负载的四档比较 |
| `results/updated-steady` | 位点批量提交优化前的四档窗口负载 |
| `results/atomic-final` | 原子 SQL 持久化后的突发负载；128/256 档失败，保留错误与部分接收计数 |
| `results/pre-optimization` | 最早多 autocommit 版本的失败/低吞吐数据 |
| `results/baseline`、`results/updated` | 较早迭代的突发负载结果，不作为最终同负载比较 |
| `results/functional-pre-atomic` | 原子持久化前的历史故障验证快照 |
| `results/checks.txt` | 最终构建、知识包与 CTest 完整输出 |
| `results/agent-scheduler-validation.txt` | Agent router / 会话超时所有权回归测试输出 |

`final/report.json` 的最终功能入口指向 `functional/report.json`；其中历史未完成的旧辅助函数功能阶段不作为验收。以实际二进制 SHA 和状态字段识别不同版本，不能把所有目录拼成一次测试。

latency CSV 的接收延迟从发送前到接收帧解析；相同 ID 的重复接收只计一次。setup/注册排除，原始 accepted ACK 延迟单独列出。窗口模式不限制已接受后等待 Job 的积压，所以 256 档接收 p95 很高。原始记录保留失败和慢样本。

凭据、登录 token、私有 manifest、数据库 dumps、依赖缓存和服务进程日志不在本目录。上传的是本地 synthetic 工作负载的测试数据。Android 构建/SQLite 原始数据另见 [android-app/test-results](../../android-app/test-results/README.md)。
