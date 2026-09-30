# RX 9070 XT / ROCm 7.2 原始实验记录

验证日期：2026-09-30。[完整报告](../../performance-report-2026-09-30-rx9070xt.md)。

- `*-cN.json`：每组 3 轮 × 12 个计时请求，另存预热；引擎真实 token、客户端耗时、输出摘要哈希。
- `*-manifest.json`：实际运行命令、配置、成功/失败、Worker 显存快照；应用组额外记录恢复检查。
- `*-metrics*.txt`：对应进程实验后 Prometheus 快照，包含预热/恢复，因此不直接作为计时吞吐分子。
- `summary.json` / `tables.md`：从完整原始请求重算，保留失败，不挑最快一轮。
- `environment.json` / `windows-hardware.json` / `rocminfo.txt`：环境、编译参数、模型 SHA-256 和设备识别。
- `runtime-evidence.txt`：GPU 卸载、KV buffer 等日志摘录；完整 `.log` 保存在本地并由 Git 忽略。
- `output-validation.json`：实际输出长度、跨配置输出哈希核对；不作为质量评测。
- `verification-ctest.txt`：20 项测试全部通过的原始记录，启用了真实 Redis 测试。
- `throughput.png` / `.svg`：三轮中位总吞吐，误差线为三轮 min/max，不是置信区间。
- `prefix-prompt.txt`：长前缀缓存实验输入。其负载不同于短 prompt 基础组。

`grpc_rocm_replicas2` 是真实启动失败：两个独立 HIP 运行时的第二个进程退出 -11。
没有对应成功吞吐。`grpc_rocm_shared2` 是两个 gRPC Worker 共享同一运行时、同一显卡，不能当作双 GPU。
`app_rocm_bridge1-startup-failure.*` 记录 Release 首次启动的 HSA 断言/-6；该组的成功三轮来自独立重试。
所有输入为人工 benchmark 文本；结果不含密码、用户登录 token 或模型 API key。

复算：

```bash
python inference/benchmarks/summarize.py docs/benchmarks/2026-09-30-rx9070xt --plot
```

需要 Python matplotlib 来生成图表；不加 `--plot` 仅重算 JSON/Markdown。
HTTP/gRPC 固定温度 0、seed 42、输出长度 128（ignore_eos）；应用自然停止、上限 128。
应用注册/连接/预热不计时。每轮记录 12 请求，总吞吐包含排队及传输。
逐请求 p50/p95 使用全组 36 样本，不能外推生产容量。
