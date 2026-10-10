# Qwen3-0.6B：RTX 3060 / CUDA 推理基础设施实验

验证日期：2026-10-07。被测代码是根目录 `inference/` 与可选 gRPC Bridge，
基于项目 `8579f0e` 加本次 CUDA 工作区改动，不是历史 subset。
结果只代表这台单卡开发机，不是生产容量，也不是跨 GPU 型号的公平比较。

## 1. 环境与本地模型复用

| 项目 | 配置 |
|---|---|
| 系统 | Ubuntu 22.04.5 / WSL2，内存约 15.5 GiB |
| CPU | Intel i5-12400F，6 核 12 线程；运行时固定 6 个推理/预填充线程 |
| GPU | NVIDIA GeForce RTX 3060，12 GiB，compute capability 8.6，编译 `sm_86` |
| 驱动 | Windows 577.00；WSL NVIDIA-SMI 575.64.04；支持 CUDA 12.9 |
| Toolkit | 用户目录 `/home/peco/tools/cuda-12.9`，nvcc 12.9.41，cuBLAS 12.9.0.13 |
| llama.cpp | `6c7a87f7e5e5cd75b8a641c3471f2dee84a6ed17`，与此前 ROCm 同一源码提交 |
| HF 本地缓存 | `~/.cache/huggingface/hub/models--Qwen--Qwen3-0.6B/snapshots/c1899de289a04d12100db370d81485cdf75e47ca` |
| 原始 safetensors SHA-256 | `f47f71177f32bcd101b7573ec9171e6a57f4f4d31148d38e382306f42996874b` |
| 本次 GGUF | `/home/peco/models/Qwen3-0.6B-local-Q8_0.gguf`，约 768 MiB 文件，量化张量约 761.80 MiB |
| GGUF SHA-256 | `e93ae7f33194c36ef20c13b9d4492a9888ddf9b1d9b0884f63d789ffcd43c51c` |

没有重新下载模型，也没有合并本地 LoRA adapter。仅使用已经缓存的基础模型，
离线 `safetensors → F16 GGUF → Q8_0 GGUF`。原始缓存保持不变。
本地权重同时含 `lm_head.weight` 和 `model.embed_tokens.weight`，已检查二者逐元素相同；
固定 converter 将二者均写出，GGUF 因此比 ROCm 实验约 610 MiB 的官方 GGUF 大。
本次 CPU/CUDA 使用同一份 GGUF；不能直接把 ROCm 的绝对吞吐与本次数据当作硬件 A/B。

`nvidia-smi` 显示的 CUDA 12.9 是驱动支持上限，不代表已经安装 nvcc。
初始 WSL 没有 toolkit，本次下载 NVIDIA 官方 redistributable 的 nvcc/cudart/CCCL/cuBLAS，
逐包 SHA-256 校验后安装至用户目录，补 `lib64 → lib` 链接，未安装 Linux GPU 驱动。
参见 [NVIDIA WSL 指南](https://docs.nvidia.com/cuda/archive/12.9.2/wsl-user-guide/index.html)
及 [CUDA 12.9 分发清单](https://developer.download.nvidia.com/compute/cuda/redist/redistrib_12.9.0.json)。

## 2. 开发内容与所有权

```text
WS → Comet → Logic → Kafka ai_request → hermes_bridge
                                         → gRPC Gateway → Worker → llama.cpp CUDA
                       ai_delta ← 增量文本（不落库、不推进游标）
                       ai_reply ← 最终回复 → 原有持久化/投递/历史链路
```

| 代码 | 本次实现 |
|---|---|
| `inference/scripts/setup_cuda_toolkit.sh` | 无 sudo、无驱动安装、组件校验与可恢复下载 |
| `inference/scripts/prepare_local_model.sh` | 本地 HF 缓存离线转换与量化，模型不入 Git |
| `inference/scripts/build_runtime.sh` | 固定引擎提交，显式 CUDA compiler/toolkit/架构；构建算子测试 |
| `inference/scripts/serve_runtime.sh` | `CUDA_VISIBLE_DEVICES`、显式 `CUDA0`、全层 offload、KV 精度开关 |
| `inference/worker/gpu_memory_probe.*`、`proto/inference.proto` | 动态 NVML 显存采样、PCI 设备匹配、Driver 回退及读数来源字段，无 SDK 链接依赖 |
| `inference/gateway/worker_registry.*`、`main.cpp` | 显式单 Gateway 闭环准入选项，避免心跳陈旧 busy 数阻塞新请求；默认模式不变 |
| `inference/benchmarks/run_matrix.py` | 复用 ROCm 消融矩阵，新增 CUDA 选择、模型哈希、禁止 CPU 静默回退 |
| `inference/benchmarks/run_app_matrix.py` | CUDA 完整 IM 测试及独立 Redis 端口 |
| `inference/tests/real_runtime_test.py` | 真 GPU 流式序列/usage/取消/deadline/Worker 掉线恢复 |

GPU 算子来自固定版本的 `ggml/src/ggml-cuda/`：矩阵乘、RMSNorm、RoPE、Flash Attention 等；
本次是算子后端的构建、接入、正确性验证和服务治理，不是手写 CUDA 内核。
没有把 HTTP、gRPC 或 Kafka 工作搬到 GPU，默认 Pi Agent 路径也没有替换。

显存观测先定位 CUDA 设备的 PCI bus ID，再动态加载 `libnvidia-ml.so.1`，执行
`nvmlInit_v2 → nvmlDeviceGetHandleByPciBusId_v2 → nvmlDeviceGetMemoryInfo`。
不能直接用 CUDA visible ordinal 查 NVML physical index：`CUDA_VISIBLE_DEVICES` 可重排设备。
NVML 不可用时回退 `cuMemGetInfo_v2`；保存线程 current context、切到 retained primary context、
采样后恢复，析构成对释放 context 和 NVML 初始化引用。两条路径均不链接 CUDA/NVML SDK。

这不是仅靠文档作出的选择：17:31 模型运行时，旧 Driver probe 仍显示约 **11247 MiB 空闲**，
而 `nvidia-smi` 显示设备约 **3984 MiB 已用**。这台 WSL 机器上 Driver 读数不能可靠反映其他进程。
修正后真实测试中 NVML 报空闲约 8787.5 MiB，紧随其后的 `nvidia-smi` 为 8788 MiB；总容量均为 12288 MiB。
`WorkerInfo` 增加向后兼容字段 17 `memory_probe_source`，区分 `nvml/cuda_driver/hip/unavailable`；
失败清零表示未知，不复用旧容量。设备计数受 Windows/驱动管理影响，不是模型独占或实时 KV 占用，
也不能当作下一次分配必成功的预算。参见 [NVIDIA NVML API 手册](https://docs.nvidia.com/deploy/pdf/NVML_API_Reference_Guide.pdf)。

## 3. 测试口径

服务矩阵沿用 ROCm：每配置预热 2 个请求，3 轮 × 12 请求；输出 128 tokens，
greedy、seed 42、关闭 thinking，基础对照关闭 prefix cache。长前缀实验输出 32 tokens。
统计吞吐取三轮中位数；TTFT/E2E 的百分位汇总 36 个请求，TPOT 不是 SSE chunk 间隔。
HTTP/gRPC 强制 ignore_eos；真实 IM 是自然结束、最多 128 tokens，因此分表解读。
编译或另一组负载同时运行的数据不能作为正式 CPU/GPU 对照。
本次固定 slot 上下文 2048、batch 512、ubatch 128、CPU 线程 6；混合输出为 32/64/128/256 tokens。
QPS 是完成请求数/整轮时间，token/s 是实际输出 token 数/整轮时间；不是单请求 decode 速度。
每配置只有 36 个样本，p99 是探索性尾延迟，不足以承诺生产 SLO；没有测到饱和点便不能称“最大容量”。

CUDA 启动显式 `--device CUDA0 -ngl 99`，要求日志出现 `offloaded 29/29 layers to GPU`；
否则配置失败。只写 `--device-type cuda` 不足以证明 GPU 执行。
实时模型测试与单元 fixture 分开保存；mock 只验证协议，不能证明 GPU 性能。

## 4. 实测结果

### 4.1 正确性与恢复

真实 CUDA gRPC 检查 **21/21 通过**，见 [完整记录](benchmarks/2026-10-07-rtx3060/real-cuda-test.json)：
全层 offload、整卡显存、request_id/sequence、唯一 terminal、精确 128 tokens、非空输出，
active/queued cancel、deadline、容量释放、满队列拒绝、5 秒 TTL 剔除、重新注册恢复。
首个日志预检因默认 verbosity=3 不输出 offload 证据而失败；加 `-lv 4` 后通过。
[失败预检](benchmarks/2026-10-07-rtx3060/real-cuda-verbosity-preflight.json)保留，
不能将日志不可见解释成一次模型正确性失败或 CPU 回退。

`test-backend-ops test -b CUDA0` 使用固定引擎的 CPU backend 数值对照：

| 算子选定用例 | 通过 | 不支持/跳过 | 数值失败 |
|---|---:|---:|---:|
| MUL_MAT，Q8_0/f16 | 119 | 0 | 0 |
| RMS_NORM，f32 | 11 | 0 | 0 |
| ROPE，head_dim=128/f32 | 109 | 10 | 0 |
| FLASH_ATTN_EXT，f16 KV | 7 | 0 | 0 |
| FLASH_ATTN_EXT，q8_0 KV | 6 | 0 | 0 |
| 合计 | 252 | 10 | 0 |

见 [参数与结果](benchmarks/2026-10-07-rtx3060/cuda-operators.json) 和同目录 `ops-*.txt`。
10 个 RoPE 跳过用例均为非连续 view + inplace 的组合；它们不能算通过，也不意味着所有 RoPE
变体已经支持。这是选定算子集合，不是全 GGML coverage，也不是模型语义质量评估。

GPU probe fixture 验证 NVML 成功、CUDA 初始化失败、双方采样失败清零、NVML 失败回退 Driver
及 current context 恢复；推理相关 CTest **7/7 通过**。它们与真实 GPU 测试分开，不把 fixture 算成 GPU 验证。
最终全项目 CTest **24/24 通过，44.02 秒**，包括 opt-in 的真实 Redis 序列集成测试；
见 [CTest 记录](benchmarks/2026-10-07-rtx3060/ctest-results.txt)，本次没有跳过该测试。

### 4.2 准入瓶颈定位与闭环模式

默认 gRPC CUDA / FA off 的 queue_wait 38 个样本（含预热）合计 27336 ms、最大 977 ms；
单请求模型 HTTP TTFT 约 20 ms，而并发 4 gRPC TTFT p50 约 1019 ms。
`WorkerRegistry::Snapshot` 使用 `max(心跳 running, 本地 active_gateway_requests)`；
释放本地 RPC 预留后，旧心跳的满载数仍可能阻塞准入直到下次 1 秒心跳。
证据是队列指标与代码，不应把这段等待笼统归因于 gRPC 序列化或 CUDA 内核。

新增 `--gateway-managed-only`：仅在单 Gateway 独占 Worker、所有生成都经 Gateway 时，
以本地 RPC 预留/释放计数作为准入真源；健康、5 秒 TTL、显存与 runtime 状态仍来自心跳。
Worker 独立容量检查不变。默认模式不变；共享 Worker、多 Gateway、直接调用 Worker 时不要开启。
单元测试覆盖“满载心跳 → RPC 完成 → 无需等新心跳即可准入”，以及健康/TTL 不被绕过。
真实 CUDA 生命周期测试在两种模式各 **21/21 通过**，闭环模式记录在
[managed/real-cuda-test.json](benchmarks/2026-10-07-rtx3060/managed/real-cuda-test.json)。

先保持 Driver probe 不变，只开闭环选项，FA off gRPC 吞吐从 255.73 提升到 470.16 token/s，
TTFT p50 从 1018.6 降至 59.2 ms；queue_wait（含预热 38 次）sum/max 均为 0 ms。
该阶段 [原始记录](benchmarks/2026-10-07-rtx3060/managed-driver-stage/tables.md)单独保留，
不与最终 NVML 版本混合选取“最好一轮”。NVML 修正后的完整复测见下表。

### 4.3 运行时 HTTP：计算能力与消融

| 配置 | 并发 / slots | 输出 token/s，中位数（min–max） | 完成 QPS | TTFT p50 / p95 ms | E2E p99 ms |
|---|---|---:|---:|---:|---:|
| CPU，FA off | 1 / 1 | 44.01（43.78–44.28） | 0.344 | 306.1 / 344.4 | 3008.3 |
| CUDA，FA off | 1 / 1 | 216.12（215.33–216.99） | 1.688 | 19.7 / 20.8 | 605.4 |
| CPU，FA on | 4 / 4 | 102.50（101.98–102.81） | 0.801 | 1053.2 / 1094.7 | 5143.0 |
| CUDA，FA off | 4 / 4 | 507.24（504.31–507.81） | 3.963 | 54.3 / 71.2 | 1034.5 |
| CUDA，FA on，f16 KV | 4 / 4 | **602.13（598.45–607.86）** | **4.704** | **52.7 / 61.8** | **866.6** |
| CUDA，FA on，Q8_0 KV | 4 / 4 | 559.81（554.93–561.92） | 4.374 | 57.8 / 78.5 | 931.0 |
| CUDA，FA off | 8 / 4 | 499.73（499.43–501.22） | 3.904 | 1058.8 / 1092.5 | 2079.5 |

每行 36/36 成功。以同机同模型为准：单请求 CUDA 是 CPU 的约 **4.91 倍**；
并发 4、FA on 为约 **5.87 倍**。FA on 相对 CUDA FA off 提升 **18.7%**。
四个 slot 增加到八个客户端并发没有增吞吐，反而引入约 1 秒首 token 等待。

| 独立消融 | 对照 → 开启 | 测量与解释 |
|---|---|---|
| Continuous batching，混合输出 | 248.95 → 323.05 token/s | +29.8%；TTFT p50 673.3 → 41.9 ms |
| 长前缀缓存，输出 32 tokens | 100.96 → 181.83 token/s | TTFT p50 162.6 → 20.1 ms；cached/prompt = 32979/33630 |
| f16 → Q8_0 KV，FA on | 896 → 476 MiB | 静态 KV 分配减少 46.9%，但吞吐降低约 7.0%；本卡优先 f16 |

上述实验输入/输出不同，不能与固定 128 tokens 行直接排排名；KV 数字来自 runtime 分配日志，
不是 `nvidia-smi` 整卡使用量。可核对 [启动证据](benchmarks/2026-10-07-rtx3060/runtime-evidence.json)。

### 4.4 gRPC 服务：默认模式与优化模式

均为并发 4、4 slots、128 tokens；优化模式只适用第 4.2 节的独占拓扑。

| 配置 | 模式 | token/s（min–max） | 完成 QPS | TTFT p50 / p95 / p99 ms | E2E p99 ms |
|---|---|---:|---:|---:|---:|
| CPU，FA on | 默认 | 101.67（95.29–102.72） | 0.794 | 1088.1 / 1612.7 / 2005.0 | 6005.8 |
| CPU，FA on | 闭环 + 最终版本 | 102.88（102.63–102.91） | 0.804 | 910.2 / 1083.5 / 1135.2 | 5096.8 |
| CUDA，FA off | 默认 | 255.73（253.76–358.69） | 1.998 | 1018.6 / 1053.1 / 1053.5 | 2034.3 |
| CUDA，FA off | 闭环 + NVML | 496.58（494.91–500.90） | 3.880 | 59.9 / 71.9 / 73.0 | 1047.1 |
| CUDA，FA on | 默认 | 491.05（479.72–541.23） | 3.836 | 66.1 / 593.7 / 601.6 | 1397.2 |
| CUDA，FA on | 闭环 + NVML | **591.61（590.47–598.44）** | **4.622** | **46.8 / 67.0 / 69.3** | **879.0** |

最终 CUDA FA on gRPC 吞吐约为 HTTP 的 98.3%；CPU/CUDA 闭环对照约 **5.75 倍**。
FA off 闭环对比默认吞吐 +94.2%，不能将它表述为 CUDA 算子本身快了 94.2%。
默认模式的独立双 runtime 为 255.27 token/s，共享单 runtime 的双 Worker 为 486.60 token/s；
这说明多 Worker 的拓扑影响，不证明单卡复制运行时能够线性扩容，也不是多 GPU 测试。

### 4.5 完整 IM 链路

从 WebSocket 发 AI 请求开始计时，到首个 `ai_delta` 得 TTFT，到收到持久化后的最终 Bot 消息得 E2E。
包含 Kafka/Bridge/Gateway/模型/持久化/Job/Comet；注册、建连、模型加载和预热不在吞吐计时内。
默认模式并发 4 的结果：

| 配置 | Bridge lanes / runtime slots | token/s | 完成 QPS | TTFT p50 / p95 ms | E2E p99 ms |
|---|---|---:|---:|---:|---:|
| CPU，FA off | 1 / 1 | 44.01 | 0.344 | 8940.1 / 9161.1 | 11841.4 |
| CUDA，FA off | 1 / 1 | 199.38 | 1.558 | 1944.0 / 2050.7 | 2661.5 |
| CUDA，FA off | 4 / 4 | 333.80 | 2.608 | 866.1 / 1187.9 | 2599.0 |
| CPU，FA on | 4 / 4 | 73.48 | 0.574 | 2071.8 / 4393.9 | 8866.5 |
| CUDA，FA on | 4 / 4 | 338.59 | 2.645 | 726.0 / 1379.5 | 2129.3 |

IM 与纯 gRPC 的输入 prompt 不同，不能直接将差额全部称为 RPC/消息中间件开销。
最终闭环模式（FA on、Bridge lanes=4、slots=4、并发 4）：

| 设备 | token/s（min–max） | 完成 QPS | TTFT p50 / p95 / p99 ms | E2E p50 / p99 ms |
|---|---:|---:|---:|---:|
| CPU | 80.18（76.88–82.74） | 0.626 | 1954.5 / 4308.6 / 5112.8 | 5790.5 / 8222.8 |
| CUDA | **384.01（382.51–389.02）** | **3.000** | **446.5 / 818.4 / 819.8** | **1231.3 / 1527.2** |

相对默认 CUDA FA on，吞吐 +13.4%，E2E p99 从 2129.3 降至 1527.2 ms；
最终同机 CPU/CUDA IM 吞吐比约 **4.79 倍**。本次所有 IM 负载样本实际输出均为 128 tokens。
各配置恢复检查均通过：收到增量后断连，历史恢复仅包含一条最终 Bot 回复；重放同一 client_msg_id
不多落一条消息。`ai_delta` 不持久化、不推进游标，`ai_reply` 沿原链路恢复；这是应用幂等，
不是 Kafka exactly-once，也没有实现断线后接续同一段模型生成。

完整基线 [tables.md](benchmarks/2026-10-07-rtx3060/tables.md)、最终闭环
[tables.md](benchmarks/2026-10-07-rtx3060/managed/tables.md)及同目录 JSON/manifest/metrics 为事实来源。
正式基线 27 配置、最终闭环 5 配置，共 **32 × 36 = 1152 请求，0 失败**；均取三轮中位数，
不计预热、恢复检查及 `managed-driver-stage/` 的 108 个中间复测请求。
概览图：[默认模式](benchmarks/2026-10-07-rtx3060/throughput.svg)、
[闭环模式](benchmarks/2026-10-07-rtx3060/managed/throughput.svg)。

## 5. 复现

```bash
# 引擎需事先 checkout 到表中提交；模型和引擎都在仓库外。
bash inference/scripts/setup_cuda_toolkit.sh
export CUDA_TOOLKIT_ROOT=/home/peco/tools/cuda-12.9
export LD_LIBRARY_PATH="$CUDA_TOOLKIT_ROOT/lib:$CUDA_TOOLKIT_ROOT/lib64:${LD_LIBRARY_PATH:-}"
bash inference/scripts/build_runtime.sh cpu
bash inference/scripts/build_runtime.sh cuda
# converter 使用 requirements/requirements-convert_hf_to_gguf.txt；bench 需要 Python 3.11+。
bash inference/scripts/prepare_local_model.sh
mkdir -p build-wsl/python-proto
/home/peco/tools/spark-cuda-venv/bin/python -m grpc_tools.protoc -I inference/proto \
  --python_out=build-wsl/python-proto --grpc_python_out=build-wsl/python-proto \
  inference/proto/inference.proto
/home/peco/tools/spark-cuda-venv/bin/python inference/benchmarks/run_matrix.py \
  --accelerator cuda --cpu-bin /home/peco/tools/llama.cpp/build-cpu/bin/llama-server \
  --model /home/peco/models/Qwen3-0.6B-local-Q8_0.gguf \
  --out docs/benchmarks/2026-10-07-rtx3060
# 闭环最终复测；前提是这些 Worker 没有其他生成调用者。
/home/peco/tools/spark-cuda-venv/bin/python inference/benchmarks/run_matrix.py \
  --accelerator cuda --cpu-bin /home/peco/tools/llama.cpp/build-cpu/bin/llama-server \
  --model /home/peco/models/Qwen3-0.6B-local-Q8_0.gguf --gateway-managed-only \
  --only grpc_cuda_p4 grpc_cpu_flash_f16 grpc_cuda_flash_f16 \
  --out docs/benchmarks/2026-10-07-rtx3060/managed
```

隔离依赖通过 `prepare_test_deps.py --secret-file /path/to/private.json` 启动，生成 0600 密码文件。
`run_app_matrix.py` 使用 `--accelerator cuda --mysql-port 3308 --mysql-user root --mysql-db spark_push
--redis-port 6381 --redis-db 11 --brokers 127.0.0.1:29092 --secret-file /path/to/private.json`。
测试结束使用 `prepare_test_deps.py --secret-file ... --stop` 停止容器，保留数据，不删除用户数据。

```bash
PYTHON=/home/peco/tools/spark-cuda-venv/bin/python
# 在已初始化的隔离依赖上跑 IM 基线，不能同时跑另一组性能测试。
"$PYTHON" inference/benchmarks/run_app_matrix.py --accelerator cuda \
  --cpu-bin /home/peco/tools/llama.cpp/build-cpu/bin/llama-server \
  --model /home/peco/models/Qwen3-0.6B-local-Q8_0.gguf \
  --mysql-port 3308 --mysql-user root --mysql-db spark_push --redis-port 6381 --redis-db 11 \
  --brokers 127.0.0.1:29092 --secret-file /home/peco/.cache/spark-push-cuda/benchmark-secret.json \
  --out docs/benchmarks/2026-10-07-rtx3060
# 完整 IM 闭环复测：同一命令加下列选项，并将 --out 改为 .../managed
# --gateway-managed-only --only app_cpu_flash_bridge4 app_cuda_flash_bridge4
"$PYTHON" inference/tests/real_runtime_test.py \
  --runtime /home/peco/tools/llama.cpp/build-cuda/bin/llama-server \
  --model /home/peco/models/Qwen3-0.6B-local-Q8_0.gguf \
  --out docs/benchmarks/2026-10-07-rtx3060
# 真实生命周期闭环测试：同一命令加 --gateway-managed-only，--out 改为 .../managed
SPARK_PUSH_RUN_REDIS_TESTS=1 SPARK_PUSH_REDIS_PORT=6381 \
  ctest --test-dir build-wsl --output-on-failure
"$PYTHON" inference/benchmarks/summarize.py docs/benchmarks/2026-10-07-rtx3060 \
  --plot --accelerator cuda
"$PYTHON" inference/benchmarks/summarize.py docs/benchmarks/2026-10-07-rtx3060/managed \
  --plot --accelerator cuda
```

`check_cuda_ops.py --help` 给出选定算子验证入口；同目录 `cuda-operators.json` 保存每个 filter、
执行命令与返回值。`environment.json` 保存平台/软件/模型信息，不保存环境变量或凭据。
日志忽略入 Git；`runtime-evidence.json` 保存白名单启动行与原日志 SHA-256，不保存完整会话。
基线在 Driver-only probe / 默认准入下采集，最终闭环组在 NVML 修正后采集；中间同 probe 的闭环
组用于独立定位准入瓶颈。其余模型、引擎、运行时参数不变，避免混淆代码阶段。

## 6. 结论边界

单卡/单机、短输出、小模型；不代表大模型、长上下文、生产并发容量。
KV Q8 改变量化缓存，不改变模型 Q8_0 权重；数值正确性与语义质量评测是两件事。
一次单卡多进程测试不等于多 GPU 水平扩展；共享 runtime 的两个 Worker 也不是两份 GPU 计算资源。
本次未验证 CANN、真实 vLLM、PagedAttention、RDMA、多机容错或 LoRA GPU 训练。
本机实验优先 `4 slots + FA on + f16 KV`；相同长前缀工作负载可开启 prefix cache。
gRPC 闭环选项必须满足独占拓扑。后续 IM 优化应单独测 Bridge/Kafka 排队与 lane 分布、再保持
同一模型和持久化语义复测；当前测量不能用来断言某个中间件就是唯一剩余瓶颈。
