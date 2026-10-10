# Logic 业务分层功能回归（2026-10-09）

对象：根目录当前 `logic/` 三层重构。环境：WSL2 Ubuntu 22.04，i5-12400F、RTX 3060 12GB。
这里记录**功能回归**，不是吞吐 A/B、最大并发或生产容量测试。

| 验证 | 最终结果 | 证据 |
|---|---|---|
| CMake 全量构建 | exit 0 | `cmake --build build-wsl -j2` |
| 独立业务 Service 单测 | 81 项检查通过 | `tests/logic_business_service_test.cpp` |
| 层边界门禁 | 通过 | `tests/logic_layer_contract_test.py` |
| 全项目 CTest | 26/26 通过，43.27 秒，无跳过 | `build-wsl/Testing/Temporary/LastTest.log`（本机） |
| 真实 HTTP/gRPC/WS | 88 项检查通过，12.17 秒 | `live/business-e2e.json` 和三组件日志 |
| 真实接口独立复跑 | 同样 88 项通过，16.65 秒 | `repeat/business-e2e.json` |
| 本地 Qwen/CUDA 完整 Agent 链路 | 预热 2 + 正式 4 请求无失败；断连/幂等检查通过 | `agent/*-manifest.json`、`*-c4.json`、日志与指标 |

业务单测用内存 Repository 调用真正的 Service，不是 HTTP mock。它能验证规则和调用副作用，
不能单独证明真实 SQL、Redis Lua 或 Kafka 的可靠性；因此另跑实际服务与 Redis 集成。
层边界是文本依赖门禁，不是 AST 或形式化架构证明。

实际链路覆盖：注册/登录、普通与管理员身份、在线单聊、accepted/delivered 序号一致、重放原
client_msg_id、MySQL 历史单行、HTTP 与 gRPC 对陌生会话一致拒绝、delivered/read 游标、图片
上传/读取/所有权/会话限制、房间订阅与在线报告、广播、旧弹幕、原文 ID、离线 Unary 受理后
WS 重连补推、旧 HTTP 文本、管理员昵称/撤 token/禁用/恢复/审计。
图片 fixture 验证项目当前魔数与字节链路，不代表完整图片解码安全测试。

Agent 测试使用已存在的 Qwen3-0.6B-local-Q8_0.gguf、CUDA llama.cpp、FA on、4 slots、Bridge
4 workers、Gateway 独占闭环；检查真实增量/最终 usage、最终消息、断开增量后历史一条最终回复，
以及同 client_msg_id 重放后历史仍为两条（用户输入 + 最终回复）。只做 1 轮 × 4 正式请求，
且与普通回归并行，本次小样本输出的 QPS/token/s 不用于性能比较。

## 隔离与复现

专用 compose 项目 spark-cuda-bench：MySQL 3308/spark_push、Redis 6381/DB11、Kafka 29092。
随机 topic/group 和随机测试账号；不清空用户业务数据库。private config/temp attachment 由
测试自动清理，依赖容器停机后保留专用数据库和消息数据，不清理用户容器。
密码在外部 0600 secret JSON；不提交或输出密码/token。运行前必须确认这些端口是专用测试环境。

```bash
python3 inference/benchmarks/prepare_test_deps.py \
  --secret-file /home/peco/.cache/spark-push-cuda/benchmark-secret.json
cmake --build build-wsl -j2
./build-wsl/tests/logic_business_service_test
SPARK_PUSH_RUN_REDIS_TESTS=1 SPARK_PUSH_REDIS_PORT=6381 \
  ctest --test-dir build-wsl --output-on-failure
/home/peco/tools/spark-cuda-venv/bin/python tests/logic_business_e2e.py \
  --secret-file /home/peco/.cache/spark-push-cuda/benchmark-secret.json \
  --out docs/benchmarks/2026-10-09-business-layer/live
LD_LIBRARY_PATH=/home/peco/tools/cuda-12.9/lib:/home/peco/tools/cuda-12.9/lib64 \
  /home/peco/tools/spark-cuda-venv/bin/python inference/benchmarks/run_app_matrix.py \
  --accelerator cuda --gateway-managed-only \
  --cuda-bin /home/peco/tools/llama.cpp/build-cuda/bin/llama-server \
  --cpu-bin /home/peco/tools/llama.cpp/build-cpu/bin/llama-server \
  --model /home/peco/models/Qwen3-0.6B-local-Q8_0.gguf \
  --mysql-port 3308 --mysql-user root --mysql-db spark_push \
  --redis-port 6381 --redis-db 11 \
  --secret-file /home/peco/.cache/spark-push-cuda/benchmark-secret.json \
  --requests 4 --repeats 1 --only app_cuda_flash_bridge4 \
  --out docs/benchmarks/2026-10-09-business-layer/agent
python3 inference/benchmarks/prepare_test_deps.py \
  --secret-file /home/peco/.cache/spark-push-cuda/benchmark-secret.json --stop
```

开发期发现并修正了测试本身的三个问题：附件 fixture 使用错误字段/ID 格式、管理员 HTTP
状态误设为 403/200（旧约定实际为 400）、ACK 后立即查询历史可能返回 404（Job 尚未建 session）。
最后一项改为等待明确的持久化结果并限时失败，不把 Kafka accepted 等同于 MySQL 完成。
最终结果文件为修正后的最终版本，不把早期测试失败解释为吞吐或产品可靠性结论。

未覆盖：生产高可用/跨机故障、全异常 SQL/Kafka 注入、大规模性能、多连接在线计数一致性、
完整安全迁移、模型语义质量。旧接口的鉴权及 JSON 边界见 `docs/logic-business-layer.md`。
