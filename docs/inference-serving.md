# Inference Serving: gRPC admission and ROCm runtime

## Scope and ownership

Current root source implementation, verified in WSL Ubuntu 24.04 on 2026-09-30.
Real CPU/HIP results are in the [RX 9070 XT report](performance-report-2026-09-30-rx9070xt.md).
Mock and HTTP fixtures are correctness tests, not GPU evidence.

```text
Client → Comet → Logic → ai_request → hermes_bridge
                                      ├─ default: Pi HTTP/SSE
                                      └─ optional: gRPC Gateway → Worker → model runtime
                           ai_delta ← streaming text
                           ai_reply ← completed text
Logic → persist_message / push_single → Job → Comet → Client
```

Spark Push owns authentication, sequencing, Kafka, persistence, delivery and
history. Inference stores no chat messages. `accepted_ack` confirms the durable
event reached Kafka; `delivered_ack` confirms downstream connection delivery.
Ephemeral `ai_delta` never advances a cursor. Final `ai_reply` uses the original
persistence path. Default Bridge remains Pi for tools, Skills, images and control
commands. Optional gRPC supports text generation; images are rejected and Pi
control commands still require the Pi gateway.

## Gateway, worker, cancellation

Workers register and heartbeat about once a second. Snapshots mark them unhealthy
after five seconds without heartbeat; workers re-register after Gateway restart.
Both servers expose gRPC health, but Gateway process health does not guarantee a
healthy model worker. Network calls never hold registry locks.

The scheduler excludes unhealthy, wrong-model and full workers, then scores
running/waiting requests and device memory pressure. Worker ID breaks ties.
Gateway reserves capacity under its admission lock before opening a worker RPC
and releases it on every return path. The worker independently enforces
`--max-concurrent` (default 4), including direct RPCs.

Gateway defaults: `--max-queue 64`, `--queue-timeout-ms 30000`. Full queues return
`RESOURCE_EXHAUSTED`, waiting deadlines return `DEADLINE_EXCEEDED`, missing
healthy workers return `UNAVAILABLE`. Duplicate active/queued IDs return
`ALREADY_EXISTS`. Global FIFO can cause head-of-line waiting between models.
Tune timeout against actual output lengths. Selection/reservation share a short
lock; worker RPCs run outside it. A session hint prefers the previous worker if
healthy, below capacity and no more than one running request above the selected
worker. Hints expire after ten minutes, bounded to 10,000 entries. Disable with
`--no-session-affinity`. This offers locality, not KV inspection or cache-hit guarantees.

Gateway validates chunk ID, sequence and terminal ordering. Queued/active Cancel,
upstream disconnect and deadline propagate downstream. Worker cancellation closes
model HTTP requests. Bridge shutdown/deadlines cancel gRPC; WS currently has no
user AI cancel event. Generation cannot resume. Model errors produce `ai_reply`
with `ok=false`, persisted as recoverable errors, not successful model answers.

HTTP/SSE backend accepts `--backend llamacpp|vllm`. Usage-only events after finish
are parsed before terminal emission. Terminal includes exact runtime tokens,
optional prefill/decode timing and selected worker. Each synchronous worker call
occupies one gRPC thread; independent requests execute concurrently. The vLLM
adapter suppresses llama.cpp-specific options and recognizes vLLM metric names.
It has contract fixture tests, but no real vLLM experiment in this report.

`temperature` uses an explicit oneof so protoc 3.12 / Ubuntu 22.04 can compile
presence semantics without experimental flags. Field 7 retains its fixed32 wire
encoding: unset uses the adapter default; explicit zero remains greedy sampling.

## Real runtime: CPU, ROCm, CUDA, CANN

Keep engine/model outside the repository. Build scripts print the engine commit
and use actual SDK backends. Match engine commit and GGUF across CPU/HIP tests.
Missing SDK guards fail: ROCm needs `/opt/rocm`, CUDA needs `nvcc`, CANN needs
`ASCEND_TOOLKIT_HOME`. `AMDGPU_TARGETS` defaults to `gfx1201` for RX 9070 XT.
Only CPU and ROCm/HIP were measured here; CUDA/CANN require actual devices.

```bash
cmake -S . -B build-wsl
cmake --build build-wsl -j2
LLAMA_SOURCE_DIR=/path/to/llama.cpp bash inference/scripts/build_runtime.sh rocm
# Other hardware: replace rocm with cpu, cuda or cann.

# Terminal 1
MODEL_PATH=/path/to/Qwen3-0.6B-Q8_0.gguf \
  LLAMA_SERVER=/path/to/llama.cpp/build-rocm/bin/llama-server \
  RUNTIME_SLOTS=4 PREFIX_CACHE=1 FLASH_ATTENTION=on \
  bash inference/scripts/serve_runtime.sh rocm
# Terminal 2
./build-wsl/inference/inference_gateway --listen 127.0.0.1:9300 --metrics-port 9301
# Terminal 3
./build-wsl/inference/inference_worker --worker-id hip-0 --listen 127.0.0.1:9400 \
  --gateway 127.0.0.1:9300 --model qwen3-0.6b --backend llamacpp \
  --model-endpoint http://127.0.0.1:8080 --max-concurrent 4 --device-type rocm
# Terminal 4
./build-wsl/inference/inference_cli --model qwen3-0.6b --max-tokens 128 \
  --prompt 'Explain batching.' --json
```

The official [Qwen3-0.6B-GGUF Q8_0](https://huggingface.co/Qwen/Qwen3-0.6B-GGUF)
SHA-256 is `9465e63a22add5354d9bb4b99e90117043c7124007664907259bd16d043bb031`.
Thinking is disabled in the template; remove that option if desired. Adapter
defaults to temperature 0.7; CLI uses 0, with `--session`, `--seed`, `--timeout-ms`,
`--no-cache-prompt`, `--ignore-eos`, `--cancel-after-chunks`, `--cancel-after-ms`.

Runtime defaults: four slots, 2048 context tokens/slot, batch 512, microbatch 128,
six CPU threads. Match worker capacity to slots. `CONTINUOUS_BATCHING=0`,
`PREFIX_CACHE=0`, `FLASH_ATTENTION=off` support ablations. Normal serving enables
prefix cache; base CPU/GPU experiments disable it. ROCm selects
`HIP_VISIBLE_DEVICES` (default 0); CUDA uses `CUDA_VISIBLE_DEVICES`.
SDK setup follows [AMD's WSL guide](https://rocm.docs.amd.com/projects/radeon-ryzen/en/docs-7.2/docs/install/installrad/wsl/install-radeon.html).

For Spark Push set `SPARK_PUSH_INFERENCE_BACKEND=grpc`,
`SPARK_PUSH_INFERENCE_GATEWAY=127.0.0.1:9300`,
`SPARK_PUSH_INFERENCE_MODEL=qwen3-0.6b`. Omit overrides for Pi.
A mock worker uses `--backend mock --model mock-model`, without a model download.

## Bridge concurrency and Kafka boundaries

Example config: `processing_workers=4`, `max_batch_records=16`. Overrides:
`SPARK_PUSH_BRIDGE_WORKERS`, `SPARK_PUSH_BRIDGE_MAX_BATCH_RECORDS`. Workers=1
restores serial processing. Shared consumers default to 1; other consumers keep
their existing behavior. Parallel mode forbids automatic commits.

A bounded batch is gathered over a 10 ms window. Logic's `ai_request` key is
`session_id`; equal keys stay serial on one lane, independent sessions may run
concurrently. All lanes join before offsets commit in original read order on the
consumer thread. Unconfirmed business/DLQ results halt and replay the batch;
later completions cannot skip earlier unfinished work. Polling pauses while a
batch executes. `max.poll.interval.ms` accounts for worst serial batch and retries.
Crashes/rebalances still require existing at-least-once dedup and durable replies;
there is no exactly-once guarantee.

Bridge knobs: `inference_max_tokens=128`, `inference_temperature_milli=700`,
`inference_cache_prompt=true`. Final chat `content.inference_usage` and
`content.inference_worker_id` contain allowlisted runtime counters/ID, shared by
online delivery and history. Arbitrary provider metadata is not persisted.

## Observability

Gateway Prometheus: `http://127.0.0.1:9301/metrics`. Includes request
total/active/failed/cancelled/rejected, waiting, queue timeouts, no-worker,
affinity selections, aggregate worker running/waiting and heartbeat age.
`inference_ttft_ms`, `inference_total_latency_ms`, `inference_queue_wait_ms`,
`inference_runtime_prefill_ms`, `inference_runtime_decode_ms` have cumulative
bucket/count/sum/max. TTFT starts at Gateway receipt. Exact engine usage appears
in `inference_{prompt,completion,cached_prompt}_tokens_total`.
`inference_chunk_interval_ms` measures transport chunks, **not token TPOT**.

Worker GetStatus probes `/health` and optional `/metrics` for engine queues/KV.
Actual `hipMemGetInfo` samples whole-device memory through an optional dynamic HIP
dependency, including other processes. Unknown values remain zero. CPU builds
do not link ROCm. Metrics are process-local, without persistent storage or worker labels.

## Reproduce and test

```bash
python3 -m venv /path/to/bench-venv
/path/to/bench-venv/bin/pip install grpcio grpcio-tools websockets
mkdir -p build-wsl/python-proto
/path/to/bench-venv/bin/python -m grpc_tools.protoc -I inference/proto \
  --python_out=build-wsl/python-proto --grpc_python_out=build-wsl/python-proto \
  inference/proto/inference.proto
/path/to/bench-venv/bin/python inference/benchmarks/run_matrix.py \
  --cpu-bin /path/to/cpu/llama-server --rocm-bin /path/to/hip/llama-server \
  --model /path/to/Qwen3-0.6B-Q8_0.gguf --out /path/to/results
```

`run_app_matrix.py` also requires real initialized MySQL/Kafka/Redis. See `--help`
and report deployment parameters. An external JSON secret file supplies
`mysql_password`; never commit it. Registration/connections/warmups are outside
serving timing. Scripts require runtime tokens, not characters/events. Raw JSON,
manifests, metrics and ignored logs are saved. Scripts stop only their own
processes. Do not compile or run competing benchmarks during collection.

`inference_tests` covers admission/capacity/cancel/routing. llama.cpp and vLLM
adapter tests use deterministic HTTP fixtures. Kafka parallel tests use a mock
broker for lane order/unfinished offsets/replay. Report recovery checks use real
Kafka/Redis/MySQL.

## Limits

Engine continuous batching, prefix cache and Flash Attention are integrated;
Spark Push does not implement PagedAttention, custom GPU kernels, RDMA,
prefill/decode disaggregation or distributed KV storage. Multiple workers on one
GPU demonstrate topology, not multi-GPU scaling. No GPU isolation, inference
TLS/auth or resumable generation exists; bind internal demo endpoints to
loopback. Flag semantics come from [llama.cpp's server](https://github.com/ggml-org/llama.cpp/blob/master/tools/server/README.md).
