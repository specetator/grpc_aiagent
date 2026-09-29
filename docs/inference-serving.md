# Inference Serving Phase 1

## Purpose and boundary

This document describes the current root source implementation, verified on
2026-09-28 with the mock worker and `inference_tests` in WSL Ubuntu 24.04.

The optional inference subsystem provides a worker registry, load based routing,
and gRPC token streaming. Spark Push still owns authentication, message sequence
numbers, Kafka events, final message persistence, WebSocket delivery, history,
and offline recovery. Inference does not store chat messages. The default Bridge
backend remains Pi, so existing Agent tools, Skills, images, and control commands
continue through the local Pi gateway.

```text
Client → Comet → Logic → ai_request → hermes_bridge
                                      ├─ default: Pi HTTP/SSE
                                      └─ optional: Inference Gateway → gRPC Worker
                           ai_delta ← streaming text
                           ai_reply ← completed text
Logic → persist_message / push_single → Job → Comet → Client
```

`ai_delta` is a transient online preview and never advances `msg_seq`.
Only the final `ai_reply` enters the existing persistence and delivery path.
A successful gRPC write is not proof that generation or final persistence succeeded.

## Build and demo

On Ubuntu/WSL with the repository dependencies installed:

```bash
cmake -S . -B build-wsl
cmake --build build-wsl --target inference_gateway inference_worker inference_cli inference_tests -j2
ctest --test-dir build-wsl -R inference_tests --output-on-failure

# Terminal 1
./build-wsl/inference/inference_gateway --listen 127.0.0.1:9300 --metrics-port 9301
# Terminal 2
./build-wsl/inference/inference_worker --worker-id worker-0 --listen 127.0.0.1:9400 --gateway 127.0.0.1:9300 --model mock-model
# Terminal 3
./build-wsl/inference/inference_cli --gateway 127.0.0.1:9300 --prompt hello
```

The CLI prints six ordered text chunks followed by `finished: stop`. A second
worker can run with a different `--worker-id` and `--listen` port. The scheduler
uses reported load and worker ID for a deterministic tie break; it does not
promise round robin distribution when both workers have the same load.

For the full Spark Push path with the **mock** worker, start the usual
Kafka/Redis/MySQL/Logic/Job/Comet stack and Bridge, then set:

```dotenv
SPARK_PUSH_INFERENCE_BACKEND=grpc
SPARK_PUSH_INFERENCE_GATEWAY=127.0.0.1:9300
SPARK_PUSH_INFERENCE_MODEL=mock-model
```

These variables override `conf/hermes_bridge.conf`. Omit them for the unchanged
Pi backend. The gRPC backend currently accepts text chat; images are rejected
explicitly. Pi specific control commands still use the Pi client and need its
usual gateway configuration.

## Registry, scheduler, and streaming

Workers register after their gRPC server starts and send a status heartbeat
about once per second. The Gateway stores worker ID, endpoint, model, memory,
queue/load and heartbeat time under a short mutex. Snapshots mark a worker
unhealthy after five seconds without heartbeat. Network calls never hold that
mutex. The worker re-registers if the Gateway restarts.
Both servers enable the standard gRPC health service. Gateway process health
does not imply that a matching healthy worker is available.

`LeastLoadedScheduler` filters unhealthy or wrong model workers, then scores
running requests, waiting requests and memory pressure with centralized weights.
Worker ID breaks equal scores. It has no prefix or KV cache awareness.

`Generate` validates input, selects a worker, forwards the request over a real
gRPC server stream and relays chunks. Each chunk carries request ID, monotonic
sequence and timestamp. A terminal chunk carries a finish reason. Gateway
tracks in-flight requests and decrements the selected worker count on every
return path. No worker returns `UNAVAILABLE`; a broken worker RPC returns an
error status. Generation is not resumable.

The mock backend emits `Hello`, `, `, `this `, `is `, `a `, `stream.` at short
fixed intervals. `GenerationBackend` also has an optional llama.cpp adapter.
It forwards chat turns to a separately running `llama-server` using its
OpenAI-compatible streaming HTTP endpoint, converts SSE deltas into ordered
gRPC chunks, and closes the HTTP request on cancellation. The model runtime,
tokenizer, batching, and KV cache are provided by llama.cpp, not implemented
by this repository. Each synchronous worker call occupies one gRPC server
thread; it does not block the entire server. There is no queue or capacity
admission in this phase.

### Optional real model demo

Build `llama-server` separately from the official
[llama.cpp repository](https://github.com/ggml-org/llama.cpp), and obtain a
GGUF model separately. For a small CPU example,
[Qwen3-0.6B-GGUF Q8_0](https://huggingface.co/Qwen/Qwen3-0.6B-GGUF)
is published by Qwen under Apache 2.0. Keep the model and llama.cpp outside
this repository. The official Q8_0 file has SHA-256
`9465e63a22add5354d9bb4b99e90117043c7124007664907259bd16d043bb031`.
The adapter uses temperature 0.7 when the request does not specify one.
The example disables Qwen3 thinking mode in the llama.cpp chat template for
a short answer; remove that option when thinking output is desired.

```bash
# Terminal 1: model runtime; substitute your actual binary and model paths.
/path/to/llama-server -m /path/to/Qwen3-0.6B-Q8_0.gguf --alias qwen3-0.6b \
  --host 127.0.0.1 --port 8080 -c 2048 \
  --chat-template-kwargs '{"enable_thinking":false}'
# Terminal 2: inference Gateway.
./build-wsl/inference/inference_gateway --listen 127.0.0.1:9300
# Terminal 3: adapter worker.
./build-wsl/inference/inference_worker --worker-id real-0 \
  --listen 127.0.0.1:9400 --gateway 127.0.0.1:9300 \
  --model qwen3-0.6b --backend llamacpp \
  --model-endpoint http://127.0.0.1:8080
# Terminal 4: request.
./build-wsl/inference/inference_cli --model qwen3-0.6b --max-tokens 32 \
  --prompt 'Say hello in one sentence.'
```

To route Spark Push requests to this worker, set
`SPARK_PUSH_INFERENCE_BACKEND=grpc`; the configured inference model is
`qwen3-0.6b` by default. The `--cancel-after-chunks 1` CLI option
demonstrates upstream cancellation.
`inference_llamacpp_adapter_test` tests SSE conversion, HTTP failure, and
cancellation against a deterministic local HTTP fixture without downloading a
model.

## Cancellation and failure semantics

Gateway `Cancel(request_id)` cancels its downstream gRPC context. An upstream
disconnect is observed by the Gateway and also cancels that context. Worker
`Cancel(request_id)` sets its local stop flag; worker generation also checks its
gRPC context before each chunk. Bridge cancels its gRPC call when Bridge shuts
down or its request deadline expires. The current Spark Push WebSocket protocol
does not yet expose a user initiated AI cancel event to Bridge.

The meaningful stages are request accepted by Spark Push, worker selected,
worker RPC started, first nonempty chunk, terminal generation, cancelled, and
failed. Generation failure must not be represented as successful final text.
Bridge publishes `ai_reply` with `ok=false` on model failure, and Logic persists
its error text as a recoverable bot message. It is not a completed model answer.
Kafka and final message behavior retain their
existing at least once and idempotent boundaries; no exactly once claim is made.

## Metrics

Gateway exposes Prometheus text at `http://127.0.0.1:9301/metrics`:

- `inference_requests_total`, `inference_requests_active`,
  `inference_requests_failed_total`, `inference_requests_cancelled_total`
- `inference_scheduler_no_worker_total`
- `inference_ttft_ms_{count,sum,max}`: Gateway request receipt to first
  nonempty chunk
- `inference_total_latency_ms_{count,sum,max}`: Gateway request receipt to RPC
  completion, including error paths
- `inference_worker_running_requests`, `inference_worker_waiting_requests`,
  `inference_worker_heartbeat_age_ms`: aggregate snapshot gauges

These are process local observations, separate from Spark Push WebSocket and
Bridge TTFT. The registry's worker status can lag a heartbeat. The exporter has
no per-worker labels or persistent metrics store.

## Current limits and next phases

Spark Push inference code has no continuous batching, paged KV cache, prefix caching, CUDA
kernels, RDMA, or distributed KV cache. It also has no queue/admission policy,
GPU resource isolation, TLS/auth on inference RPCs, or resumable generation.
Bind the demo endpoints to loopback in this phase.

Next, add a real model worker, capacity admission and queueing, and better
multi-worker load measurements. Later phases can add continuous batching and
prefill/decode metrics, GPU memory accounting, prefix aware routing, and only
then evaluate remote KV cache and its TCP/RDMA transport options.
