#!/usr/bin/env bash
set -euo pipefail
accelerator="${1:-cpu}"
model="${MODEL_PATH:-/home/peco/models/Qwen3-0.6B-Q8_0.gguf}"
slots="${RUNTIME_SLOTS:-4}"
args=(-m "$model" --alias qwen3-0.6b --host 127.0.0.1
      --port "${RUNTIME_PORT:-8080}" -t 6 -tb 6 -np "$slots"
      -c "$((slots * 2048))" -b 512 -ub 128 --metrics
      --chat-template-kwargs '{"enable_thinking":false}')
case "$accelerator" in
  cpu) args+=(-ngl 0 --device none --no-kv-offload --no-op-offload) ;;
  rocm) export HIP_VISIBLE_DEVICES="${HIP_VISIBLE_DEVICES:-0}"; args+=(-ngl 99) ;;
  cuda) export CUDA_VISIBLE_DEVICES="${CUDA_VISIBLE_DEVICES:-0}"; args+=(-ngl 99) ;;
  cann) args+=(-ngl 99) ;;
  *) echo 'usage: serve_runtime.sh cpu|rocm|cuda|cann' >&2; exit 2 ;;
esac
if [[ "${CONTINUOUS_BATCHING:-1}" == 1 ]]; then args+=(-cb); else args+=(-nocb); fi
if [[ "${PREFIX_CACHE:-1}" == 1 ]]; then args+=(--cache-prompt); else args+=(--no-cache-prompt --cache-ram 0); fi
args+=(-fa "${FLASH_ATTENTION:-off}")
exec "${LLAMA_SERVER:-/home/peco/tools/llama.cpp/build-$accelerator/bin/llama-server}" "${args[@]}"
