#!/usr/bin/env bash
# Convert the already-cached Hugging Face model OFFLINE; never download weights.
set -euo pipefail
source_dir="${LLAMA_SOURCE_DIR:-/home/peco/tools/llama.cpp}"
python_bin="${CONVERT_PYTHON:-/home/peco/tools/spark-cuda-venv/bin/python}"
model_cache="${HF_LOCAL_MODEL:-/home/peco/.cache/huggingface/hub/models--Qwen--Qwen3-0.6B}"
output_dir="${MODEL_OUTPUT_DIR:-/home/peco/models}"
if [[ -f "$model_cache/refs/main" ]]; then
  revision="$(<"$model_cache/refs/main")"
  model_cache="$model_cache/snapshots/$revision"
fi
test -f "$model_cache/model.safetensors" || { echo "Missing local weights: $model_cache" >&2; exit 2; }
mkdir -p "$output_dir"
export HF_HUB_OFFLINE=1 TRANSFORMERS_OFFLINE=1
f16_model="$output_dir/Qwen3-0.6B-local-F16.gguf"
q8_model="$output_dir/Qwen3-0.6B-local-Q8_0.gguf"
staging_dir="$(mktemp -d "$output_dir/.qwen-convert.XXXXXX")"
# Only completed files become reusable output. Failed temporary files are kept
# for diagnosis rather than mistaken for an already-prepared model on retry.
trap 'rmdir "$staging_dir" 2>/dev/null || true' EXIT
if [[ ! -f "$f16_model" ]]; then
  "$python_bin" "$source_dir/convert_hf_to_gguf.py" "$model_cache" --outtype f16 --outfile "$staging_dir/model-f16.gguf"
  mv -n "$staging_dir/model-f16.gguf" "$f16_model"
fi
if [[ ! -f "$q8_model" ]]; then
  "${LLAMA_QUANTIZE:-$source_dir/build-cpu/bin/llama-quantize}" "$f16_model" "$staging_dir/model-q8.gguf" Q8_0
  mv -n "$staging_dir/model-q8.gguf" "$q8_model"
fi
sha256sum "$model_cache/model.safetensors" "$q8_model"
