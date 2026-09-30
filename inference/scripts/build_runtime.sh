#!/usr/bin/env bash
# Build identical pinned llama.cpp source for CPU, ROCm/HIP, CUDA or Ascend CANN.
# GPU backends require their actual SDK and device; this is not a simulator.
set -euo pipefail
accelerator="${1:-cpu}"
source_dir="${LLAMA_SOURCE_DIR:-/home/peco/tools/llama.cpp}"
build_dir="${LLAMA_BUILD_DIR:-$source_dir/build-$accelerator}"
jobs="${BUILD_JOBS:-2}"
args=(-DCMAKE_BUILD_TYPE=Release -DGGML_CUDA=OFF -DGGML_HIP=OFF -DGGML_CANN=OFF)
case "$accelerator" in
  cpu) ;;
  rocm)
    test -x /opt/rocm/bin/hipcc || { echo 'ROCm HIP SDK is required' >&2; exit 2; }
    /opt/rocm/bin/rocminfo >/dev/null
    args+=(-DGGML_HIP=ON -DCMAKE_PREFIX_PATH=/opt/rocm
           -DCMAKE_HIP_COMPILER=/opt/rocm/llvm/bin/clang++
           "-DAMDGPU_TARGETS=${AMDGPU_TARGETS:-gfx1201}") ;;
  cuda)
    command -v nvcc >/dev/null || { echo 'CUDA SDK is required' >&2; exit 2; }
    args+=(-DGGML_CUDA=ON) ;;
  cann)
    test -n "${ASCEND_TOOLKIT_HOME:-}" || { echo 'ASCEND_TOOLKIT_HOME is required' >&2; exit 2; }
    args+=(-DGGML_CANN=ON "-DCANN_INSTALL_DIR=$ASCEND_TOOLKIT_HOME") ;;
  *) echo 'usage: build_runtime.sh cpu|rocm|cuda|cann' >&2; exit 2 ;;
esac
git -C "$source_dir" rev-parse HEAD
cmake -S "$source_dir" -B "$build_dir" "${args[@]}"
cmake --build "$build_dir" --target llama-server llama-bench -j"$jobs"
