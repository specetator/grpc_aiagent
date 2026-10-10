#!/usr/bin/env bash
# Build identical pinned llama.cpp source for CPU, ROCm/HIP, CUDA or Ascend CANN.
# GPU backends require their actual SDK and device; this is not a simulator.
set -euo pipefail
accelerator="${1:-cpu}"
source_dir="${LLAMA_SOURCE_DIR:-/home/peco/tools/llama.cpp}"
build_dir="${LLAMA_BUILD_DIR:-$source_dir/build-$accelerator}"
jobs="${BUILD_JOBS:-2}"
expected_commit="${LLAMA_COMMIT:-6c7a87f7e5e5cd75b8a641c3471f2dee84a6ed17}"
actual_commit="$(git -C "$source_dir" rev-parse HEAD)"
[[ "$actual_commit" == "$expected_commit" ]] || {
  echo "llama.cpp commit mismatch: expected $expected_commit, got $actual_commit" >&2; exit 2;
}
args=(-DCMAKE_BUILD_TYPE=Release -DGGML_CUDA=OFF -DGGML_HIP=OFF -DGGML_CANN=OFF
      -DLLAMA_BUILD_TESTS=ON)
case "$accelerator" in
  cpu) ;;
  rocm)
    test -x /opt/rocm/bin/hipcc || { echo 'ROCm HIP SDK is required' >&2; exit 2; }
    /opt/rocm/bin/rocminfo >/dev/null
    args+=(-DGGML_HIP=ON -DCMAKE_PREFIX_PATH=/opt/rocm
           -DCMAKE_HIP_COMPILER=/opt/rocm/llvm/bin/clang++
           "-DAMDGPU_TARGETS=${AMDGPU_TARGETS:-gfx1201}") ;;
  cuda)
    if [[ -n "${CUDA_TOOLKIT_ROOT:-}" ]]; then
      export PATH="$CUDA_TOOLKIT_ROOT/bin:$PATH"
      export LD_LIBRARY_PATH="$CUDA_TOOLKIT_ROOT/lib:$CUDA_TOOLKIT_ROOT/lib64:${LD_LIBRARY_PATH:-}"
      args+=("-DCUDAToolkit_ROOT=$CUDA_TOOLKIT_ROOT")
    fi
    cuda_compiler="$(command -v nvcc)" || { echo 'CUDA SDK is required' >&2; exit 2; }
    "$cuda_compiler" --version
    args+=(-DGGML_CUDA=ON "-DCMAKE_CUDA_COMPILER=$cuda_compiler"
           "-DCMAKE_CUDA_ARCHITECTURES=${CUDA_ARCHITECTURES:-86}") ;;
  cann)
    test -n "${ASCEND_TOOLKIT_HOME:-}" || { echo 'ASCEND_TOOLKIT_HOME is required' >&2; exit 2; }
    args+=(-DGGML_CANN=ON "-DCANN_INSTALL_DIR=$ASCEND_TOOLKIT_HOME") ;;
  *) echo 'usage: build_runtime.sh cpu|rocm|cuda|cann' >&2; exit 2 ;;
esac
printf 'llama.cpp commit: %s\n' "$actual_commit"
cmake -S "$source_dir" -B "$build_dir" "${args[@]}"
cmake --build "$build_dir" --target llama-server llama-bench llama-quantize test-backend-ops -j"$jobs"
