#!/usr/bin/env bash
# User-local, driver-free CUDA 12.9 toolkit for WSL/Linux. No sudo required.
set -euo pipefail
toolkit_dir="${CUDA_TOOLKIT_DIR:-/home/peco/tools/cuda-12.9}"
cache_dir="${CUDA_DOWNLOAD_DIR:-/home/peco/.cache/spark-push-cuda}"
mkdir -p "$toolkit_dir" "$cache_dir"
base=https://developer.download.nvidia.com/compute/cuda/redist
# Checksums from NVIDIA redistrib_12.9.0.json. Never install a Linux driver in WSL.
components=(
  'cuda_nvcc/linux-x86_64/cuda_nvcc-linux-x86_64-12.9.41-archive.tar.xz b3a0e115840e04c0cfa559263cbbe8b78a2455788e12605732aff68abc50dd34'
  'cuda_cudart/linux-x86_64/cuda_cudart-linux-x86_64-12.9.37-archive.tar.xz db950a2443a34daa10069c8df1bccec868af3144cb898cd1d903a9ea04cccfa4'
  'cuda_cccl/linux-x86_64/cuda_cccl-linux-x86_64-12.9.27-archive.tar.xz 8b1a5095669e94f2f9afd7715533314d418179e9452be61e2fde4c82a3e542aa'
  'libcublas/linux-x86_64/libcublas-linux-x86_64-12.9.0.13-archive.tar.xz bc51ee77e5891614e070066c0dabc52b609302bac2127aff850020b14f35716b'
)
for component in "${components[@]}"; do
  read -r relative checksum <<< "$component"
  archive="$cache_dir/${relative##*/}"
  if ! printf '%s  %s\n' "$checksum" "$archive" | sha256sum --check --status 2>/dev/null; then
    curl --fail --location --retry 3 --speed-time 60 --speed-limit 1024 \
      --continue-at - --output "$archive" "$base/$relative"
  fi
  printf '%s  %s\n' "$checksum" "$archive" | sha256sum --check
  tar -xJf "$archive" --strip-components=1 -C "$toolkit_dir"
done
# NVIDIA redistributables use lib/, while nvcc's default profile uses lib64/.
if [[ ! -e "$toolkit_dir/lib64" ]]; then ln -s lib "$toolkit_dir/lib64"; fi
"$toolkit_dir/bin/nvcc" --version
printf 'CUDA_TOOLKIT_ROOT=%s\n' "$toolkit_dir"
