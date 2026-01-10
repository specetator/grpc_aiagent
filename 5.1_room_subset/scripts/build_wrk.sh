#!/usr/bin/env bash
set -euo pipefail

# 源码编译 wrk 到项目内（避免 sudo/apt 依赖）
#
# 产物：
#   tools/wrk/wrk
#
# 依赖：
# - git
# - make + gcc/clang
# - openssl headers（缺少时 make 会报错；Ubuntu/Debian 通常是 libssl-dev）

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TOOLS_DIR="${ROOT_DIR}/tools/wrk"
SRC_DIR="${ROOT_DIR}/tools/wrk-src"

need_cmd() {
  local c="$1"
  command -v "${c}" >/dev/null 2>&1 || { echo "缺少命令：${c}"; exit 1; }
}

need_cmd git
need_cmd make
need_cmd gcc

mkdir -p "${ROOT_DIR}/tools"

if [[ -x "${TOOLS_DIR}/wrk" ]]; then
  echo "[OK] wrk 已存在：${TOOLS_DIR}/wrk"
  exit 0
fi

if [[ ! -d "${SRC_DIR}/.git" ]]; then
  echo "[CLONE] https://github.com/wg/wrk.git -> ${SRC_DIR}"
  rm -rf "${SRC_DIR}"
  git clone --depth 1 https://github.com/wg/wrk.git "${SRC_DIR}"
fi

echo "[BUILD] wrk"
(cd "${SRC_DIR}" && make -j"$(nproc)")

mkdir -p "${TOOLS_DIR}"
cp -f "${SRC_DIR}/wrk" "${TOOLS_DIR}/wrk"

echo "[OK] built: ${TOOLS_DIR}/wrk"
echo "用法示例："
echo "  WRK_BIN=\"${TOOLS_DIR}/wrk\" bash scripts/wrk_single_send.sh"


