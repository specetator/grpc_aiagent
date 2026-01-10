#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PID_DIR="${ROOT_DIR}/run/pids"

stop_one() {
  local name="$1"
  local pid_file="${PID_DIR}/${name}.pid"
  if [[ ! -f "${pid_file}" ]]; then
    echo "[SKIP] ${name} pid 文件不存在"
    return 0
  fi

  local pid
  pid="$(cat "${pid_file}" || true)"
  if [[ -z "${pid}" ]]; then
    rm -f "${pid_file}"
    echo "[SKIP] ${name} pid 为空"
    return 0
  fi

  if ! kill -0 "${pid}" >/dev/null 2>&1; then
    rm -f "${pid_file}"
    echo "[SKIP] ${name} 不在运行 pid=${pid}"
    return 0
  fi

  echo "[STOP] ${name} pid=${pid}"
  kill "${pid}" >/dev/null 2>&1 || true

  local i
  for i in {1..30}; do
    if ! kill -0 "${pid}" >/dev/null 2>&1; then
      rm -f "${pid_file}"
      echo "[OK] ${name} stopped"
      return 0
    fi
    sleep 0.2
  done

  echo "[KILL] ${name} pid=${pid}"
  kill -9 "${pid}" >/dev/null 2>&1 || true
  rm -f "${pid_file}"
}

mkdir -p "${PID_DIR}"

# 逆序停止，避免依赖侧报错刷屏
stop_one web_demo
stop_one job
stop_one comet
stop_one logic


