#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PID_DIR="${ROOT_DIR}/run/pids"

show_one() {
  local name="$1"
  local pid_file="${PID_DIR}/${name}.pid"
  if [[ ! -f "${pid_file}" ]]; then
    echo "${name}: stopped (no pid file)"
    return 0
  fi
  local pid
  pid="$(cat "${pid_file}" || true)"
  if [[ -z "${pid}" ]]; then
    echo "${name}: stopped (empty pid)"
    return 0
  fi
  if kill -0 "${pid}" >/dev/null 2>&1; then
    echo "${name}: running pid=${pid}"
  else
    echo "${name}: stopped (stale pid=${pid})"
  fi
}

mkdir -p "${PID_DIR}"

show_one logic
show_one comet
show_one job
show_one web_demo


