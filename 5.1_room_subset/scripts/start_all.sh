#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_DIR="${ROOT_DIR}/build"
RUN_DIR="${ROOT_DIR}/run"
PID_DIR="${RUN_DIR}/pids"
LOG_DIR="${RUN_DIR}/logs"

LOGIC_BIN="${BUILD_DIR}/logic/logic_server"
COMET_BIN="${BUILD_DIR}/comet/comet_server"
JOB_BIN="${BUILD_DIR}/job/job_server"
WEB_BIN="${BUILD_DIR}/web_demo/web_demo_server"

LOGIC_CONF="${ROOT_DIR}/conf/logic.conf"
COMET_CONF="${ROOT_DIR}/conf/comet.conf"
JOB_CONF="${ROOT_DIR}/conf/job.conf"

WEB_PORT="${WEB_PORT:-9010}"
WEB_ROOT="${ROOT_DIR}/web_demo/static"

BUILD_FIRST="${BUILD_FIRST:-0}"

mkdir -p "${PID_DIR}" "${LOG_DIR}"

need_bin() {
  local p="$1"
  if [[ ! -x "${p}" ]]; then
    echo "缺少可执行文件：${p}"
    echo "请先编译：mkdir -p build && cd build && cmake .. && make -j\$(nproc)"
    exit 1
  fi
}

start_proc() {
  local name="$1"
  shift
  local pid_file="${PID_DIR}/${name}.pid"
  local log_file="${LOG_DIR}/${name}.log"

  if [[ -f "${pid_file}" ]]; then
    local pid
    pid="$(cat "${pid_file}" || true)"
    if [[ -n "${pid}" ]] && kill -0 "${pid}" >/dev/null 2>&1; then
      echo "[SKIP] ${name} 已在运行 pid=${pid}"
      return 0
    fi
    rm -f "${pid_file}"
  fi

  echo "[START] ${name}"
  nohup "$@" >"${log_file}" 2>&1 &
  echo $! >"${pid_file}"
}

if [[ "${BUILD_FIRST}" == "1" ]]; then
  echo "[BUILD] cmake && make"
  mkdir -p "${BUILD_DIR}"
  (cd "${BUILD_DIR}" && cmake .. && make -j"$(nproc)")
fi

need_bin "${LOGIC_BIN}"
need_bin "${COMET_BIN}"
need_bin "${JOB_BIN}"
need_bin "${WEB_BIN}"

start_proc logic "${LOGIC_BIN}" "${LOGIC_CONF}"
start_proc comet "${COMET_BIN}" "${COMET_CONF}"
start_proc job "${JOB_BIN}" "${JOB_CONF}"
start_proc web_demo "${WEB_BIN}" "${WEB_PORT}" "${WEB_ROOT}"

echo
echo "已启动：日志在 ${LOG_DIR}，PID 在 ${PID_DIR}"
echo "web_demo: http://127.0.0.1:${WEB_PORT}"


