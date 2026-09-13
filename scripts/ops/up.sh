#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "$0")" && pwd)/common.sh"

FAST=0
FOREGROUND=0
SKIP_DEPS=0

usage() {
  cat <<'EOF'
用法：scripts/sparkctl.sh up [选项]

选项：
  --fast          跳过 CMake 编译，使用现有 build/ 二进制
  --skip-build    --fast 的别名
  --skip-deps     不执行 Docker Compose 依赖检查
  --foreground    前台监控 Spark 业务进程
  -h, --help      显示帮助
EOF
}

for arg in "$@"; do
  case "$arg" in
    --fast|--skip-build) FAST=1 ;;
    --skip-deps) SKIP_DEPS=1 ;;
    --foreground) FOREGROUND=1 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "未知选项：$arg" >&2; usage >&2; exit 2 ;;
  esac
done

load_project_env

stop_stale_hermes_gateway() {
  # Pi gateway occupies :8643. Drop leftover Spark-side Hermes pid files.
  rm -f "$PROJECT_ROOT/.run/hermes_gateway.pid"
}

ensure_hermes_gateway() {
  hermes_enabled || return 0
  local health_url
  health_url="$(hermes_health_url)"
  if http_ok "$health_url" 4; then
    echo "Pi Agent Gateway 已就绪：$health_url"
    return 0
  fi
  if ! hermes_is_local; then
    echo "远程 Agent API 不可达：$health_url" >&2
    echo "运维脚本不会自动启动或停止远程 Pi/Hermes。" >&2
    return 1
  fi
  local command_path
  if ! command_path="$(resolve_pi_command)"; then
    echo "未找到 pi-spark-agent；先执行 cannbot/scripts/setup_pi_agent.sh，" \
         "或设置 SPARK_PUSH_PI_COMMAND" >&2
    return 1
  fi
  mkdir -p "$PROJECT_ROOT/logs" "$PROJECT_ROOT/.run"
  stop_stale_hermes_gateway
  echo "启动独立 WSL Pi Agent Gateway..."
  nohup setsid "$command_path" gateway </dev/null \
    >>"$PROJECT_ROOT/logs/pi_gateway.out" 2>&1 &
  printf '%s\n' "$!" >"$PROJECT_ROOT/.run/pi_gateway.pid"
  local attempt
  for ((attempt = 0; attempt < 120; ++attempt)); do
    if http_ok "$health_url" 2; then
      echo "Pi Agent Gateway 已就绪：$health_url"
      return 0
    fi
    sleep 0.25
  done
  echo "Pi Agent Gateway 启动超时，最近日志：" >&2
  tail -n 40 "$PROJECT_ROOT/logs/pi_gateway.out" >&2 || true
  return 1
}

ensure_hermes_gateway

args=()
(( FAST )) && args+=(--skip-build)
(( SKIP_DEPS )) && args+=(--skip-deps)
(( FOREGROUND )) && args+=(--foreground)

cd "$PROJECT_ROOT"
exec bash "$PROJECT_ROOT/scripts/start_demo.sh" "${args[@]}"
