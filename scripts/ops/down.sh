#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "$0")" && pwd)/common.sh"

WITH_DEPS=0
KEEP_HERMES=0

usage() {
  cat <<'EOF'
用法：scripts/sparkctl.sh down [选项]

默认停止 Spark 业务进程和本机 WSL Pi Agent Gateway，保留 Docker 数据依赖。

选项：
  --with-deps     同时停止 Redis / MySQL / Kafka，不删除数据卷
  --keep-hermes   保持 Pi Agent Gateway 运行（兼容旧参数名）
  --keep-pi       保持 Pi Agent Gateway 运行
  -h, --help      显示帮助
EOF
}

for arg in "$@"; do
  case "$arg" in
    --with-deps) WITH_DEPS=1 ;;
    --keep-hermes|--keep-pi) KEEP_HERMES=1 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "未知选项：$arg" >&2; usage >&2; exit 2 ;;
  esac
done

load_project_env
args=()
(( WITH_DEPS )) && args+=(--with-deps)
bash "$PROJECT_ROOT/scripts/stop_demo.sh" "${args[@]}"

if (( ! KEEP_HERMES )) && hermes_enabled && hermes_is_local; then
  echo "stopping local Pi Agent Gateway"
  if [[ -f "$PROJECT_ROOT/.run/pi_gateway.pid" ]]; then
    pid="$(sed -n '1p' "$PROJECT_ROOT/.run/pi_gateway.pid" 2>/dev/null || true)"
    if [[ "$pid" =~ ^[0-9]+$ ]]; then
      kill "$pid" >/dev/null 2>&1 || true
      sleep 0.3
      kill -9 "$pid" >/dev/null 2>&1 || true
    fi
    rm -f "$PROJECT_ROOT/.run/pi_gateway.pid"
  fi
  pkill -f 'cannbot/scripts/pi_gateway.py' >/dev/null 2>&1 || true
  rm -f "$PROJECT_ROOT/.run/hermes_gateway.pid"
fi
