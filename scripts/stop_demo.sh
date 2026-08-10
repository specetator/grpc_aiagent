#!/usr/bin/env bash
# 停止当前项目启动的 Logic / Comet / Job / WebDemo / Hermes Bridge。
# 默认只停止业务进程；加 --with-deps 时额外停止 Docker 依赖，但不删除数据卷。
set -u

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
ENV_FILE="${SPARK_PUSH_ENV_FILE:-$ROOT/.env.local}"
BUILD_DIR="${SPARK_PUSH_BUILD_DIR:-$ROOT/build}"
WITH_DEPS=0

usage() {
  cat <<'EOF'
用法：bash scripts/stop_demo.sh [选项]

选项：
  --with-deps   同时停止 Redis / MySQL / Kafka，不删除数据卷
  -h, --help    显示帮助
EOF
}

for arg in "$@"; do
  case "$arg" in
    --with-deps) WITH_DEPS=1 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "未知选项：$arg" >&2; usage >&2; exit 2 ;;
  esac
done

target_executable() {
  case "$1" in
    logic_server) echo "$BUILD_DIR/logic/logic_server" ;;
    comet_server) echo "$BUILD_DIR/comet/comet_server" ;;
    job_server) echo "$BUILD_DIR/job/job_server" ;;
    hermes_bridge) echo "$BUILD_DIR/hermes_bridge/hermes_bridge" ;;
    web_demo_server) echo "$BUILD_DIR/web_demo/web_demo_server" ;;
    *) return 1 ;;
  esac
}

pid_is_target() {
  local pid="$1"
  local expected="$2"
  local actual
  [[ -d "/proc/$pid" ]] || return 1
  actual="$(readlink -f "/proc/$pid/exe" 2>/dev/null || true)"
  # 如果服务正在运行时重新编译，Linux 会把旧映像标记为
  # "(deleted)"；它仍然是本项目目标进程，不能因此漏停。
  actual="${actual% (deleted)}"
  [[ "$actual" == "$expected" ]]
}

collect_target_pids() {
  local name="$1"
  local expected="$2"
  local pidfile="$ROOT/.run/${name}.pid"
  local pid

  if [[ -f "$pidfile" ]]; then
    pid="$(sed -n '1p' "$pidfile" 2>/dev/null || true)"
    if [[ "$pid" =~ ^[0-9]+$ ]] && pid_is_target "$pid" "$expected"; then
      echo "$pid"
    else
      rm -f "$pidfile"
    fi
  fi

  # 兜底查找同一项目的绝对可执行文件，不使用 pgrep -x，避免误杀其他项目。
  local candidate
  while read -r candidate; do
    if [[ "$candidate" =~ ^[0-9]+$ ]] &&
       pid_is_target "$candidate" "$expected"; then
      echo "$candidate"
    fi
  done < <(pgrep -f -- "$expected" 2>/dev/null || true)
}

stop_one() {
  local name="$1"
  local expected
  expected="$(target_executable "$name")" || return

  local pids
  pids="$(collect_target_pids "$name" "$expected" | sort -nu)"
  if [[ -z "$pids" ]]; then
    rm -f "$ROOT/.run/${name}.pid"
    echo "$name not running"
    return 0
  fi

  echo "stopping $name: $pids"
  local pid
  while read -r pid; do
    [[ -z "$pid" ]] || kill "$pid" 2>/dev/null || true
  done <<< "$pids"

  local round alive
  for ((round = 0; round < 300; ++round)); do
    alive=""
    while read -r pid; do
      if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
        alive="$alive $pid"
      fi
    done <<< "$pids"
    [[ -z "$alive" ]] && break
    sleep 0.1
  done

  if [[ -n "$alive" ]]; then
    echo "$name 未在 30 秒内退出，发送 SIGKILL：$alive" >&2
    for pid in $alive; do
      if pid_is_target "$pid" "$expected"; then
        kill -KILL "$pid" 2>/dev/null || true
      fi
    done
  fi
  rm -f "$ROOT/.run/${name}.pid"
  echo "stopped $name"
}

# 先停 Hermes Bridge/Job，避免 Kafka 回调继续向 Comet 投递；再停
# Comet/Logic，最后停 WebDemo。
for process in hermes_bridge job_server comet_server logic_server web_demo_server; do
  stop_one "$process"
done

if (( WITH_DEPS )); then
  if ! command -v docker >/dev/null 2>&1; then
    echo "缺少 docker，无法停止依赖" >&2
    exit 1
  fi
  compose=(docker compose)
  if [[ -f "$ENV_FILE" ]]; then
    compose+=(--env-file "$ENV_FILE")
  fi
  echo "stopping Docker dependencies (data volumes are preserved)"
  # compose 文件只在变量插值阶段需要 MySQL 密码；down 本身不会连接 MySQL。
  # 因此即使停止时没有重新加载 .env.local，也不应因为密码缺失而无法停依赖。
  SPARK_PUSH_MYSQL_PASSWORD="${SPARK_PUSH_MYSQL_PASSWORD:-stop-only}" \
    "${compose[@]}" down
fi
