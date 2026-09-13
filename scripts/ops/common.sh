#!/usr/bin/env bash

OPS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$OPS_DIR/../.." && pwd)"
ENV_FILE="${SPARK_PUSH_ENV_FILE:-$PROJECT_ROOT/.env.local}"
BUILD_DIR="${SPARK_PUSH_BUILD_DIR:-$PROJECT_ROOT/build}"
KNOWLEDGE_ROOT="${CANN_KNOWLEDGE_ROOT:-$(cd "$PROJECT_ROOT/.." && pwd)/cann-agent-knowledge}"

[[ "$ENV_FILE" == /* ]] || ENV_FILE="$PROJECT_ROOT/$ENV_FILE"
[[ "$BUILD_DIR" == /* ]] || BUILD_DIR="$PROJECT_ROOT/$BUILD_DIR"
[[ "$KNOWLEDGE_ROOT" == /* ]] || KNOWLEDGE_ROOT="$PROJECT_ROOT/$KNOWLEDGE_ROOT"

load_project_env() {
  if [[ -f "$ENV_FILE" ]]; then
    set -a
    # shellcheck disable=SC1090
    source "$ENV_FILE"
    set +a
  fi
}

is_true() {
  [[ "${1:-}" == "1" || "${1:-}" == "true" || "${1:-}" == "yes" ]]
}

hermes_enabled() {
  is_true "${SPARK_PUSH_HERMES_ENABLED:-false}"
}

hermes_is_local() {
  local base="${SPARK_PUSH_HERMES_BASE_URL:-}"
  [[ "$base" == http://127.0.0.1:* || "$base" == http://localhost:* ]]
}

hermes_health_url() {
  local base="${SPARK_PUSH_HERMES_BASE_URL:-http://127.0.0.1:8643/v1}"
  base="${base%/}"
  base="${base%/v1}"
  printf '%s/health\n' "$base"
}

resolve_pi_command() {
  if [[ -n "${SPARK_PUSH_PI_COMMAND:-}" &&
        -x "${SPARK_PUSH_PI_COMMAND}" ]]; then
    printf '%s\n' "$SPARK_PUSH_PI_COMMAND"
    return 0
  fi
  if command -v pi-spark-agent >/dev/null 2>&1; then
    command -v pi-spark-agent
    return 0
  fi
  local user_home_path="${HOME:-/home/peco}"
  if [[ -x "$user_home_path/.local/bin/pi-spark-agent" ]]; then
    printf '%s\n' "$user_home_path/.local/bin/pi-spark-agent"
    return 0
  fi
  return 1
}

resolve_hermes_command() {
  resolve_pi_command
}

process_executable() {
  case "$1" in
    logic_server) printf '%s/logic/logic_server\n' "$BUILD_DIR" ;;
    comet_server) printf '%s/comet/comet_server\n' "$BUILD_DIR" ;;
    job_server) printf '%s/job/job_server\n' "$BUILD_DIR" ;;
    hermes_bridge) printf '%s/hermes_bridge/hermes_bridge\n' "$BUILD_DIR" ;;
    web_demo_server) printf '%s/web_demo/web_demo_server\n' "$BUILD_DIR" ;;
    *) return 1 ;;
  esac
}

process_state() {
  local name="$1"
  local pidfile="$PROJECT_ROOT/.run/${name}.pid"
  local expected pid actual
  expected="$(process_executable "$name")" || return 1
  if [[ ! -f "$pidfile" ]]; then
    printf 'stopped\n'
    return 1
  fi
  pid="$(sed -n '1p' "$pidfile" 2>/dev/null || true)"
  if [[ ! "$pid" =~ ^[0-9]+$ || ! -d "/proc/$pid" ]]; then
    printf 'stale-pid\n'
    return 1
  fi
  actual="$(readlink -f "/proc/$pid/exe" 2>/dev/null || true)"
  actual="${actual% (deleted)}"
  if [[ "$actual" != "$expected" ]]; then
    printf 'wrong-process\n'
    return 1
  fi
  printf 'running(pid=%s)\n' "$pid"
}

http_ok() {
  curl -fsS --max-time "${2:-3}" "$1" >/dev/null 2>&1
}

tcp_ok() {
  local host="$1" port="$2"
  (exec 3<>"/dev/tcp/$host/$port") >/dev/null 2>&1
}

container_state() {
  local name="$1"
  if ! command -v docker >/dev/null 2>&1; then
    printf 'docker-unavailable\n'
    return 1
  fi
  local status health
  status="$(docker inspect -f '{{.State.Status}}' "$name" 2>/dev/null || true)"
  health="$(docker inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}' "$name" 2>/dev/null || true)"
  if [[ "$status" == "running" && ("$health" == "healthy" || "$health" == "none") ]]; then
    printf 'running(%s)\n' "$health"
    return 0
  fi
  if [[ -z "$status" ]]; then
    printf 'missing\n'
  else
    printf '%s(%s)\n' "$status" "$health"
  fi
  return 1
}
