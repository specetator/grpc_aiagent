#!/usr/bin/env bash
set -u

source "$(cd "$(dirname "$0")" && pwd)/common.sh"

if (($#)); then
  case "$1" in
    -h|--help) echo '用法：scripts/sparkctl.sh health'; exit 0 ;;
    *) echo "health 不接受参数：$1" >&2; exit 2 ;;
  esac
fi
load_project_env

failures=0
check() {
  local label="$1"
  shift
  if "$@"; then
    printf '[PASS] %s\n' "$label"
  else
    printf '[FAIL] %s\n' "$label"
    failures=$((failures + 1))
  fi
}

check_process() { process_state "$1" >/dev/null 2>&1; }
check_container() { container_state "$1" >/dev/null 2>&1; }

check 'Redis container' check_container spark-redis
check 'MySQL container' check_container spark-mysql
check 'Kafka container' check_container spark-kafka
check 'Logic process' check_process logic_server
if hermes_enabled; then
  check 'Hermes Bridge process' check_process hermes_bridge
fi
check 'Comet process' check_process comet_server
check 'Job process' check_process job_server
check 'WebDemo process' check_process web_demo_server
check 'Logic HTTP /metrics' http_ok http://127.0.0.1:9101/metrics 3
check 'Comet metrics' http_ok http://127.0.0.1:9203/metrics 3
check 'Job metrics' http_ok http://127.0.0.1:9202/metrics 3
check 'WebDemo index' http_ok http://127.0.0.1:9010/index.html 3
check 'Comet WebSocket port' tcp_ok 127.0.0.1 9000
check 'Logic gRPC port' tcp_ok 127.0.0.1 9100

if hermes_enabled; then
  check 'Pi Agent Gateway /health' http_ok "$(hermes_health_url)" 4
fi

if (( failures )); then
  echo "健康检查失败项：$failures" >&2
  exit 1
fi
echo '全部健康检查通过。'
