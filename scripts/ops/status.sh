#!/usr/bin/env bash
set -u

source "$(cd "$(dirname "$0")" && pwd)/common.sh"

if (($#)); then
  case "$1" in
    -h|--help) echo '用法：scripts/sparkctl.sh status'; exit 0 ;;
    *) echo "status 不接受参数：$1" >&2; exit 2 ;;
  esac
fi
load_project_env

printf '%-22s %s\n' SERVICE STATE
printf '%-22s %s\n' '----------------------' '------------------------------'
for name in logic_server hermes_bridge comet_server job_server web_demo_server; do
  state="$(process_state "$name" 2>/dev/null || true)"
  printf '%-22s %s\n' "$name" "${state:-unknown}"
done
for name in spark-redis spark-mysql spark-kafka; do
  state="$(container_state "$name" 2>/dev/null || true)"
  printf '%-22s %s\n' "$name" "${state:-unknown}"
done

if hermes_enabled; then
  if http_ok "$(hermes_health_url)" 3; then
    printf '%-22s %s\n' hermes_gateway 'healthy'
  else
    printf '%-22s %s\n' hermes_gateway 'unreachable'
  fi
else
  printf '%-22s %s\n' hermes_gateway 'disabled'
fi

access_ip="${SPARK_PUSH_ACCESS_IP:-}"
if [[ -z "$access_ip" ]] && command -v ip >/dev/null 2>&1; then
  access_ip="$(ip -4 route get 1.1.1.1 2>/dev/null |
    sed -n 's/.* src \([0-9.][0-9.]*\).*/\1/p' | head -n 1)"
fi
echo
echo "WebDemo (WSL)：http://127.0.0.1:9010/index.html"
[[ -n "$access_ip" ]] && echo "WebDemo (Windows)：http://${access_ip}:9010/index.html"
exit 0
