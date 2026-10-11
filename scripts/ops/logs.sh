#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "$0")" && pwd)/common.sh"

SERVICE=all
LINES=80
FOLLOW=0

usage() {
  cat <<'EOF'
用法：scripts/sparkctl.sh logs [服务] [选项]

服务：all、logic、comet、job、web、bridge、gateway

选项：
  -n, --lines N   显示最后 N 行，默认 80
  -f, --follow    持续跟踪日志
  -h, --help      显示帮助
EOF
}

while (($#)); do
  case "$1" in
    all|logic|comet|job|web|bridge|gateway) SERVICE="$1"; shift ;;
    -n|--lines)
      [[ $# -ge 2 ]] || { echo '缺少行数' >&2; exit 2; }
      LINES="$2"; shift 2 ;;
    -f|--follow) FOLLOW=1; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "未知参数：$1" >&2; usage >&2; exit 2 ;;
  esac
done

[[ "$LINES" =~ ^[0-9]+$ ]] && (( LINES >= 1 && LINES <= 10000 )) || {
  echo '日志行数必须在 1..10000' >&2
  exit 2
}

declare -A files=(
  [logic]="$PROJECT_ROOT/logs/logic.log"
  [comet]="$PROJECT_ROOT/logs/comet.log"
  [job]="$PROJECT_ROOT/logs/job.log"
  [web]="$PROJECT_ROOT/logs/web.out"
  [bridge]="$PROJECT_ROOT/logs/hermes_bridge.log"
  [gateway]="$PROJECT_ROOT/logs/hermes_gateway.out"
)

selected=()
add_logs() {
  local name="$1"
  local primary="${files[$name]}"
  [[ -f "$primary" ]] && selected+=("$primary")
  # Native libraries and early startup failures still write stderr to .out.
  if [[ "$primary" == *.log ]]; then
    local secondary="${primary%.log}.out"
    [[ -f "$secondary" ]] && selected+=("$secondary")
  fi
  return 0
}
if [[ "$SERVICE" == all ]]; then
  for name in logic comet job web bridge gateway; do add_logs "$name"; done
else
  add_logs "$SERVICE"
fi

((${#selected[@]})) || { echo '没有可读取的日志文件' >&2; exit 1; }
args=(-n "$LINES")
(( FOLLOW )) && args+=(-f)
exec tail "${args[@]}" "${selected[@]}"
