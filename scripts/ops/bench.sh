#!/usr/bin/env bash
# 启动本地依赖并运行隔离账号的四类可复核压测。
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
BUILD_DIR="${SPARK_PUSH_BUILD_DIR:-$ROOT/build}"
LOG_DIR="$ROOT/logs/bench"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
PASSWORD="${SPARK_PUSH_BENCH_PASSWORD:-bench-123456}"
CONNECTIONS=8
MESSAGES_PER_CONN=100
KAFKA_MESSAGES=1000

usage() {
  cat <<'EOF'
用法：scripts/sparkctl.sh bench [选项]

启动依赖和业务服务，创建隔离压测账号，运行 E2E、建连、历史、Kafka→Job→Comet 四项测试。
结果保存到 logs/bench/bench-<UTC 时间戳>.log。

选项：
  --connections N          E2E 发送连接数（默认 8）
  --messages-per-conn N    每连接消息数（默认 100）
  --kafka-messages N       Kafka 分层消息数（默认 1000）
  --skip-start             不启动服务，直接使用当前运行实例
  -h, --help               显示帮助
EOF
}

SKIP_START=0
while (($#)); do
  case "$1" in
    --connections) CONNECTIONS="$2"; shift 2 ;;
    --messages-per-conn) MESSAGES_PER_CONN="$2"; shift 2 ;;
    --kafka-messages) KAFKA_MESSAGES="$2"; shift 2 ;;
    --skip-start) SKIP_START=1; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "未知选项：$1" >&2; usage >&2; exit 2 ;;
  esac
done

[[ "$CONNECTIONS" =~ ^[1-9][0-9]*$ && "$MESSAGES_PER_CONN" =~ ^[1-9][0-9]*$ && "$KAFKA_MESSAGES" =~ ^[1-9][0-9]*$ ]] || {
  echo '压测参数必须是正整数' >&2; exit 2;
}
mkdir -p "$LOG_DIR"
REPORT="$LOG_DIR/bench-$STAMP.log"
exec > >(tee "$REPORT") 2>&1

if (( ! SKIP_START )); then
  "$ROOT/scripts/sparkctl.sh" up --fast
fi

for binary in e2e_bench connection_bench history_bench kafka_delivery_bench; do
  [[ -x "$BUILD_DIR/load_test/$binary" ]] || {
    echo "缺少 $binary，请先执行 scripts/sparkctl.sh up（不要使用 --fast）" >&2; exit 1;
  }
done

BASE="http://127.0.0.1:9101"
SENDER="bench_${STAMP}_s"
RECEIVER="bench_${STAMP}_r"
register() {
  curl -fsS -X POST "$BASE/api/register" -H 'Content-Type: application/json' \
    --data "{\"account\":\"$1\",\"password\":\"$PASSWORD\",\"name\":\"$1\"}"
}
SENDER_JSON="$(register "$SENDER")"
RECEIVER_JSON="$(register "$RECEIVER")"
SENDER_ID="$(python3 -c 'import json,sys; print(json.load(sys.stdin)["data"]["user_id"])' <<<"$SENDER_JSON")"
RECEIVER_ID="$(python3 -c 'import json,sys; print(json.load(sys.stdin)["data"]["user_id"])' <<<"$RECEIVER_JSON")"
if (( SENDER_ID < RECEIVER_ID )); then SESSION="s_${SENDER_ID}_${RECEIVER_ID}"; else SESSION="s_${RECEIVER_ID}_${SENDER_ID}"; fi

echo "BENCH_RUN=$STAMP"
echo "BENCH_ACCOUNTS sender=$SENDER receiver=$RECEIVER session=$SESSION"
echo '--- E2E ---'
"$BUILD_DIR/load_test/e2e_bench" --sender-account "$SENDER" --sender-password "$PASSWORD" \
  --receiver-account "$RECEIVER" --receiver-password "$PASSWORD" \
  --connections "$CONNECTIONS" --messages-per-conn "$MESSAGES_PER_CONN" --timeout-ms 30000
echo '--- CONNECTION ---'
"$BUILD_DIR/load_test/connection_bench" --account "$SENDER" --password "$PASSWORD" \
  --connections 200 --threads 8 --hold-seconds 5 --rounds 1
echo '--- HISTORY ---'
"$BUILD_DIR/load_test/history_bench" --account "$SENDER" --password "$PASSWORD" \
  --session-id "$SESSION" --threads 8 --requests-per-thread 50 --limit 50
echo '--- KAFKA DELIVERY ---'
"$BUILD_DIR/load_test/kafka_delivery_bench" --account "$RECEIVER" --password "$PASSWORD" \
  --messages "$KAFKA_MESSAGES" --timeout-ms 30000
echo "BENCH_REPORT=$REPORT"
