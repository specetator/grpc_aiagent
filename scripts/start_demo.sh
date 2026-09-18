#!/usr/bin/env bash
# 一键准备依赖、编译并启动 Logic / Comet / Job / WebDemo；可选启动 Hermes Bridge。
#
# 默认模式：启动后返回终端，业务进程由 nohup 托管。
# 前台模式：bash scripts/start_demo.sh --foreground，Ctrl-C 时自动停止业务进程。
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

ENV_FILE="${SPARK_PUSH_ENV_FILE:-$ROOT/.env.local}"
BUILD_DIR="${SPARK_PUSH_BUILD_DIR:-$ROOT/build}"
BUILD_JOBS="${SPARK_PUSH_BUILD_JOBS:-$(nproc 2>/dev/null || echo 4)}"
ACCESS_IP="${SPARK_PUSH_ACCESS_IP:-}"
FOREGROUND=0
SKIP_BUILD=0
SKIP_DEPS=0

usage() {
  cat <<'EOF'
用法：bash scripts/start_demo.sh [选项]

选项：
  --foreground   保持脚本前台运行，Ctrl-C 时停止业务进程
  --skip-build   不重新执行 CMake 编译
  --skip-deps    不自动启动 Docker 依赖
  -h, --help     显示帮助

环境变量：
  SPARK_PUSH_ENV_FILE   环境文件路径，默认 .env.local
  SPARK_PUSH_BUILD_JOBS 编译并行度，默认 nproc
  SPARK_PUSH_ACCESS_IP  Windows/局域网访问用的虚拟机 IP；默认自动检测
  SPARK_PUSH_HERMES_ENABLED=true 时额外启动 hermes_bridge（现对接 Pi Agent）；
  同时需要 SPARK_PUSH_HERMES_BASE_URL、SPARK_PUSH_HERMES_API_KEY
EOF
}

for arg in "$@"; do
  case "$arg" in
    --foreground) FOREGROUND=1 ;;
    --skip-build) SKIP_BUILD=1 ;;
    --skip-deps) SKIP_DEPS=1 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "未知选项：$arg" >&2; usage >&2; exit 2 ;;
  esac
done

hermes_enabled() {
  [[ "${SPARK_PUSH_HERMES_ENABLED:-false}" == "true" ||
     "${SPARK_PUSH_HERMES_ENABLED:-false}" == "1" ]]
}

if [[ -f "$ENV_FILE" ]]; then
  # 配置文件中的 ${SPARK_PUSH_*} 会在这里展开，真实口令不会写入仓库。
  set -a
  # shellcheck disable=SC1090
  source "$ENV_FILE"
  set +a
elif [[ -z "${SPARK_PUSH_MYSQL_PASSWORD:-}" ]]; then
  echo "缺少环境文件：$ENV_FILE" >&2
  echo "请先执行：cp .env.example .env.local，并填写 SPARK_PUSH_MYSQL_PASSWORD" >&2
  exit 1
else
  echo "未找到 $ENV_FILE，使用当前 shell 注入的环境变量"
fi

# 允许把访问地址写进 .env.local；命令行环境变量仍然可以在未配置时提供默认值。
ACCESS_IP="${ACCESS_IP:-${SPARK_PUSH_ACCESS_IP:-}}"

if [[ -z "${SPARK_PUSH_MYSQL_PASSWORD:-}" ||
      "${SPARK_PUSH_MYSQL_PASSWORD}" == "replace-with-a-local-password" ]]; then
  echo "SPARK_PUSH_MYSQL_PASSWORD 未配置，请编辑 $ENV_FILE" >&2
  exit 1
fi

require_command() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "缺少命令：$1" >&2
    exit 1
  fi
}

wait_container_healthy() {
  local container="$1"
  local label="$2"
  local timeout_s="${3:-90}"
  local i status health

  for ((i = 0; i < timeout_s; ++i)); do
    status="$(docker inspect -f '{{.State.Status}}' "$container" 2>/dev/null || true)"
    health="$(docker inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}' \
      "$container" 2>/dev/null || true)"
    if [[ "$status" == "running" &&
          ("$health" == "healthy" || "$health" == "none") ]]; then
      echo "$label 已就绪"
      return 0
    fi
    if [[ "$status" == "exited" || "$status" == "dead" ]]; then
      echo "$label 启动失败，最近日志：" >&2
      docker logs --tail 40 "$container" >&2 || true
      return 1
    fi
    sleep 1
  done

  echo "$label 在 ${timeout_s}s 内没有就绪" >&2
  docker logs --tail 40 "$container" >&2 || true
  return 1
}

prepare_dependencies() {
  if (( SKIP_DEPS )); then
    echo "跳过 Docker 依赖检查（--skip-deps）"
    return
  fi

  require_command docker
  if ! docker info >/dev/null 2>&1; then
    echo "Docker daemon 未运行，请先启动 Docker Desktop 或 docker.service" >&2
    exit 1
  fi

  local compose=(docker compose)
  if [[ -f "$ENV_FILE" ]]; then
    compose+=(--env-file "$ENV_FILE")
  fi

  # 早期版本曾用 docker run/旧 compose 创建同名容器但没有 Compose label。
  # 这种已停止的残留容器会阻塞新版 compose 接管；这里只删除容器本身，
  # 不使用 -v，因此 MySQL/Redis 数据卷仍然保留。
  local expected_project="${COMPOSE_PROJECT_NAME:-$(basename "$ROOT" | \
    tr '[:upper:]' '[:lower:]' | sed 's/[^a-z0-9_-]//g')}"
  local container project status
  for container in spark-redis spark-mysql spark-kafka; do
    if ! docker inspect "$container" >/dev/null 2>&1; then
      continue
    fi
    project="$(docker inspect -f '{{index .Config.Labels "com.docker.compose.project"}}' \
      "$container" 2>/dev/null || true)"
    if [[ -n "$project" && "$project" == "$expected_project" ]]; then
      continue
    fi
    status="$(docker inspect -f '{{.State.Status}}' "$container")"
    if [[ "$status" == "running" ]]; then
      echo "发现正在运行且不属于当前 Compose 项目的容器：$container" >&2
      echo "请先停止它，或设置正确的 COMPOSE_PROJECT_NAME 后重试" >&2
      exit 1
    fi
    echo "移除旧的 $container 容器（保留数据卷）"
    docker rm "$container" >/dev/null
  done

  echo "[1/5] 启动 Redis / MySQL / Kafka..."
  "${compose[@]}" up -d redis mysql kafka

  wait_container_healthy spark-redis Redis
  wait_container_healthy spark-mysql MySQL 120
  wait_container_healthy spark-kafka Kafka 120

  # 直接在健康的 Kafka 容器内执行，避免不同 Compose 版本对 one-shot
  # kafka-init command 的解析差异；--if-not-exists 保证重复启动安全。
  echo "[2/5] 初始化 Kafka topics..."
  local topic
  for topic in push_single push_group push_to_comet broadcast_task persist_message \
      ai_request ai_delta ai_reply ai_request.dlq ai_reply.dlq persist_message.dlq; do
    docker exec spark-kafka /opt/kafka/bin/kafka-topics.sh \
      --bootstrap-server 127.0.0.1:29092 \
      --create --if-not-exists --topic "$topic" \
      --partitions 3 --replication-factor 1 >/dev/null
  done
  echo "Kafka topics 已就绪"
}

prepare_build() {
  require_command cmake
  if (( SKIP_BUILD )); then
    echo "跳过编译（--skip-build）"
  else
    echo "[3/5] 配置并编译 C++ 服务..."
    cmake -S "$ROOT" -B "$BUILD_DIR" \
      -DCMAKE_BUILD_TYPE=Release \
      -DBUILD_TESTING=ON
    cmake --build "$BUILD_DIR" -j"$BUILD_JOBS"
  fi

  local binary
  for binary in \
      "$BUILD_DIR/logic/logic_server" \
      "$BUILD_DIR/comet/comet_server" \
      "$BUILD_DIR/job/job_server" \
      "$BUILD_DIR/web_demo/web_demo_server"; do
    if [[ ! -x "$binary" ]]; then
      echo "缺少可执行文件：$binary" >&2
      echo "请移除 --skip-build 后重新执行" >&2
      exit 1
    fi
  done
  if hermes_enabled && [[ ! -x "$BUILD_DIR/hermes_bridge/hermes_bridge" ]]; then
    echo "缺少 Hermes Bridge 可执行文件：$BUILD_DIR/hermes_bridge/hermes_bridge" >&2
    echo "请移除 --skip-build 后重新执行" >&2
    exit 1
  fi
}

stop_old_processes() {
  echo "[4/5] 清理旧业务进程..."
  bash "$ROOT/scripts/stop_demo.sh" >/dev/null || true
}

start_process() {
  local name="$1"
  local logfile="$2"
  shift 2
  # nohup 只忽略 SIGHUP；在某些终端/任务执行器中，父会话结束时仍会
  # 回收同一进程组。setsid 让 Demo 服务拥有独立 session，脚本返回后
  # 仍能稳定提供 9010/9101/9000 等端口。
  nohup setsid "$@" </dev/null >>"$logfile" 2>&1 &
  local pid=$!
  printf '%s\n' "$pid" >"$ROOT/.run/${name}.pid"
  sleep 0.5
  if ! kill -0 "$pid" 2>/dev/null; then
    echo "$name 启动失败，最近日志：" >&2
    tail -n 30 "$logfile" >&2 || true
    exit 1
  fi
}

tcp_ok() {
  local host="$1"
  local port="$2"
  (exec 3<>"/dev/tcp/$host/$port") >/dev/null 2>&1
}

wait_port() {
  local port="$1"
  local label="$2"
  local timeout_s="${3:-30}"
  local i
  for ((i = 0; i < timeout_s * 4; ++i)); do
    if (exec 3<>"/dev/tcp/127.0.0.1/$port") 2>/dev/null; then
      exec 3>&-
      echo "$label 端口 $port 已监听"
      return 0
    fi
    sleep 0.25
  done
  echo "$label 未在 ${timeout_s}s 内监听端口 $port" >&2
  exit 1
}

wait_metric() {
  local url="$1"
  local metric="$2"
  local label="$3"
  local timeout_s="${4:-45}"
  local i body

  for ((i = 0; i < timeout_s * 4; ++i)); do
    body="$(curl -fsS --max-time 2 "$url" 2>/dev/null || true)"
    if printf '%s\n' "$body" | grep -Eq "^${metric}[[:space:]]+1([[:space:]]|$)"; then
      echo "$label 已就绪"
      return 0
    fi
    sleep 0.25
  done

  echo "$label 在 ${timeout_s}s 内没有就绪：$metric" >&2
  printf '%s\n' "$body" >&2
  return 1
}

detect_access_ip() {
  local detected=""
  # 优先使用默认路由对应的源地址，避免把 docker0/bridge 地址打印给用户。
  if command -v ip >/dev/null 2>&1; then
    detected="$(ip -4 route get 1.1.1.1 2>/dev/null |
      sed -n 's/.* src \([0-9.][0-9.]*\).*/\1/p' | head -n 1)"
  fi
  if [[ "$detected" =~ ^127\. || "$detected" == "0.0.0.0" ]]; then
    detected=""
  fi
  if [[ -z "$detected" ]]; then
    detected="$(hostname -I 2>/dev/null | tr ' ' '\n' |
      awk '/^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$/ && $1 !~ /^127\./ {print; exit}')"
  fi
  printf '%s\n' "$detected"
}

check_windows_access() {
  [[ -z "$ACCESS_IP" || "$ACCESS_IP" == 127.* ]] && return 0
  local url="http://${ACCESS_IP}:9010/index.html"
  if curl --noproxy '*' -fsS --max-time 3 "$url" >/dev/null 2>&1; then
    echo "Windows 访问链路已验证：$url"
  else
    echo "警告：本机无法通过 $ACCESS_IP 回环访问 WebDemo；请检查监听地址或 Ubuntu 防火墙" >&2
  fi
}

start_services() {
  mkdir -p "$ROOT/logs" "$ROOT/.run"
  stop_old_processes

  echo "[5/5] 启动 Logic / Comet / Job / WebDemo..."
  start_process logic_server "$ROOT/logs/logic.out" \
    "$BUILD_DIR/logic/logic_server" --config "$ROOT/conf/logic.conf"
  wait_port 9101 Logic

  if hermes_enabled; then
    if [[ -z "${SPARK_PUSH_HERMES_BASE_URL:-}" ||
          -z "${SPARK_PUSH_HERMES_API_KEY:-}" ]]; then
      echo "Hermes 已启用，但缺少 SPARK_PUSH_HERMES_BASE_URL 或 " \
           "SPARK_PUSH_HERMES_API_KEY" >&2
      bash "$ROOT/scripts/stop_demo.sh" || true
      return 1
    fi
    start_process hermes_bridge "$ROOT/logs/hermes_bridge.out" \
      "$BUILD_DIR/hermes_bridge/hermes_bridge" \
      --config "$ROOT/conf/hermes_bridge.conf"
    echo "Agent Bridge 已启动（对接 Pi Agent SSE /v1/chat/completions，最终消息仍落库）"
  fi

  start_process comet_server "$ROOT/logs/comet.out" \
    "$BUILD_DIR/comet/comet_server" --config "$ROOT/conf/comet.conf"
  wait_port 9000 Comet-WebSocket
  wait_port 9105 Comet-gRPC

  start_process job_server "$ROOT/logs/job.out" \
    "$BUILD_DIR/job/job_server" --config "$ROOT/conf/job.conf"
  wait_port 9202 Job-metrics

  # 9202 只能证明 Job 的 metrics socket 已打开；只有 Comet 的 gauge 变为 1，
  # 才说明 Job->Comet PushStream 已真正进入服务端处理函数。
  if grep -Eq '^[[:space:]]*use_push_stream[[:space:]]*=[[:space:]]*true' \
      "$ROOT/conf/job.conf"; then
    if ! wait_metric "http://127.0.0.1:9203/metrics" \
        "spark_push_comet_push_stream_ready" "Job→Comet PushStream"; then
      echo "下游长连接未就绪，清理本次启动的业务进程" >&2
      bash "$ROOT/scripts/stop_demo.sh" || true
      return 1
    fi
  fi

  # 9010 可能已有本项目之前启动的 WebDemo（例如旧 shell/IDE 会话仍在运行）。
  # 端口已能提供 HTTP 时直接复用，避免新实例因 bind 失败导致整套服务启动失败。
  if tcp_ok 127.0.0.1 9010 &&
      curl -fsS --max-time 2 http://127.0.0.1:9010/ >/dev/null 2>&1; then
    echo "WebDemo 已在 9010 运行，复用现有实例"
  else
    start_process web_demo_server "$ROOT/logs/web.out" \
      "$BUILD_DIR/web_demo/web_demo_server" --port 9010 \
      --doc-root "$ROOT/web_demo/static"
    wait_port 9010 WebDemo
  fi

  if [[ -z "$ACCESS_IP" ]]; then
    ACCESS_IP="$(detect_access_ip)"
  fi
  check_windows_access

  echo
  echo "项目已启动："
  echo "  浏览器：http://127.0.0.1:9010/index.html"
  if [[ -n "$ACCESS_IP" ]]; then
    echo "  Windows/局域网浏览器：http://${ACCESS_IP}:9010/index.html"
    echo "  Windows 依赖检查：Logic http://${ACCESS_IP}:9101，WebSocket ws://${ACCESS_IP}:9000/ws"
  else
    echo "  未能自动识别虚拟机 IP；执行 hostname -I 后，用其中的网卡 IP 访问 9010"
  fi
  echo "  Logic metrics：http://127.0.0.1:9101/metrics"
  echo "  Job metrics：http://127.0.0.1:9202/metrics"
  echo "  Comet metrics：http://127.0.0.1:9203/metrics"
  if hermes_enabled; then
    echo "  Pi Agent Bot：900000000001（在单聊页面输入该用户 ID）"
    echo "  Agent Bridge 日志：$ROOT/logs/hermes_bridge.out"
    echo "  Pi Gateway 日志：$ROOT/logs/pi_gateway.out"
  fi
  echo "  停止完整栈：./scripts/sparkctl.sh down"
  echo "  仅停止 Spark 业务：bash scripts/stop_demo.sh"
  echo
}

monitor_foreground() {
  trap 'echo; echo "收到停止信号，正在停止业务进程..."; bash "$ROOT/scripts/stop_demo.sh"; exit 0' INT TERM
  while true; do
    local name pid log_name
    local names=(logic_server comet_server job_server web_demo_server)
    if hermes_enabled; then names+=(hermes_bridge); fi
    for name in "${names[@]}"; do
      if [[ ! -f "$ROOT/.run/${name}.pid" ]]; then
        echo "$name PID 文件不存在，服务可能已退出" >&2
        bash "$ROOT/scripts/stop_demo.sh" || true
        exit 1
      fi
      pid="$(sed -n '1p' "$ROOT/.run/${name}.pid")"
      if ! kill -0 "$pid" 2>/dev/null; then
        echo "$name(pid=$pid) 已退出，最近日志：" >&2
        log_name="${name//_server/}"
        tail -n 30 "$ROOT/logs/${log_name}.out" >&2 || true
        bash "$ROOT/scripts/stop_demo.sh" || true
        exit 1
      fi
    done
    sleep 2
  done
}

require_command curl
prepare_dependencies
prepare_build
start_services

if (( FOREGROUND )); then
  monitor_foreground
fi
