#!/usr/bin/env bash
set -u

source "$(cd "$(dirname "$0")" && pwd)/common.sh"

if (($#)); then
  case "$1" in
    -h|--help)
      cat <<'EOF'
用法：scripts/sparkctl.sh doctor

只读检查本机命令、环境配置、Docker、Pi Agent 和 CANN 知识索引。
EOF
      exit 0
      ;;
    *) echo "doctor 不接受参数：$1" >&2; exit 2 ;;
  esac
fi
load_project_env

failures=0
pass() { printf '[PASS] %s\n' "$1"; }
fail() { printf '[FAIL] %s\n' "$1"; failures=$((failures + 1)); }
warn() { printf '[WARN] %s\n' "$1"; }

for command_name in bash cmake curl docker; do
  if command -v "$command_name" >/dev/null 2>&1; then
    pass "command: $command_name"
  else
    fail "missing command: $command_name"
  fi
done

if [[ -f "$ENV_FILE" ]]; then
  pass "environment file exists"
else
  fail "missing environment file: $ENV_FILE"
fi

if [[ -n "${SPARK_PUSH_MYSQL_PASSWORD:-}" &&
      "${SPARK_PUSH_MYSQL_PASSWORD:-}" != 'replace-with-a-local-password' ]]; then
  pass 'MySQL password is configured (value hidden)'
else
  fail 'MySQL password is not configured'
fi

if command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1; then
  pass 'Docker daemon is reachable'
else
  fail 'Docker daemon is not reachable'
fi

if hermes_enabled; then
  [[ -n "${SPARK_PUSH_HERMES_BASE_URL:-}" ]] &&
    pass 'Pi gateway base URL is configured' || fail 'Pi gateway base URL is missing'
  [[ -n "${SPARK_PUSH_HERMES_API_KEY:-}" ]] &&
    pass 'Pi gateway API key is configured (value hidden)' || fail 'Pi gateway API key is missing'
  if hermes_is_local; then
    resolve_pi_command >/dev/null 2>&1 &&
      pass 'local pi-spark-agent command found' || fail 'local Pi Agent command not found'
  else
    warn 'Pi endpoint is remote; lifecycle will not be managed locally'
  fi
else
  warn 'Pi Agent integration is disabled'
fi

if [[ -x "$KNOWLEDGE_ROOT/bin/cann-rag" ]]; then
  pass 'cann-rag command found'
  if "$KNOWLEDGE_ROOT/bin/cann-rag" verify >/dev/null 2>&1; then
    pass 'CANN knowledge index verifies successfully'
  else
    fail 'CANN knowledge index verification failed'
  fi
else
  fail "cann-rag not found: $KNOWLEDGE_ROOT/bin/cann-rag"
fi

if [[ -x "$BUILD_DIR/logic/logic_server" ]]; then
  pass 'existing build artifacts found'
else
  warn 'build artifacts missing; sparkctl up will compile them'
fi

if (( failures )); then
  echo "预检失败项：$failures" >&2
  exit 1
fi
echo '运维预检通过。'
