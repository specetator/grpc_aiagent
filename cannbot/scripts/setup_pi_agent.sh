#!/usr/bin/env bash
# Compatibility installer for Kernel Agent, powered by the Pi runtime.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
PI_HOME="${PI_CODING_AGENT_DIR:-$HOME/.pi-spark-agent}"
NODE_BIN="$HOME/.local/node/bin"
LAUNCHER="$HOME/.local/bin/kernel-agent"

mkdir -p "$PI_HOME" "$HOME/.local/bin"
export PATH="$NODE_BIN:$HOME/.local/bin:$PATH"

if ! command -v pi >/dev/null 2>&1; then
  echo "未找到 pi。请先安装 Linux Node 和 @earendil-works/pi-coding-agent。" >&2
  echo "示例：npm install -g --ignore-scripts @earendil-works/pi-coding-agent" >&2
  exit 1
fi

if [[ ! -f "$PI_HOME/.env" ]]; then
  umask 077
  printf '%s\n' \
    '# Kernel Agent runtime environment. Do not commit.' \
    'CLI_RELAY_API_KEY=' \
    >"$PI_HOME/.env"
  chmod 600 "$PI_HOME/.env"
  echo "已创建 $PI_HOME/.env（请填写 CLI_RELAY_API_KEY）"
fi

python3 "$ROOT/cannbot/scripts/sync_pi_agent.py" --install --replace --pi-home "$PI_HOME"

cat >"$LAUNCHER" <<EOF
#!/usr/bin/env bash
set -euo pipefail
export PI_CODING_AGENT_DIR="${PI_HOME}"
export PATH="${NODE_BIN}:\$HOME/.local/bin:\$PATH"
export CANN_KNOWLEDGE_ROOT="\${CANN_KNOWLEDGE_ROOT:-$ROOT/../cann-agent-knowledge}"
ROOT="\${SPARK_PUSH_KERNEL_CWD:-\${SPARK_PUSH_PI_CWD:-$ROOT}}"
if [[ -f "\$PI_CODING_AGENT_DIR/.env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source "\$PI_CODING_AGENT_DIR/.env"
  set +a
fi
if [[ -f "\$ROOT/.env.local" ]]; then
  set -a
  # shellcheck disable=SC1091
  source "\$ROOT/.env.local"
  set +a
fi
export SPARK_PUSH_HERMES_API_KEY="\${SPARK_PUSH_HERMES_API_KEY:-\${SPARK_PUSH_PI_API_KEY:-\${API_SERVER_KEY:-}}}"
export SPARK_PUSH_PI_API_KEY="\${SPARK_PUSH_PI_API_KEY:-\$SPARK_PUSH_HERMES_API_KEY}"
cd "\$ROOT"
if [[ "\${1:-}" == "gateway" ]]; then
  shift
  exec python3 "$ROOT/cannbot/scripts/pi_gateway.py" "\$@"
fi
exec pi --append-system-prompt "$ROOT/cannbot/pi/spark-chat-system-prompt.md" "\$@"
EOF
chmod 755 "$LAUNCHER"
cat >"$HOME/.local/bin/pi-spark-agent" <<'EOF'
#!/usr/bin/env bash
# Legacy entry point; keep existing deployments and scripts working.
exec "$(dirname "$0")/kernel-agent" "$@"
EOF
chmod 755 "$HOME/.local/bin/pi-spark-agent"
echo "启动器：$LAUNCHER"
echo "交互式：cd $ROOT && kernel-agent"
echo "API：   kernel-agent gateway"
echo "配置目录：$PI_HOME"
