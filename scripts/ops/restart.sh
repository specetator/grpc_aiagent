#!/usr/bin/env bash
set -euo pipefail

OPS_DIR="$(cd "$(dirname "$0")" && pwd)"

usage() {
  cat <<'EOF'
用法：scripts/sparkctl.sh restart [--fast] [--skip-deps] [--foreground]

停止 Spark 和本机 Hermes 后重新启动；Docker 数据依赖保持不删除。
EOF
}

for arg in "$@"; do
  case "$arg" in
    --fast|--skip-build|--skip-deps|--foreground) ;;
    -h|--help) usage; exit 0 ;;
    *) echo "未知选项：$arg" >&2; usage >&2; exit 2 ;;
  esac
done

bash "$OPS_DIR/down.sh"
exec bash "$OPS_DIR/up.sh" "$@"
