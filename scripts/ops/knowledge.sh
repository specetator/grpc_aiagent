#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "$0")" && pwd)/common.sh"

SYNC=0
FULL=0
SKIP_EVAL=0
SOURCE_ID=''

usage() {
  cat <<'EOF'
用法：scripts/sparkctl.sh knowledge [选项]

默认执行 ingest → build → verify → eval。索引按 generation 原子切换，
Hermes 下次工具调用会自动读取新 generation，不需要重启。

选项：
  --source ID    只重新摄入指定来源，保留其他来源
  --sync         摄入前同步来源；Git 来源使用匿名非交互模式
  --full         显式执行全量摄入
  --skip-eval    跳过固定评测集
  -h, --help     显示帮助
EOF
}

while (($#)); do
  case "$1" in
    --source)
      [[ $# -ge 2 ]] || { echo '缺少 source ID' >&2; exit 2; }
      SOURCE_ID="$2"; shift 2 ;;
    --sync) SYNC=1; shift ;;
    --full) FULL=1; shift ;;
    --skip-eval) SKIP_EVAL=1; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "未知参数：$1" >&2; usage >&2; exit 2 ;;
  esac
done

RAG="$KNOWLEDGE_ROOT/bin/cann-rag"
[[ -x "$RAG" ]] || { echo "找不到 cann-rag：$RAG" >&2; exit 1; }

cd "$KNOWLEDGE_ROOT"
if (( SYNC )); then
  if [[ -n "$SOURCE_ID" ]]; then
    "$RAG" source sync "$SOURCE_ID"
  else
    "$RAG" source sync
  fi
fi

ingest_args=()
[[ -n "$SOURCE_ID" ]] && ingest_args+=(--source "$SOURCE_ID")
(( FULL )) && ingest_args+=(--full)
"$RAG" ingest "${ingest_args[@]}"
"$RAG" build
"$RAG" verify
(( SKIP_EVAL )) || "$RAG" eval
"$RAG" status
