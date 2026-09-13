#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

usage() {
  cat <<'EOF'
Spark Push 本地运维入口

用法：scripts/sparkctl.sh <命令> [选项]

命令：
  up          启动 Hermes（启用时）、依赖和 Spark 服务
  down        停止 Spark 和本机 Hermes，默认保留 Docker 依赖
  restart     重启完整应用栈
  status      查看进程、容器和访问地址
  health      执行严格健康检查，失败时返回非零
  logs        查看或跟踪服务日志
  knowledge   更新并验证 CANN RAG generation
  doctor      启动前检查命令、配置、Docker、Hermes 和知识索引
  help        显示本帮助

常用示例：
  ./scripts/sparkctl.sh doctor
  ./scripts/sparkctl.sh up
  ./scripts/sparkctl.sh up --fast
  ./scripts/sparkctl.sh status
  ./scripts/sparkctl.sh logs bridge -f
  ./scripts/sparkctl.sh knowledge --source custom-docs
  ./scripts/sparkctl.sh restart --fast
  ./scripts/sparkctl.sh down --with-deps

可选环境变量：
  SPARK_PUSH_ENV_FILE        环境文件，默认项目根目录 .env.local
  SPARK_PUSH_BUILD_DIR       构建目录，默认项目根目录 build
  SPARK_PUSH_HERMES_COMMAND  独立 WSL Hermes 启动器绝对路径
  CANN_KNOWLEDGE_ROOT        CANN RAG 工作区路径
EOF
}

command_name="${1:-help}"
shift || true
case "$command_name" in
  up|start) exec bash "$SCRIPT_DIR/ops/up.sh" "$@" ;;
  down|stop) exec bash "$SCRIPT_DIR/ops/down.sh" "$@" ;;
  restart) exec bash "$SCRIPT_DIR/ops/restart.sh" "$@" ;;
  status) exec bash "$SCRIPT_DIR/ops/status.sh" "$@" ;;
  health) exec bash "$SCRIPT_DIR/ops/health.sh" "$@" ;;
  logs|log) exec bash "$SCRIPT_DIR/ops/logs.sh" "$@" ;;
  knowledge|kb) exec bash "$SCRIPT_DIR/ops/knowledge.sh" "$@" ;;
  doctor) exec bash "$SCRIPT_DIR/ops/doctor.sh" "$@" ;;
  help|-h|--help) usage ;;
  *) echo "未知命令：$command_name" >&2; usage >&2; exit 2 ;;
esac
