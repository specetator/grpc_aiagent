#!/usr/bin/env bash
set -euo pipefail
source "$(cd "$(dirname "$0")" && pwd)/common.sh"

usage() {
  cat <<'EOF'
用法：scripts/sparkctl.sh media <命令> [选项]

  describe --config /绝对路径/worker.json   核对配置指纹，不连接 Logic
  run      --config /绝对路径/worker.json   前台运行已有媒体 worker
  start    --config /绝对路径/worker.json   启动 systemd 用户服务，失败时有限重启
  stop                                    正常停止本项目创建的服务，保留恢复状态
  status                                  查看进程状态；active 不代表后端授权已通过
  logs                                    查看最近 50 条服务日志

选项：--unit spark-media-worker[-名称]      独立实例名，默认 spark-media-worker
环境：SPARK_PUSH_MEDIA_PYTHON              Python 可执行文件，默认 python3

配置必须是服务账号拥有的 0600 JSON。不会读取 .env.local 或私人 Agent 配置。
start 创建临时用户服务，不安装开机启动；每个实例须使用独立 state_dir。
EOF
}

action="${1:-help}"; shift || true
unit=spark-media-worker
config_file=
while (($#)); do
  case "$1" in
    --config) [[ $# -ge 2 ]] || { usage >&2; exit 2; }; config_file="$2"; shift 2 ;;
    --unit) [[ $# -ge 2 ]] || { usage >&2; exit 2; }; unit="$2"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) usage >&2; exit 2 ;;
  esac
done
[[ "$unit" =~ ^spark-media-worker(-[a-z0-9_-]{1,32})?$ ]] || { echo '无效媒体服务名称' >&2; exit 2; }
identity="Spark Push media worker: $PROJECT_ROOT"
worker="$PROJECT_ROOT/cannbot/scripts/media_worker.py"

owned_unit() {
  local description
  description="$(systemctl --user show "$unit.service" --property=Description --value 2>/dev/null || true)"
  [[ "$description" == "$identity" ]] || { echo '该名称下没有本项目创建的媒体服务，未操作其他进程。' >&2; return 1; }
}

case "$action" in
  describe|run|start)
    [[ "$config_file" == /* ]] || { echo '请提供 --config 的绝对路径' >&2; exit 2; }
    python_bin="$(command -v "${SPARK_PUSH_MEDIA_PYTHON:-python3}")"
    [[ -x "$python_bin" ]] || { echo 'Python 不可执行' >&2; exit 2; }
    if [[ "$action" == describe ]]; then exec "$python_bin" "$worker" --config "$config_file" --describe; fi
    if [[ "$action" == run ]]; then exec "$python_bin" "$worker" --config "$config_file"; fi
    "$python_bin" "$worker" --config "$config_file" --describe >/dev/null
    # systemd owns process identity/signals/restart; no pidfile or broad pkill.
    systemd-run --user --unit="$unit" --description="$identity" --collect \
      --service-type=exec --property=Restart=on-failure --property=RestartSec=10 \
      --property=StartLimitIntervalSec=120 --property=StartLimitBurst=3 \
      --property=UMask=0077 --property=WorkingDirectory="$PROJECT_ROOT" \
      --property=KillMode=mixed --property=TimeoutStopSec=90 \
      --setenv=PYTHONDONTWRITEBYTECODE=1 --setenv=PYTHONUNBUFFERED=1 \
      "$python_bin" "$worker" --config "$config_file"
    echo '媒体 worker 已交给用户进程管理器；请通过 status / logs 查看实际状态。'
    ;;
  stop) owned_unit; systemctl --user stop "$unit.service" ;;
  status) owned_unit; systemctl --user status "$unit.service" --no-pager ;;
  logs) journalctl --user --unit="$unit.service" --lines=50 --no-pager ;;
  help|-h|--help) usage ;;
  *) usage >&2; exit 2 ;;
esac
