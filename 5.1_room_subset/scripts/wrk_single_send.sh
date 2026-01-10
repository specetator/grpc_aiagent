#!/usr/bin/env bash
set -euo pipefail

# 一键 wrk 压测 /api/message/send：
# - 自动注册/登录两个用户（sender + target），并导出 token/target_id 给 wrk Lua 脚本
# - 启动 wrk 对 `/api/message/send` 发起并发压测（single_chat 场景）
#
# 设计要点（中文注释说明）：
# - 脚本负责准备测试账户（避免手动准备脏数据），并注入运行时环境变量供 Lua 使用
# - 考虑账号已存在且密码不匹配的历史脏数据场景，会自动添加后缀重试一次
# - 尽量保持对外无副作用：生成的账号仅用于本次运行，不会持久影响压测口径
#
# 依赖：
# - curl
# - python3（用于解析 JSON）
# - wrk
#
# 用法：
#   bash scripts/wrk_single_send.sh
#   BASE_URL=http://127.0.0.1:9101 THREADS=8 CONNS=200 DURATION=10s bash scripts/wrk_single_send.sh

BASE_URL="${BASE_URL:-http://127.0.0.1:9101}"
THREADS="${THREADS:-8}"
CONNS="${CONNS:-200}"
DURATION="${DURATION:-10s}" 
SEED="${SEED:-$RANDOM}"

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LUA_SCRIPT="${ROOT_DIR}/scripts/lua_single_send.lua"

# need_cmd: 检查运行时依赖命令是否存在，缺失时给出友好提示并退出
need_cmd() {
  local c="$1"
  command -v "${c}" >/dev/null 2>&1 || {
    echo "缺少命令：${c}"
    if [[ "${c}" == "wrk" ]]; then
      echo "安装建议："
      echo "- Ubuntu/Debian: sudo apt-get update && sudo apt-get install -y wrk"
      echo "- 或者：先跑 bash scripts/build_wrk.sh（源码编译到 tools/ 目录），然后："
      echo "  WRK_BIN=\"${ROOT_DIR}/tools/wrk/wrk\" bash scripts/wrk_single_send.sh"
    fi
    exit 1
  }
}

need_cmd curl
need_cmd python3

# WRK_BIN 支持自定义路径：
# - 默认使用系统 PATH 中的 `wrk`
# - 若指定其他路径，请确保可执行权限（例如本仓库编译出的二进制）
WRK_BIN="${WRK_BIN:-wrk}"
if [[ "${WRK_BIN}" == "wrk" ]]; then
  need_cmd wrk
else
  if [[ ! -x "${WRK_BIN}" ]]; then
    echo "WRK_BIN 不可执行或不存在：${WRK_BIN}"
    exit 1
  fi
fi

# md5_hex: 从 stdin 读取原始字节并输出小写 32 位 md5（用于与服务端约定的密码字段兼容）
md5_hex() {
  # 生成 32 位小写 md5
  python3 -c 'import hashlib,sys; s=sys.stdin.buffer.read(); print(hashlib.md5(s).hexdigest())'
}

# json_get: 从 stdin 读取 JSON，按点路径提取字段（如 data.token 或 code），若不存在则打印空字符串
json_get() {
  local key="$1"
  python3 -c '
import json,sys
key=sys.argv[1]
j=json.loads(sys.stdin.read() or "{}")
def walk(o, path):
  cur=o
  for p in path.split("."):
    if isinstance(cur, dict) and p in cur:
      cur=cur[p]
    else:
      return None
  return cur
v=walk(j, key)
if v is None:
  print("")
elif isinstance(v, (dict, list)):
  print(json.dumps(v, ensure_ascii=False))
else:
  print(v)
' "${key}"
}

# post_json: 发送 HTTP POST JSON 请求到被测服务，返回原始响应体（stdout）
post_json() {
  local path="$1"
  local body="$2"
  curl -sS -H 'Content-Type: application/json' -d "${body}" "${BASE_URL}${path}"
}

# register_or_login: 对于给定 account/password（plain），尝试注册，若已存在则尝试登录
# - password 通过 md5_hex 转换为服务约定的密码字段
# - 若 register 返回 409（账号已存在），会执行 login
# - 若 login 返回 401（密码不匹配），认为可能是历史脏数据，脚本会自动为本次运行生成一个带后缀的新账号并重试一次
# - 成功时打印两列："user_id token"，调用方可使用 read 读取
register_or_login() {
  local account="$1"
  local pass_plain="$2"
  local pass_md5
  pass_md5="$(printf "%s" "${pass_plain}" | md5_hex)"

  # 允许在账号冲突且密码不匹配时自动换一个新账号，避免因历史脏数据导致脚本“看似成功”但 token/uid 为空。
  # 该 suffix 只影响本次运行，不影响压测口径。
  local try_suffix="${TRY_SUFFIX:-0}"

  local reg_body
  reg_body="$(python3 - <<PY
import json
print(json.dumps({"account":"${account}","password":"${pass_md5}","name":"${account}"}, ensure_ascii=False))
PY
)"
  local resp
  resp="$(post_json "/api/register" "${reg_body}")"
  local code
  code="$(printf "%s" "${resp}" | json_get "code")"
  if [[ "${code}" == "409" ]]; then
    # 账号已存在，尝试登录
    local login_body
    login_body="$(python3 - <<PY
import json
print(json.dumps({"account":"${account}","password":"${pass_md5}"}, ensure_ascii=False))
PY
)"
    resp="$(post_json "/api/login" "${login_body}")"
    code="$(printf "%s" "${resp}" | json_get "code")"
  fi

  if [[ "${code}" != "0" ]]; then
    # 常见情况：账号已存在但密码不匹配（历史遗留数据）。此时自动切一个新账号重试一次。
    if [[ "${try_suffix}" == "0" && "${code}" == "401" ]]; then
      local new_account="${account}_r$(date +%s)"
      TRY_SUFFIX=1 register_or_login "${new_account}" "${pass_plain}"
      return
    fi
    echo "register/login 失败：${resp}" >&2
    return 1
  fi

  local token user_id
  token="$(printf "%s" "${resp}" | json_get "data.token")"
  user_id="$(printf "%s" "${resp}" | json_get "data.user_id")"
  if [[ -z "${token}" || -z "${user_id}" ]]; then
    echo "register/login 返回缺少 token/user_id：${resp}" >&2
    return 1
  fi

  # 输出两列：user_id token（便于调用方 read 解析）
  printf "%s %s\n" "${user_id}" "${token}"
}

# 生成 sender/target 的账号与密码（含随机 SEED，方便并发多次运行时的隔离）
sender_account="wrk_sender_${SEED}"
target_account="wrk_target_${SEED}"
sender_pass="pass_${SEED}_a"
target_pass="pass_${SEED}_b"

# 注册或登录 sender/target，并读取返回的 user_id 与 token
sender_out="$(register_or_login "${sender_account}" "${sender_pass}")" || exit 1
target_out="$(register_or_login "${target_account}" "${target_pass}")" || exit 1
read -r sender_uid sender_token <<<"${sender_out}"
read -r target_uid _target_token <<<"${target_out}"

# 将 sender token 与 target id 导出为环境变量，供 wrk 的 Lua 脚本读取
export SPARKPUSH_WRK_TOKEN="${sender_token}"
export SPARKPUSH_WRK_TARGET_ID="${target_uid}"
export SPARKPUSH_WRK_PATH="${WRK_PATH:-/api/message/send}"

echo "[wrk] base=${BASE_URL} path=${SPARKPUSH_WRK_PATH} threads=${THREADS} conns=${CONNS} duration=${DURATION}"
echo "[wrk] sender_uid=${sender_uid} target_uid=${target_uid}"

# 使用 exec 替换当前 shell 进程为 wrk，确保信号转发和退出码由 wrk 决定
exec "${WRK_BIN}" -t"${THREADS}" -c"${CONNS}" -d"${DURATION}" --latency -s "${LUA_SCRIPT}" "${BASE_URL}"


