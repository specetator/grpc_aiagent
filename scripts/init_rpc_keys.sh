#!/usr/bin/env bash
# Generate local workload credentials without printing them or overwriting existing ones.
set -euo pipefail
rpc_root="$(cd "$(dirname "$0")/.." && pwd)"
rpc_file="${SPARK_RPC_ENV_FILE:-$rpc_root/.env.rpc}"
umask 077
if [[ -e "$rpc_file" ]]; then
  echo "RPC credential file already exists: $rpc_file"
  exit 0
fi
python3 - "$rpc_file" <<'PY'
import os
import secrets
import sys
fd = os.open(sys.argv[1], os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
with os.fdopen(fd, "w") as out:
    for role in ("COMET", "JOB", "ADMIN"):
        out.write(f"SPARK_RPC_{role}_KEY={secrets.token_hex(32)}\n")
PY
echo "Created local RPC credential file: $rpc_file"
