#!/usr/bin/env bash
# Unified Spark Push regression: build, protocol tests, optional browser/E2E.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
BUILD_DIR="${SPARK_PUSH_BUILD_DIR:-$ROOT/build-wsl}"
if [[ ! -d "$BUILD_DIR" ]]; then
  BUILD_DIR="${SPARK_PUSH_BUILD_DIR:-$ROOT/build}"
fi

echo "== compile =="
cmake --build "$BUILD_DIR" -j"${SPARK_PUSH_JOBS:-2}"

echo "== protocol / unit tests =="
ctest --test-dir "$BUILD_DIR" --output-on-failure
PYTHONPATH="$ROOT/cannbot/scripts" python3 "$ROOT/cannbot/scripts/test_agent_router.py"
PYTHONPATH="$ROOT/cannbot/scripts" python3 "$ROOT/cannbot/scripts/test_pi_gateway.py"
python3 "$ROOT/tests/client_reliability_contract_test.py"
python3 "$ROOT/cannbot/scripts/validate_knowledge.py" --strict

if [[ "${SPARK_PUSH_RUN_BROWSER:-0}" == "1" ]]; then
  echo "== browser protocol tests =="
  node "$ROOT/tests/agent_channel_browser_test.cjs"
fi

if [[ "${SPARK_PUSH_RUN_E2E:-0}" == "1" ]]; then
  echo "== isolated-account E2E =="
  echo "Set SPARK_PUSH_E2E_SENDER / SPARK_PUSH_E2E_RECEIVER and run load_test/e2e_bench."
  "$BUILD_DIR/load_test/e2e_bench" \
    --sender-account "${SPARK_PUSH_E2E_SENDER:?}" \
    --sender-password "${SPARK_PUSH_E2E_SENDER_PASSWORD:?}" \
    --receiver-account "${SPARK_PUSH_E2E_RECEIVER:?}" \
    --receiver-password "${SPARK_PUSH_E2E_RECEIVER_PASSWORD:?}" \
    --connections 2 --messages-per-conn 10 --timeout-ms 30000
fi

echo "regress ok"
