#!/usr/bin/env bash
# Product entry point; Pi remains the runtime dependency and compatible state home.
set -euo pipefail
exec bash "$(dirname "$0")/setup_pi_agent.sh" "$@"
