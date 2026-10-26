#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=tools/real_migration/llumnix_experiment_env.sh
source "$SCRIPT_DIR/llumnix_experiment_env.sh"

if [[ -f "$LLUMNIX_VENV/bin/activate" ]]; then
  source "$LLUMNIX_VENV/bin/activate"
fi

if [[ -f "$EXP_DIR/service.pid" ]]; then
  pid="$(cat "$EXP_DIR/service.pid")"
  if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
    echo "Stopping service PID $pid..."
    kill "$pid" || true
    sleep 3
  fi
fi

echo "Stopping Ray..."
ray stop || true

echo "Done."

