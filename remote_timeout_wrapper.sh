#!/usr/bin/env bash
set +e
chmod +x "$HOME/remote_run_third_baseline_timeout.sh"
timeout 300s "$HOME/remote_run_third_baseline_timeout.sh"
code=$?
cd "$HOME/llumnix-ray-main" || exit "$code"
source "$HOME/venvs/llumnix/bin/activate"
ray stop >/dev/null 2>&1 || true
exit "$code"
