#!/usr/bin/env bash
set -euo pipefail

MODEL_PATH="${MODEL_PATH:-$HOME/models/Qwen2.5-7B-Instruct}"
MODEL_URL="${MODEL_URL:-http://10.112.184.62:39090/}"
LOG_PATH="${LOG_PATH:-$HOME/llumnix_v018_setup_logs/model_transfer.log}"

mkdir -p "$MODEL_PATH" "$(dirname "$LOG_PATH")"
wget -c -r -np -nH \
  --exclude-directories=.cache \
  --reject='index.html*' \
  --directory-prefix="$MODEL_PATH" \
  "$MODEL_URL" \
  2>&1 | tee -a "$LOG_PATH"
