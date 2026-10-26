#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

export LLUMNIX_REPO="${LLUMNIX_REPO:-$HOME/llumnix-ray-main}"
export LLUMNIX_VENV="${LLUMNIX_VENV:-$HOME/venvs/llumnix-vllm063}"
export MODEL_PATH="${MODEL_PATH:-$HOME/models/opt-125m}"
export CUDA_VISIBLE_DEVICES="${CUDA_VISIBLE_DEVICES:-1,2}"
export INITIAL_INSTANCES="${INITIAL_INSTANCES:-2}"
export MAX_MODEL_LEN="${MAX_MODEL_LEN:-512}"
export MAX_NUM_BATCHED_TOKENS="${MAX_NUM_BATCHED_TOKENS:-1024}"
export GPU_MEMORY_UTILIZATION="${GPU_MEMORY_UTILIZATION:-0.5}"
export MIGRATION_BACKEND="${MIGRATION_BACKEND:-rayrpc}"
export PORT="${PORT:-1234}"
export RAY_PORT="${RAY_PORT:-6389}"

IFS=',' read -r -a gpu_ids <<< "$CUDA_VISIBLE_DEVICES"
if [[ "${#gpu_ids[@]}" -ne "$INITIAL_INSTANCES" ]]; then
  echo "CUDA_VISIBLE_DEVICES must list one GPU per instance." >&2
  exit 1
fi

for gpu_id in "${gpu_ids[@]}"; do
  used_mib="$({
    nvidia-smi -i "$gpu_id" \
      --query-compute-apps=used_memory \
      --format=csv,noheader,nounits 2>/dev/null || true
  } | awk '{sum += $1} END {print sum + 0}')"
  if (( used_mib > 256 )); then
    echo "GPU $gpu_id is busy (${used_mib} MiB compute memory); refusing to start." >&2
    exit 2
  fi
done

exec bash "$SCRIPT_DIR/start_2gpu_service.sh"
