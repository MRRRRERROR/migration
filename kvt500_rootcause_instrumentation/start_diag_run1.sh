#!/usr/bin/env bash
set -eo pipefail

PROJECT=/data/home/zylu/llumnix-kvt500-diag/project
OVERLAY=/data/home/zylu/llumnix-kvt500-diag/runtime_overlay
ROOT=/data/home/zylu/llumnix-v018-main/results/kvt500_rootcause_confirm/run_20260923_163645
PYTHON=/data/home/zylu/.conda/envs/llumnix5090_vllm018/bin/python

cd "$PROJECT"
source /opt/miniconda3/etc/profile.d/conda.sh
conda activate llumnix5090_vllm018
set -u
export PYTHONPATH="$OVERLAY:$PROJECT/python/discovery:$PROJECT/python:$PROJECT"
export COMPLEX_CONFIG_E_C32_ROOT="$ROOT/runs/run_1"
export COMPLEX_CONFIG_E_TRACE=/data/home/zylu/llumnix-v018-main/results/complex_mixed_high_load/config_e/complex_mixed_high_load_config_e.jsonl
export KVT500_DIAG_LOG="$ROOT/kvt500_block_lifecycle.jsonl"
export PAPER_SUPPLEMENT_ALLOW_CONTENDED=1
export MIGRATION_POLICY_ALLOW_CONTENDED=1
export MIGRATION_POLICY_GPU_MEMORY_UTILIZATION=0.50
export VLLM_HOST_IP=127.0.0.1

set +e
"$PYTHON" -u tools/run_complex_mixed_config_e_c32.py \
  --phase run --policy tpot_tail --repeat 1 \
  2>&1 | tee "$ROOT/logs/tpot_tail_run1_console.log"
status=${PIPESTATUS[0]}
set -e
printf '%s\n' "$status" > "$ROOT/logs/tpot_tail_run1.exitcode"
exit "$status"
