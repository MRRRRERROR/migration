#!/bin/bash
set -o pipefail

policy="$1"
repeat_id="$2"
run_group="$3"
log_name="$4"

source /opt/miniconda3/etc/profile.d/conda.sh
conda activate llumnix5090_vllm018
cd /data/home/zylu/llumnix-v018-main

export PAPER_SUPPLEMENT_ROOT=/data/home/zylu/llumnix-v018-main/results/5090_paper_supplement_20260821_170651
export PAPER_SUPPLEMENT_ALLOW_CONTENDED=1

python -u /tmp/fig12_v2_single_run.py \
  --policy "$policy" \
  --repeat "$repeat_id" \
  --run-group "$run_group" \
  2>&1 | tee -a "$PAPER_SUPPLEMENT_ROOT/04_step_summaries/$log_name"
