#!/bin/bash
set -o pipefail

source /opt/miniconda3/etc/profile.d/conda.sh
conda activate llumnix5090_vllm018
cd /data/home/zylu/llumnix-v018-main

export PAPER_SUPPLEMENT_ROOT=/data/home/zylu/llumnix-v018-main/results/5090_paper_supplement_20260821_170651
export PAPER_SUPPLEMENT_ALLOW_CONTENDED=1

python -u /tmp/fig12_v2_single_run.py \
  --policy tpot_tail \
  --repeat 2 \
  --run-group retry_tpot_run2_once \
  2>&1 | tee -a \
  "$PAPER_SUPPLEMENT_ROOT/04_step_summaries/fig12_v2_tpot_run2_retry_once_console.log"
