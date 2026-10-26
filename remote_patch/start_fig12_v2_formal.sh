#!/bin/bash
set -e
source /opt/miniconda3/etc/profile.d/conda.sh
conda activate llumnix5090_vllm018
cd /data/home/zylu/llumnix-v018-main
export PAPER_SUPPLEMENT_ROOT=/data/home/zylu/llumnix-v018-main/results/5090_paper_supplement_20260821_170651
export PAPER_SUPPLEMENT_ALLOW_CONTENDED=1
mkdir -p "$PAPER_SUPPLEMENT_ROOT/04_step_summaries"
python -u run_figure12_v2.py --phase formal 2>&1 | tee "$PAPER_SUPPLEMENT_ROOT/04_step_summaries/fig12_v2_formal_console.log"
