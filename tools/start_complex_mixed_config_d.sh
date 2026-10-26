#!/usr/bin/env bash
set -eo pipefail

cd /data/home/zylu/llumnix-v018-main
source /opt/miniconda3/etc/profile.d/conda.sh
conda activate llumnix5090_vllm018

export COMPLEX_CONFIG_D_ROOT="${COMPLEX_CONFIG_D_ROOT:-/data/home/zylu/llumnix-v018-main/results/complex_mixed_high_load/config_d/run_20260905_153725}"
export COMPLEX_CONFIG_D_TRACE="${COMPLEX_CONFIG_D_TRACE:-/data/home/zylu/llumnix-v018-main/results/complex_mixed_high_load/config_d/complex_mixed_high_load_config_d.jsonl}"
export PAPER_SUPPLEMENT_ALLOW_CONTENDED=1
export MIGRATION_POLICY_ALLOW_CONTENDED=1

mkdir -p "$COMPLEX_CONFIG_D_ROOT/04_step_summaries"
python -u tools/run_complex_mixed_config_d.py --phase all \
  2>&1 | tee -a "$COMPLEX_CONFIG_D_ROOT/04_step_summaries/config_d_console.log"
