#!/usr/bin/env bash
set -eo pipefail

ROOT=/data/home/zylu/llumnix-v018-main/results/5090_multi_baseline_500req_20260721_204013
REPO=/data/home/zylu/llumnix-v018-main

source /opt/miniconda3/etc/profile.d/conda.sh
conda activate llumnix5090_vllm018
cd "$REPO"

export MULTI_BASELINE_ROOT="$ROOT"
export MODEL_PATH=/data/models/Qwen2.5-7B-Instruct
export CUDA_VISIBLE_DEVICES=0,1,2,3
export VLLM_HOST_IP=127.0.0.1

nohup python -u "$ROOT/00_config/run_multi_baseline_500req.py" all \
  >> "$ROOT/formal_console_resume_20260721_2111.log" 2>&1 < /dev/null &
echo "$!" > "$ROOT/runner.pid"
echo "runner_pid=$!"
