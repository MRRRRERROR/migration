#!/usr/bin/env bash
set -euo pipefail

repo=/data/home/zylu/llumnix-v018-main
log="$repo/results/single_gpu_natural_drain_eos_active/experiment.log"
cd "$repo"
exec /data/home/zylu/venvs/llumnix5090/bin/python tools/single_gpu_natural_drain.py \
  --dataset /data/huxin/docker_data/eassy-method2/LLMServingSim/workloads/sharegpt-qwen3-32b-300-sps10.jsonl \
  --formal-duration 180 \
  > "$log" 2>&1
