#!/usr/bin/env bash
set -euo pipefail
project=/data/home/zylu/llumnix-v018-main
export PATH=/data/home/zylu/.conda/envs/llumnix5090_vllm018/bin:$PATH
export PYTHONPATH=/data/home/zylu/llumnix-kvt500-2gpu/runtime_overlay:$project/python/discovery:$project/python:$project
cd "$project"
exec python -u tools/two_gpu_power_prepare.py "$@"
