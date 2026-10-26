#!/usr/bin/env bash
set -euo pipefail
project=/data/home/zylu/llumnix-v018-main
export PATH=/data/home/zylu/.conda/envs/llumnix5090_vllm018/bin:$PATH
export PYTHONPATH=$project/tools/power_drain_observer:/data/home/zylu/llumnix-kvt500-2gpu/runtime_overlay:$project/python/discovery:$project/python:$project
export CUDA_VISIBLE_DEVICES=0,1
export VLLM_HOST_IP=127.0.0.1
export MODEL_PATH=/data/models/Qwen2.5-7B-Instruct
export KVT_LEASE_TRACE=1
export KVT_LEASE_RECHECK=1
export KVT_LEASE_FAULT=
export KVT500_TRACE_NATIVE=1
export LLUMNIX_ENABLE_STRUCTURED_MIGRATION_LOG=1
cd "$project"
exec python -u tools/two_gpu_power_drain.py "$@"
