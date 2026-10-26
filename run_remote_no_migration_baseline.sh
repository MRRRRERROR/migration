#!/usr/bin/env bash
set -eo pipefail

source /opt/miniconda3/etc/profile.d/conda.sh
conda activate llumnix5090_vllm018
cd /data/home/zylu/llumnix-v018-main

pkill -f "redis-server.*6379" || true
pkill -f "./bin/gateway" || true
pkill -f "./bin/scheduler" || true
pkill -f "vllm serve" || true
pkill -f "discovery.discovery" || true
pkill -f "benchmark_serving.py" || true
fuser -k -9 6379/tcp 8100/tcp 8101/tcp 8102/tcp 8103/tcp 18088/tcp 18089/tcp 31100/tcp 31101/tcp 31102/tcp 31103/tcp 36218/tcp 36226/tcp 36234/tcp 36242/tcp 2>/dev/null || true
sleep 5

export CUDA_VISIBLE_DEVICES=0,1,2,3
export CUDA_HOME=$CONDA_PREFIX
export PATH=$CUDA_HOME/bin:$PATH
export LD_LIBRARY_PATH=$CUDA_HOME/lib:$CUDA_HOME/lib64:$CONDA_PREFIX/lib:/data/home/zylu/opt/barex/usr/lib:${LD_LIBRARY_PATH:-}
export MODEL_NAME=Qwen/Qwen2.5-7B-Instruct
export MODEL_PATH=/data/models/Qwen2.5-7B-Instruct
export VLLM_USE_V1=1
export BASELINE_REQUESTS=${BASELINE_REQUESTS:-50}
export BASELINE_WORKERS=${BASELINE_WORKERS:-20}
export BASELINE_MAX_TOKENS=${BASELINE_MAX_TOKENS:-32}
export BASELINE_INSTANCES=${BASELINE_INSTANCES:-2,4}

ts=$(date +%Y%m%d_%H%M%S)
export RESULT_DIR=/data/home/zylu/llumnix-v018-main/results/baseline_no_migration_${ts}
run_log=/data/home/zylu/llumnix-v018-main/baseline_no_migration_${ts}.log

mkdir -p "$RESULT_DIR"
python remote_no_migration_baseline.py 2>&1 | tee "$run_log"
cp "$run_log" "$RESULT_DIR/run.log"

grep -E "BASELINE_[24]_SUMMARY|BASELINE_[24]_FATAL|RUNNER_EXCEPTION" "$run_log" || true
