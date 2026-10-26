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
fuser -k -9 6379/tcp 8100/tcp 18088/tcp 18089/tcp 31100/tcp 36218/tcp 2>/dev/null || true
sleep 3

export CUDA_VISIBLE_DEVICES=0,1,2,3
export CUDA_HOME=$CONDA_PREFIX
export PATH=$CUDA_HOME/bin:$PATH
export LD_LIBRARY_PATH=$CUDA_HOME/lib:$CUDA_HOME/lib64:$CONDA_PREFIX/lib:/data/home/zylu/opt/barex/usr/lib:${LD_LIBRARY_PATH:-}
export MODEL_NAME=Qwen/Qwen2.5-7B-Instruct
export MODEL_PATH=/data/models/Qwen2.5-7B-Instruct
export VLLM_USE_V1=1

ts=$(date +%Y%m%d_%H%M%S)
export RESULT_DIR=/data/home/zylu/llumnix-v018-main/results/ordinary_multireq_stability_${ts}
run_log=/data/home/zylu/llumnix-v018-main/ordinary_multireq_stability_${ts}.log

mkdir -p "$RESULT_DIR"
python remote_multireq_stability.py 2>&1 | tee "$run_log"
cp "$run_log" "$RESULT_DIR/run.log"

grep -E "BATCH(10|20|50)_(OK|NONEMPTY|FIRST_FATAL)" "$run_log" || true
