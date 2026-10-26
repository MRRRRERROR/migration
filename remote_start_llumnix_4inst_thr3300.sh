#!/usr/bin/env bash
set -eo pipefail

source /opt/miniconda3/etc/profile.d/conda.sh
conda activate llumnix5090_cuda13
cd /data/home/zylu/llumnix-ray-main

pkill -f "llumnix.entrypoints.vllm_v1.api_server" || true
pkill -f "benchmark/benchmark_serving.py" || true
ray stop --force || true
sleep 5

mkdir -p /data/home/zylu/ray_tmp
mkdir -p /data/home/zylu/llumnix-ray-main/5090_results_0719_migration_thr3300

export RAY_TMPDIR=/data/home/zylu/ray_tmp
unset HEAD_NODE_IP
unset VLLM_HOST_IP

export CUDA_HOME="$CONDA_PREFIX"
export PATH="$CUDA_HOME/bin:$PATH"
export LD_LIBRARY_PATH="$CUDA_HOME/lib:$CUDA_HOME/lib64:$CONDA_PREFIX/lib:${LD_LIBRARY_PATH:-}"
export VLLM_USE_FLASHINFER_SAMPLER=0
export CUDA_VISIBLE_DEVICES=0,1,2,3
export LLUMNIX_REMAININGSTEPS_BUSY_THRESHOLD=3300

nohup python -u -m llumnix.entrypoints.vllm_v1.api_server \
  --config-file configs/vllm.yml \
  --port 1234 \
  --ray-cluster-port 6389 \
  --initial-instances 4 \
  --model /data/models/Qwen2.5-7B-Instruct \
  --trust-remote-code \
  --max-model-len 1024 \
  --max-num-batched-tokens 2048 \
  --max-num-seqs 16 \
  --gpu-memory-utilization 0.65 \
  --dispatch-policy flood \
  --enable-routine-migration \
  --pair-migration-frequency 1 \
  --pair-migration-policy defrag \
  --migrate-out-threshold 3300 \
  --enable-engine-migration-interface \
  --migration-backend rayrpc \
  --migration-buffer-blocks 32 \
  --request-migration-policy SR \
  --log-instance-info \
  --log-filename qwen_5090_4inst_migration_engine_if_thr3300_0719.log \
  --attention-backend flashinfer \
  --enforce-eager \
  > qwen_5090_4inst_migration_engine_if_thr3300_0719.console.log 2>&1 &

echo "$!" > /data/home/zylu/llumnix-ray-main/qwen_5090_4inst_migration_engine_if_thr3300_0719.pid
echo "started pid $(cat /data/home/zylu/llumnix-ray-main/qwen_5090_4inst_migration_engine_if_thr3300_0719.pid)"
