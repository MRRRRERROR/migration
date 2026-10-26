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
export RESULT_DIR=/data/home/zylu/llumnix-v018-main/minimal_single_after_hybrid_modules_constants_${ts}_logs
run_log=/data/home/zylu/llumnix-v018-main/minimal_single_after_hybrid_modules_constants_${ts}.log

python remote_single_then_optional5.py 2>&1 | tee "$run_log"

{
  echo
  echo "## 0721 single after hybrid_modules import cleanup"
  echo "- 当前阶段: 第一阶段，普通单请求稳定"
  echo "- 运行命令: python remote_single_then_optional5.py"
  echo "- 结果目录: $RESULT_DIR"
  echo "- run log: $run_log"
  echo "- raw response / benchmark summary:"
  grep -E "SINGLE_|BATCH5_" "$run_log" || true
  echo "- 第一条 fatal traceback:"
  grep -nA70 -m1 "Traceback" logs/vllm_0_minimal.log || true
  echo "- 是否已经产出可用实验数据: 否"
  echo "- 下一步计划: 基于本轮第一条 fatal 继续处理"
} >> AI_HANDOFF.md

