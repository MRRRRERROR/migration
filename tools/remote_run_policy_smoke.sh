#!/usr/bin/env bash
set -eo pipefail
source /opt/miniconda3/etc/profile.d/conda.sh
conda activate llumnix5090_vllm018
cd /data/home/zylu/llumnix-v018-main

export CUDA_VISIBLE_DEVICES=0,1,2,3
export CUDA_HOME="$CONDA_PREFIX"
export PATH="$CUDA_HOME/bin:$PATH"
export LD_LIBRARY_PATH="$CUDA_HOME/lib:$CUDA_HOME/lib64:$CONDA_PREFIX/lib:${LD_LIBRARY_PATH:-}"
export VLLM_USE_V1=1
export VLLM_HOST_IP=127.0.0.1
export MIGRATION_POLICY_ROOT=/data/home/zylu/llumnix-v018-main/results/5090_migration_policy_baselines_20260819_201922
export MIGRATION_POLICY_RUN_KIND=smoke
export MIGRATION_POLICY_REQUEST_NUM=20
export MIGRATION_POLICY_CONCURRENCY=10
export MIGRATION_POLICY_REPEATS=1
export MIGRATION_POLICY_GPU_MEMORY_UTILIZATION=0.55
export MIGRATION_POLICY_SEED=20260819
export MIGRATION_POLICY_ALLOW_CONTENDED=1

mkdir -p "$MIGRATION_POLICY_ROOT/01_smoke"
python -u run_migration_policy_baselines.py smoke 2>&1 \
  | tee "$MIGRATION_POLICY_ROOT/01_smoke/smoke_all_policies.log"
