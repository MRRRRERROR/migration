#!/usr/bin/env bash

# Shared defaults for the real migration smoke/benchmark scripts.
# Override any value by exporting it before running the scripts.

export LLUMNIX_REPO="${LLUMNIX_REPO:-$HOME/llumnix-ray-main}"
export LLUMNIX_VENV="${LLUMNIX_VENV:-$HOME/venvs/llumnix}"
export MODEL_PATH="${MODEL_PATH:-$HOME/models/opt-125m}"

export HOST="${HOST:-127.0.0.1}"
export PORT="${PORT:-1234}"
export RAY_PORT="${RAY_PORT:-6389}"
export CUDA_VISIBLE_DEVICES="${CUDA_VISIBLE_DEVICES:-0,1}"

export INITIAL_INSTANCES="${INITIAL_INSTANCES:-2}"
export MIGRATION_BACKEND="${MIGRATION_BACKEND:-rayrpc}"
export MAX_MODEL_LEN="${MAX_MODEL_LEN:-2048}"
export MAX_NUM_BATCHED_TOKENS="${MAX_NUM_BATCHED_TOKENS:-4096}"
export MIGRATION_BUFFER_BLOCKS="${MIGRATION_BUFFER_BLOCKS:-32}"
export GPU_MEMORY_UTILIZATION="${GPU_MEMORY_UTILIZATION:-0.9}"

export EXP_ROOT="${EXP_ROOT:-$HOME/llumnix_experiments}"
export EXP_NAME="${EXP_NAME:-real_migration_$(date +%Y%m%d_%H%M%S)}"
export EXP_DIR="${EXP_DIR:-$EXP_ROOT/$EXP_NAME}"
export SERVICE_LOG="${SERVICE_LOG:-$EXP_DIR/service.log}"
export BENCH_LOG="${BENCH_LOG:-$EXP_DIR/benchmark.log}"

export HEAD_NODE_IP="${HEAD_NODE_IP:-127.0.0.1}"
export HEAD_NODE="${HEAD_NODE:-1}"
export RAY_DEDUP_LOGS="${RAY_DEDUP_LOGS:-0}"
