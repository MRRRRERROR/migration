#!/usr/bin/env bash
set -euo pipefail

active_dir=/data/home/zylu/llumnix-v018-main/results/single_gpu_natural_drain_eos_active
mkdir -p "$active_dir"
tmux kill-session -t natural_drain_eos 2>/dev/null || true
tmux new-session -d -s natural_drain_eos \
  "CUDA_VISIBLE_DEVICES=0 LD_LIBRARY_PATH=/data/home/zylu/venvs/llumnix5090/lib/python3.12/site-packages/nvidia/cu13/lib VLLM_ATTENTION_BACKEND=TRITON_ATTN VLLM_USE_FLASHINFER_SAMPLER=0 VLLM_DISABLE_REQUEST_ID_RANDOMIZATION=1 VLLM_DEEP_GEMM_WARMUP=skip /data/home/zylu/venvs/llumnix5090/bin/vllm serve /data/models/Qwen2.5-7B-Instruct --enable-log-requests --attention-backend TRITON_ATTN --enforce-eager --max-model-len 4096 --max-num-batched-tokens 8192 --max-num-seqs 64 --gpu-memory-utilization 0.85 --served-model-name Qwen2.5-7B-Instruct --port 18100 > $active_dir/server.log 2>&1"
tmux ls | grep natural_drain_eos
