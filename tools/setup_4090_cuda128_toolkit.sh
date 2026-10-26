#!/usr/bin/env bash
set -euo pipefail

MAMBA="${MAMBA:-$HOME/micromamba-complete/bin/micromamba}"
CUDA_PREFIX="${CUDA_PREFIX:-$HOME/cuda-12.8}"
LOG_POINTER="${LOG_POINTER:-$HOME/llumnix_v018_setup_logs/latest}"
LOGROOT="$(cat "$LOG_POINTER")"

"$MAMBA" create -y -p "$CUDA_PREFIX" \
  -c nvidia -c conda-forge \
  cuda-nvcc=12.8 cuda-cudart-dev=12.8 cuda-driver-dev=12.8 \
  2>&1 | tee "$LOGROOT/cuda128_toolkit_install.log"

"$CUDA_PREFIX/bin/nvcc" --version | tee "$LOGROOT/cuda128_nvcc.log"
echo "CUDA_TOOLKIT_EXIT=0" | tee "$LOGROOT/cuda128_toolkit_exit.txt"
