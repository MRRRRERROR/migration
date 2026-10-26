#!/usr/bin/env bash
set -euo pipefail

CONDA_ROOT="${CONDA_ROOT:-$HOME/miniconda-v018}"
CUDA_PREFIX="${CUDA_PREFIX:-$HOME/cuda-12.8}"
INSTALLER="${INSTALLER:-$HOME/llumnix_v018_deploy_staging/Miniconda3-latest-Linux-x86_64.sh}"
LOG_POINTER="${LOG_POINTER:-$HOME/llumnix_v018_setup_logs/latest}"
LOGROOT="$(cat "$LOG_POINTER")"

if [[ ! -x "$CONDA_ROOT/bin/conda" ]]; then
  curl -fL --retry 3 \
    -o "$INSTALLER" \
    https://mirrors.tuna.tsinghua.edu.cn/anaconda/miniconda/Miniconda3-latest-Linux-x86_64.sh
  bash "$INSTALLER" -b -p "$CONDA_ROOT"
fi

"$CONDA_ROOT/bin/conda" create -y -p "$CUDA_PREFIX" \
  --override-channels -c nvidia -c conda-forge \
  cuda-nvcc=12.8 cuda-cudart-dev=12.8 cuda-driver-dev=12.8 \
  2>&1 | tee "$LOGROOT/cuda128_toolkit_install.log"

"$CUDA_PREFIX/bin/nvcc" --version | tee "$LOGROOT/cuda128_nvcc.log"
echo "CUDA_TOOLKIT_EXIT=0" | tee "$LOGROOT/cuda128_toolkit_exit.txt"
