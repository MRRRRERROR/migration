#!/usr/bin/env bash
set -euo pipefail

CONDA_ROOT="${CONDA_ROOT:-$HOME/miniconda-v018}"
CUDA_PREFIX="${CUDA_PREFIX:-$HOME/cuda-12.8}"
LOG_POINTER="${LOG_POINTER:-$HOME/llumnix_v018_setup_logs/latest}"
LOGROOT="$(cat "$LOG_POINTER")"

"$CONDA_ROOT/bin/conda" install -y -p "$CUDA_PREFIX" \
  --override-channels -c nvidia -c conda-forge \
  'libcublas-dev=12.8.*' \
  2>&1 | tee "$LOGROOT/cuda128_build_deps.log"

TARGET_INCLUDE="$CUDA_PREFIX/targets/x86_64-linux/include"
mkdir -p "$CUDA_PREFIX/include"
for entry in "$TARGET_INCLUDE"/*; do
  name="$(basename "$entry")"
  if [[ ! -e "$CUDA_PREFIX/include/$name" ]]; then
    ln -s "../targets/x86_64-linux/include/$name" "$CUDA_PREFIX/include/$name"
  fi
done

grep -m1 '#define CUDA_VERSION' "$CUDA_PREFIX/include/cuda.h"
test -f "$CUDA_PREFIX/include/cublas_v2.h"
echo "CUDA_BUILD_DEPS_EXIT=0" | tee "$LOGROOT/cuda128_build_deps_exit.txt"
