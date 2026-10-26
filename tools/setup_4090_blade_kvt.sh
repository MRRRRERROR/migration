#!/usr/bin/env bash
set -euo pipefail

VENV="${VENV:-$HOME/venvs/llumnix-vllm018-cu128}"
CUDA_HOME="${CUDA_HOME:-$HOME/cuda-12.8}"
BLADE_SRC="${BLADE_SRC:-$HOME/src-v018/blade-kvt}"
LOG_POINTER="${LOG_POINTER:-$HOME/llumnix_v018_setup_logs/latest}"
LOGROOT="$(cat "$LOG_POINTER")"

source "$VENV/bin/activate"
export CUDA_HOME
export CUDA_PATH="$CUDA_HOME"
export CUDACXX="$CUDA_HOME/bin/nvcc"
export PATH="$CUDA_HOME/bin:$VENV/bin:$PATH"
export LD_LIBRARY_PATH="$CUDA_HOME/lib:$CUDA_HOME/lib64:${LD_LIBRARY_PATH:-}"
export CMAKE_PREFIX_PATH="$CUDA_HOME:${CMAKE_PREFIX_PATH:-}"

cd "$BLADE_SRC"
python -m pip install --no-build-isolation --no-deps . \
  2>&1 | tee "$LOGROOT/blade_kvt_build_install.log"

python - <<'PY' 2>&1 | tee "$LOGROOT/blade_kvt_import_check.log"
import blade_kvt
from blade_kvt import kvtransfer_ops

print("blade_kvt", blade_kvt.__file__)
print("kvtransfer_ops", kvtransfer_ops.__file__)
PY

echo "BLADE_KVT_EXIT=0" | tee "$LOGROOT/blade_kvt_exit.txt"
