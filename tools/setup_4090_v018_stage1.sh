#!/usr/bin/env bash
set -euo pipefail

VENV="${VENV:-$HOME/venvs/llumnix-vllm018-cu128}"
LOG_POINTER="${LOG_POINTER:-$HOME/llumnix_v018_setup_logs/latest}"
LOGROOT="$(cat "$LOG_POINTER")"

source "$VENV/bin/activate"
python -m pip install vllm==0.18.1 \
  -i https://pypi.tuna.tsinghua.edu.cn/simple \
  2>&1 | tee -a "$LOGROOT/vllm_install.log"

python - <<'PY' 2>&1 | tee "$LOGROOT/vllm_import.log"
import torch
import vllm

print("torch", torch.__version__, "cuda", torch.version.cuda)
print("cuda_available", torch.cuda.is_available())
print("vllm", vllm.__version__)
PY

echo "INSTALL_EXIT=0" | tee "$LOGROOT/vllm_exit.txt"
