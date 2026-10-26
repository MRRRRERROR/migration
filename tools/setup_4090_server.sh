#!/usr/bin/env bash
set -euo pipefail

# Reproducible setup for the Ray-based Llumnix v0 stack on RTX 4090 hosts.
# The repository must already be present. Existing virtualenvs are preserved.

LLUMNIX_REPO="${LLUMNIX_REPO:-$HOME/llumnix-ray-main}"
LLUMNIX_VENV="${LLUMNIX_VENV:-$HOME/venvs/llumnix-vllm063}"
MODEL_REPO="${MODEL_REPO:-facebook/opt-125m}"
MODEL_PATH="${MODEL_PATH:-$HOME/models/opt-125m}"
SETUP_LOG_DIR="${SETUP_LOG_DIR:-$HOME/llumnix_setup_logs/$(date +%Y%m%d_%H%M%S)}"
PYPI_INDEX_URL="${PYPI_INDEX_URL:-https://pypi.tuna.tsinghua.edu.cn/simple}"
HF_ENDPOINT="${HF_ENDPOINT:-https://hf-mirror.com}"
export HF_ENDPOINT

mkdir -p "$SETUP_LOG_DIR" "$(dirname "$LLUMNIX_VENV")" "$(dirname "$MODEL_PATH")"

if [[ ! -f "$LLUMNIX_REPO/setup.py" ]]; then
  echo "Llumnix repository not found at $LLUMNIX_REPO" >&2
  exit 1
fi

{
  date -Is
  hostname
  uname -a
  python3 --version
  nvidia-smi
  nvcc --version || true
} | tee "$SETUP_LOG_DIR/system.txt"

if [[ -f "$LLUMNIX_VENV/bin/activate" ]]; then
  echo "Reusing existing virtualenv: $LLUMNIX_VENV"
elif [[ -e "$LLUMNIX_VENV" ]]; then
  echo "Path exists but is not a virtualenv: $LLUMNIX_VENV" >&2
  exit 2
else
  python3 -m venv "$LLUMNIX_VENV"
fi
source "$LLUMNIX_VENV/bin/activate"
python -m pip install --index-url "$PYPI_INDEX_URL" \
  --upgrade pip wheel 'setuptools<71' 'setuptools_scm==7.1.0' \
  2>&1 | tee "$SETUP_LOG_DIR/bootstrap.log"

cd "$LLUMNIX_REPO"
SETUPTOOLS_SCM_PRETEND_VERSION=0.1.1 \
  python -m pip install --no-build-isolation \
    --index-url "$PYPI_INDEX_URL" \
    -c requirements/constraints_vllm063_2024.txt \
    -e '.[vllm]' \
  2>&1 | tee "$SETUP_LOG_DIR/install.log"

python - <<'PY' 2>&1 | tee "$SETUP_LOG_DIR/import_check.log"
import ray
import torch
import vllm
import llumnix

print("torch", torch.__version__)
print("torch_cuda", torch.version.cuda)
print("cuda_available", torch.cuda.is_available())
print("device_count", torch.cuda.device_count())
if torch.cuda.is_available():
    print("device_0", torch.cuda.get_device_name(0))
print("vllm", vllm.__version__)
print("ray", ray.__version__)
print("llumnix", getattr(llumnix, "__version__", "editable"))
PY

if [[ ! -f "$MODEL_PATH/config.json" ]]; then
  python - <<PY 2>&1 | tee "$SETUP_LOG_DIR/model_download.log"
from huggingface_hub import snapshot_download
snapshot_download(
    repo_id="$MODEL_REPO",
    local_dir="$MODEL_PATH",
    ignore_patterns=["*.h5", "*.msgpack", "*.ot"],
)
PY
fi

cat > "$SETUP_LOG_DIR/activate.env" <<EOF
export LLUMNIX_REPO="$LLUMNIX_REPO"
export LLUMNIX_VENV="$LLUMNIX_VENV"
export MODEL_PATH="$MODEL_PATH"
export CUDA_VISIBLE_DEVICES="1,2"
EOF

echo "Setup complete. Logs: $SETUP_LOG_DIR"
echo "Virtualenv: $LLUMNIX_VENV"
echo "Model: $MODEL_PATH"
