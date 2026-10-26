#!/usr/bin/env bash
set -euo pipefail

VENV="${VENV:-$HOME/venvs/llumnix-vllm018-cu128}"
REPO="${REPO:-$HOME/llumnix-v018-main-v018}"
STAGE="${STAGE:-$HOME/llumnix_v018_deploy_staging}"
LOG_POINTER="${LOG_POINTER:-$HOME/llumnix_v018_setup_logs/latest}"
LOGROOT="$(cat "$LOG_POINTER")"

source "$VENV/bin/activate"

python -m pip install \
  'ray[default]==2.56.1' \
  redis readerwriterlock netifaces2 overrides \
  'grpcio-tools>=1.59.5' setuptools_scm cmake \
  'nvidia-cuda-nvcc-cu12==12.8.93' \
  -i https://pypi.tuna.tsinghua.edu.cn/simple \
  2>&1 | tee "$LOGROOT/stack_dependencies.log"

SITE_PACKAGES="$(python -c 'import site; print(site.getsitepackages()[0])')"
if [[ ! -f "$STAGE/vllm_official_backup.tar.gz" ]]; then
  tar -czf "$STAGE/vllm_official_backup.tar.gz" \
    -C "$SITE_PACKAGES" -T "$STAGE/vllm_overlay_files.txt"
fi
tar -xzf "$STAGE/vllm_python_overlay.tar.gz" -C "$SITE_PACKAGES"

SETUPTOOLS_SCM_PRETEND_VERSION=0.1.1.dev1 \
  python -m pip install --no-build-isolation --no-deps -e "$REPO/python/llumnix" \
  2>&1 | tee "$LOGROOT/llumnix_install.log"

python - <<'PY' 2>&1 | tee "$LOGROOT/stack_import_check.log"
import ray
import torch
import vllm
import llumnix

print("torch", torch.__version__, "cuda", torch.version.cuda)
print("cuda_available", torch.cuda.is_available())
print("vllm", vllm.__version__)
print("ray", ray.__version__)
print("llumnix", llumnix.__file__)
PY

find "$VENV/lib" -path '*/nvidia/*' -type f -name nvcc -print \
  | tee "$LOGROOT/nvcc_paths.txt"
echo "STAGE2_EXIT=0" | tee "$LOGROOT/stage2_exit.txt"
