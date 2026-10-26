#!/usr/bin/env bash
set -u

cd /data/home/zylu/llumnix-ray-main
source /data/home/zylu/venvs/llumnix5090/bin/activate

SETUP_DIR="/data/home/zylu/llumnix_results/setup_5090_$(date +%Y%m%d_%H%M%S)"
mkdir -p "$SETUP_DIR"
echo "SETUP_DIR=$SETUP_DIR"

{
  echo "date: $(date)"
  echo "host: $(hostname)"
  python --version
pip --version
nvidia-smi
} > "$SETUP_DIR/env_before_install.txt"

pip install 'setuptools_scm<8' >> "$SETUP_DIR/pip_install_vllm.log" 2>&1
SETUPTOOLS_SCM_PRETEND_VERSION=0.1.1 pip install --no-build-isolation -e '.[vllm]' > "$SETUP_DIR/pip_install_vllm.log" 2>&1
rc=$?
echo "install_exit=$rc"
tail -n 120 "$SETUP_DIR/pip_install_vllm.log"
exit "$rc"
