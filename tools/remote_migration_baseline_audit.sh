#!/usr/bin/env bash
set -eo pipefail

source /opt/miniconda3/etc/profile.d/conda.sh
conda activate llumnix5090_vllm018
cd /data/home/zylu/llumnix-v018-main

ts="$(date +%Y%m%d_%H%M%S)"
root="/data/home/zylu/llumnix-v018-main/results/5090_migration_baselines_${ts}"
mkdir -p "$root"/{00_config,01_audit,02_smoke,03_runs,04_step_summaries,05_final,raw_logs}
printf '%s\n' "$root" | tee /tmp/current_migration_baseline_root_0819.txt

{
  echo "=== ENV ==="
  hostname
  git rev-parse HEAD
  git status --short
  python - <<'PY'
import blade_kvt
import llumnix
import torch
import vllm

print("torch", torch.__version__, torch.version.cuda)
print("vllm", vllm.__version__, vllm.__file__)
print("llumnix", llumnix.__file__)
print("blade_kvt", blade_kvt.__file__)
PY
  nvidia-smi --query-gpu=index,name,memory.used,memory.total,utilization.gpu --format=csv,noheader
  echo "=== PROCS ==="
  pgrep -af 'gateway|scheduler|vllm|redis-server|discovery.discovery' || true
  echo "=== MIGRATION SYMBOLS ==="
  grep -RniE 'blocking.?copy|recompute|migrate_out_prestop|MooncakeMigrationFrontend|MooncakeConnector|_kvt_suspend|do_suspend|source_release|migration_mode' \
    python/llumnix/llumnix /data/home/zylu/tmp/blade-kvt/blade_kvt 2>/dev/null | head -n 400 || true
} | tee "$root/01_audit/raw_capability_snapshot.txt"
