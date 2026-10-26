#!/usr/bin/env bash
set -eo pipefail

source /opt/miniconda3/etc/profile.d/conda.sh
conda activate llumnix5090_vllm018
cd /data/home/zylu/llumnix-v018-main

ROOT="/data/home/zylu/llumnix-v018-main/results/5090_4inst_formal_$(date +%Y%m%d_%H%M%S)"
export ROOT
mkdir -p "$ROOT"
echo "FORMAL_ROOT $ROOT"

for r in 1 2 3; do
  echo "FORMAL_BASELINE_RUN $r"
  BASELINE_INSTANCES=4 BASELINE_REQUESTS=50 BASELINE_WORKERS=20 BASELINE_MAX_TOKENS=32 \
    bash run_remote_no_migration_baseline.sh > "$ROOT/baseline_run${r}.console.log" 2>&1
  grep -E "BASELINE_4_SUMMARY|BASELINE_4_FATAL|RUNNER_EXCEPTION" \
    "$ROOT/baseline_run${r}.console.log" | tee "$ROOT/baseline_run${r}.summary.txt" || true
  sleep 5
done

for r in 1 2 3; do
  echo "FORMAL_MIGRATION_RUN $r"
  VLLM_HOST_IP=127.0.0.1 MIGRATION_INSTANCES=4 MIGRATION_REQUESTS=50 MIGRATION_WORKERS=20 MIGRATION_MAX_TOKENS=256 \
    bash run_remote_minimal_migration_probe.sh > "$ROOT/migration_run${r}.console.log" 2>&1
  grep -E "MIGRATION_PROBE_SUMMARY|MIGRATION_PROBE_FATAL|RUNNER_EXCEPTION" \
    "$ROOT/migration_run${r}.console.log" | tee "$ROOT/migration_run${r}.summary.txt" || true
  sleep 5
done

python - <<'PY'
from pathlib import Path
import os

root = Path(os.environ.get("ROOT", ""))
print("FORMAL_DONE", root)
for f in sorted(root.glob("*.summary.txt")):
    print("---", f.name)
    print(f.read_text(errors="replace")[:4000])
PY
