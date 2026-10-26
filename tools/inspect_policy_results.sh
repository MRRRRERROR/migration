#!/usr/bin/env bash
set -eo pipefail

source /opt/miniconda3/etc/profile.d/conda.sh
conda activate llumnix5090_vllm018
set -u

ROOT=/data/home/zylu/llumnix-v018-main/results/5090_migration_policy_baselines_20260819_201922
cd "$ROOT"

python - <<'PY'
import glob
import json
import os

paths = glob.glob("03_runs/**/*", recursive=True)
print("\n".join(paths[:80]))

summary = glob.glob("03_runs/*/*/run_*/run_summary.json")[0]
print("--- sample summary ---", summary)
print(json.dumps(json.load(open(summary)), indent=2)[:10000])

print("--- sample files ---")
for path in glob.glob(os.path.dirname(summary) + "/**/*", recursive=True):
    if os.path.isfile(path):
        print(path, os.path.getsize(path))
PY
