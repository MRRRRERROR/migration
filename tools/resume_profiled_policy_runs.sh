#!/usr/bin/env bash
set -eo pipefail

source /opt/miniconda3/etc/profile.d/conda.sh
conda activate llumnix5090_vllm018
set -u

REPO=/data/home/zylu/llumnix-v018-main
ROOT=$REPO/results/5090_migration_policy_baselines_20260819_201922
cd "$REPO"

python - <<'PY'
import subprocess

rows = subprocess.check_output([
    "nvidia-smi", "--query-gpu=index,memory.free", "--format=csv,noheader,nounits"
], text=True).splitlines()
free = [int(row.split(",")[1].strip()) for row in rows]
print("GPU_FREE_MIB", free, flush=True)
if len(free) < 4 or min(free[:4]) < 19000:
    raise SystemExit(75)
PY

export MIGRATION_POLICY_ROOT="$ROOT"
export MIGRATION_POLICY_REPEATS=1
export MIGRATION_POLICY_ALLOW_CONTENDED=1
export MIGRATION_POLICY_GPU_MEMORY_UTILIZATION=0.55
export MIGRATION_POLICY_SEED=20260819

export MIGRATION_POLICY_RUN_KIND=smoke
export MIGRATION_POLICY_REQUEST_NUM=20
export MIGRATION_POLICY_CONCURRENCY=10
export MIGRATION_POLICY_FORCE_RERUN=1
python -u run_migration_policy_baselines.py run-one --workload stable_short --group tpot_tail --repeat 1
python -u run_migration_policy_baselines.py run-one --workload stable_short --group composite_proposed --repeat 1

export MIGRATION_POLICY_RUN_KIND=formal
export MIGRATION_POLICY_REQUEST_NUM=500
export MIGRATION_POLICY_CONCURRENCY=50
export MIGRATION_POLICY_REPEATS=3
export MIGRATION_POLICY_FORCE_RERUN=0

for workload in stable_short mixed_tail long_output_migration; do
    for policy in tpot_tail composite_proposed; do
        for repeat in 1 2 3; do
            python -u run_migration_policy_baselines.py run-one \
                --workload "$workload" --group "$policy" --repeat "$repeat"
        done
    done
done

python - <<'PY'
import glob
import json

paths = glob.glob("/data/home/zylu/llumnix-v018-main/results/5090_migration_policy_baselines_20260819_201922/03_runs/*/*/run_*/run_summary.json")
rows = [json.load(open(path)) for path in paths]
assert len(rows) == 54, len(rows)
for policy in ("tpot_tail", "composite_proposed"):
    selected = [row for row in rows if row["migration_policy"] == policy]
    assert len(selected) == 9, (policy, len(selected))
    assert sum(row.get("Scheduler_fatal_count", 0) for row in selected) == 0
    assert sum(row.get("profiling_initialized_count", 0) for row in selected) == 9
    evidence = sum(row.get("KVTResp_code_0_count", 0) for row in selected)
    evidence += sum(row.get("FinishedReq_lines", 0) for row in selected)
    assert evidence > 0, (policy, "no real migration evidence")
print("PROFILED_POLICY_VALIDATION_OK", flush=True)
PY

python -u run_migration_policy_baselines.py finalize
