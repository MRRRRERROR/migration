#!/usr/bin/env bash
set -euo pipefail

REPO=/data/home/zylu/llumnix-v018-main
ROOT=$REPO/results/5090_migration_policy_baselines_20260819_201922
STAMP=$(date +%Y%m%d_%H%M%S)
ARCHIVE=$ROOT/06_archived_failed_runs/${STAMP}_missing_profiling_tail_composite

mkdir -p "$ARCHIVE/03_runs" "$ARCHIVE/01_smoke" "$ARCHIVE/05_final" "$ROOT/00_config/runner_backups"
cp "$REPO/run_migration_policy_baselines.py" "$ROOT/00_config/runner_backups/run_migration_policy_baselines.py.$STAMP"

for policy in tpot_tail composite_proposed; do
    for workload in stable_short mixed_tail long_output_migration; do
        source_path="$ROOT/03_runs/$workload/$policy"
        if [[ -d "$source_path" ]]; then
            mkdir -p "$ARCHIVE/03_runs/$workload"
            mv "$source_path" "$ARCHIVE/03_runs/$workload/$policy"
        fi
    done
    smoke_path="$ROOT/01_smoke/stable_short/$policy"
    if [[ -d "$smoke_path" ]]; then
        mkdir -p "$ARCHIVE/01_smoke/stable_short"
        mv "$smoke_path" "$ARCHIVE/01_smoke/stable_short/$policy"
    fi
done

for name in migration_policy_aggregated.json llumnix_5090_migration_policy_comparison.html VALIDATION_REPORT.md; do
    if [[ -f "$ROOT/05_final/$name" ]]; then
        mv "$ROOT/05_final/$name" "$ARCHIVE/05_final/$name"
    fi
done

printf '%s\n' "$ARCHIVE"
