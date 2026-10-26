#!/usr/bin/env bash
set -euo pipefail

REPO=/data/home/zylu/llumnix-v018-main
ROOT=$REPO/results/5090_migration_policy_baselines_20260819_201922
DEST=$ROOT/06_archived_failed_runs/20260820_tpot_tail_contended_oom_smoke

mkdir -p "$DEST"
cp -a "$REPO/logs/." "$DEST/"
cp "$ROOT/04_step_summaries/tpot_tail_profiled_smoke.log" "$DEST/launcher.log"
printf '%s\n' \
  'GPU shared with ubuntu SGLang TP=4; vLLM startup rejected because 4.14 GiB free < 17.25 GiB requested.' \
  > "$DEST/README.txt"
find "$DEST" -maxdepth 1 -type f -printf '%f %s bytes\n' | sort
