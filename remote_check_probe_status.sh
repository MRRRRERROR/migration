#!/usr/bin/env bash
set -eo pipefail

source /opt/miniconda3/etc/profile.d/conda.sh
conda activate llumnix5090_vllm018
cd /data/home/zylu/llumnix-v018-main

latest="$(ls -td results/minimal_migration_probe_* 2>/dev/null | head -1 || true)"
echo "latest=$latest"

echo "processes:"
ps -ef | grep -E 'gateway|scheduler|vllm serve|remote_minimal_migration_probe|python.*minimal' | grep -v grep || true

echo "ports:"
ss -ltnp | grep -E '18089|18088|8100|8101|8102|8103|31100|31101|31102|31103' || true

if [ -n "$latest" ]; then
  echo "files:"
  find "$latest" -maxdepth 2 -type f | sort | tail -40
  echo "run log:"
  tail -n 80 "$latest"/minimal_migration_probe/run.log 2>/dev/null || true
fi
