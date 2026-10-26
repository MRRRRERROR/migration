#!/usr/bin/env bash
set -eo pipefail

root="$(cat /tmp/current_migration_baseline_root_0819.txt)"
source /opt/miniconda3/etc/profile.d/conda.sh
conda activate llumnix5090_vllm018
cd /data/home/zylu/llumnix-v018-main

{
  echo "=== MOONCAKE IMPORT ==="
  python - <<'PY'
mods = [
    "mooncake",
    "mooncake_transfer_engine",
    "vllm.distributed.kv_transfer.kv_connector.v1.mooncake_connector",
]
for name in mods:
    try:
        module = __import__(name, fromlist=["*"])
        print(name, "OK", getattr(module, "__file__", None))
    except Exception as exc:
        print(name, "FAIL", type(exc).__name__, str(exc))
try:
    from llumnix.migration_frontend.vllm_v1.mooncake_migration_frontend import MooncakeMigrationFrontend
    print("MooncakeMigrationFrontend OK", MooncakeMigrationFrontend)
except Exception as exc:
    print("MooncakeMigrationFrontend FAIL", type(exc).__name__, str(exc))
PY
  echo "=== PRESTOP ENTRY ==="
  grep -RniE 'enable_pre_stop|pre.?stop|migrate_out_prestop|drain|terminating' \
    python/llumnix tests scripts configs 2>/dev/null | head -n 300 || true
  echo "=== RECOMPUTE ==="
  grep -RniE 'recompute.?migration|migration.?recompute|num_computed_tokens *= *0' \
    python/llumnix /data/home/zylu/tmp/blade-kvt/blade_kvt 2>/dev/null | head -n 200 || true
  echo "=== PRIOR WORKLOAD FILES ==="
  find results/5090_multi_baseline_500req_20260819_153800 -type f \
    | grep -Ei 'request|manifest|workload' | head -n 150 || true
} | tee "$root/01_audit/raw_capability_detail.txt"
