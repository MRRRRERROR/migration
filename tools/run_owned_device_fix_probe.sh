#!/usr/bin/env bash
set -euo pipefail
project=/data/home/zylu/llumnix-v018-main
stage=/data/home/zylu/llumnix-kvt500-2gpu/shutdown_repair_20261007_01/lifecycle3
overlay=/data/home/zylu/llumnix-kvt500-2gpu/runtime_overlay/blade_kvt
snapshot=$stage/frozen_candidate
test ! -d "$snapshot"
mkdir "$snapshot"
cp -a "$overlay/hybrid_connector" "$snapshot/"
cp "$overlay/kvtransfer_ops.cpython-312-x86_64-linux-gnu.so" "$snapshot/"
cp "$stage/barex_protocol.cpp" "$stage/barex_protocol.h" "$snapshot/"
printf '%s\n' '{"formal_experiment_allowed":false,"candidate_only":true,"reason":"owned TCP descriptor fix awaits migration and lifecycle validation"}' > "$stage/FREEZE_MIGRATION_VALIDATION_REVOKED.json"
find "$snapshot" -type f \( -name '*.py' -o -name '*.cpp' -o -name '*.h' -o -name '*.so' \) -exec sha256sum {} + > "$snapshot/SHA256SUMS"
export LLUMNIX_SERVING_FROZEN_DIR=$snapshot
exec bash "$project/tools/run_real_migration_exit_probe.sh" --root "$project/results/migration_exit_followup_20261007_04_owned_device" --workload-root "$project/results/two_gpu_power_drain_20261007_shutdown_fixed_01"
