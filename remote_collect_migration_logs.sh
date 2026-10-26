source /opt/miniconda3/etc/profile.d/conda.sh
conda activate llumnix5090_cuda13
cd /data/home/zylu/llumnix-ray-main

EXP_DIR=/data/home/zylu/llumnix-ray-main/5090_results_0719_migration_thr3300
mkdir -p "$EXP_DIR"
OUT="$EXP_DIR/migration_v1_events_pre_rpc_autorun.log"

grep -R "MIGRATION_EVENT_V1\|migration failed\|frontend_migration" /tmp/ray/session_latest/logs > "$OUT" || true

echo "===== event counts ====="
grep -oE "event=[a-zA-Z0-9_]+" "$OUT" | sort | uniq -c | sort -nr || true

echo
echo "===== migration trace tail ====="
grep -E "frontend_migrate_enter|frontend_core_update|frontend_encode|frontend_migrate_rpc_try|frontend_rpc|frontend_migrate_done|frontend_migrate_failed|TimeoutError|RuntimeError|migration failed|migrate_out_done" "$OUT" | tail -n 240 || true

echo
echo "===== output file ====="
wc -l "$OUT"
