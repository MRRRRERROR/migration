set -eo pipefail

source /opt/miniconda3/etc/profile.d/conda.sh
conda activate llumnix5090_cuda13
cd /data/home/zylu/llumnix-ray-main

EXP_DIR=/data/home/zylu/llumnix-ray-main/5090_results_0719_migration_thr3300
mkdir -p "$EXP_DIR"

LLUMNIX_GREP="$EXP_DIR/grep_llumnix_migration_api_0719_adapter_survey.txt"
VLLM_GREP="$EXP_DIR/grep_vllm024_kv_api_0719_adapter_survey.txt"
VLLM_MODULES="$EXP_DIR/vllm024_kv_modules_0719_adapter_survey.txt"
HYBRID_EXPECT="$EXP_DIR/llumnix_expected_hybrid_api_0719_adapter_survey.txt"

echo "===== Llumnix migration API grep ====="
grep -RniE "hybrid_connector|MigrationFrontend|frontend_unavailable|migrate_out|migrate_in|commit|pre_alloc|prealloc|recv|send|req2corereq|core_update_params|MsgpackEncoder|PlaceholderModule|kvtbackend|D_DISAGG|SRC_INFO|MIGRATE_TO" \
  llumnix > "$LLUMNIX_GREP" || true
wc -l "$LLUMNIX_GREP"

echo
echo "===== vLLM path ====="
VLLM_PATH=$(python - <<'PY'
import pathlib, vllm
print(pathlib.Path(vllm.__file__).parent)
PY
)
echo "$VLLM_PATH"

echo
echo "===== vLLM 0.24 KV/connectors grep ====="
grep -RniE "connector|kv_transfer|kv_cache|block_table|block_manager|cache_engine|request_state|scheduler|KVConnector|KVTransfer|swap_blocks|copy_blocks|allocate_slots|cache_blocks|block_pool|computed_block|EngineCoreRequest|EngineCoreOutput|CoreRequest|SchedulerOutput" \
  "$VLLM_PATH" > "$VLLM_GREP" || true
wc -l "$VLLM_GREP"

echo
echo "===== vLLM module walk ====="
python - <<'PY' > "$VLLM_MODULES"
import pkgutil
import vllm
import vllm.v1
for m in pkgutil.walk_packages(vllm.v1.__path__, prefix="vllm.v1."):
    name = m.name
    if any(k in name.lower() for k in [
        "hybrid", "transfer", "connector", "kv", "cache", "block", "sched",
        "offload", "worker", "engine"
    ]):
        print(name)
PY
wc -l "$VLLM_MODULES"

echo
echo "===== expected hybrid/import API snippets ====="
{
  echo "### llumnix/backends/vllm_v1/core.py imports and frontend init/migrate"
  nl -ba llumnix/backends/vllm_v1/core.py | sed -n '25,70p'
  nl -ba llumnix/backends/vllm_v1/core.py | sed -n '910,925p'
  nl -ba llumnix/backends/vllm_v1/core.py | sed -n '1054,1215p'
  echo
  echo "### llumnix/backends/vllm_v1/migration_frontend.py"
  nl -ba llumnix/backends/vllm_v1/migration_frontend.py | sed -n '1,160p'
} > "$HYBRID_EXPECT"
wc -l "$HYBRID_EXPECT"

echo
echo "===== head: expected hybrid imports ====="
sed -n '1,90p' "$HYBRID_EXPECT"

echo
echo "===== likely vLLM replacement modules ====="
grep -E "vllm/v1/(worker/gpu/kv_connector|worker/kv_connector|simple_kv_offload|kv_offload|core/kv_cache|core/sched|engine)" "$VLLM_GREP" | head -n 120 || true

echo
echo "===== files written ====="
printf '%s\n' "$LLUMNIX_GREP" "$VLLM_GREP" "$VLLM_MODULES" "$HYBRID_EXPECT"
