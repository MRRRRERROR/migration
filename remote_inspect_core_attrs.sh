cd /data/home/zylu/llumnix-ray-main

echo "===== static grep core attrs ====="
grep -nE "kv_caches|kv_cache|cache_config|scheduler|requests|running|waiting|block_ids|blocks|gpu_cache|model_executor|worker|executor" \
  llumnix/backends/vllm_v1/core.py | head -n 240 || true

echo
echo "===== wrapper class snippets ====="
grep -nE "^class .*Llumnix|def __init__|def add_request|def migrate_out|def remove|def abort|def _process_engine_step" \
  llumnix/backends/vllm_v1/core.py | head -n 160 || true

echo
echo "===== EngineCoreProcLlumnix init area ====="
nl -ba llumnix/backends/vllm_v1/core.py | sed -n '280,380p'

echo
echo "===== EngineCoreProcWrapperLlumnix init area ====="
nl -ba llumnix/backends/vllm_v1/core.py | sed -n '120,180p'
