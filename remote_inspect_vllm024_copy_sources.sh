source /opt/miniconda3/etc/profile.d/conda.sh
conda activate llumnix5090_cuda13
cd /data/home/zylu/llumnix-ray-main

EXP_DIR=/data/home/zylu/llumnix-ray-main/5090_results_0719_migration_thr3300
OUT="$EXP_DIR/vllm024_copy_source_snippets_0719.txt"

python - <<'PY' > "$OUT"
import inspect

from vllm.v1.worker.gpu import kv_connector
from vllm.v1.simple_kv_offload import copy_backend
from vllm.v1.kv_offload.cpu import gpu_worker
from vllm.v1.core import kv_cache_manager
from vllm.v1.core import block_pool
from vllm.v1.core.sched import scheduler

def maybe(title, obj, name=None):
    if name is None:
        return (title, obj)
    return (title, getattr(obj, name, None))

items = [
    maybe("gpu.kv_connector.copy_kv_blocks", kv_connector.copy_kv_blocks),
    maybe("gpu.kv_connector.KVConnector", kv_connector.KVConnector),
    maybe("gpu.kv_connector.ActiveKVConnector", kv_connector.ActiveKVConnector),
    maybe("simple.copy_backend.copy_blocks", copy_backend.copy_blocks),
    maybe("simple.copy_backend.DmaCopyBackend", copy_backend.DmaCopyBackend),
    maybe("cpu.gpu_worker.SingleDirectionOffloadingHandler", gpu_worker.SingleDirectionOffloadingHandler),
    maybe("cpu.gpu_worker.CpuGpuOffloadingHandlers", gpu_worker.CpuGpuOffloadingHandlers),
    maybe("kv_cache_manager.KVCacheManager.allocate_slots", kv_cache_manager.KVCacheManager, "allocate_slots"),
    maybe("kv_cache_manager.KVCacheManager.free", kv_cache_manager.KVCacheManager, "free"),
    maybe("kv_cache_manager.KVCacheManager.get_block_ids", kv_cache_manager.KVCacheManager, "get_block_ids"),
    maybe("block_pool.BlockPool.get_new_blocks", block_pool.BlockPool, "get_new_blocks"),
    maybe("block_pool.BlockPool.free_blocks", block_pool.BlockPool, "free_blocks"),
    maybe("scheduler.Scheduler.add_request", scheduler.Scheduler, "add_request"),
    maybe("scheduler.Scheduler.finish_requests", scheduler.Scheduler, "finish_requests"),
    maybe("scheduler.Scheduler.schedule", scheduler.Scheduler, "schedule"),
]

for title, obj in items:
    print("\n" + "=" * 100)
    print(title)
    if obj is None:
        print("MISSING")
        continue
    try:
        print(inspect.signature(obj))
    except Exception as e:
        print("SIGNATURE_FAILED", repr(e))
    try:
        src = inspect.getsource(obj)
        print(src[:6000])
    except Exception as e:
        print("SOURCE_FAILED", repr(e))
PY

wc -l "$OUT"
sed -n '1,260p' "$OUT"
