source /opt/miniconda3/etc/profile.d/conda.sh
conda activate llumnix5090_cuda13
cd /data/home/zylu/llumnix-ray-main

EXP_DIR=/data/home/zylu/llumnix-ray-main/5090_results_0719_migration_thr3300
OUT="$EXP_DIR/vllm024_candidate_sources_0719.txt"

python - <<'PY' > "$OUT"
import importlib
import inspect

mods = [
    "vllm.v1.worker.gpu.kv_connector",
    "vllm.v1.worker.kv_connector_model_runner_mixin",
    "vllm.v1.kv_offload.base",
    "vllm.v1.kv_offload.factory",
    "vllm.v1.kv_offload.cpu.gpu_worker",
    "vllm.v1.simple_kv_offload.copy_backend",
    "vllm.v1.simple_kv_offload.manager",
    "vllm.v1.simple_kv_offload.worker",
    "vllm.v1.core.kv_cache_manager",
    "vllm.v1.core.single_type_kv_cache_manager",
    "vllm.v1.core.block_pool",
    "vllm.v1.core.sched.scheduler",
    "vllm.v1.request",
]

for modname in mods:
    print("\n" + "=" * 100)
    print("MODULE", modname)
    try:
        mod = importlib.import_module(modname)
        print("FILE", getattr(mod, "__file__", None))
    except Exception as e:
        print("IMPORT_FAILED", repr(e))
        continue

    for name, obj in sorted(vars(mod).items()):
        if name.startswith("_"):
            continue
        if inspect.isclass(obj) or inspect.isfunction(obj):
            try:
                sig = str(inspect.signature(obj))
            except Exception:
                sig = "<?>"
            if any(k in name.lower() for k in [
                "connector", "cache", "kv", "block", "request", "sched",
                "copy", "swap", "load", "store", "transfer", "offload",
                "allocate", "free", "manager", "worker"
            ]):
                print("SYM", name, sig)
PY

wc -l "$OUT"
sed -n '1,240p' "$OUT"
