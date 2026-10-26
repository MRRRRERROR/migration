source /opt/miniconda3/etc/profile.d/conda.sh
conda activate llumnix5090_cuda13
cd /data/home/zylu/llumnix-ray-main

python - <<'PY'
import pkgutil
import vllm
import vllm.v1

mods = []
for m in pkgutil.walk_packages(vllm.v1.__path__, prefix="vllm.v1."):
    name = m.name
    if any(k in name.lower() for k in ["hybrid", "transfer", "connector", "kv", "disagg", "engine_proxy"]):
        mods.append(name)
print("\n".join(mods[:300]))
print("count", len(mods))
PY
