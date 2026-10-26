source /opt/miniconda3/etc/profile.d/conda.sh
conda activate llumnix5090_cuda13
cd /data/home/zylu/llumnix-ray-main

echo "===== core migrate call snippet ====="
nl -ba llumnix/backends/vllm_v1/core.py | sed -n '1150,1210p'

echo
echo "===== migration_frontend module path check from env ====="
python - <<'PY'
import inspect
from llumnix.backends.vllm_v1.migration_frontend import MigrationFrontend
print(inspect.getsourcefile(MigrationFrontend))
print(inspect.getsource(MigrationFrontend.migrate)[:1600])
PY
