set -eo pipefail

source /opt/miniconda3/etc/profile.d/conda.sh
conda activate llumnix5090_vllm018
set -u

cd /data/home/zylu/tmp/blade-kvt
export ACCL_BAREX_INSTALL_PATH=/data/home/zylu/opt/barex/usr
export LD_LIBRARY_PATH=/data/home/zylu/opt/barex/usr/lib:${LD_LIBRARY_PATH:-}
export CPATH=/data/home/zylu/opt/barex/usr/include:${CPATH:-}
export CPLUS_INCLUDE_PATH=/data/home/zylu/opt/barex/usr/include:${CPLUS_INCLUDE_PATH:-}
export C_INCLUDE_PATH=/data/home/zylu/opt/barex/usr/include:${C_INCLUDE_PATH:-}
export LIBRARY_PATH=/data/home/zylu/opt/barex/usr/lib:${LIBRARY_PATH:-}

rm -rf build dist blade_kvt.egg-info
python setup.py bdist_wheel 2>&1 | tee /data/home/zylu/llumnix-v018-main/rebuild_blade_kvt_0720_backport_barex_path.log
pip install dist/blade_kvt-*.whl --force-reinstall --no-deps 2>&1 | tee /data/home/zylu/llumnix-v018-main/reinstall_blade_kvt_0720_backport_barex_path.log

python - <<'PY'
from pathlib import Path
import blade_kvt
from blade_kvt.hybrid_connector import HybridConnector
from blade_kvt.hybrid_connector.migration import MIGRATION_TRIGGER_POLICY
from blade_kvt.hybrid_connector.engine_proxy import scheduler_rpc_host
print("blade_kvt file", Path(blade_kvt.__file__).resolve())
print("HybridConnector ok", HybridConnector)
print("MIGRATION_TRIGGER_POLICY", MIGRATION_TRIGGER_POLICY)
print("scheduler_rpc_host ok", scheduler_rpc_host)
PY
