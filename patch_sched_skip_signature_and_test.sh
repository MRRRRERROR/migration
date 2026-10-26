#!/usr/bin/env bash
set -eo pipefail

source /opt/miniconda3/etc/profile.d/conda.sh
conda activate llumnix5090_vllm018

src_dir=/data/home/zylu/tmp/blade-kvt
work_dir=/data/home/zylu/llumnix-v018-main
ts=$(date +%Y%m%d_%H%M%S)
backup_dir="${src_dir}/backups/sched_skip_signature_${ts}"
diff_file="${work_dir}/blade_kvt_sched_skip_signature_diff_${ts}.patch"
install_log="${work_dir}/install_blade_kvt_sched_skip_signature_${ts}.log"
verify_log="${work_dir}/verify_sched_skip_signature_${ts}.log"
test_log="${work_dir}/minimal_single_request_signature_${ts}.log"

mkdir -p "$backup_dir"
cd "$src_dir"
cp blade_kvt/hybrid_connector/engine_proxy.py "$backup_dir/engine_proxy.py"

python - <<'PY'
from pathlib import Path

p = Path("blade_kvt/hybrid_connector/engine_proxy.py")
s = p.read_text()

old = '''def sched_skip_migrating_req(scheduler, request_id: str) -> bool:
    """Return whether scheduler should skip a request that is being migrated.

    This vLLM patch imports the symbol unconditionally.  The current blade_kvt
    build does not expose a matching state checker, so the safe ordinary-serving
    fallback is to not skip requests.  This does not mark migration successful.
    """
    for attr in (
        "sched_skip_migrating_req",
        "skip_migrating_req",
        "is_migrating_req",
        "is_migrating",
    ):
        checker = getattr(scheduler, attr, None)
        if callable(checker):
            try:
                return bool(checker(request_id))
            except TypeError:
                return bool(checker(scheduler, request_id))

    connector = getattr(scheduler, "connector", None)
    if connector is not None:
        checker = getattr(connector, "sched_skip_migrating_req", None)
        if callable(checker):
            return bool(checker(request_id))

    return False
'''

new = '''def sched_skip_migrating_req(*args) -> bool:
    """Return whether scheduler should skip a request that is being migrated.

    vLLM's patched scheduler may call this as sched_skip_migrating_req(reqid)
    while older Llumnix code may call sched_skip_migrating_req(scheduler, reqid).
    Fallback is ordinary scheduling: never skip and never mark migration done.
    """
    if len(args) == 1:
        scheduler = None
        request_id = args[0]
        try:
            scheduler = _sched()
        except Exception:
            scheduler = None
    elif len(args) >= 2:
        scheduler, request_id = args[0], args[1]
    else:
        return False

    if scheduler is None:
        return False

    for attr in (
        "skip_migrating_req",
        "is_migrating_req",
        "is_migrating",
    ):
        checker = getattr(scheduler, attr, None)
        if callable(checker):
            try:
                return bool(checker(request_id))
            except TypeError:
                return bool(checker(scheduler, request_id))

    connector = getattr(scheduler, "connector", None)
    if connector is not None:
        checker = getattr(connector, "sched_skip_migrating_req", None)
        if callable(checker):
            try:
                return bool(checker(request_id))
            except TypeError:
                return bool(checker(scheduler, request_id))

    return False
'''

if old not in s:
    raise SystemExit("target sched_skip_migrating_req block not found")

p.write_text(s.replace(old, new, 1))
print("patched sched_skip_migrating_req single/double arg compatibility")
PY

git diff -- blade_kvt/hybrid_connector/engine_proxy.py | tee "$diff_file"

export CUDA_HOME=$CONDA_PREFIX
export CUDA_PATH=$CONDA_PREFIX
export PATH=$CUDA_HOME/bin:$PATH
export LD_LIBRARY_PATH="/data/home/zylu/opt/barex/usr/lib:$CUDA_HOME/lib:$CUDA_HOME/lib64:$CONDA_PREFIX/lib:${LD_LIBRARY_PATH:-}"
export CPATH="/data/home/zylu/opt/barex/usr/include:${CPATH:-}"
export CPLUS_INCLUDE_PATH="/data/home/zylu/opt/barex/usr/include:${CPLUS_INCLUDE_PATH:-}"
export C_INCLUDE_PATH="/data/home/zylu/opt/barex/usr/include:${C_INCLUDE_PATH:-}"
export LIBRARY_PATH="/data/home/zylu/opt/barex/usr/lib:${LIBRARY_PATH:-}"
export ACCL_BAREX_INSTALL_PATH="/data/home/zylu/opt/barex/usr"
export TORCH_CUDA_ARCH_LIST="12.0"
export MAX_JOBS=4

python setup.py bdist_wheel 2>&1 | tee "$install_log"
wheel=$(ls -t dist/blade_kvt-*.whl | head -1)
pip install --force-reinstall --no-deps "$wheel" 2>&1 | tee -a "$install_log"

cd "$work_dir"
python - <<'PY' 2>&1 | tee "$verify_log"
from blade_kvt.hybrid_connector.engine_proxy import sched_skip_migrating_req
print("sched_skip_migrating_req import ok", sched_skip_migrating_req)
print("single_arg_result", sched_skip_migrating_req("ordinary-request"))
print("double_arg_result", sched_skip_migrating_req(object(), "ordinary-request"))
PY

python remote_minimal_status_no_cleanup.py 2>&1 | tee "$test_log"

{
  echo
  echo "## 0720 sched_skip_migrating_req signature compat"
  echo "- 备份目录: $backup_dir"
  echo "- diff 文件: $diff_file"
  echo "- 安装日志: $install_log"
  echo "- import 验证日志: $verify_log"
  echo "- 最小请求测试日志: $test_log"
  echo "- 测试输出摘要:"
  grep -E "SINGLE_STATUS|SINGLE_RAW_JSON|SINGLE_TEXT|SINGLE_FINISH_REASON|SINGLE_USAGE|SINGLE_OUTPUT_TOKENS|FIVE_|TEN_" "$test_log" || true
  echo "- 第一条 fatal 摘要:"
  grep -nA45 -m1 "Traceback" logs/vllm_0_minimal.log || true
} >> AI_HANDOFF.md

