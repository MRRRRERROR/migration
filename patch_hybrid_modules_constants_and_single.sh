#!/usr/bin/env bash
set -eo pipefail

source /opt/miniconda3/etc/profile.d/conda.sh
conda activate llumnix5090_vllm018

src_dir=/data/home/zylu/tmp/blade-kvt
work_dir=/data/home/zylu/llumnix-v018-main
ts=$(date +%Y%m%d_%H%M%S)
backup_dir="${src_dir}/backups/hybrid_modules_constants_${ts}"
diff_file="${work_dir}/blade_kvt_hybrid_modules_constants_diff_${ts}.patch"
install_log="${work_dir}/install_blade_kvt_hybrid_modules_constants_${ts}.log"
verify_log="${work_dir}/verify_hybrid_modules_constants_${ts}.log"
run_log="${work_dir}/minimal_single_after_hybrid_modules_constants_${ts}.log"
result_dir="${work_dir}/minimal_single_after_hybrid_modules_constants_${ts}_logs"

mkdir -p "$backup_dir"
cd "$src_dir"
cp blade_kvt/hybrid_connector/hybrid_modules.py "$backup_dir/hybrid_modules.py"

python - <<'PY'
from pathlib import Path

p = Path("blade_kvt/hybrid_connector/hybrid_modules.py")
s = p.read_text()

anchor = '''# value: bool
_ABORTED = "hbreqisaborted"
'''
constants = '''
# vLLM 0.18 patched fallback keys formerly imported from
# vllm.v1.hybrid_connector.kvtbackend.  They are request kv_transfer_params keys;
# absent keys evaluate false for ordinary requests.
P_REMOTE_DECODE = "remote_decode"
P_IGNORE_OUTPUTS = "ignore_outputs"
'''

has_remote_assignment = 'P_REMOTE_DECODE =' in s
has_ignore_assignment = 'P_IGNORE_OUTPUTS =' in s
if not (has_remote_assignment and has_ignore_assignment):
    if anchor not in s:
        raise SystemExit("anchor for hybrid connector fallback keys not found")
    s = s.replace(anchor, anchor + constants, 1)

p.write_text(s)
print("patched hybrid_modules fallback key definitions")
PY

git diff -- blade_kvt/hybrid_connector/hybrid_modules.py | tee "$diff_file"

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
from blade_kvt.hybrid_connector.hybrid_modules import P_REMOTE_DECODE, P_IGNORE_OUTPUTS
from blade_kvt.hybrid_connector.engine_proxy import sched_skip_migrating_req
print("hybrid_modules keys ok", P_REMOTE_DECODE, P_IGNORE_OUTPUTS)
print("sched_skip single ok", sched_skip_migrating_req("ordinary-request"))
PY

pkill -f "redis-server.*6379" || true
pkill -f "./bin/gateway" || true
pkill -f "./bin/scheduler" || true
pkill -f "vllm serve" || true
pkill -f "discovery.discovery" || true
pkill -f "benchmark_serving.py" || true
fuser -k -9 6379/tcp 8100/tcp 18088/tcp 18089/tcp 31100/tcp 36218/tcp 2>/dev/null || true
sleep 3

export CUDA_VISIBLE_DEVICES=0,1,2,3
export CUDA_HOME=$CONDA_PREFIX
export PATH=$CUDA_HOME/bin:$PATH
export LD_LIBRARY_PATH=$CUDA_HOME/lib:$CUDA_HOME/lib64:$CONDA_PREFIX/lib:/data/home/zylu/opt/barex/usr/lib:${LD_LIBRARY_PATH:-}
export MODEL_NAME=Qwen/Qwen2.5-7B-Instruct
export MODEL_PATH=/data/models/Qwen2.5-7B-Instruct
export VLLM_USE_V1=1
export RESULT_DIR="$result_dir"

python remote_single_then_optional5.py 2>&1 | tee "$run_log"

{
  echo
  echo "## 0721 hybrid_modules fallback key definitions"
  echo "- 当前阶段: 第一阶段，普通单请求稳定"
  echo "- 修改文件: /data/home/zylu/tmp/blade-kvt/blade_kvt/hybrid_connector/hybrid_modules.py"
  echo "- 备份路径: $backup_dir"
  echo "- diff 文件: $diff_file"
  echo "- 安装日志: $install_log"
  echo "- 运行命令: python remote_single_then_optional5.py"
  echo "- raw response / benchmark summary:"
  grep -E "SINGLE_|BATCH5_" "$run_log" || true
  echo "- 第一条 fatal traceback:"
  grep -nA70 -m1 "Traceback" logs/vllm_0_minimal.log || true
  echo "- 是否已经产出可用实验数据: 否"
  echo "- 下一步计划: 基于本轮第一条 fatal 继续处理"
} >> AI_HANDOFF.md

