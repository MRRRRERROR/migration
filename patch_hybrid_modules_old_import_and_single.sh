#!/usr/bin/env bash
set -eo pipefail

source /opt/miniconda3/etc/profile.d/conda.sh
conda activate llumnix5090_vllm018

src_dir=/data/home/zylu/tmp/blade-kvt
work_dir=/data/home/zylu/llumnix-v018-main
ts=$(date +%Y%m%d_%H%M%S)
backup_dir="${src_dir}/backups/hybrid_modules_old_import_${ts}"
diff_file="${work_dir}/blade_kvt_hybrid_modules_old_import_diff_${ts}.patch"
install_log="${work_dir}/install_blade_kvt_hybrid_modules_old_import_${ts}.log"
verify_log="${work_dir}/verify_hybrid_modules_old_import_${ts}.log"
run_log="${work_dir}/minimal_single_after_hybrid_modules_import_${ts}.log"
result_dir="${work_dir}/minimal_single_after_hybrid_modules_import_${ts}_logs"

mkdir -p "$backup_dir"
cd "$src_dir"
cp blade_kvt/hybrid_connector/hybrid_modules.py "$backup_dir/hybrid_modules.py"

python - <<'PY'
from pathlib import Path

p = Path("blade_kvt/hybrid_connector/hybrid_modules.py")
s = p.read_text()

insert_after = '''# value: bool
_ABORTED = "hbreqisaborted"
'''
insert = insert_after + '''
# vLLM 0.18 patched fallback keys formerly imported from
# vllm.v1.hybrid_connector.kvtbackend.  They are request kv_transfer_params keys;
# absent keys evaluate false for ordinary requests.
P_REMOTE_DECODE = "remote_decode"
P_IGNORE_OUTPUTS = "ignore_outputs"
'''
if "P_REMOTE_DECODE" not in s:
    if insert_after not in s:
        raise SystemExit("anchor for P_REMOTE_DECODE not found")
    s = s.replace(insert_after, insert, 1)

old = '''        from vllm.v1.hybrid_connector.kvtbackend import (
            P_IGNORE_OUTPUTS,
            P_REMOTE_DECODE,
        )

'''
if old not in s:
    raise SystemExit("old vllm.v1.hybrid_connector import block not found")
s = s.replace(old, "", 1)

p.write_text(s)
print("patched hybrid_modules old vllm.v1.hybrid_connector import")
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

cat > remote_single_then_optional5.py <<'PY'
import json
import os
import shutil
import subprocess
import time
import traceback
from pathlib import Path
from urllib import request as urlrequest
from urllib.error import HTTPError, URLError

from tests.local.utils import (
    LOG_DIR,
    NAMING_DIR,
    cleanup_processes,
    get_discovery_command,
    get_gateway_command,
    get_redis_command,
    get_vllm_command,
    start_process,
    wait_for_service,
)

RESULT_DIR = Path(os.environ["RESULT_DIR"])
HANDOFF = Path("AI_HANDOFF.md")

def post_completion(prompt, max_tokens=5):
    payload = {
        "model": os.environ.get("MODEL_PATH", "/data/models/Qwen2.5-7B-Instruct"),
        "prompt": prompt,
        "max_tokens": max_tokens,
        "stream": False,
        "temperature": 0.0,
    }
    req = urlrequest.Request(
        "http://localhost:18089/v1/completions",
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urlrequest.urlopen(req, timeout=180) as resp:
            return resp.status, resp.read().decode("utf-8", errors="replace")
    except HTTPError as e:
        return e.code, e.read().decode("utf-8", errors="replace")
    except URLError as e:
        return 0, repr(e)

def parse_result(raw):
    try:
        data = json.loads(raw)
    except Exception:
        return "", None, {}, None
    choice = (data.get("choices") or [{}])[0]
    text = choice.get("text", "")
    finish_reason = choice.get("finish_reason")
    usage = data.get("usage") or {}
    return text, finish_reason, usage, usage.get("completion_tokens")

def first_fatal():
    p = Path("logs/vllm_0_minimal.log")
    if not p.exists():
        return "logs/vllm_0_minimal.log missing"
    lines = p.read_text(errors="replace").splitlines()
    for idx, line in enumerate(lines):
        if "EngineCore encountered a fatal error" in line or "EngineCore failed to start" in line or "Traceback (most recent call last)" in line:
            return "\n".join(f"{j+1}: {lines[j]}" for j in range(idx, min(idx + 70, len(lines))))
    return "NO_ENGINECORE_FATAL_FOUND"

def save_logs():
    RESULT_DIR.mkdir(parents=True, exist_ok=True)
    for p in [Path("logs/vllm_0_minimal.log"), Path("logs/gateway_minimal.log"), Path("logs/runtime_0_minimal.log"), HANDOFF]:
        if p.exists():
            shutil.copy2(p, RESULT_DIR / p.name)

def print_result(prefix, status, raw):
    text, finish_reason, usage, out_tokens = parse_result(raw)
    print(f"{prefix}_STATUS {status}")
    print(f"{prefix}_RAW_JSON {raw}")
    print(f"{prefix}_TEXT {text!r}")
    print(f"{prefix}_FINISH_REASON {finish_reason}")
    print(f"{prefix}_USAGE {usage}")
    print(f"{prefix}_OUTPUT_TOKENS {out_tokens}")
    return text

def main():
    os.environ.setdefault("MODEL_NAME", "Qwen/Qwen2.5-7B-Instruct")
    os.environ.setdefault("MODEL_PATH", "/data/models/Qwen2.5-7B-Instruct")
    shutil.rmtree(NAMING_DIR, ignore_errors=True)
    shutil.rmtree(LOG_DIR, ignore_errors=True)
    processes = []
    first_raw = ""
    try:
        redis_command = get_redis_command()
        processes.append(start_process(redis_command, "Redis"))
        gateway_command = get_gateway_command(18089)
        processes.append(start_process(gateway_command, "Gateway"))
        vllm_command = get_vllm_command(0, 8100, 31100, "neutral", "minimal_neutral_0")
        processes.append(start_process(vllm_command, "vLLM-0", Path("logs/vllm_0_minimal.log")))
        discovery_command = get_discovery_command(0, "neutral", 8100, 31100)
        processes.append(start_process(discovery_command, "Runtime-0", Path("logs/runtime_0_minimal.log")))

        wait_for_service("http://localhost:8100/health", timeout=240)
        time.sleep(10)

        status, first_raw = post_completion("Hello, briefly introduce yourself.", max_tokens=5)
        text = print_result("SINGLE", status, first_raw)
        time.sleep(3)
        fatal = first_fatal()
        print("SINGLE_FIRST_FATAL")
        print(fatal)
        save_logs()

        if status == 200 and text and fatal == "NO_ENGINECORE_FATAL_FOUND":
            ok = 0
            for i in range(5):
                s, raw = post_completion(f"Say hello in one short sentence. #{i}", max_tokens=5)
                t = print_result(f"BATCH5_{i}", s, raw)
                ok += int(s == 200 and bool(t))
            time.sleep(3)
            fatal2 = first_fatal()
            print(f"BATCH5_OK {ok} TOTAL 5")
            print("BATCH5_FIRST_FATAL")
            print(fatal2)
            save_logs()
    except Exception:
        print("RUNNER_EXCEPTION")
        traceback.print_exc()
        save_logs()
    finally:
        save_logs()
        cleanup_processes(processes)

if __name__ == "__main__":
    main()
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
  echo "## 0721 hybrid_modules old import cleanup"
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

