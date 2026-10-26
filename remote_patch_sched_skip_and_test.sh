#!/usr/bin/env bash
set -eo pipefail

source /opt/miniconda3/etc/profile.d/conda.sh
conda activate llumnix5090_vllm018

src_dir="/data/home/zylu/tmp/blade-kvt"
work_dir="/data/home/zylu/llumnix-v018-main"
ts="$(date +%Y%m%d_%H%M%S)"
backup_dir="${src_dir}/backups/sched_skip_migrating_req_${ts}"
diff_file="${work_dir}/blade_kvt_sched_skip_migrating_req_diff_${ts}.patch"
install_log="${work_dir}/install_blade_kvt_sched_skip_${ts}.log"
verify_log="${work_dir}/verify_sched_skip_migrating_req_${ts}.log"
test_log="${work_dir}/minimal_single_request_status_${ts}.log"
result_dir="${work_dir}/minimal_single_request_status_${ts}_logs"

mkdir -p "$backup_dir"
mkdir -p "$result_dir"

cd "$src_dir"
cp blade_kvt/hybrid_connector/engine_proxy.py "$backup_dir/engine_proxy.py"

python - <<'PY'
from pathlib import Path

p = Path("blade_kvt/hybrid_connector/engine_proxy.py")
s = p.read_text()

if "def sched_skip_migrating_req(" not in s:
    marker = "\ndef sched_allocate_slots("
    helper = '''

def sched_skip_migrating_req(scheduler, request_id: str) -> bool:
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
    if marker not in s:
        raise SystemExit("insert marker not found")
    s = s.replace(marker, helper + marker, 1)
    p.write_text(s)
    print("patched engine_proxy.py with sched_skip_migrating_req")
else:
    print("sched_skip_migrating_req already present")
PY

git diff -- blade_kvt/hybrid_connector/engine_proxy.py | tee "$diff_file"

export CUDA_HOME="$CONDA_PREFIX"
export CUDA_PATH="$CONDA_PREFIX"
export PATH="$CUDA_HOME/bin:$PATH"
export LD_LIBRARY_PATH="/data/home/zylu/opt/barex/usr/lib:$CUDA_HOME/lib:$CUDA_HOME/lib64:$CONDA_PREFIX/lib:${LD_LIBRARY_PATH:-}"
export CPATH="/data/home/zylu/opt/barex/usr/include:${CPATH:-}"
export CPLUS_INCLUDE_PATH="/data/home/zylu/opt/barex/usr/include:${CPLUS_INCLUDE_PATH:-}"
export C_INCLUDE_PATH="/data/home/zylu/opt/barex/usr/include:${C_INCLUDE_PATH:-}"
export LIBRARY_PATH="/data/home/zylu/opt/barex/usr/lib:${LIBRARY_PATH:-}"
export ACCL_BAREX_INSTALL_PATH="/data/home/zylu/opt/barex/usr"
export TORCH_CUDA_ARCH_LIST="12.0"
export MAX_JOBS=4

python setup.py bdist_wheel 2>&1 | tee "$install_log"
wheel="$(ls -t dist/blade_kvt-*.whl | head -1)"
pip install --force-reinstall --no-deps "$wheel" 2>&1 | tee -a "$install_log"

cd "$work_dir"
python - <<'PY' 2>&1 | tee "$verify_log"
from blade_kvt.hybrid_connector.engine_proxy import sched_skip_migrating_req
print("sched_skip_migrating_req import ok", sched_skip_migrating_req)
print("fallback_result", sched_skip_migrating_req(object(), "ordinary-request"))
PY

cat > remote_minimal_status_no_cleanup.py <<'PY'
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
    body = json.dumps(payload).encode("utf-8")
    req = urlrequest.Request(
        "http://localhost:18089/v1/completions",
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urlrequest.urlopen(req, timeout=180) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
            return resp.status, raw
    except HTTPError as e:
        return e.code, e.read().decode("utf-8", errors="replace")

def parse_result(status, raw):
    try:
        data = json.loads(raw)
    except Exception:
        return "", None, None, None
    choice = (data.get("choices") or [{}])[0]
    text = choice.get("text", "")
    finish_reason = choice.get("finish_reason")
    usage = data.get("usage") or {}
    out_tokens = usage.get("completion_tokens")
    return text, finish_reason, usage, out_tokens

def first_fatal():
    p = Path("logs/vllm_0_minimal.log")
    if not p.exists():
        return "logs/vllm_0_minimal.log missing"
    lines = p.read_text(errors="ignore").splitlines()
    for idx, line in enumerate(lines):
        if "EngineCore encountered a fatal error" in line or "EngineCore failed to start" in line or "Traceback (most recent call last)" in line:
            return "\\n".join(f"{j+1}: {lines[j]}" for j in range(idx, min(idx + 45, len(lines))))
    return "NO_ENGINECORE_FATAL_FOUND"

def save_logs():
    RESULT_DIR.mkdir(parents=True, exist_ok=True)
    for p in [Path("logs/vllm_0_minimal.log"), Path("logs/gateway_minimal.log"), Path("logs/runtime_0_minimal.log"), HANDOFF]:
        if p.exists():
            shutil.copy2(p, RESULT_DIR / p.name)

def main():
    os.environ.setdefault("MODEL_NAME", "Qwen/Qwen2.5-7B-Instruct")
    os.environ.setdefault("MODEL_PATH", "/data/models/Qwen2.5-7B-Instruct")
    shutil.rmtree(NAMING_DIR, ignore_errors=True)
    shutil.rmtree(LOG_DIR, ignore_errors=True)
    LOG_DIR.mkdir(exist_ok=True)
    NAMING_DIR.mkdir(parents=True, exist_ok=True)
    processes = []
    records = []
    try:
        processes.append(start_process("Redis", get_redis_command(), "redis_minimal.log"))
        time.sleep(2)
        processes.append(start_process("Gateway", get_gateway_command(
            policy="round-robin",
            enable_pd=False,
            enable_full_mode_scheduling=False,
            separate_pd_scheduling=False,
            connector_type="HybridConnector",
        ), "gateway_minimal.log"))
        processes.append(start_process("vLLM-0", get_vllm_command(
            instance_type="neutral",
            port=8100,
            cuda=0,
            enable_full_mode_scheduling=False,
            tag="minimal_neutral_0",
            connector_type="HybridConnector",
        ), "vllm_0_minimal.log"))
        processes.append(start_process("Runtime-0", get_discovery_command("neutral", 8100), "runtime_0_minimal.log"))

        if not wait_for_service("http://localhost:8100/health", timeout=120):
            raise TimeoutError("vLLM-0 health timeout")
        time.sleep(10)

        status, raw = post_completion("Hello, my name is", max_tokens=5)
        text, finish_reason, usage, out_tokens = parse_result(status, raw)
        records.append(("single", status, raw, text, finish_reason, usage, out_tokens))
        print("SINGLE_STATUS", status)
        print("SINGLE_RAW_JSON", raw)
        print("SINGLE_TEXT", repr(text))
        print("SINGLE_FINISH_REASON", finish_reason)
        print("SINGLE_USAGE", usage)
        print("SINGLE_OUTPUT_TOKENS", out_tokens)

        if status == 200 and text:
            for count in (5, 10):
                ok = 0
                batch = []
                for i in range(count):
                    st, rw = post_completion(f"Hello, request {i}:", max_tokens=5)
                    tx, fr, us, ot = parse_result(st, rw)
                    batch.append((st, rw, tx, fr, us, ot))
                    if st == 200 and tx:
                        ok += 1
                print(f"BATCH_{count}_OK", ok, "TOTAL", count)
                for i, item in enumerate(batch):
                    st, rw, tx, fr, us, ot = item
                    print(f"BATCH_{count}_{i}_STATUS", st)
                    print(f"BATCH_{count}_{i}_TEXT", repr(tx))
                    print(f"BATCH_{count}_{i}_FINISH_REASON", fr)
                    print(f"BATCH_{count}_{i}_OUTPUT_TOKENS", ot)

    except Exception:
        print("MINIMAL_STATUS_EXCEPTION")
        print(traceback.format_exc())
    finally:
        save_logs()
        fatal = first_fatal()
        with HANDOFF.open("a", encoding="utf-8") as f:
            f.write("\\n## sched_skip_migrating_req 后最小请求状态测试\\n")
            f.write(f"- 结果目录: {RESULT_DIR}\\n")
            for rec in records:
                name, status, raw, text, finish_reason, usage, out_tokens = rec
                f.write(f"- {name} status: {status}\\n")
                f.write(f"- {name} raw response: {raw}\\n")
                f.write(f"- {name} text: {text!r}\\n")
                f.write(f"- {name} finish_reason: {finish_reason}\\n")
                f.write(f"- {name} usage: {usage}\\n")
                f.write(f"- {name} output_tokens: {out_tokens}\\n")
            f.write("- 第一条 EngineCore fatal traceback:\\n```text\\n")
            f.write(fatal)
            f.write("\\n```\\n")
        save_logs()
        cleanup_processes(processes)

if __name__ == "__main__":
    main()
PY

pkill -f '[v]llm serve' || true
pkill -f '[.]\/bin\/gateway' || true
pkill -f '[r]edis-server.*6379' || true
pkill -f '[d]iscovery.discovery' || true
for port in 6379 8100 18088 18089 31100 36218 36219 36220; do
  fuser -k -9 "${port}/tcp" 2>/dev/null || true
done
sleep 3

RESULT_DIR="$result_dir" python remote_minimal_status_no_cleanup.py 2>&1 | tee "$test_log"

{
  echo
  echo "## ${ts} sched_skip_migrating_req 源码补丁、安装、验证"
  echo "- blade_kvt 分支: $(git -C "$src_dir" branch --show-current)"
  echo "- 备份目录: ${backup_dir}"
  echo "- 修改文件: /data/home/zylu/tmp/blade-kvt/blade_kvt/hybrid_connector/engine_proxy.py"
  echo "- diff 文件: ${diff_file}"
  echo "- 安装命令: python setup.py bdist_wheel; pip install --force-reinstall --no-deps ${wheel}"
  echo "- 安装日志: ${install_log}"
  echo "- import 验证日志: ${verify_log}"
  echo "- 最小请求测试日志: ${test_log}"
  echo "- 最小请求日志备份目录: ${result_dir}"
  echo "- 测试输出摘要:"
  grep -E "SINGLE_STATUS|SINGLE_RAW_JSON|SINGLE_TEXT|SINGLE_FINISH_REASON|SINGLE_USAGE|SINGLE_OUTPUT_TOKENS|BATCH_[0-9]+_OK|MINIMAL_STATUS_EXCEPTION" "$test_log" | sed 's/^/  /' || true
  echo "- 第一条 fatal 摘要:"
  python - <<'PY'
from pathlib import Path
p = Path("logs/vllm_0_minimal.log")
if not p.exists():
    print("logs/vllm_0_minimal.log missing")
else:
    lines = p.read_text(errors="ignore").splitlines()
    for idx, line in enumerate(lines):
        if "EngineCore encountered a fatal error" in line or "EngineCore failed to start" in line or "Traceback (most recent call last)" in line:
            for j in range(idx, min(idx + 45, len(lines))):
                print(f"{j+1}: {lines[j]}")
            break
    else:
        print("NO_ENGINECORE_FATAL_FOUND")
PY
} >> AI_HANDOFF.md
