#!/usr/bin/env bash
set -eo pipefail

source /opt/miniconda3/etc/profile.d/conda.sh
conda activate llumnix5090_vllm018
cd /data/home/zylu/llumnix-v018-main

ts="$(date +%Y%m%d_%H%M%S)"
backup_dir="/data/home/zylu/llumnix-v018-main/backups/test_utils_attention_cli_${ts}"
mkdir -p "$backup_dir"
cp tests/local/utils.py "$backup_dir/utils.py"

python - <<'PY'
from pathlib import Path

p = Path("tests/local/utils.py")
s = p.read_text()
needle = '        f"--enable-log-requests "\\n'
insert = '        f"--attention-backend TRITON_ATTN "\\n'
if insert not in s:
    s = s.replace(needle, needle + insert, 1)
p.write_text(s)
print("patched tests/local/utils.py: add --attention-backend TRITON_ATTN")
PY

echo "=== tests/local/utils.py attention cli diff ==="
diff -u "$backup_dir/utils.py" tests/local/utils.py || true

pkill -f '[v]llm serve' || true
pkill -f '[.]\/bin\/gateway' || true
pkill -f '[r]edis-server.*6379' || true
pkill -f '[d]iscovery.discovery' || true
for port in 6379 8100 18088 18089 31100 36218 36219 36220; do
  fuser -k -9 "${port}/tcp" 2>/dev/null || true
done
sleep 3

log="minimal_single_request_after_attention_cli_${ts}.log"
bash remote_run_minimal_single_request.sh 2>&1 | tee "$log"

{
  echo
  echo "## ${ts} tests/local/utils.py 增加 --attention-backend TRITON_ATTN"
  echo "- 备份目录: ${backup_dir}"
  echo "- 修改文件: /data/home/zylu/llumnix-v018-main/tests/local/utils.py"
  echo "- 修改: vLLM 命令增加 --attention-backend TRITON_ATTN"
  echo "- 测试命令: bash remote_run_minimal_single_request.sh"
  echo "- 测试日志: /data/home/zylu/llumnix-v018-main/${log}"
  echo "- 测试结果摘要:"
  tail -n 30 "$log" | sed 's/^/  /'
  echo "- 最新 traceback / fatal 摘要:"
  python - <<'PY'
from pathlib import Path
patterns = [
    "Traceback", "TypeError", "RuntimeError", "AttributeError",
    "EngineCore encountered", "EngineCore failed", "AcceleratorError",
    "unsupported PTX", "ERROR", "ValueError", "Using ",
]
for lp in [Path("logs/vllm_0_minimal.log"), Path("logs/gateway_minimal.log"), Path("logs/runtime_0_minimal.log")]:
    if not lp.exists():
        continue
    hits = []
    for i, line in enumerate(lp.read_text(errors="ignore").splitlines(), 1):
        if any(p in line for p in patterns):
            hits.append(f"{lp}:{i}:{line}")
    for h in hits[-100:]:
        print(h)
PY
} >> AI_HANDOFF.md
