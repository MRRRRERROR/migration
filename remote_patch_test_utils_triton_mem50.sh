#!/usr/bin/env bash
set -eo pipefail

source /opt/miniconda3/etc/profile.d/conda.sh
conda activate llumnix5090_vllm018
cd /data/home/zylu/llumnix-v018-main

ts="$(date +%Y%m%d_%H%M%S)"
backup_dir="/data/home/zylu/llumnix-v018-main/backups/test_utils_triton_mem50_${ts}"
mkdir -p "$backup_dir"
cp tests/local/utils.py "$backup_dir/utils.py"

python - <<'PY'
from pathlib import Path

p = Path("tests/local/utils.py")
s = p.read_text()
s = s.replace("--max-num-batched-tokens 2048 --gpu-memory-utilization 0.65 ", "--max-num-batched-tokens 2048 --gpu-memory-utilization 0.50 ")
if 'command = "VLLM_ATTENTION_BACKEND=TRITON_ATTN " + command' not in s:
    anchor = 'command = "VLLM_DEEP_GEMM_WARMUP=skip " + command\n'
    s = s.replace(anchor, anchor + '    command = "VLLM_ATTENTION_BACKEND=TRITON_ATTN " + command\n', 1)
p.write_text(s)
print("patched tests/local/utils.py: TRITON_ATTN + gpu-memory-utilization 0.50")
PY

echo "=== tests/local/utils.py diff ==="
diff -u "$backup_dir/utils.py" tests/local/utils.py || true

pkill -f '[v]llm serve' || true
pkill -f '[.]\/bin\/gateway' || true
pkill -f '[r]edis-server.*6379' || true
pkill -f '[d]iscovery.discovery' || true
sleep 3

log="minimal_single_request_after_utils_triton_mem50_${ts}.log"
bash remote_run_minimal_single_request.sh 2>&1 | tee "$log"

{
  echo
  echo "## ${ts} tests/local/utils.py TRITON_ATTN + 显存 0.50"
  echo "- 备份目录: ${backup_dir}"
  echo "- 修改文件: /data/home/zylu/llumnix-v018-main/tests/local/utils.py"
  echo "- 修改: get_vllm_command 默认显存比例 0.50；命令增加 VLLM_ATTENTION_BACKEND=TRITON_ATTN"
  echo "- 测试命令: bash remote_run_minimal_single_request.sh"
  echo "- 测试日志: /data/home/zylu/llumnix-v018-main/${log}"
  echo "- 测试结果摘要:"
  tail -n 30 "$log" | sed 's/^/  /'
  echo "- 最新 traceback / fatal 摘要:"
  python - <<'PY'
from pathlib import Path
patterns = [
    "Traceback", "TypeError", "RuntimeError", "AttributeError",
    "EngineCore encountered", "AcceleratorError", "unsupported PTX",
    "ERROR", "ValueError",
]
for lp in [Path("logs/vllm_0_minimal.log"), Path("logs/gateway_minimal.log"), Path("logs/runtime_0_minimal.log")]:
    if not lp.exists():
        continue
    hits = []
    for i, line in enumerate(lp.read_text(errors="ignore").splitlines(), 1):
        if any(p in line for p in patterns):
            hits.append(f"{lp}:{i}:{line}")
    for h in hits[-80:]:
        print(h)
PY
} >> AI_HANDOFF.md
