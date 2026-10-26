#!/usr/bin/env bash
set -eo pipefail

source /opt/miniconda3/etc/profile.d/conda.sh
conda activate llumnix5090_vllm018
cd /data/home/zylu/llumnix-v018-main

ts="$(date +%Y%m%d_%H%M%S)"
backup="remote_minimal_single_request.py.bak_triton_mem50_${ts}"
cp remote_minimal_single_request.py "$backup"

python - <<'PY'
from pathlib import Path

p = Path("remote_minimal_single_request.py")
s = p.read_text()

s = s.replace(
    '"--gpu-memory-utilization", "0.65",',
    '"--gpu-memory-utilization", "0.50",',
)

if '"VLLM_ATTENTION_BACKEND": "TRITON_ATTN",' not in s:
    marker = '"VLLM_LOGGING_LEVEL": "DEBUG",\n'
    s = s.replace(marker, marker + '            "VLLM_ATTENTION_BACKEND": "TRITON_ATTN",\n', 1)

p.write_text(s)
print("patched remote_minimal_single_request.py: TRITON_ATTN + gpu-memory-utilization 0.50")
PY

pkill -f '[v]llm serve' || true
pkill -f '[.]\/bin\/gateway' || true
pkill -f '[r]edis-server.*6379' || true
pkill -f '[d]iscovery.discovery' || true
sleep 3

echo "=== script diff ==="
diff -u "$backup" remote_minimal_single_request.py || true

echo "=== gpu apps before ==="
nvidia-smi --query-compute-apps=pid,process_name,used_memory --format=csv,noheader || true

log="minimal_single_request_triton_mem50_${ts}.log"
bash remote_run_minimal_single_request.sh 2>&1 | tee "$log"

{
  echo
  echo "## ${ts} TRITON_ATTN + 显存 0.50 最小单请求验证"
  echo "- 脚本备份: /data/home/zylu/llumnix-v018-main/${backup}"
  echo "- 修改文件: /data/home/zylu/llumnix-v018-main/remote_minimal_single_request.py"
  echo "- 修改: vLLM 子进程环境增加 VLLM_ATTENTION_BACKEND=TRITON_ATTN；显存比例改为 0.50"
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
