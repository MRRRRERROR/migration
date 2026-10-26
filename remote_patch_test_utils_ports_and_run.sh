#!/usr/bin/env bash
set -eo pipefail

source /opt/miniconda3/etc/profile.d/conda.sh
conda activate llumnix5090_vllm018
cd /data/home/zylu/llumnix-v018-main

ts="$(date +%Y%m%d_%H%M%S)"
backup_dir="/data/home/zylu/llumnix-v018-main/backups/test_utils_ports_${ts}"
mkdir -p "$backup_dir"
cp tests/local/utils.py "$backup_dir/utils.py"

python - <<'PY'
from pathlib import Path

p = Path("tests/local/utils.py")
s = p.read_text()
s = s.replace("KVT_PORT_OFFSET: int = 20000", "KVT_PORT_OFFSET: int = 23000")
s = s.replace("BLLM_KVTRANS_PORT_BASE={33218 + 8 * cuda}", "BLLM_KVTRANS_PORT_BASE={36218 + 8 * cuda}")
p.write_text(s)
print("patched tests/local/utils.py: KVT_PORT_OFFSET=23000, BLLM_KVTRANS_PORT_BASE=36218")
PY

echo "=== tests/local/utils.py port diff ==="
diff -u "$backup_dir/utils.py" tests/local/utils.py || true

pkill -f '[v]llm serve' || true
pkill -f '[.]\/bin\/gateway' || true
pkill -f '[r]edis-server.*6379' || true
pkill -f '[d]iscovery.discovery' || true
for port in 6379 8100 18088 18089 31100 36218 36219 36220; do
  fuser -k -9 "${port}/tcp" 2>/dev/null || true
done
sleep 3

echo "=== ports before ==="
ss -ltnp | grep -E "6379|8100|18088|18089|31100|36218|36219|36220" || true

log="minimal_single_request_after_port_shift_${ts}.log"
bash remote_run_minimal_single_request.sh 2>&1 | tee "$log"

{
  echo
  echo "## ${ts} tests/local/utils.py 端口偏移后最小单请求验证"
  echo "- 备份目录: ${backup_dir}"
  echo "- 修改文件: /data/home/zylu/llumnix-v018-main/tests/local/utils.py"
  echo "- 修改: KVT_PORT_OFFSET=23000; BLLM_KVTRANS_PORT_BASE=36218"
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
    "unsupported PTX", "ERROR", "ValueError", "Address already in use",
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
