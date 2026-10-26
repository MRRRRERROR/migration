#!/usr/bin/env bash
set -eo pipefail

source /opt/miniconda3/etc/profile.d/conda.sh
conda activate llumnix5090_vllm018
cd /data/home/zylu/llumnix-v018-main

ts="$(date +%Y%m%d_%H%M%S)"
backup_dir="/data/home/zylu/llumnix-v018-main/backups/test_utils_force_attention_cli_${ts}"
mkdir -p "$backup_dir"
cp tests/local/utils.py "$backup_dir/utils.py"

python - <<'PY'
from pathlib import Path

p = Path("tests/local/utils.py")
s = p.read_text()
line = '        f"--enable-log-requests "\n'
insert = '        f"--attention-backend TRITON_ATTN "\n'
if insert not in s:
    s = s.replace(line, line + insert, 1)
p.write_text(s)
PY

echo "=== attention cli actual diff ==="
diff -u "$backup_dir/utils.py" tests/local/utils.py || true

pkill -f '[v]llm serve' || true
pkill -f '[.]\/bin\/gateway' || true
pkill -f '[r]edis-server.*6379' || true
pkill -f '[d]iscovery.discovery' || true
for port in 6379 8100 18088 18089 31100 36218 36219 36220; do
  fuser -k -9 "${port}/tcp" 2>/dev/null || true
done
sleep 3

log="minimal_single_request_after_force_attention_cli_${ts}.log"
bash remote_run_minimal_single_request.sh 2>&1 | tee "$log"

{
  echo
  echo "## ${ts} 强制增加 --attention-backend TRITON_ATTN 后最小单请求验证"
  echo "- 备份目录: ${backup_dir}"
  echo "- 修改文件: /data/home/zylu/llumnix-v018-main/tests/local/utils.py"
  echo "- 修改: vLLM 命令增加 --attention-backend TRITON_ATTN"
  echo "- 测试命令: bash remote_run_minimal_single_request.sh"
  echo "- 测试日志: /data/home/zylu/llumnix-v018-main/${log}"
  echo "- 测试结果摘要:"
  tail -n 30 "$log" | sed 's/^/  /'
  echo "- 最新 traceback / fatal 摘要:"
  python remote_inspect_minimal_logs.py
} >> AI_HANDOFF.md
