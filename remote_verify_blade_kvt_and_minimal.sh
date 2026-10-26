set -eo pipefail

source /opt/miniconda3/etc/profile.d/conda.sh
conda activate llumnix5090_vllm018

HANDOFF=/data/home/zylu/llumnix-v018-main/AI_HANDOFF.md
TS=$(date +%Y%m%d_%H%M%S)
VERIFY_LOG="/data/home/zylu/llumnix-v018-main/verify_blade_kvt_after_source_reinstall_$TS.log"
TEST_LOG="/data/home/zylu/llumnix-v018-main/minimal_single_request_after_allocate_slots_$TS.log"

cd /data/home/zylu/llumnix-v018-main

python - <<'PY' 2>&1 | tee "$VERIFY_LOG"
from pathlib import Path
import importlib.metadata as md
import blade_kvt
from blade_kvt.hybrid_connector import HybridConnector
from blade_kvt.hybrid_connector.migration import MIGRATION_TRIGGER_POLICY
from blade_kvt.hybrid_connector.engine_proxy import scheduler_rpc_host
print("blade_kvt file", Path(blade_kvt.__file__).resolve())
print("blade_kvt version", md.version("blade-kvt"))
print("HybridConnector ok", HybridConnector)
print("MIGRATION_TRIGGER_POLICY", MIGRATION_TRIGGER_POLICY)
print("scheduler_rpc_host ok", scheduler_rpc_host)
PY

bash /data/home/zylu/llumnix-v018-main/remote_run_minimal_single_request.sh 2>&1 | tee "$TEST_LOG"

LATEST_TRACE=$(grep -RniE "Traceback|TypeError|RuntimeError|EngineCore encountered a fatal error|Exception" logs "$TEST_LOG" 2>/dev/null | tail -n 120 || true)
RESULT_LINE=$(grep -E "MINIMAL_SINGLE_REQUEST_RESULT|Response received" "$TEST_LOG" | tail -n 20 || true)

{
  echo
  echo "## $TS blade_kvt 源码回写后 import 与单请求验证"
  echo "- import 验证日志: $VERIFY_LOG"
  echo "- 单请求测试日志: $TEST_LOG"
  echo "- 测试结果:"
  echo '```text'
  if [ -n "$RESULT_LINE" ]; then
    echo "$RESULT_LINE"
  else
    echo "未抓到 MINIMAL_SINGLE_REQUEST_RESULT。"
  fi
  echo '```'
  echo "- 最新 traceback / fatal 摘要:"
  echo '```text'
  if [ -n "$LATEST_TRACE" ]; then
    echo "$LATEST_TRACE"
  else
    echo "未抓到 Traceback/EngineCore fatal。"
  fi
  echo '```'
} >> "$HANDOFF"

echo "DONE verify/minimal"
echo "verify_log=$VERIFY_LOG"
echo "test_log=$TEST_LOG"
