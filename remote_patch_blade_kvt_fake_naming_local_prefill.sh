set -eo pipefail

source /opt/miniconda3/etc/profile.d/conda.sh
conda activate llumnix5090_vllm018
set -u

SRC=/data/home/zylu/tmp/blade-kvt
HANDOFF=/data/home/zylu/llumnix-v018-main/AI_HANDOFF.md
TS=$(date +%Y%m%d_%H%M%S)
BACKUP_DIR="$SRC/backups/fake_naming_local_prefill_$TS"
DIFF_FILE="/data/home/zylu/llumnix-v018-main/blade_kvt_fake_naming_local_prefill_diff_$TS.patch"
PATCH_LOG="/data/home/zylu/llumnix-v018-main/blade_kvt_fake_naming_local_prefill_$TS.log"
TEST_LOG="/data/home/zylu/llumnix-v018-main/minimal_single_request_after_fake_naming_local_prefill_$TS.log"

cd "$SRC"
BRANCH=$(git branch --show-current)
mkdir -p "$BACKUP_DIR"
cp blade_kvt/hybrid_connector/kvtbackend.py "$BACKUP_DIR/kvtbackend.py"

python - <<'PY'
from pathlib import Path

p = Path("blade_kvt/hybrid_connector/kvtbackend.py")
s = p.read_text()

old = '''        disagg = get_param(req, D_DISAGG, True)
        if not disagg:
            return 0, 0

        return 1, 0
'''

new = '''        disagg = get_param(req, D_DISAGG, True)
        if not disagg:
            return 0, 0

        if self._pmgr is None and get_param(req, "remote_host") is None:
            logger.info(
                "fake naming has no prefill peer; prefill locally. req=%s",
                req.request_id,
            )
            return 0, 0

        return 1, 0
'''

if old in s:
    s = s.replace(old, new, 1)
elif "fake naming has no prefill peer" in s:
    print("fake naming local prefill compat already present")
else:
    raise SystemExit("get_operations target block not found")

p.write_text(s)
PY

python -m py_compile blade_kvt/hybrid_connector/kvtbackend.py
git diff -- blade_kvt/hybrid_connector/kvtbackend.py | tee "$DIFF_FILE"

{
  echo
  echo "## $TS blade_kvt fake naming 本地 prefill 兼容补丁"
  echo "- 分支: $BRANCH"
  echo "- 备份目录: $BACKUP_DIR"
  echo "- 修改文件: blade_kvt/hybrid_connector/kvtbackend.py"
  echo "- 补丁作用: fake:// naming 且没有显式 remote_host 时不再强制远端 prefill，避免单实例普通请求因无 P peer 触发 RuntimeError: no p。"
  echo "- diff 文件: $DIFF_FILE"
} >> "$HANDOFF"

bash /data/home/zylu/llumnix-v018-main/remote_rebuild_blade_kvt.sh 2>&1 | tee "$PATCH_LOG" || true

cd /data/home/zylu/llumnix-v018-main
python - <<'PY'
from pathlib import Path
import importlib.metadata as md
import blade_kvt
from blade_kvt.hybrid_connector import HybridConnector
print("blade_kvt file", Path(blade_kvt.__file__).resolve())
print("blade_kvt version", md.version("blade-kvt"))
print("HybridConnector ok", HybridConnector)
PY

bash /data/home/zylu/llumnix-v018-main/remote_run_minimal_single_request.sh 2>&1 | tee "$TEST_LOG"

LATEST_TRACE=$(grep -RniE "Traceback|TypeError|RuntimeError|AttributeError|EngineCore encountered a fatal error|Exception" logs "$TEST_LOG" 2>/dev/null | tail -n 120 || true)
RESULT_LINE=$(grep -E "MINIMAL_SINGLE_REQUEST_RESULT|Response received" "$TEST_LOG" | tail -n 20 || true)

{
  echo
  echo "### fake naming 本地 prefill 补丁后安装和最小单请求结果"
  echo "- 安装命令: bash /data/home/zylu/llumnix-v018-main/remote_rebuild_blade_kvt.sh"
  echo "- 测试命令: bash /data/home/zylu/llumnix-v018-main/remote_run_minimal_single_request.sh"
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

echo "DONE fake naming local prefill patch/test"
echo "backup=$BACKUP_DIR"
echo "diff=$DIFF_FILE"
