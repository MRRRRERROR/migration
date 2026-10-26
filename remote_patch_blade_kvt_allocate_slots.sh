set -eo pipefail

source /opt/miniconda3/etc/profile.d/conda.sh
conda activate llumnix5090_vllm018
set -u

SRC=/data/home/zylu/tmp/blade-kvt
HANDOFF=/data/home/zylu/llumnix-v018-main/AI_HANDOFF.md
TS=$(date +%Y%m%d_%H%M%S)
BACKUP_DIR="$SRC/backups/allocate_slots_compat_$TS"
DIFF_FILE="/data/home/zylu/llumnix-v018-main/blade_kvt_allocate_slots_compat_diff_$TS.patch"
PATCH_LOG="/data/home/zylu/llumnix-v018-main/blade_kvt_allocate_slots_compat_$TS.log"

cd "$SRC"

BRANCH=$(git branch --show-current || true)
if [ -z "$BRANCH" ]; then
  BRANCH="codex/backport-site-compat-$TS"
  git checkout -b "$BRANCH"
fi

case "$BRANCH" in
  codex/backport-site-compat-*|codex/allocate-slots-compat-*)
    ;;
  *)
    NEW_BRANCH="codex/allocate-slots-compat-$TS"
    git checkout -b "$NEW_BRANCH"
    BRANCH="$NEW_BRANCH"
    ;;
esac

mkdir -p "$BACKUP_DIR"
cp blade_kvt/hybrid_connector/engine_proxy.py "$BACKUP_DIR/engine_proxy.py"

python - <<'PY'
from pathlib import Path

p = Path("blade_kvt/hybrid_connector/engine_proxy.py")
s = p.read_text()

if "import inspect" not in s.splitlines()[:40]:
    lines = s.splitlines()
    insert_at = 0
    for i, line in enumerate(lines):
        if line.startswith("import ") or line.startswith("from "):
            insert_at = i + 1
    lines.insert(insert_at, "import inspect")
    s = "\n".join(lines) + "\n"

old = '''    new_blocks = self.kv_cache_manager.allocate_slots(
        req,
        num_new_tokens + prealloc,
        num_new_local_computed_tokens,
        new_computed_blocks,
        delay_cache_blocks=True,
        skip_new_block_zeroing=skip_new_block_zeroing)
'''

new = '''    allocate_kwargs = {
        "delay_cache_blocks": True,
        "skip_new_block_zeroing": skip_new_block_zeroing,
    }
    allocate_params = inspect.signature(
        self.kv_cache_manager.allocate_slots).parameters
    allocate_kwargs = {
        key: value for key, value in allocate_kwargs.items()
        if key in allocate_params
    }
    new_blocks = self.kv_cache_manager.allocate_slots(
        req,
        num_new_tokens + prealloc,
        num_new_local_computed_tokens,
        new_computed_blocks,
        **allocate_kwargs)
'''

if old in s:
    s = s.replace(old, new, 1)
elif "allocate_kwargs = {" in s and "skip_new_block_zeroing" in s:
    print("allocate_slots compat already present")
else:
    raise SystemExit("target allocate_slots block not found")

p.write_text(s)
PY

python -m py_compile blade_kvt/hybrid_connector/engine_proxy.py
git diff -- blade_kvt/hybrid_connector/engine_proxy.py | tee "$DIFF_FILE"

{
  echo
  echo "## $TS blade_kvt allocate_slots 兼容补丁"
  echo "- 分支: $BRANCH"
  echo "- 备份目录: $BACKUP_DIR"
  echo "- 修改文件: blade_kvt/hybrid_connector/engine_proxy.py"
  echo "- 补丁作用: 依据当前 vLLM KVCacheManager.allocate_slots 签名过滤 delay_cache_blocks 与 skip_new_block_zeroing，避免不支持参数导致 EngineCore fatal。"
  echo "- diff 文件: $DIFF_FILE"
} >> "$HANDOFF"

bash /data/home/zylu/llumnix-v018-main/remote_rebuild_blade_kvt.sh 2>&1 | tee "$PATCH_LOG"

cd /data/home/zylu/llumnix-v018-main
bash /data/home/zylu/llumnix-v018-main/remote_run_minimal_single_request.sh

LATEST_TRACE=$(grep -RniE "Traceback|TypeError|RuntimeError|EngineCore encountered a fatal error|Exception" logs minimal_single_request_0720.log 2>/dev/null | tail -n 80 || true)

{
  echo
  echo "### 安装和最小单请求结果"
  echo "- 安装命令: bash /data/home/zylu/llumnix-v018-main/remote_rebuild_blade_kvt.sh"
  echo "- 测试命令: bash /data/home/zylu/llumnix-v018-main/remote_run_minimal_single_request.sh"
  echo "- 最新 traceback / fatal 摘要:"
  echo '```text'
  if [ -n "$LATEST_TRACE" ]; then
    echo "$LATEST_TRACE"
  else
    echo "未抓到 Traceback/EngineCore fatal。"
  fi
  echo '```'
} >> "$HANDOFF"

echo "DONE patch/test"
echo "branch=$BRANCH"
echo "backup=$BACKUP_DIR"
echo "diff=$DIFF_FILE"
