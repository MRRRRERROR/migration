set -eo pipefail

source /opt/miniconda3/etc/profile.d/conda.sh
conda activate llumnix5090_vllm018
set -u

REPO=/data/home/zylu/tmp/blade-kvt
HANDOFF=/data/home/zylu/llumnix-v018-main/AI_HANDOFF.md
STAMP=$(date +%Y%m%d_%H%M%S)
BRANCH=codex/backport-site-compat-$STAMP
BACKUP=/data/home/zylu/tmp/blade-kvt_backups/site_compat_$STAMP

cd "$REPO"
git checkout -b "$BRANCH"
mkdir -p "$BACKUP"

cd /data/home/zylu/llumnix-v018-main
SITE=$(python - <<'PY'
from pathlib import Path
import blade_kvt
print(Path(blade_kvt.__file__).resolve().parent)
PY
)
cd "$REPO"
SRC=$REPO/blade_kvt

FILES=(
  hybrid_connector/__init__.py
  hybrid_connector/engine_proxy.py
  hybrid_connector/hybrid_modules.py
  hybrid_connector/kvtbackend.py
  hybrid_connector/migration/__init__.py
  hybrid_connector/migration/backend.py
  kv_transfer_impl.py
)

CHANGED=()
for f in "${FILES[@]}"; do
  if ! cmp -s "$SRC/$f" "$SITE/$f"; then
    mkdir -p "$BACKUP/$(dirname "$f")"
    cp "$SRC/$f" "$BACKUP/$f.source_before"
    cp "$SITE/$f" "$BACKUP/$f.installed_used"
    cp "$SITE/$f" "$SRC/$f"
    CHANGED+=("$f")
  fi
done

DIFF=/data/home/zylu/llumnix-v018-main/blade_kvt_backport_diff_$STAMP.patch
if ((${#CHANGED[@]})); then
  DIFF_PATHS=()
  for f in "${CHANGED[@]}"; do
    DIFF_PATHS+=("blade_kvt/$f")
  done
  git diff -- "${DIFF_PATHS[@]}" > "$DIFF"
else
  : > "$DIFF"
fi

{
  echo
  echo "## 0720 blade_kvt site-packages 兼容补丁回写"
  echo "- 时间: $STAMP"
  echo "- 分支: $BRANCH"
  echo "- 源码仓库: $REPO"
  echo "- installed blade_kvt: $SITE"
  echo "- 备份目录: $BACKUP"
  echo "- 回写文件:"
  for f in "${CHANGED[@]}"; do
    echo "  - blade_kvt/$f"
  done
  echo "- diff 文件: $DIFF"
} >> "$HANDOFF"

echo "BRANCH=$BRANCH"
echo "BACKUP=$BACKUP"
echo "DIFF=$DIFF"
echo "CHANGED=${CHANGED[*]}"
wc -l "$DIFF"
