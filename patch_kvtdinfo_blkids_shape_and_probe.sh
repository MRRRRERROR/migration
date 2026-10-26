#!/usr/bin/env bash
set -eo pipefail

source /opt/miniconda3/etc/profile.d/conda.sh
conda activate llumnix5090_vllm018

LLUMNIX_DIR=/data/home/zylu/llumnix-v018-main
BLADE_DIR=/data/home/zylu/tmp/blade-kvt
ts=$(date +%Y%m%d_%H%M%S)

cd "$BLADE_DIR"
branch=$(git branch --show-current)
echo "blade_kvt branch: $branch"

backup_dir="$BLADE_DIR/backups/kvtdinfo_blkids_shape_${ts}"
mkdir -p "$backup_dir"
cp blade_kvt/hybrid_connector/migration/backend.py "$backup_dir/backend.py"
echo "backup_dir=$backup_dir"

python - <<'PY'
from pathlib import Path

p = Path("blade_kvt/hybrid_connector/migration/backend.py")
s = p.read_text()
old = "blkids=blockids[0],"
new = "blkids=blockids,"
count = s.count(old)
if count != 2:
    raise SystemExit(f"expected 2 occurrences of {old!r}, found {count}")
p.write_text(s.replace(old, new))
print("patched migration/backend.py KVTDInfo.blkids shape occurrences:", count)
PY

diff_path="$LLUMNIX_DIR/blade_kvt_kvtdinfo_blkids_shape_diff_${ts}.patch"
git diff -- blade_kvt/hybrid_connector/migration/backend.py | tee "$diff_path"
echo "diff_path=$diff_path"

python setup.py bdist_wheel 2>&1 | tee "$LLUMNIX_DIR/install_blade_kvt_kvtdinfo_blkids_shape_build_${ts}.log"
wheel=$(ls -t dist/*.whl | head -n 1)
python -m pip install --force-reinstall --no-deps "$wheel" 2>&1 | tee "$LLUMNIX_DIR/install_blade_kvt_kvtdinfo_blkids_shape_pip_${ts}.log"

cd "$LLUMNIX_DIR"
python - <<'PY'
from pathlib import Path
import blade_kvt

p = Path(blade_kvt.__file__).resolve().parent / "hybrid_connector" / "migration" / "backend.py"
s = p.read_text()
print("installed_backend", p)
print("installed_blockids_occurrences", s.count("blkids=blockids,"))
assert s.count("blkids=blockids,") >= 2
from blade_kvt.hybrid_connector.migration.backend import MigrationBackend
print("MigrationBackend import ok", MigrationBackend)
PY

{
  echo
  echo "## 0721 KVTDInfo blkids shape patch"
  echo "- 当前阶段: 第四阶段，修复最小迁移第一条 fatal"
  echo "- 备份路径: $backup_dir"
  echo "- 修改文件: $BLADE_DIR/blade_kvt/hybrid_connector/migration/backend.py"
  echo "- git diff: $diff_path"
  echo "- 安装命令: python setup.py bdist_wheel; python -m pip install --force-reinstall --no-deps $wheel"
  echo "- 安装结果: MigrationBackend import ok"
  echo "- 第一条 fatal traceback: msgspec.ValidationError Expected array got int at RKVTDInfo.dinfo.blkids"
  echo "- 下一步计划: 重跑四实例最小迁移 probe"
  echo "- 是否已经产出可用实验数据: 已有 baseline，迁移数据尚未成功"
} >> AI_HANDOFF.md

bash "$LLUMNIX_DIR/run_remote_minimal_migration_probe.sh"
