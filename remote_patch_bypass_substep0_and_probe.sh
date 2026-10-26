#!/usr/bin/env bash
set -eo pipefail

source /opt/miniconda3/etc/profile.d/conda.sh
conda activate llumnix5090_vllm018
set -u

LLUMNIX_DIR=/data/home/zylu/llumnix-v018-main
BLADE_DIR=/data/home/zylu/tmp/blade-kvt
TS=$(date +%Y%m%d_%H%M%S)
BACKUP_DIR="$BLADE_DIR/backups/bypass_substep0_$TS"
DIFF_FILE="$LLUMNIX_DIR/blade_kvt_bypass_substep0_diff_$TS.patch"

cd "$BLADE_DIR"
mkdir -p "$BACKUP_DIR"
cp blade_kvt/hybrid_connector/migration/kvtmigration.py "$BACKUP_DIR/kvtmigration.py"

python - <<'PY'
from pathlib import Path

p = Path("blade_kvt/hybrid_connector/migration/kvtmigration.py")
s = p.read_text()

old_init = '''        self._m = MigrationBackend(cfg, role, self._p.naming_cli())
        self._m.set_decode_backend(self._d)
        
        if role == KVConnectorRole.SCHEDULER:
'''
new_init = '''        self._m = MigrationBackend(cfg, role, self._p.naming_cli())
        self._m.set_decode_backend(self._d)
        self._bypass_main_step_pending = False
        
        if role == KVConnectorRole.SCHEDULER:
'''
if old_init not in s:
    raise SystemExit("init anchor not found")
s = s.replace(old_init, new_init, 1)

old_methods = '''    def bypass_bind(self, meta: BackendMeta):
        self._p.bypass_bind(meta)

    def bypass_clear(self):
        self._p.bypass_clear()
        return
'''
new_methods = '''    def bypass_bind(self, meta: BackendMeta):
        # vLLM 0.18 can publish the parent/main metadata to the bypass
        # worker with substepid=0. Route that through the main-step
        # PBackend path so it is flushed correctly, instead of asserting in
        # the substep-only bypass path. Non-zero substeps keep the original
        # bypass behavior.
        if getattr(meta, "substepid", None) == 0:
            self._p.bind_backend_metadata(meta)
            self._bypass_main_step_pending = True
            return
        self._bypass_main_step_pending = False
        self._p.bypass_bind(meta)

    def bypass_clear(self):
        if self._bypass_main_step_pending:
            self._p.clear_backend_metadata()
            self._bypass_main_step_pending = False
            return
        self._p.bypass_clear()
        return
'''
if old_methods not in s:
    raise SystemExit("bypass methods anchor not found")
s = s.replace(old_methods, new_methods, 1)
p.write_text(s)
print("patched migration/kvtmigration.py for bypass substepid=0")
PY

git diff -- blade_kvt/hybrid_connector/migration/kvtmigration.py | tee "$DIFF_FILE"

export CUDA_HOME="$CONDA_PREFIX"
export PATH="$CUDA_HOME/bin:$PATH"
export CPATH="/data/home/zylu/opt/barex/usr/include:${CPATH:-}"
export CPLUS_INCLUDE_PATH="/data/home/zylu/opt/barex/usr/include:${CPLUS_INCLUDE_PATH:-}"
export C_INCLUDE_PATH="/data/home/zylu/opt/barex/usr/include:${C_INCLUDE_PATH:-}"
export LIBRARY_PATH="/data/home/zylu/opt/barex/usr/lib:${LIBRARY_PATH:-}"
export LD_LIBRARY_PATH="/data/home/zylu/opt/barex/usr/lib:$CONDA_PREFIX/lib:$CONDA_PREFIX/lib64:${LD_LIBRARY_PATH:-}"

rm -rf build dist *.egg-info
python setup.py bdist_wheel 2>&1 | tee "$LLUMNIX_DIR/install_blade_kvt_bypass_substep0_$TS.log"
pip install dist/*.whl --force-reinstall --no-deps

cd "$LLUMNIX_DIR"
python - <<'PY'
from blade_kvt.hybrid_connector.migration.kvtmigration import KVTMigration
print("KVTMigration import ok", KVTMigration)
PY

{
  echo
  echo "## $(date '+%Y-%m-%d %H:%M:%S') bypass substepid=0 compatibility"
  echo "- 当前阶段：第四阶段，minimal migration probe 前置 fatal 修复。"
  echo "- 修改文件：/data/home/zylu/tmp/blade-kvt/blade_kvt/hybrid_connector/migration/kvtmigration.py"
  echo "- 备份路径：$BACKUP_DIR"
  echo "- diff 文件：$DIFF_FILE"
  echo "- 安装命令：python setup.py bdist_wheel && pip install dist/*.whl --force-reinstall --no-deps"
  echo "- import 验证：KVTMigration import ok"
  echo "- 本轮处理 fatal：bypass_bind is the bypass substep path, got substepid=0"
  echo "- 下一步：重跑 minimal migration probe，观察第一条 fatal。"
  echo "- 是否已经产出可用迁移实验数据：否；已有 baseline_no_migration 可用。"
} >> "$LLUMNIX_DIR/AI_HANDOFF.md"

bash "$LLUMNIX_DIR/run_remote_minimal_migration_probe.sh"
