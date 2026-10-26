#!/usr/bin/env bash
set -eo pipefail

source /opt/miniconda3/etc/profile.d/conda.sh
conda activate llumnix5090_vllm018

TS="$(date +%Y%m%d_%H%M%S)"
LLUMNIX_DIR="/data/home/zylu/llumnix-v018-main"
BLADE_DIR="/data/home/zylu/tmp/blade-kvt"
BACKUP_DIR="$BLADE_DIR/backups/migration_d_workers_info_$TS"
DIFF_FILE="$LLUMNIX_DIR/blade_kvt_migration_d_workers_info_diff_$TS.patch"
LOG_FILE="$LLUMNIX_DIR/install_blade_kvt_migration_d_workers_info_$TS.log"

cd "$BLADE_DIR"
git checkout codex/backport-site-compat-20260720_220335
mkdir -p "$BACKUP_DIR"
cp blade_kvt/hybrid_connector/migration/backend.py "$BACKUP_DIR/backend.py"
cp blade_kvt/hybrid_connector/migration/kvtmigration.py "$BACKUP_DIR/kvtmigration.py"

python - <<'PY'
from pathlib import Path

backend = Path("blade_kvt/hybrid_connector/migration/backend.py")
s = backend.read_text()

old = """        self._naming_cli = ncli\n        self._naming_url = vllm_config.kv_transfer_config.get_from_extra_config(\n"""
new = """        self._naming_cli = ncli\n        self._decode_backend = None\n        self._naming_url = vllm_config.kv_transfer_config.get_from_extra_config(\n"""
if old in s and "self._decode_backend = None" not in s:
    s = s.replace(old, new, 1)

marker = """    def get_peer(\n        self,\n        hint: Optional[tuple[str, int]] = None,\n        exclude: Optional[tuple[str, int]] = None,\n    ) -> Optional[tuple[str, int]]:\n"""
helper = """    def set_decode_backend(self, decode_backend: Any) -> None:\n        self._decode_backend = decode_backend\n\n    def _get_d_workers_info(self) -> list[str]:\n        decode_backend = getattr(self, \"_decode_backend\", None)\n        if decode_backend is None:\n            return []\n\n        event = getattr(decode_backend, \"_workers_info_event\", None)\n        if event is not None and not event.is_set():\n            event.wait(timeout=10.0)\n\n        workers_info = list(getattr(decode_backend, \"_workers_info\", []) or [])\n        return [winfo for winfo in workers_info if winfo]\n\n"""
if helper.strip() not in s:
    if marker not in s:
        raise SystemExit("backend.py marker not found")
    s = s.replace(marker, helper + marker, 1)

s = s.replace("d_workers_info=[],", "d_workers_info=self._get_d_workers_info(),")
backend.write_text(s)

kvtmigration = Path("blade_kvt/hybrid_connector/migration/kvtmigration.py")
s = kvtmigration.read_text()
old = "        self._m = MigrationBackend(cfg, role, self._p.naming_cli())\n        \n"
new = "        self._m = MigrationBackend(cfg, role, self._p.naming_cli())\n        self._m.set_decode_backend(self._d)\n        \n"
if old not in s:
    raise SystemExit("kvtmigration.py target not found")
if "self._m.set_decode_backend(self._d)" not in s:
    s = s.replace(old, new, 1)
kvtmigration.write_text(s)
PY

git diff -- blade_kvt/hybrid_connector/migration/backend.py blade_kvt/hybrid_connector/migration/kvtmigration.py | tee "$DIFF_FILE"

rm -rf build dist *.egg-info
python setup.py bdist_wheel 2>&1 | tee "$LOG_FILE"
pip install dist/*.whl --force-reinstall --no-deps 2>&1 | tee -a "$LOG_FILE"

cd "$LLUMNIX_DIR"
python - <<'PY'
from blade_kvt.hybrid_connector.migration.kvtmigration import KVTMigration
from blade_kvt.hybrid_connector.migration.backend import MigrationBackend
print("KVTMigration import ok", KVTMigration)
print("MigrationBackend import ok", MigrationBackend)
print("has set_decode_backend", hasattr(MigrationBackend, "set_decode_backend"))
print("has _get_d_workers_info", hasattr(MigrationBackend, "_get_d_workers_info"))
PY

bash "$LLUMNIX_DIR/run_remote_minimal_migration_probe.sh"

cat >> "$LLUMNIX_DIR/AI_HANDOFF.md" <<EOF

## $TS migration d_workers_info compat
- 当前阶段：第四阶段，最小真实迁移验证。
- 本轮修改文件：blade_kvt/hybrid_connector/migration/backend.py；blade_kvt/hybrid_connector/migration/kvtmigration.py。
- 备份路径：$BACKUP_DIR
- git diff：$DIFF_FILE
- 安装命令：cd $BLADE_DIR && python setup.py bdist_wheel && pip install dist/*.whl --force-reinstall --no-deps
- 测试命令：bash $LLUMNIX_DIR/run_remote_minimal_migration_probe.sh
- 最新结果目录：$(ls -td "$LLUMNIX_DIR"/results/minimal_migration_probe_* | head -1)
- 当前说明：本轮只处理第一条 fatal：KVTDInfo.d_workers_info 为空。迁移请求现在从同一 KVTMigration 的 DBackend 读取真实 workers_info。
EOF
