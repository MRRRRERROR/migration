from pathlib import Path

frontend = Path("/data/home/zylu/llumnix-v018-main/python/llumnix/llumnix/migration_frontend/vllm_v1/kvt_migration_frontend.py")
backend = Path("/data/home/zylu/tmp/blade-kvt/blade_kvt/hybrid_connector/migration/backend.py")

s = frontend.read_text()
s = s.replace(
    '        migration_event_id = f"{req.request_id}:{time.time_ns()}:{uuid.uuid4().hex[:8]}"\n',
    '        migration_event_id = req.request_id\n',
    1,
)
frontend.write_text(s)

s = backend.read_text()
s = s.replace(
    '    if migration_event_id is None and req is not None:\n        migration_event_id = _eff_get_param(req, _MIGRATION_EFFICIENCY_EVENT_ID)\n',
    '    if migration_event_id is None and req is not None:\n        migration_event_id = _eff_get_param(req, _MIGRATION_EFFICIENCY_EVENT_ID)\n    if migration_event_id is None and request_id is not None:\n        migration_event_id = request_id\n',
    1,
)
backend.write_text(s)
print("patched migration_event_id to request_id-compatible form")
