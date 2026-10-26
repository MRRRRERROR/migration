source /opt/miniconda3/etc/profile.d/conda.sh
conda activate llumnix5090_cuda13
cd /data/home/zylu/llumnix-ray-main

python - <<'PY'
from pathlib import Path

p = Path("llumnix/backends/vllm_v1/core.py")
s = p.read_text()

old = '''            logger.info(
                "MIGRATION_EVENT_V1 event=frontend_migrate_start src_instance_id={} "
                "dst_instance_id={} request_id={} timestamp={}".format(
                    self.instance_id,
                    dst_instance_context.instance_id,
                    migrate_out_request.request_id,
                    time.time(),
                )
            )
            migration_task = asyncio.gather(
                asyncio.wait_for(self.migration_frontend.migrate(target_request, dst_instance_context), timeout=30.0),
                return_exceptions=True)
'''

new = '''            logger.info(
                "MIGRATION_EVENT_V1 event=frontend_migrate_start src_instance_id={} "
                "dst_instance_id={} request_id={} timestamp={}".format(
                    self.instance_id,
                    dst_instance_context.instance_id,
                    migrate_out_request.request_id,
                    time.time(),
                )
            )
            if self.migration_frontend is None:
                logger.error(
                    "MIGRATION_EVENT_V1 event=frontend_unavailable src_instance_id={} "
                    "dst_instance_id={} request_id={} timestamp={} error={}".format(
                        self.instance_id,
                        dst_instance_context.instance_id,
                        migrate_out_request.request_id,
                        time.time(),
                        "vllm.v1.hybrid_connector missing",
                    )
                )
                self.utility_call(True, 'reset_request_migrating_state', migrate_out_request.request_id)
                continue
            migration_task = asyncio.gather(
                asyncio.wait_for(self.migration_frontend.migrate(target_request, dst_instance_context), timeout=30.0),
                return_exceptions=True)
'''

if "event=frontend_unavailable" in s:
    print("frontend unavailable guard already present")
elif old in s:
    s = s.replace(old, new, 1)
    p.write_text(s)
    print("patched frontend unavailable guard")
else:
    raise SystemExit("target migrate start block not found")
PY

python -m py_compile llumnix/backends/vllm_v1/core.py
grep -nE "frontend_unavailable|frontend_migrate_start" llumnix/backends/vllm_v1/core.py
