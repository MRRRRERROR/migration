from pathlib import Path

p = Path("/data/home/zylu/llumnix-ray-main/llumnix/backends/vllm_v1/migration_frontend.py")
s = p.read_text()

backup = p.with_suffix(p.suffix + ".bak_pre_rpc_debug_autopatch")
if not backup.exists():
    backup.write_text(s)

if "import time\n" not in s:
    s = s.replace("import struct\n", "import struct\nimport time\n", 1)

old = '''    async def migrate(self, req: EngineCoreRequest, dst_instance_context: InstanceContext):
        core_update_params(req, {SRC_INFO: self.naming_instance_id})
        msgbuf = bytearray.fromhex("00 00 00 00 00 00 00 00")
        struct.pack_into("=II", msgbuf, 0, MIGRATE_TO_REQ, 0)
        reqbufs = self._enc.encode_into(req, msgbuf, 8)
        assert len(reqbufs) == 1  # Need Support MsgpackEncoder.aux_buffers
        struct.pack_into("=I", msgbuf, 4, len(msgbuf) - 8)
        reqid = req.request_id
'''

new = '''    async def migrate(self, req: EngineCoreRequest, dst_instance_context: InstanceContext):
        reqid = req.request_id
        logger.info(
            "MIGRATION_EVENT_V1 event=frontend_migrate_enter request_id={} "
            "dst_instance_id={} timestamp={}".format(
                reqid, dst_instance_context.instance_id, time.time()
            )
        )
        logger.info(
            "MIGRATION_EVENT_V1 event=frontend_core_update_start request_id={} timestamp={}".format(
                reqid, time.time()
            )
        )
        core_update_params(req, {SRC_INFO: self.naming_instance_id})
        logger.info(
            "MIGRATION_EVENT_V1 event=frontend_core_update_done request_id={} timestamp={}".format(
                reqid, time.time()
            )
        )
        msgbuf = bytearray.fromhex("00 00 00 00 00 00 00 00")
        struct.pack_into("=II", msgbuf, 0, MIGRATE_TO_REQ, 0)
        logger.info(
            "MIGRATION_EVENT_V1 event=frontend_encode_start request_id={} timestamp={}".format(
                reqid, time.time()
            )
        )
        reqbufs = self._enc.encode_into(req, msgbuf, 8)
        logger.info(
            "MIGRATION_EVENT_V1 event=frontend_encode_done request_id={} reqbufs_len={} msgbuf_len={} timestamp={}".format(
                reqid, len(reqbufs), len(msgbuf), time.time()
            )
        )
        assert len(reqbufs) == 1  # Need Support MsgpackEncoder.aux_buffers
        struct.pack_into("=I", msgbuf, 4, len(msgbuf) - 8)
'''

if "event=frontend_migrate_enter" in s:
    print("pre-rpc debug already patched")
elif old in s:
    s = s.replace(old, new, 1)
    p.write_text(s)
    print("patched pre-rpc migration frontend debug")
else:
    print("target block not found")
    lines = s.splitlines()
    for i in range(55, min(105, len(lines))):
        print(f"{i + 1}: {lines[i]}")
    raise SystemExit(1)
