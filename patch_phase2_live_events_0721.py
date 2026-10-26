from pathlib import Path

ROOT = Path("/data/home/zylu/llumnix-v018-main")
BLADE = Path("/data/home/zylu/tmp/blade-kvt")

frontend = ROOT / "python/llumnix/llumnix/migration_frontend/vllm_v1/kvt_migration_frontend.py"
backend = BLADE / "blade_kvt/hybrid_connector/migration/backend.py"


def patch_frontend() -> None:
    s = frontend.read_text()
    if "def _migration_efficiency_event(" not in s:
        s = s.replace(
            "import asyncio\nimport itertools\nimport queue\nimport threading\n",
            "import asyncio\nimport itertools\nimport json\nimport os\nimport queue\nimport threading\nimport time\nimport uuid\n",
            1,
        )
        marker = "logger = init_logger(__name__)\n\n\n"
        helper = r'''
_MIGRATION_EFFICIENCY_EVENT_ID = "migration_efficiency_event_id"
_MIGRATION_EFFICIENCY_SRC = "migration_efficiency_source_instance_id"
_MIGRATION_EFFICIENCY_DST = "migration_efficiency_destination_instance_id"
_MIGRATION_EFFICIENCY_LOG = os.getenv(
    "LLUMNIX_MIGRATION_EFFICIENCY_LOG",
    "/data/home/zylu/llumnix-v018-main/logs/migration_efficiency_events.jsonl",
)
_MIGRATION_EFFICIENCY_LOG_LOCK = threading.Lock()


def _migration_efficiency_event(event_name: str, **fields):
    event = {
        "event_name": event_name,
        "timestamp_ns": time.time_ns(),
        "timestamp_ms": time.time() * 1000,
    }
    event.update(fields)
    path = _MIGRATION_EFFICIENCY_LOG
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with _MIGRATION_EFFICIENCY_LOG_LOCK:
            with open(path, "a", encoding="utf-8") as f:
                f.write(json.dumps(event, sort_keys=True, default=str) + "\n")
    except Exception:
        logger.exception("failed to write migration efficiency event: %s", event_name)


def _req_len(req, attr, default=None):
    val = getattr(req, attr, default)
    try:
        return len(val) if val is not None else default
    except TypeError:
        return default


'''
        s = s.replace(marker, marker + helper, 1)

    old = """        core_update_params(req, {SRC_INFO: self.peer_addr_port})\n        core_update_params(req, {OUTPUT_TOKENS_N: num_output_tokens})\n        core_update_params(req, {MIGRATION_TRIGGER_POLICY: trigger_policy})\n"""
    new = """        migration_event_id = f\"{req.request_id}:{time.time_ns()}:{uuid.uuid4().hex[:8]}\"\n        source_instance_id = str(getattr(self, \"peer_addr_port\", \"\"))\n        destination_instance_id = f\"{dst_host}:{dst_port}\"\n        core_update_params(req, {SRC_INFO: self.peer_addr_port})\n        core_update_params(req, {OUTPUT_TOKENS_N: num_output_tokens})\n        core_update_params(req, {MIGRATION_TRIGGER_POLICY: trigger_policy})\n        core_update_params(req, {\n            _MIGRATION_EFFICIENCY_EVENT_ID: migration_event_id,\n            _MIGRATION_EFFICIENCY_SRC: source_instance_id,\n            _MIGRATION_EFFICIENCY_DST: destination_instance_id,\n        })\n        _migration_efficiency_event(\n            \"migration_initiated\",\n            migration_event_id=migration_event_id,\n            request_id=req.request_id,\n            source_instance_id=source_instance_id,\n            destination_instance_id=destination_instance_id,\n            seq_len_config=os.getenv(\"LLUMNIX_MIGRATION_EFFICIENCY_SEQ_LEN\"),\n            prompt_len=_req_len(req, \"prompt_token_ids\"),\n            generated_len=_req_len(req, \"output_token_ids\", 0),\n            total_context_len=(\n                (_req_len(req, \"prompt_token_ids\", 0) or 0)\n                + (_req_len(req, \"output_token_ids\", 0) or 0)\n            ),\n            target_output_len=num_output_tokens,\n            trigger_policy=trigger_policy,\n        )\n"""
    if old in s and "_MIGRATION_EFFICIENCY_EVENT_ID: migration_event_id" not in s:
        s = s.replace(old, new, 1)
    frontend.write_text(s)


def patch_backend() -> None:
    s = backend.read_text()
    if "def _migration_efficiency_event(" not in s:
        s = s.replace(
            "import asyncio\nimport struct\nimport time\n",
            "import asyncio\nimport json\nimport os\nimport struct\nimport threading\nimport time\n",
            1,
        )
        marker = "logger = get_logger(__name__)\n\n\n"
        if marker not in s:
            marker = "logger = get_logger(__name__)\n\n"
        helper = r'''
_MIGRATION_EFFICIENCY_EVENT_ID = "migration_efficiency_event_id"
_MIGRATION_EFFICIENCY_SRC = "migration_efficiency_source_instance_id"
_MIGRATION_EFFICIENCY_DST = "migration_efficiency_destination_instance_id"
_MIGRATION_EFFICIENCY_LOG = os.getenv(
    "LLUMNIX_MIGRATION_EFFICIENCY_LOG",
    "/data/home/zylu/llumnix-v018-main/logs/migration_efficiency_events.jsonl",
)
_MIGRATION_EFFICIENCY_LOG_LOCK = threading.Lock()


def _safe_len(val, default=None):
    try:
        return len(val) if val is not None else default
    except TypeError:
        return default


def _eff_get_param(req, key, default=None):
    try:
        return get_param(req, key, default)
    except Exception:
        try:
            return core_get_param(req, key, default)
        except Exception:
            return default


def _migration_efficiency_event(event_name: str, req=None, **fields):
    request_id = fields.pop("request_id", None)
    if request_id is None and req is not None:
        request_id = getattr(req, "request_id", None)
    migration_event_id = fields.pop("migration_event_id", None)
    if migration_event_id is None and req is not None:
        migration_event_id = _eff_get_param(req, _MIGRATION_EFFICIENCY_EVENT_ID)
    source_instance_id = fields.pop("source_instance_id", None)
    if source_instance_id is None and req is not None:
        source_instance_id = _eff_get_param(req, _MIGRATION_EFFICIENCY_SRC)
    destination_instance_id = fields.pop("destination_instance_id", None)
    if destination_instance_id is None and req is not None:
        destination_instance_id = _eff_get_param(req, _MIGRATION_EFFICIENCY_DST)
    prompt_len = fields.pop("prompt_len", None)
    if prompt_len is None and req is not None:
        prompt_len = _safe_len(getattr(req, "prompt_token_ids", None))
    generated_len = fields.pop("generated_len", None)
    if generated_len is None and req is not None:
        generated_len = _safe_len(getattr(req, "output_token_ids", None), 0)
    total_context_len = fields.pop("total_context_len", None)
    if total_context_len is None:
        total_context_len = (prompt_len or 0) + (generated_len or 0)
    event = {
        "event_name": event_name,
        "timestamp_ns": time.time_ns(),
        "timestamp_ms": time.time() * 1000,
        "migration_event_id": migration_event_id,
        "request_id": request_id,
        "source_instance_id": source_instance_id,
        "destination_instance_id": destination_instance_id,
        "seq_len_config": os.getenv("LLUMNIX_MIGRATION_EFFICIENCY_SEQ_LEN"),
        "prompt_len": prompt_len,
        "generated_len": generated_len,
        "total_context_len": total_context_len,
    }
    event.update(fields)
    path = _MIGRATION_EFFICIENCY_LOG
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with _MIGRATION_EFFICIENCY_LOG_LOCK:
            with open(path, "a", encoding="utf-8") as f:
                f.write(json.dumps(event, sort_keys=True, default=str) + "\n")
    except Exception:
        logger.exception("failed to write migration efficiency event: %s", event_name)


'''
        s = s.replace(marker, marker + helper, 1)

    replacements = [
        (
            "        req = self._reqstate.get(susreq.reqid)\n",
            "        req = self._reqstate.get(susreq.reqid)\n        _migration_efficiency_event(\"final_suspend_begin\", req=req, request_id=susreq.reqid)\n",
        ),
        (
            "        logger.info(\n            \"suspend end: reqid=%s, stopped=%s, outputs=%s\",\n            req.request_id,\n            stopped,\n            req.num_output_tokens,\n        )\n",
            "        logger.info(\n            \"suspend end: reqid=%s, stopped=%s, outputs=%s\",\n            req.request_id,\n            stopped,\n            req.num_output_tokens,\n        )\n        _migration_efficiency_event(\n            \"source_release\",\n            req=req,\n            stopped=stopped,\n            output_tokens=req.num_output_tokens,\n            suspend_code=resp.code,\n        )\n",
        ),
        (
            "            prealloc = core_get_param(req, OUTPUT_TOKENS_N, 0)\n",
            "            prealloc = core_get_param(req, OUTPUT_TOKENS_N, 0)\n            _migration_efficiency_event(\"prealloc_begin\", req=req, prealloc=prealloc)\n",
        ),
        (
            "                logger.warning(\"no migrate in slots available\")\n",
            "                _migration_efficiency_event(\"prealloc_abort\", req=req, prealloc=prealloc, code=CODE_MIGRATE_REJECTED_BUSY)\n                logger.warning(\"no migrate in slots available\")\n",
        ),
        (
            "            writer.write(msgbuf)\n\n            # though SRC_INFO is set to tuple, but it is received as list\n",
            "            writer.write(msgbuf)\n            _migration_efficiency_event(\"prealloc_ack\", req=req, prealloc=prealloc, code=CODE_OK)\n\n            # though SRC_INFO is set to tuple, but it is received as list\n",
        ),
        (
            "            core_update_params(req, {SRC_INFO: srcinfo})\n",
            "            core_update_params(req, {SRC_INFO: srcinfo})\n            core_update_params(req, {_MIGRATION_EFFICIENCY_SRC: str(srcinfo)})\n            core_update_params(req, {_MIGRATION_EFFICIENCY_DST: str(getattr(self, \"peer_id\", None) or getattr(self, \"_inst_id\", None))})\n",
        ),
        (
            "        start_ts = time.monotonic()\n\n        resp: Optional[KVTResp] = None\n",
            "        start_ts = time.monotonic()\n        _migration_efficiency_event(\n            \"stage_copy_begin\",\n            req=req,\n            stage_id=0,\n            stage_kind=\"kvt_suspend\",\n            cached_tokens=req.num_computed_tokens,\n            max_tokens=alltokens,\n            block_count=sum(len(x) for x in blockids),\n            block_size=blksize,\n        )\n        _migration_efficiency_event(\"final_copy_begin\", req=req, stage_id=0, stage_kind=\"kvt_suspend\")\n\n        resp: Optional[KVTResp] = None\n",
        ),
        (
            "        logger.info(\n            \"migrate end kvt: dur_ms=%s, reqid=%s cached=%s max=%s kvtresp=%s\",\n            dur_ms, req.request_id, req.num_computed_tokens, alltokens, resp)\n",
            "        logger.info(\n            \"migrate end kvt: dur_ms=%s, reqid=%s cached=%s max=%s kvtresp=%s\",\n            dur_ms, req.request_id, req.num_computed_tokens, alltokens, resp)\n        _migration_efficiency_event(\n            \"stage_copy_end\",\n            req=req,\n            stage_id=0,\n            stage_kind=\"kvt_suspend\",\n            copy_time_ms=dur_ms,\n            stage_count=1,\n            per_stage_copy_time_ms=[dur_ms],\n            total_copy_time_ms=dur_ms,\n            final_stage_copy_time_ms=dur_ms,\n            kvt_code=getattr(resp, \"code\", None),\n            kvt_cached=getattr(resp, \"cached\", None),\n            kvt_computed=getattr(resp, \"computed\", None),\n            output_token_count=_safe_len(getattr(resp, \"output_token_ids\", None), 0),\n        )\n        _migration_efficiency_event(\n            \"final_copy_end\",\n            req=req,\n            stage_id=0,\n            final_stage_copy_time_ms=dur_ms,\n            kvt_code=getattr(resp, \"code\", None),\n        )\n",
        ),
        (
            "        start_ts = time.monotonic()\n        kvtresp = await self._kvt_rpc(srcinfo, kvtreq)\n",
            "        start_ts = time.monotonic()\n        _migration_efficiency_event(\n            \"stage_copy_begin\",\n            req=req,\n            stage_id=0,\n            stage_kind=\"kvt\",\n            cached_tokens=req.num_computed_tokens,\n            max_tokens=alltokens,\n            block_count=sum(len(x) for x in blockids),\n            block_size=blksize,\n        )\n        kvtresp = await self._kvt_rpc(srcinfo, kvtreq)\n",
        ),
        (
            "        assert kvtresp.code == CODE_OK\n        req.num_computed_tokens = kvtresp.cached\n        return\n",
            "        _migration_efficiency_event(\n            \"stage_copy_end\",\n            req=req,\n            stage_id=0,\n            stage_kind=\"kvt\",\n            copy_time_ms=dur_ms,\n            stage_count=1,\n            per_stage_copy_time_ms=[dur_ms],\n            total_copy_time_ms=dur_ms,\n            kvt_code=getattr(kvtresp, \"code\", None),\n            kvt_cached=getattr(kvtresp, \"cached\", None),\n            kvt_computed=getattr(kvtresp, \"computed\", None),\n        )\n        assert kvtresp.code == CODE_OK\n        req.num_computed_tokens = kvtresp.cached\n        return\n",
        ),
        (
            "            resp = await self._kvt_suspend(srcinfo, req, blocks)\n            assert resp.output_token_ids is not None\n\n            req.append_output_token_ids(resp.output_token_ids)\n            req.num_computed_tokens = resp.cached\n",
            "            resp = await self._kvt_suspend(srcinfo, req, blocks)\n            assert resp.output_token_ids is not None\n\n            _migration_efficiency_event(\"commit_begin\", req=req, kvt_code=getattr(resp, \"code\", None))\n            req.append_output_token_ids(resp.output_token_ids)\n            req.num_computed_tokens = resp.cached\n            _migration_efficiency_event(\n                \"commit_done\",\n                req=req,\n                kvt_cached=getattr(resp, \"cached\", None),\n                output_token_count=_safe_len(getattr(resp, \"output_token_ids\", None), 0),\n            )\n            _migration_efficiency_event(\"dst_resume\", req=req, dst_first_token_after_resume_ts=None, downtime_resume_ms=\"unavailable\")\n",
        ),
        (
            "            logger.info(\n                \"migration end. reqid=%s, duration_ms=%s\",\n                req.request_id,\n                migration_e2e_time_ms,\n            )\n",
            "            logger.info(\n                \"migration end. reqid=%s, duration_ms=%s\",\n                req.request_id,\n                migration_e2e_time_ms,\n            )\n            _migration_efficiency_event(\n                \"migration_done\",\n                req=req,\n                total_migration_time_ms=migration_e2e_time_ms,\n                dst_first_token_after_resume_ts=None,\n                downtime_resume_ms=\"unavailable\",\n            )\n",
        ),
        (
            "        if req is None:\n            return\n",
            "        if req is None:\n            _migration_efficiency_event(\"request_finished\", request_id=reqid)\n            return\n",
        ),
        (
            "        if evt is None:\n            return\n        self._loop.call_soon_threadsafe(evt.set)\n",
            "        if evt is None:\n            _migration_efficiency_event(\"request_finished\", req=req, request_id=reqid)\n            return\n        _migration_efficiency_event(\"request_finished\", req=req, request_id=reqid)\n        self._loop.call_soon_threadsafe(evt.set)\n",
        ),
    ]
    for old, new in replacements:
        if old in s and new not in s:
            s = s.replace(old, new, 1)
    backend.write_text(s)


patch_frontend()
patch_backend()
print("patched phase2 live migration events")
