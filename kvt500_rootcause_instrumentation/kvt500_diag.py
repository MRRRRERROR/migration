from __future__ import annotations

import json
import os
import threading
import time
from pathlib import Path
from typing import Any


_LOCK = threading.Lock()
_ATTEMPTS: dict[str, str] = {}
_SEQUENCES: dict[str, int] = {}


def _request_id(req_or_id: Any) -> str:
    if isinstance(req_or_id, str):
        return req_or_id
    return str(getattr(req_or_id, "request_id", "unknown"))


def start_attempt(req_or_id: Any) -> str:
    request_id = _request_id(req_or_id)
    with _LOCK:
        existing = _ATTEMPTS.get(request_id)
        if existing is not None:
            return existing
        sequence = _SEQUENCES.get(request_id, 0) + 1
        _SEQUENCES[request_id] = sequence
        attempt_id = f"{request_id}#mig{sequence}"
        _ATTEMPTS[request_id] = attempt_id
        return attempt_id


def attempt_id(req_or_id: Any) -> str:
    request_id = _request_id(req_or_id)
    with _LOCK:
        return _ATTEMPTS.get(request_id, f"{request_id}#mig1")


def is_active(req_or_id: Any) -> bool:
    with _LOCK:
        return _request_id(req_or_id) in _ATTEMPTS


def finish_attempt(req_or_id: Any) -> None:
    with _LOCK:
        _ATTEMPTS.pop(_request_id(req_or_id), None)


def block_groups(value: Any) -> list[list[int]] | None:
    if value is None:
        return None
    if hasattr(value, "blocks"):
        value = value.blocks
    result: list[list[int]] = []
    try:
        for group in value:
            ids: list[int] = []
            for block in group:
                block_id = getattr(block, "block_id", block)
                ids.append(int(block_id))
            result.append(ids)
        return result
    except Exception:
        return None


def request_fields(req: Any) -> dict[str, Any]:
    if req is None:
        return {
            "request_status": None,
            "finished": None,
            "preempted": None,
            "computed_tokens": None,
            "seen_tokens": None,
        }
    status = getattr(req, "status", None)
    status_text = getattr(status, "name", str(status) if status is not None else None)
    try:
        finished = bool(req.is_finished())
    except Exception:
        finished = bool(getattr(req, "finished", False))
    return {
        "request_status": status_text,
        "finished": finished,
        "preempted": bool(status_text and "PREEMPT" in status_text.upper()),
        "computed_tokens": getattr(req, "num_computed_tokens", None),
        "seen_tokens": getattr(req, "num_computed_tokens", None),
    }


def emit(event: str, req_or_id: Any, **fields: Any) -> None:
    try:
        request_id = _request_id(req_or_id)
        groups = fields.pop("block_groups", None)
        normalized = block_groups(groups) if groups is not None else None
        payload: dict[str, Any] = {
            "timestamp": time.time(),
            "timestamp_ns": time.time_ns(),
            "attempt_id": attempt_id(request_id),
            "request_id": request_id,
            "src_instance": fields.pop("src_instance", None),
            "dst_instance": fields.pop("dst_instance", None),
            "component": fields.pop("component", None),
            "event": event,
            "request_status": fields.pop("request_status", None),
            "finished": fields.pop("finished", None),
            "preempted": fields.pop("preempted", None),
            "computed_tokens": fields.pop("computed_tokens", None),
            "seen_tokens": fields.pop("seen_tokens", None),
            "scheduler_membership": fields.pop("scheduler_membership", None),
            "inflight_kvt_membership": fields.pop("inflight_kvt_membership", None),
            "block_group_count": len(normalized) if normalized is not None else None,
            "block_group_sizes": [len(group) for group in normalized] if normalized is not None else None,
            "block_groups": normalized,
            "access_group_index": fields.pop("access_group_index", None),
            "access_block_index": fields.pop("access_block_index", None),
            "migration_state": fields.pop("migration_state", None),
            "pid": os.getpid(),
            "thread": threading.current_thread().name,
        }
        payload.update(fields)
        path = Path(os.environ.get("KVT500_DIAG_LOG", "kvt500_block_lifecycle.jsonl"))
        path.parent.mkdir(parents=True, exist_ok=True)
        line = (json.dumps(payload, ensure_ascii=False, default=str) + "\n").encode("utf-8")
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o644)
        try:
            os.write(fd, line)
        finally:
            os.close(fd)
    except Exception:
        # Diagnostics must never alter migration behavior.
        return


def emit_request(event: str, req: Any, **fields: Any) -> None:
    snapshot = request_fields(req)
    snapshot.update(fields)
    emit(event, req, **snapshot)
