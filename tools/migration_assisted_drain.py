#!/usr/bin/env python3
"""Paired Natural vs Migration-Assisted Drain experiment on two GPUs."""

from __future__ import annotations

import argparse
import asyncio
import csv
import html
import json
import logging
import math
import os
import random
import re
import signal
import statistics
import subprocess
import time
from collections import Counter
from dataclasses import asdict, dataclass, fields
from datetime import datetime
from pathlib import Path
from typing import Any

import aiohttp
import grpc
import psutil
import redis
import requests
from transformers import AutoTokenizer

from llumnix.cms.proto.cms_pb2 import InstanceMetadata, InstanceStatus
from llumnix.server.proto import llumlet_server_pb2, llumlet_server_pb2_grpc


REPO = Path(__file__).resolve().parents[1]
MODEL = "/data/models/Qwen2.5-7B-Instruct"
DATASET = Path("/data/huxin/docker_data/eassy-method2/LLMServingSim/workloads/sharegpt-qwen3-32b-300-sps10.jsonl")
HOST = "127.0.0.1"
SOURCE_PORT, DEST_PORT = 8100, 8101
SOURCE_RPC, DEST_RPC = 49000, 49001
SOURCE_KVT, DEST_KVT = SOURCE_PORT + 23000, DEST_PORT + 23000
GPU_RELEASE_MIB = 64.0


@dataclass
class Req:
    request_id: str
    arrival_offset_s: float
    prompt_index: int
    sampling_seed: int
    prompt: str = ""
    arrival_time: float | None = None
    source_execution_start: float | None = None
    migration_trigger_time: float | None = None
    destination_prealloc_start: float | None = None
    destination_prealloc_end: float | None = None
    migration_transfer_start: float | None = None
    migration_transfer_end: float | None = None
    source_request_release_time: float | None = None
    destination_resume_time: float | None = None
    request_finish_time: float | None = None
    migration_result: str = "not_requested"
    migration_error: str | None = None
    finish_reason: str | None = None
    completion_tokens: int | None = None
    prompt_tokens: int | None = None
    http_status: int | None = None
    error: str | None = None
    state_at_drain: str | None = None


def write_json(path: Path, value: Any) -> None:
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2), encoding="utf-8")


def iso(ts: float | None = None) -> str:
    return datetime.fromtimestamp(ts or time.time()).astimezone().isoformat(timespec="milliseconds")


def gpu_row(gpu: int) -> dict[str, Any]:
    cmd = ["nvidia-smi", f"--id={gpu}", "--query-gpu=memory.used,utilization.gpu,power.draw", "--format=csv,noheader,nounits"]
    try:
        vals = [x.strip() for x in subprocess.check_output(cmd, text=True, timeout=3).strip().split(",")]
        compute = subprocess.check_output(["nvidia-smi", "--query-compute-apps=pid,gpu_uuid", "--format=csv,noheader,nounits"], text=True, timeout=3)
        return {"gpu_memory_used_mib": float(vals[0]), "gpu_utilization_pct": float(vals[1]),
                "gpu_power_w": float(vals[2]), "compute_processes": len([x for x in compute.splitlines() if x.strip()])}
    except Exception as exc:
        return {"gpu_memory_used_mib": None, "gpu_utilization_pct": None, "gpu_power_w": None,
                "compute_processes": None, "gpu_error": repr(exc)}


def assert_idle_gpus(gpus: tuple[int, int]) -> None:
    raw = subprocess.check_output(["nvidia-smi", "--query-compute-apps=pid,gpu_uuid,used_memory", "--format=csv,noheader,nounits"], text=True)
    if raw.strip():
        raise RuntimeError("GPU compute processes already exist; refusing to interfere:\n" + raw)
    for gpu in gpus:
        if (gpu_row(gpu)["gpu_memory_used_mib"] or 99999) > GPU_RELEASE_MIB:
            raise RuntimeError(f"GPU {gpu} is not idle")


def load_prompts(path: Path, tokenizer: AutoTokenizer) -> list[str]:
    prompts = []
    with path.open(encoding="utf-8") as fh:
        for line in fh:
            row = json.loads(line)
            ids = row.get("input_tok_ids")
            if not ids:
                continue
            prompt = tokenizer.decode(ids, skip_special_tokens=True).strip()
            n = len(tokenizer.encode(prompt, add_special_tokens=True))
            if 16 <= n <= 2800:
                prompts.append(prompt)
    if not prompts:
        raise RuntimeError("No usable prompts")
    return prompts


def make_trace(prompts: list[str], duration: float, qps: float, seed: int, run: int) -> list[Req]:
    rng = random.Random(seed)
    offset, i, out = 0.0, 0, []
    while offset < duration:
        idx = (i + seed) % len(prompts)
        out.append(Req(f"drain-r{run}-{i:05d}", offset, idx, seed + i, prompts[idx]))
        i += 1
        offset += rng.expovariate(qps)
    return out


def save_trace(path: Path, trace: list[Req]) -> None:
    with path.open("w", encoding="utf-8") as fh:
        for r in trace:
            fh.write(json.dumps({"request_id": r.request_id, "arrival_offset_s": r.arrival_offset_s,
                                 "prompt_index": r.prompt_index, "sampling_seed": r.sampling_seed,
                                 "prompt": r.prompt}, ensure_ascii=False) + "\n")


def start_proc(name: str, command: str, log: Path, env: dict[str, str] | None = None) -> subprocess.Popen:
    log.parent.mkdir(parents=True, exist_ok=True)
    stream = log.open("w", encoding="utf-8")
    proc = subprocess.Popen(command, shell=True, cwd=REPO, stdout=stream, stderr=subprocess.STDOUT,
                            env={**os.environ, **(env or {})}, start_new_session=True)
    proc._drain_log_stream = stream  # type: ignore[attr-defined]
    logging.info("started %s pid=%s", name, proc.pid)
    return proc


def stop_proc(proc: subprocess.Popen | None, timeout: float = 30) -> float | None:
    if proc is None:
        return None
    try:
        parent = psutil.Process(proc.pid)
        targets = parent.children(recursive=True) + [parent]
        for p in targets:
            try: p.terminate()
            except psutil.Error: pass
        _, alive = psutil.wait_procs(targets, timeout=timeout)
        for p in alive:
            try: p.kill()
            except psutil.Error: pass
    except psutil.Error:
        pass
    try: proc.wait(timeout=5)
    except subprocess.TimeoutExpired: pass
    stream = getattr(proc, "_drain_log_stream", None)
    if stream: stream.close()
    return time.time()


async def shutdown_source_and_measure(proc: subprocess.Popen, gpu: int, timeout: float = 30) -> tuple[float, float]:
    """Terminate our source process tree while independently timing process and GPU exit."""
    try:
        parent = psutil.Process(proc.pid)
        targets = parent.children(recursive=True) + [parent]
    except psutil.Error:
        targets = []
    for p in targets:
        try: p.terminate()
        except psutil.Error: pass
    deadline = time.time() + timeout
    process_exit = gpu_released = None
    while time.time() < deadline and (process_exit is None or gpu_released is None):
        if process_exit is None and proc.poll() is not None: process_exit = time.time()
        row = gpu_row(gpu)
        if gpu_released is None and row["gpu_memory_used_mib"] is not None and row["gpu_memory_used_mib"] <= GPU_RELEASE_MIB:
            gpu_released = time.time()
        await asyncio.sleep(.5)
    for p in targets:
        try:
            if p.is_running(): p.kill()
        except psutil.Error: pass
    try: proc.wait(timeout=5)
    except subprocess.TimeoutExpired: pass
    process_exit = process_exit or time.time()
    while gpu_released is None:
        row = gpu_row(gpu)
        if row["gpu_memory_used_mib"] is not None and row["gpu_memory_used_mib"] <= GPU_RELEASE_MIB:
            gpu_released = time.time()
        else: await asyncio.sleep(.5)
    stream = getattr(proc, "_drain_log_stream", None)
    if stream: stream.close()
    return process_exit, gpu_released


def vllm_command(gpu: int, port: int, rpc: int, tag: str) -> tuple[str, dict[str, str]]:
    kvt_cfg = json.dumps({"kv_connector": "HybridConnector", "kv_role": "kv_both",
                         "kv_connector_extra_config": {"backend": "kvt+migration", "kvt_inst_id": tag,
                                                       "rpc_port": port + 23000}}, separators=(",", ":"))
    command = (f"vllm serve --enable-log-requests --attention-backend TRITON_ATTN --enforce-eager "
               f"--max-model-len 4096 --max-num-batched-tokens 8192 --gpu-memory-utilization 0.55 "
               f"--model {MODEL} --port {port} --generation-config auto --kv-transfer-config '{kvt_cfg}'")
    env = {"CUDA_VISIBLE_DEVICES": str(gpu), "VLLM_HOST_IP": HOST, "VLLM_USE_V1": "1",
           "VLLM_ENABLE_LLUMNIX": "1", "LLUMNIX_ENABLE_MIGRATION": "1", "LLUMNIX_RPC_PORT": str(rpc),
           "LLUMNIX_CMS_REDIS_ADDRESS": "127.0.0.1", "LLUMNIX_CMS_REDIS_PORT": "6379",
           "LLUMNIX_REPORT_INSTANCE_STATUS_INTERVAL_S": "0.1", "LLUMNIX_MAX_REQ_MIG_IN": "50",
           "LLUMNIX_MAX_REQ_MIG_OUT": "50", "BLLM_KVTRANS_PORT_BASE": str(36218 + 8 * gpu),
           "VLLM_DISABLE_REQUEST_ID_RANDOMIZATION": "1", "VLLM_DEEP_GEMM_WARMUP": "skip",
           "VLLM_ATTENTION_BACKEND": "TRITON_ATTN", "VLLM_LOGGING_LEVEL": "DEBUG",
           "PYTHONPATH": f"{REPO / 'python/llumnix'}:{REPO / 'python/discovery'}:{os.environ.get('PYTHONPATH','')}"}
    return command, env


def wait_http(port: int, timeout: float = 240) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            if requests.get(f"http://127.0.0.1:{port}/health", timeout=2).status_code == 200:
                return
        except requests.RequestException:
            pass
        time.sleep(2)
    raise TimeoutError(f"vLLM {port} did not become ready")


async def prometheus(port: int) -> tuple[int, int]:
    async with aiohttp.ClientSession() as s:
        async with s.get(f"http://127.0.0.1:{port}/metrics", timeout=aiohttp.ClientTimeout(total=3)) as resp:
            body = await resp.text()
    vals = {"vllm:num_requests_running": 0.0, "vllm:num_requests_waiting": 0.0}
    for line in body.splitlines():
        for key in vals:
            if line.startswith(key + "{") or line.startswith(key + " "):
                try: vals[key] += float(line.rsplit(" ", 1)[1])
                except ValueError: pass
    return round(vals["vllm:num_requests_running"]), round(vals["vllm:num_requests_waiting"])


def cms_instances() -> dict[int, tuple[str, InstanceStatus, InstanceMetadata]]:
    db = redis.Redis(host="127.0.0.1", port=6379)
    out = {}
    for key in db.scan_iter("llumnix:instance_metadata:*"):
        md = InstanceMetadata(); md.ParseFromString(db.get(key))
        status_raw = db.get(b"llumnix:instance_status:" + key.split(b":")[-1])
        st = InstanceStatus()
        if status_raw: st.ParseFromString(status_raw)
        out[md.api_server_port] = (key.decode().split(":")[-1], st, md)
    return out


def wait_cms_instances(timeout: float = 60) -> dict[int, tuple[str, InstanceStatus, InstanceMetadata]]:
    deadline = time.time() + timeout
    while time.time() < deadline:
        found = cms_instances()
        if SOURCE_PORT in found and DEST_PORT in found:
            return found
        time.sleep(.2)
    raise TimeoutError("source/destination did not register in CMS")


async def trigger_all_migration(source_rpc: int, destination_kvt: int) -> dict[str, Any]:
    channel = grpc.aio.insecure_channel(f"127.0.0.1:{source_rpc}")
    stub = llumlet_server_pb2_grpc.LlumletStub(channel)
    req = llumlet_server_pb2.MigrateRequest(src_engine_id="source", dst_engine_id="destination",
        dst_engine_ip=HOST, dst_engine_port=destination_kvt, migration_type="NUM_REQ",
        migration_req_policy="LR", num_reqs=-1, trigger_policy="neutral_failover")
    started = time.time()
    try:
        resp = await stub.Migrate(req, timeout=120)
        return {"trigger_time": started, "response_time": time.time(), "success": resp.success, "message": resp.message}
    except Exception as exc:
        return {"trigger_time": started, "response_time": time.time(), "success": False, "message": repr(exc)}
    finally:
        await channel.close()


async def send_one(session: aiohttp.ClientSession, rec: Req) -> None:
    rec.arrival_time = time.time()
    payload = {"model": MODEL, "messages": [{"role": "user", "content": rec.prompt}],
               "seed": rec.sampling_seed, "stream": True, "stream_options": {"include_usage": True}}
    try:
        async with session.post(f"http://127.0.0.1:{SOURCE_PORT}/v1/chat/completions", json=payload,
                                headers={"X-Request-Id": rec.request_id}, timeout=aiohttp.ClientTimeout(total=900)) as resp:
            rec.http_status = resp.status
            if resp.status != 200:
                rec.error = (await resp.text())[:1000]; return
            buf = b""
            async for chunk in resp.content.iter_chunked(8192):
                buf += chunk
                while b"\n" in buf:
                    raw, buf = buf.split(b"\n", 1)
                    line = raw.decode(errors="replace").strip()
                    if not line.startswith("data: ") or line == "data: [DONE]": continue
                    try: event = json.loads(line[6:])
                    except json.JSONDecodeError: continue
                    choices = event.get("choices") or []
                    if choices:
                        content = (choices[0].get("delta") or {}).get("content")
                        if content and rec.source_execution_start is None: rec.source_execution_start = time.time()
                        if choices[0].get("finish_reason"): rec.finish_reason = choices[0]["finish_reason"]
                    usage = event.get("usage") or {}
                    if usage:
                        rec.prompt_tokens = usage.get("prompt_tokens"); rec.completion_tokens = usage.get("completion_tokens")
    except Exception as exc:
        rec.error = repr(exc)
    finally:
        rec.request_finish_time = time.time()


def write_csv(path: Path, rows: list[dict[str, Any]], columns: list[str] | None = None) -> None:
    columns = columns or (list(rows[0]) if rows else [])
    with path.open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=columns); w.writeheader(); w.writerows(rows)


def parse_log_time(line: str, path: Path) -> float | None:
    m = re.search(r"(?:INFO|DEBUG|WARNING|ERROR)\s+(\d{4}-\d\d-\d\d \d\d:\d\d:\d\d[,.]\d+)", line)
    if m:
        try: return datetime.strptime(m.group(1).replace(",", "."), "%Y-%m-%d %H:%M:%S.%f").astimezone().timestamp()
        except ValueError: pass
    return path.stat().st_mtime


def enrich_from_logs(recs: list[Req], source_log: Path, dest_log: Path, trigger: float | None) -> list[dict[str, Any]]:
    events = []
    aliases = {r.request_id: r for r in recs}
    patterns = [
        ("destination_prealloc_start", re.compile(r"migrate\. req=(\S+) prealloc=")),
        ("migration_transfer_start", re.compile(r"disagg start kvt\. reqid=(\S+)")),
        ("migration_transfer_end", re.compile(r"migrate end kvt:.*reqid=(\S+)")),
        ("destination_resume_time", re.compile(r"add req\. reqid=(\S+)")),
        ("source_request_release_time", re.compile(r"FinishedReq=.*?(?:reqid=|request_id=)?([\w.-]+)")),
    ]
    for role, path in (("source", source_log), ("destination", dest_log)):
        if not path.exists(): continue
        for line in path.read_text(errors="replace").splitlines():
            ts = parse_log_time(line, path)
            for event, pat in patterns:
                m = pat.search(line)
                if not m: continue
                raw_id = m.group(1).rstrip(",)")
                rec = next((v for k, v in aliases.items() if k in raw_id), None)
                if rec:
                    if getattr(rec, event) is None: setattr(rec, event, ts)
                    if event == "destination_prealloc_start" and rec.destination_prealloc_end is None:
                        rec.destination_prealloc_end = ts
                    events.append({"timestamp": ts, "request_id": rec.request_id, "event": event, "role": role, "evidence": line[:1000]})
            if "KVTResp(code=" in line or "Migration dispatch summary" in line or "migration failed" in line.lower() or "abort" in line.lower():
                rid = next((k for k in aliases if k in line), "")
                events.append({"timestamp": ts, "request_id": rid, "event": "migration_evidence", "role": role, "evidence": line[:1000]})
    for r in recs:
        if r.state_at_drain and trigger:
            r.migration_trigger_time = trigger
            related = [e["evidence"] for e in events if e["request_id"] == r.request_id]
            if any("KVTResp(code=404" in x or "KVTResp(code=500" in x or "failed_migration" in x.lower() or ("abort req" in x.lower() and "migration.suspend" not in x) for x in related):
                r.migration_result = "failed"; r.migration_error = " | ".join(related[-3:])[:2000]
            elif any("code=0" in x or "FinishedReq=" in x or "migration end" in x.lower() for x in related): r.migration_result = "success"
            elif any("was accepted" in x for x in related): r.migration_result = "accepted_pending_evidence"
            elif r.request_finish_time and r.request_finish_time <= trigger: r.migration_result = "completed_before_trigger"
            else: r.migration_result = "unclassified"
    return sorted(events, key=lambda x: x["timestamp"] or 0)


async def run_group(run_dir: Path, trace_template: list[Req], mode: str, source_gpu: int, dest_gpu: int) -> dict[str, Any]:
    group = run_dir / mode; group.mkdir(parents=True, exist_ok=True)
    recs = [Req(**{f.name: getattr(r, f.name) for f in fields(Req) if f.name in {"request_id","arrival_offset_s","prompt_index","sampling_seed","prompt"}}) for r in trace_template]
    assert_idle_gpus((source_gpu, dest_gpu))
    redis_proc = start_proc("redis", "redis-server --port 6379 --save '' --appendonly no", group / "redis.log")
    source = dest = None
    gpu_metrics: list[dict[str, Any]] = []; inst_metrics: list[dict[str, Any]] = []
    events_path = group / "events.jsonl"
    start_wall = drain = evacuated = shutdown = process_exit = gpu_released = None
    trigger_result = None; destination_drained = None
    try:
        await asyncio.sleep(1)
        source_cmd, source_env = vllm_command(source_gpu, SOURCE_PORT, SOURCE_RPC, f"drain_source_{mode}")
        dest_cmd, dest_env = vllm_command(dest_gpu, DEST_PORT, DEST_RPC, f"drain_dest_{mode}")
        source = start_proc("source", source_cmd, group / "source_vllm.log", source_env)
        dest = start_proc("destination", dest_cmd, group / "destination_vllm.log", dest_env)
        await asyncio.gather(asyncio.to_thread(wait_http, SOURCE_PORT), asyncio.to_thread(wait_http, DEST_PORT))
        registered = await asyncio.to_thread(wait_cms_instances)
        source_rpc = registered[SOURCE_PORT][2].llumlet_port
        destination_kvt = registered[DEST_PORT][2].kvt_port
        start_wall = time.time()
        with events_path.open("a", encoding="utf-8") as fh: fh.write(json.dumps({"event":"experiment_start","timestamp":start_wall})+"\n")
        stop_mon = asyncio.Event()
        async def monitor():
            while not stop_mon.is_set():
                now = time.time()
                try: sr, sw = await prometheus(SOURCE_PORT)
                except Exception: sr, sw = -1, -1
                try: dr, dw = await prometheus(DEST_PORT)
                except Exception: dr, dw = -1, -1
                statuses = cms_instances()
                ss = statuses.get(SOURCE_PORT, ("", InstanceStatus(), InstanceMetadata()))[1]
                ds = statuses.get(DEST_PORT, ("", InstanceStatus(), InstanceMetadata()))[1]
                unfinished = sum(r.request_finish_time is None for r in recs if r.arrival_time is not None)
                inst_metrics.append({"timestamp":now,"elapsed_since_start":now-start_wall,
                    "elapsed_since_drain":None if drain is None else now-drain,
                    "source_running":ss.num_running_requests,"source_waiting":ss.num_waiting_requests,
                    "source_prometheus_running_raw":sr,"source_prometheus_waiting_raw":sw,
                    "source_migrating_out":ss.num_migrate_out_reqs,
                    "destination_running":ds.num_running_requests,"destination_waiting":ds.num_waiting_requests,
                    "destination_prometheus_running_raw":dr,"destination_prometheus_waiting_raw":dw,
                    "destination_migrating_in":ds.num_migrate_in_reqs,"client_unfinished":unfinished})
                for gpu, role, proc in ((source_gpu,"source",source),(dest_gpu,"destination",dest)):
                    row = gpu_row(gpu); row.update({"timestamp":now,"elapsed_since_start":now-start_wall,
                        "elapsed_since_drain":None if drain is None else now-drain,"gpu":gpu,"role":role,
                        "root_process_alive": bool(proc and proc.poll() is None)})
                    gpu_metrics.append(row)
                await asyncio.sleep(.5)
        mon = asyncio.create_task(monitor())
        tasks = []
        async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=0)) as session:
            loop_start = asyncio.get_running_loop().time()
            for r in recs:
                await asyncio.sleep(max(0, loop_start + r.arrival_offset_s - asyncio.get_running_loop().time()))
                tasks.append(asyncio.create_task(send_one(session, r)))
            await asyncio.sleep(max(0, loop_start + max(x.arrival_offset_s for x in recs) - asyncio.get_running_loop().time()))
            # Admission closes at the configured duration, stored in trace metadata by final sentinel gap.
            duration = float(run_dir.parent.joinpath("config.json").exists() and json.loads(run_dir.parent.joinpath("config.json").read_text())["duration_s"] or 180)
            await asyncio.sleep(max(0, loop_start + duration - asyncio.get_running_loop().time()))
            drain = time.time()
            drain_status = cms_instances().get(SOURCE_PORT, ("", InstanceStatus(), InstanceMetadata()))[1]
            sr, sw = drain_status.num_running_requests, drain_status.num_waiting_requests
            for r in recs:
                if r.arrival_time and r.request_finish_time is None:
                    r.state_at_drain = "running" if r.source_execution_start else "waiting_or_prefill"
            drain_ids = {r.request_id for r in recs if r.state_at_drain}
            with events_path.open("a", encoding="utf-8") as fh: fh.write(json.dumps({"event":"drain_start","timestamp":drain,"running":sr,"waiting":sw,"unfinished":len(drain_ids)})+"\n")
            if mode == "migration": trigger_result = await trigger_all_migration(source_rpc, destination_kvt)
            stable = 0
            while True:
                st = cms_instances().get(SOURCE_PORT, ("", InstanceStatus(), InstanceMetadata()))[1]
                cr, cw = st.num_running_requests, st.num_waiting_requests
                source_owned = [r for r in recs if r.request_id in drain_ids and r.request_finish_time is None]
                if mode == "migration" and trigger_result and trigger_result["success"]:
                    # Accepted pre-stop migration hands all selected drain requests off; failures remain visible in source counters.
                    source_owned = [] if cr == 0 and cw == 0 and st.num_migrate_out_reqs == 0 else source_owned
                if cr == 0 and cw == 0 and st.num_migrate_out_reqs == 0 and not source_owned: stable += 1
                else: stable = 0
                if stable >= 2:
                    evacuated = time.time(); break
                if time.time() - drain > 900: raise TimeoutError("source evacuation exceeded 900s")
                await asyncio.sleep(.25)
            with events_path.open("a", encoding="utf-8") as fh: fh.write(json.dumps({"event":"source_evacuated","timestamp":evacuated})+"\n")
            shutdown = time.time()
            process_exit, gpu_released = await shutdown_source_and_measure(source, source_gpu)
            source = None
            await asyncio.gather(*tasks)
            # A migrated request may outlive the source and its original HTTP stream.
            # Keep destination alive until its own instance-level state is empty.
            destination_deadline = time.time() + 900
            while time.time() < destination_deadline:
                dst = cms_instances().get(DEST_PORT, ("", InstanceStatus(), InstanceMetadata()))[1]
                if dst.num_running_requests == 0 and dst.num_waiting_requests == 0 and dst.num_migrate_in_reqs == 0:
                    destination_drained = time.time()
                    break
                await asyncio.sleep(.25)
        stop_mon.set(); await mon
    finally:
        stop_proc(source); stop_proc(dest); stop_proc(redis_proc)
    migration_events = enrich_from_logs(recs, group / "source_vllm.log", group / "destination_vllm.log",
                                       trigger_result["trigger_time"] if trigger_result else None)
    write_csv(group / "request_metrics.csv", [asdict(r) for r in recs])
    write_csv(group / "instance_metrics.csv", inst_metrics)
    write_csv(group / "gpu_metrics.csv", gpu_metrics)
    write_csv(group / "migration_events.csv", migration_events,
              ["timestamp","request_id","event","role","evidence"])
    completed = [r for r in recs if r.finish_reason]
    drain_recs = [r for r in recs if r.state_at_drain]
    accepted = sum(r.migration_result == "success" for r in drain_recs)
    failed = sum(r.migration_result == "failed" for r in drain_recs)
    summary = {"mode":mode,"experiment_start":start_wall,"drain_start":drain,"source_evacuated":evacuated,
        "source_shutdown_start":shutdown,"source_process_exit":process_exit,"source_gpu_released":gpu_released,
        "destination_drained":destination_drained,
        "drain_start_snapshot": next(({"running":x["source_running"],"waiting":x["source_waiting"],
             "migrating":x["source_migrating_out"],"unfinished":x["client_unfinished"]} for x in reversed(inst_metrics) if x["timestamp"] <= drain), {}),
        "source_evacuation_time_s":evacuated-drain,"source_gpu_release_time_s":gpu_released-drain,
        "request_completion_time_s":None if destination_drained is None else destination_drained-drain,
        "migration_trigger":trigger_result,"migration_success_count":accepted,"migration_failure_count":failed,
        "migration_success_rate": accepted/len(drain_recs) if drain_recs else None,
        "finish_reason_counts":dict(Counter(r.finish_reason or "unknown" for r in completed)),
        "kvt_404_count":sum("KVTResp(code=404" in e["evidence"] for e in migration_events),
        "kvt_500_count":sum("KVTResp(code=500" in e["evidence"] for e in migration_events),
        "abort_count":len({e["request_id"] for e in migration_events if e["request_id"] and "abort req" in e["evidence"].lower() and "migration.suspend" not in e["evidence"]})}
    write_json(group / "summary.json", summary)
    return summary


def plot_run(run_dir: Path) -> None:
    mig = run_dir / "migration"; plot = run_dir / "plots"; plot.mkdir(exist_ok=True)
    rows = list(csv.DictReader((mig / "instance_metrics.csv").open(encoding="utf-8")))
    after = [r for r in rows if r["elapsed_since_drain"]]
    grows = [r for r in csv.DictReader((mig/"gpu_metrics.csv").open(encoding="utf-8")) if r["role"]=="source" and r["elapsed_since_drain"]]
    def svg(path: Path, series: list[tuple[str, list[tuple[float,float]], str]], title: str, ylabel: str) -> None:
        w,h,pad=900,480,55
        allpts=[p for _,pts,_ in series for p in pts]
        xmax=max((p[0] for p in allpts),default=1) or 1; ymax=max((p[1] for p in allpts),default=1) or 1
        def xy(p): return pad+p[0]/xmax*(w-2*pad), h-pad-p[1]/ymax*(h-2*pad)
        lines=[]
        for label,pts,color in series:
            coords=" ".join(f"{xy(p)[0]:.1f},{xy(p)[1]:.1f}" for p in pts)
            lines.append(f"<polyline fill='none' stroke='{color}' stroke-width='2' points='{coords}'/><text x='{pad+len(lines)*190-180}' y='42' fill='{color}'>{html.escape(label)}</text>")
        content=f"<svg xmlns='http://www.w3.org/2000/svg' width='{w}' height='{h}'><rect width='100%' height='100%' fill='white'/><text x='{w/2}' y='22' text-anchor='middle' font-size='18'>{html.escape(title)}</text><line x1='{pad}' y1='{h-pad}' x2='{w-pad}' y2='{h-pad}' stroke='black'/><line x1='{pad}' y1='{pad}' x2='{pad}' y2='{h-pad}' stroke='black'/><text x='{w/2}' y='{h-10}' text-anchor='middle'>drain_start 后秒数</text><text x='15' y='{h/2}' transform='rotate(-90 15 {h/2})' text-anchor='middle'>{html.escape(ylabel)}</text>{''.join(lines)}</svg>"
        path.write_text(content,encoding="utf-8")
    x=[float(r["elapsed_since_drain"]) for r in after]
    svg(plot/"request_counts.svg",[(label,list(zip(x,[float(r[k]) for r in after])),color) for (k,label,color) in (("source_running","source running","#1f77b4"),("source_migrating_out","source migrating","#d62728"),("destination_running","destination running","#2ca02c"))],"Source / Destination 请求状态","请求数")
    gx=[float(r["elapsed_since_drain"]) for r in grows]
    svg(plot/"source_gpu.svg",[("memory MiB",list(zip(gx,[float(r["gpu_memory_used_mib"]) for r in grows])),"#1f77b4"),("util %",list(zip(gx,[float(r["gpu_utilization_pct"]) for r in grows])),"#ff7f0e")],"Source GPU 实际释放","数值")


def report(root: Path, pairs: list[dict[str, Any]], config: dict[str, Any]) -> None:
    rows="".join(f"<tr><td>{i}</td><td>{p['migration']['drain_start_snapshot'].get('running')}</td><td>{p['migration']['drain_start_snapshot'].get('waiting')}</td><td>{p['migration']['migration_success_count']}</td><td>{p['migration']['migration_failure_count']}</td><td>{p['natural']['source_evacuation_time_s']:.3f}</td><td>{p['migration']['source_evacuation_time_s']:.3f}</td><td>{p['migration']['source_gpu_release_time_s']:.3f}</td></tr>" for i,p in enumerate(pairs,1))
    sections="".join(f"<h2>第 {i} 轮</h2><img src='run_{i}/plots/request_counts.svg'><img src='run_{i}/plots/source_gpu.svg'><p>请求迁移明细：<a href='run_{i}/migration/request_metrics.csv'>request_metrics.csv</a>；事件证据：<a href='run_{i}/migration/migration_events.csv'>migration_events.csv</a></p>" for i in range(1,len(pairs)+1))
    body=f"""<!doctype html><html lang='zh-CN'><meta charset='utf-8'><title>Migration-Assisted Drain</title><style>body{{font-family:Arial,sans-serif;max-width:1200px;margin:30px auto;line-height:1.55}}table{{border-collapse:collapse;width:100%}}th,td{{border:1px solid #ccc;padding:8px;text-align:right}}th:first-child,td:first-child{{text-align:left}}img{{max-width:48%;margin:1%}}code{{white-space:pre-wrap}}</style><h1>Migration-Assisted Drain 实验</h1><p>Natural 与 Migration 使用每轮同一份 request_trace.jsonl。迁移调用为项目现有 Llumlet/KVT 的预停止接口 NUM_REQ=-1；请求未设置 max_tokens/max_completion_tokens，也未设置 ignore_eos。</p><table><tr><th>轮次</th><th>running</th><th>waiting</th><th>迁移成功</th><th>迁移失败</th><th>Natural Drain(s)</th><th>Migration Evacuation(s)</th><th>Source GPU Release(s)</th></tr>{rows}</table><p>A：drain_start→source_evacuated；B：drain_start→source_gpu_released；C：请求最终完成另见 summary，不与 A/B 混淆。</p>{sections}<h2>配置</h2><code>{html.escape(json.dumps(config,ensure_ascii=False,indent=2))}</code></html>"""
    (root/"report.html").write_text(body,encoding="utf-8")


async def main_async(args: argparse.Namespace) -> None:
    stamp=datetime.now().strftime("%Y%m%d_%H%M%S"); root=Path(args.output or REPO/f"results/migration_assisted_drain_{stamp}")
    root.mkdir(parents=True,exist_ok=False)
    logging.basicConfig(level=logging.INFO,format="%(asctime)s %(levelname)s %(message)s",handlers=[logging.FileHandler(root/"experiment.log"),logging.StreamHandler()])
    tokenizer=AutoTokenizer.from_pretrained(MODEL,trust_remote_code=True); prompts=load_prompts(DATASET,tokenizer)
    model_cfg=json.loads((Path(MODEL)/"generation_config.json").read_text()) if (Path(MODEL)/"generation_config.json").exists() else {}
    config={"model":MODEL,"vllm_version":__import__("vllm").__version__,"source_gpu":args.source_gpu,"destination_gpu":args.dest_gpu,
        "max_model_len":4096,"gpu_memory_utilization":.55,"qps":args.qps,"duration_s":args.duration,"runs":args.runs,
        "seeds":[args.seed+i for i in range(args.runs)],"dataset":str(DATASET),"prompt_filter_tokens":[16,2800],
        "arrival":"Poisson open-loop, deterministic persisted trace","request_max_tokens":"omitted","request_max_completion_tokens":"omitted",
        "request_ignore_eos":"omitted","generation_config_mode":"auto","generation_config_max_new_tokens":model_cfg.get("max_new_tokens"),
        "generation_config_max_length":model_cfg.get("max_length"),"server_output_limit":"max-model-len=4096 context boundary",
        "migration_api":"Llumlet.Migrate(NUM_REQ=-1, LR, neutral_failover)","migration_code_paths":["python/llumnix/llumnix/rpc_server.py","python/llumnix/llumnix/llumlet.py","python/llumnix/llumnix/migration_frontend/vllm_v1/kvt_migration_frontend.py","blade_kvt/hybrid_connector/migration/backend.py"]}
    write_json(root/"config.json",config); pairs=[]
    for i in range(1,args.runs+1):
        rd=root/f"run_{i}"; rd.mkdir(); trace=make_trace(prompts,args.duration,args.qps,args.seed+i-1,i); save_trace(rd/"request_trace.jsonl",trace)
        natural=await run_group(rd,trace,"natural",args.source_gpu,args.dest_gpu)
        migration=await run_group(rd,trace,"migration",args.source_gpu,args.dest_gpu)
        pairs.append({"natural":natural,"migration":migration}); plot_run(rd)
    aggregate={"runs":pairs,"mean_natural_drain_s":statistics.fmean(p["natural"]["source_evacuation_time_s"] for p in pairs),
        "mean_migration_evacuation_s":statistics.fmean(p["migration"]["source_evacuation_time_s"] for p in pairs),
        "mean_migration_gpu_release_s":statistics.fmean(p["migration"]["source_gpu_release_time_s"] for p in pairs),
        "strict_paired_trace":True}
    write_json(root/"aggregate_summary.json",aggregate)
    # Required root-level concatenated artifacts.
    for name in ("request_metrics.csv","instance_metrics.csv","migration_events.csv","gpu_metrics.csv"):
        all_rows=[]
        for i in range(1,args.runs+1):
            for mode in ("natural","migration"):
                p=root/f"run_{i}"/mode/name
                if p.exists():
                    for row in csv.DictReader(p.open(encoding="utf-8")): row.update({"run":i,"mode":mode}); all_rows.append(row)
        write_csv(root/name,all_rows)
    # Root trace contains all per-run traces with run id.
    with (root/"request_trace.jsonl").open("w",encoding="utf-8") as out:
        for i in range(1,args.runs+1):
            for line in (root/f"run_{i}"/"request_trace.jsonl").read_text(encoding="utf-8").splitlines():
                row=json.loads(line); row["run"]=i; out.write(json.dumps(row,ensure_ascii=False)+"\n")
    report(root,pairs,config); print(f"RESULT_DIR={root}")


def main() -> None:
    ap=argparse.ArgumentParser(); ap.add_argument("--duration",type=float,default=180); ap.add_argument("--qps",type=float,default=.5); ap.add_argument("--runs",type=int,default=3); ap.add_argument("--seed",type=int,default=20260923); ap.add_argument("--source-gpu",type=int,default=0); ap.add_argument("--dest-gpu",type=int,default=1); ap.add_argument("--output")
    asyncio.run(main_async(ap.parse_args()))


if __name__ == "__main__": main()
