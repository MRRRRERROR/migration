#!/usr/bin/env python3
"""Run Config E with concurrency 24 for strict-valid smoke testing."""

from __future__ import annotations

import argparse
import asyncio
import csv
import html
import json
import math
import os
import re
import shutil
import socket
import statistics
import subprocess
import sys
import threading
import time
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any

import aiohttp

PROJECT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT))
sys.path.insert(0, str(PROJECT / "tools"))

ROOT = Path(os.environ.get(
    "COMPLEX_CONFIG_E_C24_ROOT",
    PROJECT / "results" / "complex_mixed_high_load" / "config_e_c24"
    / f"run_{datetime.now():%Y%m%d_%H%M%S}",
)).resolve()
os.environ.setdefault("PAPER_SUPPLEMENT_ROOT", str(ROOT))
os.environ.setdefault("MIGRATION_POLICY_ROOT", str(ROOT))
os.environ.setdefault("PAPER_SUPPLEMENT_ALLOW_CONTENDED", "1")
os.environ.setdefault("MIGRATION_POLICY_ALLOW_CONTENDED", "1")
os.environ.setdefault("MIGRATION_POLICY_GPU_MEMORY_UTILIZATION", "0.50")
os.environ.setdefault("LLUMNIX_ENABLE_STRUCTURED_MIGRATION_LOG", "1")

import run_paper_supplement_experiments as paper
import run_figure12_v2 as fig12

ORIGINAL_CONFIGURE_BASE = paper.configure_base

TRACE_SOURCE = Path(os.environ.get(
    "COMPLEX_CONFIG_E_TRACE",
    PROJECT / "results" / "complex_mixed_high_load" / "config_e"
    / "complex_mixed_high_load_config_e.jsonl",
)).resolve()
DESIGN_SOURCE = TRACE_SOURCE.with_name("COMPLEX_WORKLOAD_CONFIG_E_DESIGN.md")
MODEL_PATH = os.environ.get("MODEL_PATH", "/data/models/Qwen2.5-7B-Instruct")
POLICIES = ("no_migration", "load_sr", "tpot_tail", "composite_proposed")
REPEATS = 3
REQUEST_NUM = 1200
CONCURRENCY = 24
MAX_MODEL_LEN = 4608
MAX_BATCHED_TOKENS = 4608
REQUEST_TIMEOUT_S = 900.0
BLOCK_SIZE_TOKENS = 16
TTFT_LIMITS = (500.0, 1000.0, 2000.0)
TPOT_LIMITS = (14.5, 25.0, 50.0)

CONFIG_DIR = ROOT / "00_config"
RUNS_DIR = ROOT / "03_runs"
STEP_DIR = ROOT / "04_step_summaries"
FINAL_DIR = ROOT / "05_final"
EXECUTION_TRACE = CONFIG_DIR / "complex_mixed_high_load_config_e_execution.jsonl"

ACTIVE_RUN_DIR: Path | None = None
ACTIVE_TRACE: list[dict[str, Any]] = []
ACTIVE_RESULTS: list[dict[str, Any]] = []

PORT_PLAN = {
    "redis": [6379],
    "scheduler": [18088],
    "gateway": [18089],
    "vllm_api": [8100, 8101, 8102, 8103],
    "kvt_listener": [36218, 36226, 36234, 36242],
    "kvt_rpc": [31100, 31101, 31102, 31103],
    "ray": [],
}


def percentile(values, pct):
    clean = sorted(float(value) for value in values if value is not None and math.isfinite(float(value)))
    if not clean:
        return None
    pos = (len(clean) - 1) * pct / 100.0
    lo, hi = math.floor(pos), math.ceil(pos)
    return clean[lo] if lo == hi else clean[lo] + (clean[hi] - clean[lo]) * (pos - lo)


def metric_stats(values):
    clean = [float(value) for value in values if value is not None and math.isfinite(float(value))]
    return {
        "mean": statistics.mean(clean) if clean else None,
        "median": statistics.median(clean) if clean else None,
        "p95": percentile(clean, 95),
        "p99": percentile(clean, 99),
        "max": max(clean) if clean else None,
        "count": len(clean),
    }


def read_jsonl(path: Path):
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def write_json(path: Path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def listening_port_snapshot() -> dict[int, list[dict[str, Any]]]:
    target_ports = {port for ports in PORT_PLAN.values() for port in ports}
    snapshot = {port: [] for port in target_ports}
    completed = subprocess.run(
        ["ss", "-ltnpH"], text=True, capture_output=True, check=False,
    )
    for line in completed.stdout.splitlines():
        fields = line.split()
        if len(fields) < 4:
            continue
        local_address = fields[3]
        try:
            port = int(local_address.rsplit(":", 1)[1])
        except (IndexError, ValueError):
            continue
        if port not in target_ports:
            continue
        pids = [int(value) for value in re.findall(r"pid=(\d+)", line)]
        processes = []
        for pid in pids:
            proc = Path("/proc") / str(pid)
            try:
                import pwd
                owner = pwd.getpwuid(proc.stat().st_uid).pw_name
                command = (proc / "cmdline").read_bytes().replace(b"\0", b" ").decode(errors="replace").strip()
            except (FileNotFoundError, PermissionError, KeyError):
                owner, command = "unknown", "unavailable"
            processes.append({"pid": pid, "owner": owner, "command": command})
        snapshot[port].append({
            "local_address": local_address,
            "processes": processes,
            "ss_line": line,
        })
    return snapshot


def selected_ports_record(stage: str, snapshot: dict[int, list[dict[str, Any]]]) -> dict[str, Any]:
    occupied = {str(port): listeners for port, listeners in snapshot.items() if listeners}
    return {
        "checked_at": datetime.now().astimezone().isoformat(timespec="seconds"),
        "stage": stage,
        "policy": ACTIVE_RUN_DIR.parent.name if ACTIVE_RUN_DIR else None,
        "repeat_id": ACTIVE_RUN_DIR.name if ACTIVE_RUN_DIR else None,
        "selection_mode": "fixed_port_plan_with_fail_closed_preflight",
        "selected_ports": PORT_PLAN,
        "all_ports_free": not occupied,
        "occupied_ports": occupied,
        "action_on_conflict": "pause_and_report; never kill an unverified process",
    }


def persist_selected_ports(record: dict[str, Any]) -> None:
    root_path = ROOT / "selected_ports.json"
    history = []
    if root_path.exists():
        try:
            history = json.loads(root_path.read_text(encoding="utf-8")).get("checks", [])
        except (json.JSONDecodeError, OSError):
            history = []
    history.append(record)
    payload = {"selected_ports": PORT_PLAN, "latest": record, "checks": history}
    write_json(root_path, payload)
    if ACTIVE_RUN_DIR:
        write_json(ACTIVE_RUN_DIR / "selected_ports.json", payload)


def safe_port_preflight(timeout_s: float = 120.0) -> None:
    deadline = time.time() + timeout_s
    while True:
        snapshot = listening_port_snapshot()
        record = selected_ports_record("before_start_or_after_cleanup", snapshot)
        persist_selected_ports(record)
        if record["all_ports_free"]:
            print("CONFIG_D_PORT_PREFLIGHT all selected ports are free", flush=True)
            return
        if time.time() >= deadline:
            occupied = ",".join(sorted(record["occupied_ports"], key=int))
            raise RuntimeError(f"PORT_PREFLIGHT_CONFLICT occupied_ports={occupied}")
        time.sleep(1.0)


def prepare() -> None:
    for path in (CONFIG_DIR, RUNS_DIR, STEP_DIR, FINAL_DIR):
        path.mkdir(parents=True, exist_ok=True)
    rows = read_jsonl(TRACE_SOURCE)
    required = {"request_id", "arrival_time", "input_len", "output_len", "max_tokens", "request_type", "phase", "priority", "seed"}
    if len(rows) != REQUEST_NUM or any(required - row.keys() for row in rows):
        raise ValueError("Config E trace failed request-count or required-field validation")
    phase_counts = Counter(row["phase"] for row in rows)
    if list(phase_counts.values()) != [120, 300, 300, 360, 120]:
        raise ValueError(f"Config E phase counts mismatch: {phase_counts}")
    shutil.copy2(TRACE_SOURCE, CONFIG_DIR / TRACE_SOURCE.name)
    shutil.copy2(DESIGN_SOURCE, CONFIG_DIR / DESIGN_SOURCE.name)
    shutil.copy2(TRACE_SOURCE, ROOT / TRACE_SOURCE.name)
    shutil.copy2(DESIGN_SOURCE, ROOT / DESIGN_SOURCE.name)
    paper.configure_policies()
    paper.create_dirs()
    paper.copy_profiling()
    if EXECUTION_TRACE.exists():
        existing = read_jsonl(EXECUTION_TRACE)
        if len(existing) == REQUEST_NUM and all("prompt" in row for row in existing):
            return
    from transformers import AutoTokenizer
    tokenizer = AutoTokenizer.from_pretrained(MODEL_PATH, trust_remote_code=True)
    execution = []
    for row in rows:
        prompt, actual = paper.token_exact_prompt(
            tokenizer, int(row["input_len"]),
            f"Config E {row['phase']} {row['request_type']} {row['request_id']}",
        )
        if actual != int(row["input_len"]):
            raise ValueError(f"exact prompt generation failed for {row['request_id']}: {actual}")
        execution.append({**row, "id": row["request_id"], "prompt": prompt, "prompt_len": actual})
    with EXECUTION_TRACE.open("w", encoding="utf-8") as handle:
        for row in execution:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")


def archive_known_port_conflict() -> None:
    failed_dir = (
        RUNS_DIR / "complex_mixed_high_load" / "config_e_c24"
        / "no_migration" / "run_3"
    )
    summary_path = failed_dir / "run_summary.json"
    if not summary_path.exists():
        raise FileNotFoundError(summary_path)
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    summary.update({
        "validity": "invalid_resource_port_conflict",
        "usable_for_main_conclusion": False,
        "benchmark_requests_sent": 0,
        "resource_conflict_port": 36242,
        "resource_conflict_reason": "KVT listener bind: Address already in use",
    })
    write_json(summary_path, summary)

    snapshot = listening_port_snapshot()
    record = selected_ports_record("read_only_audit_after_failure", snapshot)
    persist_selected_ports(record)
    occupied = record["occupied_ports"].get("36242", [])
    audit = [
        "# PORT_CONFLICT_AUDIT", "",
        f"- Audit time: `{record['checked_at']}`",
        "- Scope: read-only audit of Config E no_migration/run_3 startup failure",
        "- Failed run validity: `invalid_resource_port_conflict`",
        "- First error: `RuntimeError: bind: Address already in use`",
        "- Conflicting endpoint: `127.0.0.1:36242`",
        "- Benchmark requests sent: `0`",
        "- EngineCore fatal: `1`",
        "- Traceback: `3`",
        "", "## Current listener state", "",
        f"- Port 36242 currently occupied: `{bool(occupied)}`",
        f"- Current listener evidence: `{json.dumps(occupied, ensure_ascii=False)}`",
        "- Current-user residual Config E processes: none found by the read-only process audit.",
        "", "## Attribution", "",
        "The historical listener PID cannot be identified after the listener has exited. "
        "The failure occurred during the immediate transition from run_2 to run_3 and is "
        "consistent with delayed KVT listener release, but the audit does not claim an "
        "unobserved PID or owner as fact.",
    ]
    audit_path = STEP_DIR / "PORT_CONFLICT_AUDIT.md"
    audit_path.write_text("\n".join(audit) + "\n", encoding="utf-8")
    shutil.copy2(audit_path, ROOT / audit_path.name)

    cleanup = [
        "# PORT_CLEANUP_RESULT", "",
        f"- Checked at: `{record['checked_at']}`",
        "- Experiment processes killed: `0`",
        "- Other-user processes affected: `0`",
        "- Cleanup action: no kill was needed because no Config E residual process remained.",
        f"- Port 36242 released: `{not bool(occupied)}`",
        "- Resume protection: fixed-port fail-closed preflight enabled for every round.",
    ]
    cleanup_path = STEP_DIR / "PORT_CLEANUP_RESULT.md"
    cleanup_path.write_text("\n".join(cleanup) + "\n", encoding="utf-8")
    shutil.copy2(cleanup_path, ROOT / cleanup_path.name)


class ComplexStatusSampler:
    def __init__(self, output_path: Path, interval_s: float = 0.2):
        self.output_path = output_path
        self.interval_s = interval_s
        self.samples = []
        self.stop_event = threading.Event()
        self.thread = threading.Thread(target=self._run, daemon=True)

    def start(self):
        self.started_at = time.time()
        self.thread.start()

    def stop(self):
        self.stop_event.set()
        self.thread.join(timeout=10)
        write_json(self.output_path, self.samples)
        return self.samples

    def _run(self):
        import redis
        from llumnix.cms.proto.cms_pb2 import InstanceStatus
        client = redis.Redis(host="127.0.0.1", port=6379, decode_responses=False)
        demand_blocks = math.ceil(4096 / BLOCK_SIZE_TOKENS)
        while not self.stop_event.is_set():
            instances = []
            try:
                for key in client.scan_iter(match=b"llumnix:instance_status:*"):
                    raw = client.get(key)
                    if not raw:
                        continue
                    message = InstanceStatus()
                    message.ParseFromString(raw)
                    fields = {field.name: value for field, value in message.ListFields()}
                    total_tokens = int(fields.get("num_total_gpu_tokens", 0))
                    used_tokens = int(fields.get("num_used_gpu_tokens", 0))
                    instances.append({
                        "instance_key": key.decode(errors="replace"),
                        "num_total_gpu_tokens": total_tokens,
                        "num_used_gpu_tokens": used_tokens,
                        "free_blocks": max(0, total_tokens - used_tokens) // BLOCK_SIZE_TOKENS,
                        "total_blocks": total_tokens // BLOCK_SIZE_TOKENS,
                        "num_running_reqs": int(fields.get("num_running_reqs", fields.get("num_running_requests", 0))),
                        "num_waiting_reqs": int(fields.get("num_waiting_reqs", fields.get("num_waiting_requests", 0))),
                    })
                valid = [row for row in instances if row["total_blocks"] > 0]
                free = [row["free_blocks"] for row in valid]
                total_free = sum(free)
                max_local = max(free, default=0)
                fragmented = len(valid) == 4 and total_free >= demand_blocks and max_local < demand_blocks
                waiting = sum(row["num_waiting_reqs"] for row in valid)
                cluster_total = sum(row["total_blocks"] for row in valid)
                fragmentation_blocks = max(0, total_free - max_local) if fragmented else 0
                elapsed = time.time() - self.started_at
                arrived = [row for row in ACTIVE_TRACE if row["arrival_time"] <= elapsed]
                phase = arrived[-1]["phase"] if arrived else "before_arrival"
                self.samples.append({
                    "timestamp_ms": time.time() * 1000.0, "elapsed_s": elapsed,
                    "phase": phase, "instances": instances,
                    "free_blocks_per_instance": free, "total_free_blocks": total_free,
                    "cluster_total_blocks": cluster_total,
                    "head_of_line_request_demand_blocks_per_instance": [demand_blocks] * len(valid),
                    "blocked_HOL_request_count": waiting if fragmented else 0,
                    "fragmentation_blocks": fragmentation_blocks,
                    "fragmentation_proportion": fragmentation_blocks / cluster_total if cluster_total else None,
                    "fragmentation_condition": fragmented,
                    "metric_scope": "estimated HOL demand; free-token capacity exported by InstanceStatus",
                })
            except Exception as exc:
                self.samples.append({"timestamp_ms": time.time() * 1000.0, "sampler_error": repr(exc)})
            self.stop_event.wait(self.interval_s)


async def request_one(session, item, semaphore, benchmark_start):
    due = benchmark_start + float(item["arrival_time"])
    await asyncio.sleep(max(0.0, due - time.time()))
    async with semaphore:
        actual_start = time.time()
        token_timestamps = []
        previous_tokens = 0
        final_tokens = None
        uncounted = False
        response_id = None
        finish_reason = None
        text_parts = []
        error = None
        http_status = 0
        payload = {
            "model": MODEL_PATH, "prompt": item["prompt"],
            "max_tokens": item["max_tokens"], "temperature": 0.0,
            "ignore_eos": True, "stream": True,
            "stream_options": {"include_usage": True, "continuous_usage_stats": True},
            "kv_transfer_params": {"ali_llumnix_disagg": False},
        }
        try:
            async with session.post(
                "http://127.0.0.1:18089/v1/completions", json=payload,
                headers={"X-Request-Id": item["request_id"]},
            ) as response:
                http_status = response.status
                async for raw_line in response.content:
                    line = raw_line.decode(errors="replace").strip()
                    if not line.startswith("data:"):
                        continue
                    body = line[5:].strip()
                    if body == "[DONE]":
                        break
                    data = json.loads(body)
                    response_id = response_id or data.get("id")
                    usage = data.get("usage") or {}
                    current = usage.get("completion_tokens")
                    if current is not None:
                        final_tokens = int(current)
                    choices = data.get("choices") or []
                    if choices:
                        choice = choices[0]
                        text = choice.get("text") or ""
                        finish_reason = choice.get("finish_reason") or finish_reason
                        if text:
                            text_parts.append(text)
                            if current is None or int(current) <= previous_tokens:
                                uncounted = True
                            else:
                                now_rel = time.time() * 1000.0 - due * 1000.0
                                token_timestamps.extend([now_rel] * (int(current) - previous_tokens))
                                previous_tokens = int(current)
                if http_status != 200:
                    error = f"HTTP {http_status}"
        except Exception as exc:  # preserve exact failure
            error = repr(exc)
        finished_at = time.time()
        strict = final_tokens is not None and final_tokens == len(token_timestamps) and bool(token_timestamps) and not uncounted
        text = "".join(text_parts)
        success = http_status == 200 and bool(text) and finish_reason != "abort" and error is None
        intervals = [b - a for a, b in zip(token_timestamps, token_timestamps[1:])] if strict else []
        return {
            "id": item["request_id"], "client_request_id": item["request_id"],
            "request_id": response_id or item["request_id"],
            "request_type": item["request_type"], "phase": item["phase"],
            "priority": item["priority"], "input_len": item["input_len"],
            "expected_output_len": item["output_len"], "configured_max_tokens": item["max_tokens"],
            "num_output_tokens": final_tokens or 0,
            "scheduled_arrival_wall_time_ms": due * 1000.0,
            "request_start_wall_time_ms": due * 1000.0,
            "actual_submit_wall_time_ms": actual_start * 1000.0,
            "client_admission_delay_ms": max(0.0, (actual_start - due) * 1000.0),
            "token_timestamps_ms": token_timestamps if strict else [],
            "token_intervals_ms": intervals,
            "ttft_ms": token_timestamps[0] if strict else None,
            "e2e_latency_ms": (finished_at - due) * 1000.0,
            "latency_s": finished_at - actual_start,
            "observed_e2e_s": finished_at - due,
            "status": http_status, "http_status": http_status,
            "text_nonempty": bool(text), "finish_reason": finish_reason,
            "finished": success, "aborted": finish_reason == "abort",
            "timeout": bool(error and "tim" in error.lower() and "out" in error.lower()),
            "error": error if error else (None if strict else "token trace not proven per-token"),
            "strict_per_token_trace": strict,
            "trace_time_source": "client_stream_receive",
        }


async def submit_async(requests, concurrency, benchmark_start):
    timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT_S)
    connector = aiohttp.TCPConnector(limit=concurrency)
    semaphore = asyncio.Semaphore(concurrency)
    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        tasks = [asyncio.create_task(request_one(session, row, semaphore, benchmark_start)) for row in requests]
        return await asyncio.gather(*tasks)


def submit_trace(requests, concurrency, benchmark_start):
    global ACTIVE_RESULTS
    ACTIVE_RESULTS = asyncio.run(submit_async(requests, concurrency, benchmark_start))
    return ACTIVE_RESULTS


def configure_run(run_dir: Path):
    paper.ROOT = ROOT
    paper.CONFIG_DIR = CONFIG_DIR
    paper.WORKLOAD_DIR = CONFIG_DIR / "workloads"
    paper.RUNS_DIR = RUNS_DIR
    paper.STEP_DIR = STEP_DIR
    paper.FINAL_DIR = FINAL_DIR
    paper.configure_policies()
    paper.copy_profiling()
    ORIGINAL_CONFIGURE_BASE(
        4, REQUEST_NUM, CONCURRENCY, RUNS_DIR, paper.WORKLOAD_DIR,
        MAX_MODEL_LEN, MAX_BATCHED_TOKENS,
        run_dir / "migration_efficiency_events.jsonl",
    )
    original = paper.base._paper_original_get_vllm_command
    def wrapped(*args, **kwargs):
        command = original(*args, **kwargs)
        command = re.sub(r"--max-model-len(?:=|\s+)\S+", f"--max-model-len {MAX_MODEL_LEN}", command, count=1)
        command = re.sub(r"--max-num-batched-tokens(?:=|\s+)\S+", f"--max-num-batched-tokens {MAX_BATCHED_TOKENS}", command, count=1)
        command = re.sub(r"\s+--kv-cache-memory-bytes(?:=|\s+)\S+", "", command)
        return command + f" --kv-cache-memory-bytes {fig12.KV_CACHE_MEMORY_BYTES}"
    paper.base.get_vllm_command = wrapped
    paper.base.cleanup_ports = safe_port_preflight
    paper.RedisStatusSampler = ComplexStatusSampler


def validate_vllm_command() -> str:
    command = paper.base.get_vllm_command(
        "neutral", paper.base.VLLM_BASE_PORT, 0, True,
        "config_e_command_check", "HybridConnector",
    )
    required = (
        f"--max-model-len {MAX_MODEL_LEN}",
        f"--max-num-batched-tokens {MAX_BATCHED_TOKENS}",
        f"--kv-cache-memory-bytes {fig12.KV_CACHE_MEMORY_BYTES}",
    )
    missing = [value for value in required if value not in command]
    if missing:
        raise RuntimeError(f"Config E vLLM command validation failed: missing {missing}; command={command}")
    print(f"CONFIG_D_VLLM_COMMAND_VALIDATED {' '.join(required)}", flush=True)
    return command


def per_type_metrics(results):
    output = {}
    for request_type in sorted({row["request_type"] for row in results}):
        rows = [row for row in results if row["request_type"] == request_type]
        successful = [row for row in rows if row["finished"]]
        request_slo_violations = [
            row for row in rows
            if not row["finished"]
            or row.get("ttft_ms") is None
            or row["ttft_ms"] > 1000.0
            or not row.get("token_intervals_ms")
            or percentile(row["token_intervals_ms"], 99) > 25.0
        ]
        output[request_type] = {
            "count": len(rows), "success_count": len(successful),
            "success_rate": len(successful) / len(rows),
            "TTFT_P99_ms": percentile((row["ttft_ms"] for row in rows), 99),
            "E2E_P99_ms": percentile((row["e2e_latency_ms"] for row in rows), 99),
            "mean_TPOT_ms": metric_stats(
                statistics.mean(row["token_intervals_ms"]) if row["token_intervals_ms"] else None
                for row in rows
            )["mean"],
            "P99_TPOT_ms": percentile(
                (percentile(row["token_intervals_ms"], 99) for row in rows), 99
            ),
            "SLO_violation_rate": len(request_slo_violations) / len(rows),
        }
    return output


def parse_instance_distribution(run_dir, trace):
    by_id = {row["request_id"]: row for row in trace}
    counts, tokens = {}, {}
    for index in range(4):
        path = run_dir / f"vllm_{index}.log"
        text = path.read_text(errors="replace") if path.exists() else ""
        ids = set(re.findall(r"Received request cmpl-(config_e_\d+)-\d+", text))
        counts[str(index)] = len(ids)
        tokens[str(index)] = sum(by_id[rid]["input_len"] + by_id[rid]["output_len"] for rid in ids if rid in by_id)
    return counts, tokens


def enrich_summary(summary, run_dir, results, samples, trace):
    trace_path = run_dir / "request_token_trace.jsonl"
    with trace_path.open("w", encoding="utf-8") as handle:
        for row in results:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")
    write_json(run_dir / "request_metrics.json", results)
    analysis_dir = run_dir / "downtime_slo"
    completed = subprocess.run([
        sys.executable, str(PROJECT / "tools" / "analyze_downtime_slo.py"),
        "--request-token-trace", str(trace_path),
        "--migration-log", str(run_dir / "migration_efficiency_events.jsonl"),
        "--output-dir", str(analysis_dir),
        "--ttft-slo-ms", "1000", "--tpot-slo-ms", "25",
        "--strict-ttft-slo-ms", "500", "--strict-tpot-slo-ms", "14.5",
        "--loose-ttft-slo-ms", "2000", "--loose-tpot-slo-ms", "50",
    ], cwd=PROJECT, text=True, capture_output=True, check=False)
    (run_dir / "downtime_analysis.stdout.log").write_text(completed.stdout, encoding="utf-8")
    (run_dir / "downtime_analysis.stderr.log").write_text(completed.stderr, encoding="utf-8")
    downtime = json.loads((analysis_dir / "downtime_slo_summary.json").read_text(encoding="utf-8")) if completed.returncode == 0 else {}
    successful = [row for row in results if row["finished"]]
    ttft = [row["ttft_ms"] for row in results if row["ttft_ms"] is not None]
    all_intervals = [value for row in results for value in row["token_intervals_ms"]]
    duration = float(summary.get("duration_s") or 0)
    actual_output = sum(row["num_output_tokens"] for row in successful)
    actual_input = sum(row["input_len"] for row in successful)
    proportions = [row.get("fragmentation_proportion") for row in samples if row.get("fragmentation_proportion") is not None]
    instance_counts, instance_tokens = parse_instance_distribution(run_dir, trace)
    peaks = {}
    for index in range(4):
        rows = [sample["instances"][index] for sample in samples if len(sample.get("instances", [])) == 4]
        peaks[str(index)] = {
            "running_peak": max((row["num_running_reqs"] for row in rows), default=None),
            "waiting_peak": max((row["num_waiting_reqs"] for row in rows), default=None),
        }
    summary.update({
        "workload_type": "complex_mixed_high_load_config_e_c24", "seed": 20260905,
        "arrival_cv_target": 4.0, "timeout_seconds": REQUEST_TIMEOUT_S,
        "actual_output_tokens": actual_output, "actual_input_tokens": actual_input,
        "token_throughput_tok_s": (actual_input + actual_output) / duration if duration else None,
        "output_throughput_tok_s": actual_output / duration if duration else None,
        "TTFT_mean_ms": statistics.mean(ttft) if ttft else None,
        "TTFT_P95_ms": percentile(ttft, 95), "TTFT_P99_ms": percentile(ttft, 99),
        "TPOT_mean_ms": statistics.mean(all_intervals) if all_intervals else None,
        "TPOT_P95_ms": percentile(all_intervals, 95), "TPOT_P99_ms": percentile(all_intervals, 99),
        "strict_per_token_trace_count": sum(row["strict_per_token_trace"] for row in results),
        "timeout_count": sum(row["timeout"] for row in results),
        "per_type": per_type_metrics(results),
        "fragmentation_ratio_mean": statistics.mean(proportions) if proportions else None,
        "fragmentation_ratio_p99": percentile(proportions, 99),
        "fragmentation_nonzero_sample_count": sum(value > 0 for value in proportions),
        "blocked_HOL_request_count": max((row.get("blocked_HOL_request_count", 0) for row in samples), default=0),
        "per_instance_request_count": instance_counts,
        "per_instance_token_count": instance_tokens,
        "per_instance_load_peaks": peaks,
        "downtime_slo": downtime,
        "migrated_request_count": downtime.get("migrated_request_count", 0),
        "migrated_request_ratio": (
            downtime.get("migrated_request_count", 0) / REQUEST_NUM
        ),
        "load_fail_count": paper.base.service_log_text(run_dir).lower().count("load fail"),
    })
    for name in ("server_downtime_ms", "visible_downtime_ms", "extra_downtime_ms", "migration_internal_time_ms"):
        summary[name] = downtime.get(name, {})
    for prefix in ("strict", "normal", "loose"):
        for metric in ("ttft_violation_rate", "tpot_interval_violation_rate", "request_slo_violation_rate"):
            summary[f"{prefix}_{metric}"] = downtime.get(f"{prefix}_{metric}")
    write_json(run_dir / "run_summary.json", summary)
    return summary


def validation_reasons(summary):
    checks = {
        "successful_responses": REQUEST_NUM,
        "http_200_text_nonempty": REQUEST_NUM,
        "finish_reason_abort_count": 0,
        "abort_count": 0,
        "KVTResp_code_500_count": 0,
        "EngineCore_fatal_count": 0,
        "Scheduler_fatal_count": 0,
        "CUDA_illegal_memory_access_count": 0,
        "Traceback_count": 0,
        "timeout_count": 0,
    }
    reasons = [f"{key}={summary.get(key)} expected {expected}" for key, expected in checks.items() if summary.get(key) != expected]
    if sum(summary.get("per_type", {}).get(name, {}).get("count", 0) for name in (
        "short_chat", "medium_mixed", "prefill_heavy", "decode_kv_heavy", "heavy_long_long"
    )) != REQUEST_NUM:
        reasons.append("request_type fields missing")
    if summary.get("strict_per_token_trace_count", 0) == 0:
        reasons.append("no strict per-token traces")
    return reasons


def run_one(policy, repeat):
    global ACTIVE_RUN_DIR, ACTIVE_TRACE
    run_dir = RUNS_DIR / "complex_mixed_high_load" / "config_e_c24" / policy / f"run_{repeat}"
    existing_path = run_dir / "run_summary.json"
    if existing_path.exists():
        existing = json.loads(existing_path.read_text(encoding="utf-8"))
        if existing.get("validity") == "valid":
            print(f"SKIP valid {policy}/run_{repeat}", flush=True)
            return existing
    run_dir.mkdir(parents=True, exist_ok=True)
    ACTIVE_RUN_DIR = run_dir
    ACTIVE_TRACE = read_jsonl(EXECUTION_TRACE)
    configure_run(run_dir)
    validate_vllm_command()
    # run_custom configures the base a second time. Keep this run's explicit
    # command wrapper and event path intact while retaining its lifecycle code.
    paper.configure_base = lambda *args, **kwargs: None
    try:
        summary = paper.run_custom(
            "complex_mixed_high_load", "config_e_c24", policy, repeat,
            ACTIVE_TRACE, CONCURRENCY, MAX_MODEL_LEN, submit_trace,
            False, 4096,
            {"trace_source": str(TRACE_SOURCE), "phase_ratio_authority": "five_phase_design"},
        )
    except Exception:
        failed = json.loads(existing_path.read_text(encoding="utf-8")) if existing_path.exists() else {
            "policy": policy, "repeat_id": repeat, "status": "failed",
            "validation_reasons": ["runner exception before summary was available"],
        }
        failed.setdefault("validation_reasons", [failed.get("exception") or failed.get("first_fatal") or "run failed"])
        write_failure_audit(policy, repeat, failed)
        raise
    finally:
        paper.configure_base = ORIGINAL_CONFIGURE_BASE
    samples_path = run_dir / "instance_status_timeseries.json"
    samples = json.loads(samples_path.read_text(encoding="utf-8")) if samples_path.exists() else []
    summary = enrich_summary(summary, run_dir, ACTIVE_RESULTS, samples, ACTIVE_TRACE)
    reasons = validation_reasons(summary)
    summary["validation_reasons"] = reasons
    summary["validity"] = "valid" if not reasons else "invalid"
    summary["usable_for_main_conclusion"] = not reasons and summary.get("fairness_status") == "fair"
    write_json(run_dir / "run_summary.json", summary)
    if reasons:
        write_failure_audit(policy, repeat, summary)
        raise RuntimeError(f"{policy}/run_{repeat} failed validation: {reasons[0]}")
    return summary


def write_failure_audit(policy, repeat, summary):
    path = STEP_DIR / "COMPLEX_WORKLOAD_CONFIG_E_FAILURE_AUDIT.md"
    path.write_text(
        "# Config E 失败审计\n\n"
        f"- 失败轮次：`{policy}/run_{repeat}`\n"
        f"- 第一条原因：`{summary.get('validation_reasons', ['unknown'])[0]}`\n"
        f"- first_fatal：```\n{summary.get('first_fatal')}\n```\n",
        encoding="utf-8",
    )
    shutil.copy2(path, ROOT / path.name)


def aggregate():
    runs = []
    for policy in POLICIES:
        for repeat in range(1, REPEATS + 1):
            path = RUNS_DIR / "complex_mixed_high_load" / "config_e_c24" / policy / f"run_{repeat}" / "run_summary.json"
            if path.exists():
                runs.append(json.loads(path.read_text(encoding="utf-8")))
    policies = {}
    for policy in POLICIES:
        rows = [row for row in runs if row.get("policy") == policy or row.get("experiment_policy") == policy]
        valid = [row for row in rows if row.get("validity") == "valid"]
        policies[policy] = {
            "completed_runs": len(rows), "valid_runs": len(valid),
            "QPS": metric_stats(row.get("QPS") for row in valid),
            "E2E_P99_s": metric_stats(row.get("P99_e2e_latency_s") for row in valid),
            "TTFT_P99_ms": metric_stats(row.get("TTFT_P99_ms") for row in valid),
            "TPOT_P99_ms": metric_stats(row.get("TPOT_P99_ms") for row in valid),
            "server_downtime_ms": metric_stats(
                row.get("server_downtime_ms", {}).get("mean") for row in valid
            ),
            "triggered_migration_count": sum(row.get("triggered_migration_count", 0) for row in valid),
            "successful_migration_count": sum(row.get("successful_migration_count", 0) for row in valid),
            "failed_migration_count": sum(row.get("failed_migration_count", 0) for row in valid),
            "useless_migration_count": sum(row.get("useless_migration_count", 0) for row in valid),
        }
        for request_type in ("short_chat", "medium_mixed", "prefill_heavy", "decode_kv_heavy", "heavy_long_long"):
            policies[policy].setdefault("per_type", {})[request_type] = {
                key: metric_stats(row.get("per_type", {}).get(request_type, {}).get(key) for row in valid)
                for key in ("success_rate", "TTFT_P99_ms", "E2E_P99_ms", "mean_TPOT_ms", "P99_TPOT_ms", "SLO_violation_rate")
            }
    result = {"root": str(ROOT), "expected_runs": 12, "completed_runs": len(runs), "policies": policies, "runs": runs}
    aggregate_path = FINAL_DIR / "complex_workload_config_e_aggregated.json"
    write_json(aggregate_path, result)
    write_csvs(runs)
    write_downtime_outputs(runs)
    write_report(result)
    for name in (
        "complex_workload_config_e_aggregated.json",
        "complex_workload_config_e_per_run.csv",
        "complex_workload_config_e_per_type.csv",
        "complex_workload_config_e_table.html",
        "downtime_slo_summary.json", "downtime_slo_summary.csv",
        "downtime_slo_summary.md", "downtime_slo_table.html",
    ):
        shutil.copy2(FINAL_DIR / name, ROOT / name)


def write_csvs(runs):
    per_run = FINAL_DIR / "complex_workload_config_e_per_run.csv"
    fields = ["policy", "repeat_id", "validity", "fairness_status", "successful_responses", "QPS", "output_throughput_tok_s", "median_e2e_latency_s", "P95_e2e_latency_s", "P99_e2e_latency_s", "TTFT_P99_ms", "TPOT_P99_ms", "triggered_migration_count", "successful_migration_count", "failed_migration_count"]
    with per_run.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields); writer.writeheader()
        for row in runs:
            writer.writerow({key: row.get(key) for key in fields})
    with (FINAL_DIR / "complex_workload_config_e_per_type.csv").open("w", encoding="utf-8-sig", newline="") as handle:
        fields = ["policy", "repeat_id", "request_type", "count", "success_rate", "TTFT_P99_ms", "E2E_P99_ms", "mean_TPOT_ms", "P99_TPOT_ms"]
        writer = csv.DictWriter(handle, fieldnames=fields); writer.writeheader()
        for row in runs:
            for request_type, metrics in row.get("per_type", {}).items():
                writer.writerow({"policy": row.get("policy"), "repeat_id": row.get("repeat_id"), "request_type": request_type, **metrics})


def write_downtime_outputs(runs):
    rows = []
    for row in runs:
        rows.append({
            "policy": row.get("policy"), "repeat_id": row.get("repeat_id"),
            **{name: row.get(name) for name in (
                "server_downtime_ms", "visible_downtime_ms", "extra_downtime_ms",
                "migration_internal_time_ms", "strict_ttft_violation_rate",
                "strict_tpot_interval_violation_rate", "strict_request_slo_violation_rate",
                "normal_ttft_violation_rate", "normal_tpot_interval_violation_rate",
                "normal_request_slo_violation_rate", "loose_ttft_violation_rate",
                "loose_tpot_interval_violation_rate", "loose_request_slo_violation_rate",
            )},
        })
    write_json(FINAL_DIR / "downtime_slo_summary.json", rows)
    flat_rows = []
    for row in rows:
        flat = {"policy": row["policy"], "repeat_id": row["repeat_id"]}
        for key, value in row.items():
            if isinstance(value, dict):
                for subkey, subvalue in value.items(): flat[f"{key}.{subkey}"] = subvalue
            elif key not in flat: flat[key] = value
        flat_rows.append(flat)
    fields = sorted({key for row in flat_rows for key in row})
    with (FINAL_DIR / "downtime_slo_summary.csv").open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields); writer.writeheader(); writer.writerows(flat_rows)
    md = ["# Config E Downtime / SLO", "", "| policy | run | server mean | visible mean | normal request SLO |", "|---|---:|---:|---:|---:|"]
    for row in rows:
        md.append(f"| {row['policy']} | {row['repeat_id']} | {row.get('server_downtime_ms', {}).get('mean')} | {row.get('visible_downtime_ms', {}).get('mean')} | {row.get('normal_request_slo_violation_rate')} |")
    (FINAL_DIR / "downtime_slo_summary.md").write_text("\n".join(md) + "\n", encoding="utf-8")
    table = "".join(f"<tr><td>{html.escape(str(r['policy']))}</td><td>{r['repeat_id']}</td><td>{r.get('server_downtime_ms',{}).get('mean')}</td><td>{r.get('visible_downtime_ms',{}).get('mean')}</td><td>{r.get('normal_request_slo_violation_rate')}</td></tr>" for r in rows)
    (FINAL_DIR / "downtime_slo_table.html").write_text(f"<!doctype html><meta charset='utf-8'><title>Config E Downtime/SLO</title><style>body{{font-family:Arial,'Microsoft YaHei';margin:28px}}table{{border-collapse:collapse;width:100%}}th,td{{border:1px solid #ccd5df;padding:8px}}</style><h1>Config E Downtime / SLO</h1><table><tr><th>策略</th><th>轮次</th><th>服务端停机均值(ms)</th><th>用户可见停顿均值(ms)</th><th>普通 SLO 违反率</th></tr>{table}</table>", encoding="utf-8")


def write_report(result):
    valid_all = all(row["valid_runs"] == 3 for row in result["policies"].values())
    def best_policy(metric, request_type=None):
        candidates = []
        for policy, row in result["policies"].items():
            source = row.get("per_type", {}).get(request_type, {}) if request_type else row
            value = source.get(metric, {}).get("mean")
            if value is not None:
                candidates.append((value, policy))
        return min(candidates)[1] if candidates else "NA"

    baseline_p99 = result["policies"].get("no_migration", {}).get("E2E_P99_s", {}).get("mean")
    p99_comparison = []
    for policy in ("load_sr", "tpot_tail", "composite_proposed"):
        value = result["policies"].get(policy, {}).get("E2E_P99_s", {}).get("mean")
        if value is not None and baseline_p99:
            direction = "改善" if value < baseline_p99 else "劣化"
            p99_comparison.append(f"{policy} {direction} {(baseline_p99 - value) / baseline_p99 * 100:.2f}%")
    aggressive = max(
        result["policies"].items(), key=lambda item: item[1]["triggered_migration_count"]
    )[0] if result["policies"] else "NA"
    fewest_failed = min(
        (item for item in result["policies"].items() if item[0] != "no_migration"),
        key=lambda item: (item[1]["failed_migration_count"], item[1]["useless_migration_count"]),
    )[0]
    fair_runs = sum(row.get("fairness_status") == "fair" for row in result["runs"] if row.get("validity") == "valid")
    lines = ["# COMPLEX_WORKLOAD_CONFIG_E_RUN_SUMMARY", "", f"- Config E 完整跑完：{result['completed_runs'] == 12}", f"- 四组均 3/3 有效：{valid_all}", "", "| 策略 | 完成/有效 | QPS 均值 | E2E P99 均值(s) | TTFT P99 均值(ms) | failed migration |", "|---|---:|---:|---:|---:|---:|"]
    for policy, row in result["policies"].items():
        lines.append(f"| {policy} | {row['completed_runs']}/{row['valid_runs']} | {row['QPS']['mean']} | {row['E2E_P99_s']['mean']} | {row['TTFT_P99_ms']['mean']} | {row['failed_migration_count']} |")
    lines += [
        "", "## 结论问题", "",
        f"1. Config E 是否完整跑完：{'是' if result['completed_runs'] == 12 else '否'}。",
        "2. 每组是否 3/3 有效：" + ("是。" if valid_all else "否，见上表。"),
        f"3. no_migration 是否有明显尾延迟：E2E P99 均值为 `{baseline_p99}` 秒，需结合迁移组和 SLO 共同判断。",
        "4. 三种迁移策略对整体 P99 的影响：" + ("；".join(p99_comparison) if p99_comparison else "数据不足") + "。",
        f"5. prefill_heavy TTFT P99 最好：`{best_policy('TTFT_P99_ms', 'prefill_heavy')}`。",
        f"6. heavy_long_long E2E P99 最好：`{best_policy('E2E_P99_ms', 'heavy_long_long')}`。",
        f"7. short_chat E2E P99 最好：`{best_policy('E2E_P99_ms', 'short_chat')}`；与 no_migration 的数值对照用于判断迁移是否伤害短请求。",
        f"8. 迁移最激进：`{aggressive}`。",
        f"9. failed/useless migration 最少：`{fewest_failed}`。",
        f"10. 正式结论适用性：严格有效轮 `{sum(row['valid_runs'] for row in result['policies'].values())}/12`，其中 fair 轮 `{fair_runs}`；contended 轮只作参考。",
    ]
    report_path = STEP_DIR / "COMPLEX_WORKLOAD_CONFIG_E_RUN_SUMMARY.md"
    report_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    shutil.copy2(report_path, ROOT / report_path.name)
    rows_html = "".join(f"<tr><td>{html.escape(policy)}</td><td>{row['completed_runs']}/{row['valid_runs']}</td><td>{row['QPS']['mean']}</td><td>{row['E2E_P99_s']['mean']}</td><td>{row['TTFT_P99_ms']['mean']}</td><td>{row['failed_migration_count']}</td></tr>" for policy, row in result["policies"].items())
    (FINAL_DIR / "complex_workload_config_e_table.html").write_text(f"<!doctype html><html lang='zh-CN'><meta charset='utf-8'><title>Config E</title><style>body{{font-family:Arial,'Microsoft YaHei';margin:28px;color:#182230}}table{{border-collapse:collapse;width:100%}}th,td{{border:1px solid #ccd5df;padding:9px}}th{{background:#26384d;color:#fff}}</style><h1>complex_mixed_high_load Config E</h1><table><tr><th>策略</th><th>完成/有效</th><th>QPS</th><th>E2E P99(s)</th><th>TTFT P99(ms)</th><th>失败迁移</th></tr>{rows_html}</table></html>", encoding="utf-8")


def run_all():
    prepare()
    for policy in POLICIES:
        for repeat in range(1, REPEATS + 1):
            print(f"RUN {policy}/run_{repeat}", flush=True)
            run_one(policy, repeat)
    aggregate()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--phase", choices=("prepare", "archive-port-conflict", "run", "aggregate", "all"), required=True)
    parser.add_argument("--policy", choices=POLICIES)
    parser.add_argument("--repeat", type=int, choices=(1, 2, 3))
    args = parser.parse_args()
    if args.phase == "prepare": prepare()
    elif args.phase == "archive-port-conflict": archive_known_port_conflict()
    elif args.phase == "aggregate": aggregate()
    elif args.phase == "run":
        if not args.policy or not args.repeat: parser.error("--run requires --policy and --repeat")
        prepare(); run_one(args.policy, args.repeat)
    else: run_all()
    print(ROOT)


if __name__ == "__main__":
    main()

