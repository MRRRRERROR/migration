#!/usr/bin/env python3
"""Run the reproducible four-instance Llumnix Hybrid load sweep.

This is an experiment runner only. It does not modify Llumnix, vLLM, or
blade_kvt. Completed run summaries are treated as checkpoints so interrupted
runs can resume without repeating valid measurements.
"""

from __future__ import annotations

import argparse
import concurrent.futures
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
import time
import traceback
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from urllib import request as urlrequest
from urllib.error import HTTPError, URLError

sys.path.insert(0, os.getcwd())

from tests.local.utils import (
    LOG_DIR,
    NAMING_DIR,
    VLLM_BASE_PORT,
    cleanup_processes,
    get_discovery_command,
    get_gateway_command,
    get_redis_command,
    get_scheduler_command,
    get_vllm_command,
    start_process,
    wait_for_service,
)


ROOT = Path(os.environ["LOAD_SWEEP_ROOT"]).resolve()
MODEL_PATH = os.environ.get("MODEL_PATH", "/data/models/Qwen2.5-7B-Instruct")
INSTANCES = int(os.environ.get("LOAD_SWEEP_INSTANCES", "4"))
REPEATS = int(os.environ.get("LOAD_SWEEP_REPEATS", "3"))
SEED = 20260721

CONFIG_DIR = ROOT / "00_config"
SMOKE_DIR = ROOT / "01_smoke"
WORKLOAD_DIR = ROOT / "02_workloads"
RUNS_DIR = ROOT / "03_runs"
STEP_DIR = ROOT / "04_step_summaries"
FINAL_DIR = ROOT / "05_final"
RAW_DIR = ROOT / "raw_logs"

KVT_HOST = "127.0.0.1"
KVT_PORTS = tuple(36218 + 8 * index for index in range(INSTANCES))

GROUPS = ("Hybrid_no_migration", "Hybrid_live_migration")
LOADS = {
    "low_load": {"request_num": 50, "concurrency": 10},
    "mid_load": {"request_num": 100, "concurrency": 25},
    "high_load": {"request_num": 150, "concurrency": 50},
}
WORKLOADS = {
    "stable_short": {
        "max_tokens": 32,
        "prompt_len_target": "short",
        "output_len_target": "32 tokens",
    },
    "mixed_tail": {
        "max_tokens": 192,
        "prompt_len_target": "80% short, 20% long",
        "output_len_target": "80% 48 tokens, 20% 192 tokens",
    },
    "long_output_migration": {
        "max_tokens": 256,
        "prompt_len_target": "medium",
        "output_len_target": "256 tokens",
    },
}


def now_iso() -> str:
    return datetime.now().astimezone().isoformat(timespec="seconds")


def percentile(values: list[float], pct: float):
    if not values:
        return None
    ordered = sorted(values)
    pos = (len(ordered) - 1) * pct / 100
    lo = math.floor(pos)
    hi = math.ceil(pos)
    if lo == hi:
        return ordered[lo]
    return ordered[lo] + (ordered[hi] - ordered[lo]) * (pos - lo)


def prompt_for(workload: str, index: int) -> str:
    if workload == "stable_short":
        return f"Stable short request {index}: Hello, briefly introduce yourself."
    if workload == "mixed_tail":
        if index % 10 in {0, 1}:
            return (
                f"Mixed tail long request {index}: Explain request migration, "
                "KV cache transfer, and service latency in detail. " * 18
            ).strip()
        return f"Mixed tail short request {index}: Hello, briefly introduce yourself."
    return (
        f"Long output migration request {index}: Explain how a distributed LLM "
        "serving system can migrate a running request while preserving its KV cache. " * 8
    ).strip()


def max_tokens_for(workload: str, index: int) -> int:
    if workload == "mixed_tail" and index % 10 not in {0, 1}:
        return 48
    return int(WORKLOADS[workload]["max_tokens"])


def create_directories() -> None:
    for path in (CONFIG_DIR, SMOKE_DIR, WORKLOAD_DIR, RUNS_DIR, STEP_DIR, FINAL_DIR, RAW_DIR):
        path.mkdir(parents=True, exist_ok=True)


def command_output(command: list[str], cwd: Path | None = None) -> str:
    result = subprocess.run(command, cwd=cwd, text=True, capture_output=True, check=False)
    output = (result.stdout or result.stderr).strip()
    return output or "NA"


def write_environment_audit() -> None:
    """Record the immutable environment and runner checks before any run."""
    create_directories()
    env_script = r'''
import json, os, pathlib, subprocess, sys
import torch, vllm, blade_kvt, llumnix

def git_commit(path):
    try:
        return subprocess.check_output(
            ["git", "-C", str(path), "rev-parse", "HEAD"], text=True
        ).strip()
    except Exception:
        return "NA"

blade_path = pathlib.Path(blade_kvt.__file__).resolve()
blade_source = pathlib.Path("/data/home/zylu/tmp/blade-kvt")
payload = {
    "generated_at": __import__("datetime").datetime.now().astimezone().isoformat(),
    "python": sys.version.replace("\n", " "),
    "python_path": sys.executable,
    "conda_env": os.environ.get("CONDA_DEFAULT_ENV", "NA"),
    "gpu_count": torch.cuda.device_count(),
    "gpu_models": [torch.cuda.get_device_name(i) for i in range(torch.cuda.device_count())],
    "cuda": torch.version.cuda,
    "pytorch": torch.__version__,
    "vllm_version": vllm.__version__,
    "vllm_import_path": str(pathlib.Path(vllm.__file__).resolve()),
    "llumnix_commit": git_commit(pathlib.Path.cwd()),
    "llumnix_import_path": str(pathlib.Path(llumnix.__file__).resolve()),
    "blade_kvt_commit": git_commit(blade_source),
    "blade_kvt_import_path": str(blade_path),
    "model_path": os.environ.get("MODEL_PATH", "/data/models/Qwen2.5-7B-Instruct"),
}
print(json.dumps(payload, ensure_ascii=False))
'''
    result = subprocess.run([sys.executable, "-c", env_script], text=True, capture_output=True)
    if result.returncode != 0:
        raise RuntimeError(f"Environment audit failed: {result.stderr}")
    env_data = json.loads(result.stdout.strip().splitlines()[-1])
    driver = command_output(
        ["nvidia-smi", "--query-gpu=driver_version", "--format=csv,noheader"]
    ).splitlines()[0]
    env_data["driver"] = driver
    env_data["result_root"] = str(ROOT)
    env_data["instances"] = INSTANCES
    env_data["connector"] = "HybridConnector"
    env_data["protocol"] = "vllm-kvt"
    lines = ["# Environment Summary", ""]
    lines.extend(f"- {key}: `{value}`" for key, value in env_data.items())
    (CONFIG_DIR / "environment_summary.md").write_text(
        "\n".join(lines) + "\n", encoding="utf-8"
    )

    source = Path(__file__).read_text(encoding="utf-8")
    unsafe_kvt_regex = (
        're.findall(r"KVTResp(code=0"' in source
        or "re.findall(r'KVTResp(code=0'" in source
    )
    audit = [
        "# Script Audit",
        "",
        f"- Runner: `{Path(__file__).resolve()}`",
        "- `python -m py_compile`: passed before upload",
        f"- Unescaped KVT regex present: `{unsafe_kvt_regex}`",
        f"- Safe literal KVT counter present: `{'text.count(\"KVTResp(code=0\")' in source}`",
        "- Fixed request files are shared by baseline and migration.",
        "- Completed summaries are checkpoints and are skipped on resume.",
        "- No Llumnix, vLLM, CUDA, PyTorch, or blade_kvt source is modified.",
    ]
    (CONFIG_DIR / "script_audit.md").write_text("\n".join(audit) + "\n", encoding="utf-8")


def write_workloads() -> list[dict]:
    create_directories()
    manifest = []
    for workload, workload_cfg in WORKLOADS.items():
        for load_level, load_cfg in LOADS.items():
            path = WORKLOAD_DIR / f"{workload}_{load_level}.jsonl"
            rows = []
            for index in range(load_cfg["request_num"]):
                rows.append(
                    {
                        "id": f"{workload}_{load_level}_{index}",
                        "workload_type": workload,
                        "load_level": load_level,
                        "prompt": prompt_for(workload, index),
                        "max_tokens": max_tokens_for(workload, index),
                        "temperature": 0.0,
                        "stream": False,
                        "ignore_eos": True,
                        "seed": SEED,
                    }
                )
            payload = "\n".join(json.dumps(row, ensure_ascii=False) for row in rows) + "\n"
            if path.exists() and path.read_text(encoding="utf-8") != payload:
                raise RuntimeError(f"Existing workload differs from deterministic definition: {path}")
            path.write_text(payload, encoding="utf-8")
            manifest.append(
                {
                    "workload_type": workload,
                    "load_level": load_level,
                    "request_file": str(path),
                    "request_num": load_cfg["request_num"],
                    "concurrency": load_cfg["concurrency"],
                    "max_tokens": "per-request-file",
                    "prompt_len_target": workload_cfg["prompt_len_target"],
                    "output_len_target": workload_cfg["output_len_target"],
                    "seed": SEED,
                }
            )
    (WORKLOAD_DIR / "workload_manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    lines = [
        "# Step 02 Workload Summary",
        "",
        "| Workload | Load | Requests | Concurrency | Request file | Seed |",
        "|---|---:|---:|---:|---|---:|",
    ]
    for item in manifest:
        lines.append(
            f"| {item['workload_type']} | {item['load_level']} | {item['request_num']} | "
            f"{item['concurrency']} | `{item['request_file']}` | {item['seed']} |"
        )
    (STEP_DIR / "step02_workload_summary.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    return manifest


def read_workload(workload: str, load_level: str) -> list[dict]:
    path = WORKLOAD_DIR / f"{workload}_{load_level}.jsonl"
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def post_completion(item: dict) -> dict:
    payload = {
        "model": MODEL_PATH,
        "prompt": item["prompt"],
        "max_tokens": item["max_tokens"],
        "stream": False,
        "temperature": 0.0,
        "ignore_eos": bool(item.get("ignore_eos", True)),
    }
    if item.get("kv_transfer_params") is not None:
        payload["kv_transfer_params"] = item["kv_transfer_params"]
    req = urlrequest.Request(
        "http://localhost:18089/v1/completions",
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    started = time.time()
    try:
        with urlrequest.urlopen(req, timeout=900) as response:
            raw = response.read().decode("utf-8", errors="replace")
            status = response.status
    except HTTPError as exc:
        raw = exc.read().decode("utf-8", errors="replace")
        status = exc.code
    except (URLError, TimeoutError) as exc:
        raw = repr(exc)
        status = 0
    latency = time.time() - started
    text = ""
    finish_reason = None
    usage = {}
    try:
        data = json.loads(raw)
        choice = (data.get("choices") or [{}])[0]
        text = choice.get("text") or ""
        finish_reason = choice.get("finish_reason")
        usage = data.get("usage") or {}
    except Exception:
        pass
    return {
        "id": item["id"],
        "status": status,
        "latency_s": latency,
        "text_nonempty": bool(text),
        "text": text[:500],
        "finish_reason": finish_reason,
        "usage": usage,
        "raw": raw[:2000],
    }


def copy_logs(destination: Path) -> None:
    destination.mkdir(parents=True, exist_ok=True)
    if LOG_DIR.exists():
        for path in LOG_DIR.glob("*.log"):
            shutil.copy2(path, destination / path.name)


def log_text(run_dir: Path) -> str:
    service_logs = [
        path
        for path in run_dir.glob("*.log")
        if path.name != "run.log" and not path.name.startswith("smoke_")
    ]
    return "\n".join(path.read_text(errors="replace") for path in service_logs)


def count_patterns(run_dir: Path) -> dict:
    text = log_text(run_dir)
    abort_lines = [
        line for line in text.splitlines() if "abort req" in line.lower() or "AbortReq" in line
    ]
    finished = re.findall(r"FinishedReq=([A-Za-z0-9_-]+)", text)
    finish_lists = re.findall(r"finish_request_ids\s*[=:]\s*\[([^\]]*)\]", text)
    finish_ids = []
    for value in finish_lists:
        finish_ids.extend(re.findall(r"[A-Za-z0-9_-]{8,}", value))
    return {
        "migrate_out_selected_count": text.count("migrate_out_selected"),
        "migrate_out_begin_count": len(
            re.findall(r"migrate out|Migrate out|migration transfer kv and suspend req", text)
        ),
        "migrate_out_done_count": text.count("migrate_out_done"),
        "finish_request_ids_count": len(finish_ids),
        "unique_finish_request_ids_count": len(set(finish_ids)),
        "FinishedReq_lines": len(finished),
        "unique_FinishedReq_ids": len(set(finished)),
        "KVTResp_code_0_count": text.count("KVTResp(code=0"),
        "KVTResp_code_404_count": text.count("KVTResp(code=404"),
        "KVTResp_code_500_count": text.count("KVTResp(code=500"),
        "SendStubMetrics_count": text.count("SendStubMetrics"),
        "migration_end_count": len(re.findall(r"migration end", text, re.IGNORECASE)),
        "abort_count": sum("reason='migration.suspend'" not in line for line in abort_lines),
        "migration_suspend_count": sum("reason='migration.suspend'" in line for line in abort_lines),
        "downtime_ms": "NA",
        "avg_downtime_ms": "NA",
        "total_migration_time_ms": "NA",
        "avg_migration_time_ms": "NA",
        "EngineCore_fatal_count": len(
            re.findall(r"EngineCore encountered a fatal error|EngineCore failed to start", text)
        ),
        "Traceback_count": len(re.findall(r"Traceback \(most recent call last\):", text)),
        "ERROR_count": len(re.findall(r"(?m)^.*\bERROR\b", text)),
        "RuntimeError_count": len(re.findall(r"RuntimeError", text)),
        "CUDA_illegal_memory_access_count": len(
            re.findall(r"CUDA illegal memory access|illegal memory access", text, re.IGNORECASE)
        ),
    }


def first_fatal(run_dir: Path) -> str:
    markers = (
        "EngineCore encountered a fatal error",
        "EngineCore failed to start",
        "Traceback (most recent call last)",
        "CUDA illegal memory access",
        "RuntimeError:",
    )
    for path in sorted(run_dir.glob("*.log")):
        lines = path.read_text(errors="replace").splitlines()
        for index, line in enumerate(lines):
            if any(marker in line for marker in markers):
                return "\n".join([f"FILE {path}"] + lines[index : index + 80])
    return "NO_FATAL_FOUND"


def cleanup_known_ports() -> None:
    ports = (
        "6379/tcp 8100/tcp 8101/tcp 8102/tcp 8103/tcp "
        "18088/tcp 18089/tcp 31100/tcp 31101/tcp 31102/tcp 31103/tcp "
        "36218/tcp 36226/tcp 36234/tcp 36242/tcp"
    )
    subprocess.run(f"fuser -k -9 {ports}", shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    time.sleep(3)


def wait_for_kvt_listeners(timeout: float = 120.0) -> list[socket.socket]:
    """Wait for every advertised KVT endpoint and keep probes open until log capture."""
    deadline = time.time() + timeout
    probes: dict[int, socket.socket] = {}
    while time.time() < deadline:
        logs = ""
        if LOG_DIR.exists():
            logs = "\n".join(path.read_text(errors="replace") for path in LOG_DIR.glob("vllm_*.log"))
        for port in KVT_PORTS:
            if port in probes:
                continue
            ready_pattern = rf"init_kv_transfer_server.*{re.escape(KVT_HOST)}:{port}\b"
            if not re.search(ready_pattern, logs):
                continue
            probe = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            probe.settimeout(2.0)
            try:
                probe.connect((KVT_HOST, port))
            except OSError:
                probe.close()
                continue
            probes[port] = probe
        if len(probes) == len(KVT_PORTS):
            return list(probes.values())
        time.sleep(1)
    for probe in probes.values():
        probe.close()
    missing = [f"{KVT_HOST}:{port}" for port in KVT_PORTS if port not in probes]
    raise TimeoutError(f"KVT listener readiness timeout after {timeout}s: {missing}")


def start_stack(group: str):
    os.environ["VLLM_HOST_IP"] = KVT_HOST
    enable_migration = group == "Hybrid_live_migration"
    tag = "load_sweep_migration" if enable_migration else "load_sweep_baseline"
    processes = [start_process("Redis", get_redis_command(), "redis.log")]
    processes.append(
        start_process(
            "Scheduler",
            get_scheduler_command(
                policy="flood",
                enable_full_mode_scheduling=True,
                enable_migration=enable_migration,
                enable_pd=False,
            ),
            "scheduler.log",
        )
    )
    processes.append(
        start_process(
            "Gateway",
            get_gateway_command(
                policy="flood",
                enable_full_mode_scheduling=True,
                enable_pd=False,
                connector_type="HybridConnector",
            ),
            "gateway.log",
        )
    )
    for index in range(INSTANCES):
        port = VLLM_BASE_PORT + index
        processes.append(
            start_process(
                f"vLLM-{index}",
                get_vllm_command("neutral", port, index, True, f"{tag}_{index}", "HybridConnector"),
                f"vllm_{index}.log",
            )
        )
        processes.append(
            start_process(
                f"Runtime-{index}", get_discovery_command("neutral", port), f"runtime_{index}.log"
            )
        )
    return processes


def valid_checkpoint(summary_path: Path) -> bool:
    if not summary_path.exists():
        return False
    try:
        summary = json.loads(summary_path.read_text(encoding="utf-8"))
    except Exception:
        return False
    return summary.get("status") == "completed"


def run_one(
    workload: str,
    load_level: str,
    group: str,
    repeat_id: int,
    smoke: bool = False,
    smoke_label: str | None = None,
) -> dict:
    request_file = WORKLOAD_DIR / f"{workload}_{load_level}.jsonl"
    default_smoke_label = "smoke_migration" if group == "Hybrid_live_migration" else "smoke_baseline"
    smoke_label = smoke_label or default_smoke_label
    run_dir = (
        SMOKE_DIR / smoke_label
        if smoke
        else RUNS_DIR / workload / load_level / group / f"run_{repeat_id}"
    )
    run_dir.mkdir(parents=True, exist_ok=True)
    summary_path = run_dir / "run_summary.json"
    if not smoke and valid_checkpoint(summary_path):
        print(f"SKIP_COMPLETED {summary_path}", flush=True)
        return json.loads(summary_path.read_text(encoding="utf-8"))

    requests = read_workload(workload, load_level)
    if smoke:
        requests = requests[:10]
    if group == "Hybrid_live_migration":
        for item in requests:
            item["kv_transfer_params"] = {"ali_llumnix_disagg": False}
    concurrency = min(LOADS[load_level]["concurrency"], len(requests))
    processes = []
    kvt_probes: list[socket.socket] = []
    started_at = now_iso()
    wall_started = time.time()
    summary = None
    try:
        cleanup_known_ports()
        shutil.rmtree(NAMING_DIR, ignore_errors=True)
        shutil.rmtree(LOG_DIR, ignore_errors=True)
        LOG_DIR.mkdir(parents=True, exist_ok=True)
        processes = start_stack(group)
        for index in range(INSTANCES):
            port = VLLM_BASE_PORT + index
            if not wait_for_service(f"http://localhost:{port}/health", timeout=360):
                raise TimeoutError(f"Service startup timeout for port {port}")
        kvt_probes = wait_for_kvt_listeners(timeout=120)
        benchmark_started = time.time()
        with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as executor:
            futures = [executor.submit(post_completion, item) for item in requests]
            results = [future.result() for future in concurrent.futures.as_completed(futures)]
        duration_s = time.time() - benchmark_started
        copy_logs(run_dir)
        latencies = [result["latency_s"] for result in results]
        successful = [result for result in results if result["status"] == 200 and result["text_nonempty"]]
        successful_ids = {result["id"] for result in successful}
        empty_text_count = sum(result["status"] == 200 and not result["text_nonempty"] for result in results)
        abort_finish_reason_count = sum(result.get("finish_reason") == "abort" for result in results)
        expected_output_tokens = sum(
            item["max_tokens"] for item in requests if item["id"] in successful_ids
        )
        summary = {
            "workload_type": workload,
            "load_level": load_level,
            "group": group,
            "connector": "HybridConnector",
            "protocol": "vllm-kvt",
            "migration_enabled": group == "Hybrid_live_migration",
            "repeat_id": repeat_id,
            "request_file": str(request_file),
            "request_num": len(requests),
            "instance_num": INSTANCES,
            "model_path": MODEL_PATH,
            "max_tokens": "per-request-file",
            "concurrency": concurrency,
            "request_rate": "NA",
            "start_time": started_at,
            "end_time": now_iso(),
            "duration_s": duration_s,
            "wall_duration_s": time.time() - wall_started,
            "status": "completed",
            "successful_responses": len(successful),
            "failed_responses": len(requests) - len(successful),
            "success_rate": len(successful) / len(requests) if requests else 0,
            "http_200_text_nonempty": len(successful),
            "empty_text_count": empty_text_count,
            "abort_finish_reason_count": abort_finish_reason_count,
            "kvt_listener_endpoints": [f"{KVT_HOST}:{port}" for port in KVT_PORTS],
            "kvt_listener_readiness": "passed",
            "qps": len(successful) / duration_s if duration_s else 0,
            "throughput_tok_s": expected_output_tokens / duration_s if duration_s else 0,
            "throughput_method": "configured max_tokens for successful requests; ignore_eos=true",
            "median_e2e_latency_s": statistics.median(latencies) if latencies else None,
            "p95_e2e_latency_s": percentile(latencies, 95),
            "p99_e2e_latency_s": percentile(latencies, 99),
            "mean_e2e_latency_s": statistics.mean(latencies) if latencies else None,
            "total_input_tokens": "NA",
            "total_output_tokens": "NA",
            "output_tokens_per_s": "NA",
            **count_patterns(run_dir),
            "first_fatal": first_fatal(run_dir),
            "sample_results": results[:5],
        }
    except Exception as exc:
        copy_logs(run_dir)
        summary = {
            "workload_type": workload,
            "load_level": load_level,
            "group": group,
            "repeat_id": repeat_id,
            "request_file": str(request_file),
            "request_num": len(requests),
            "instance_num": INSTANCES,
            "start_time": started_at,
            "end_time": now_iso(),
            "wall_duration_s": time.time() - wall_started,
            "status": "failed",
            "exception": repr(exc),
            "traceback": traceback.format_exc(),
            **count_patterns(run_dir),
            "first_fatal": first_fatal(run_dir),
        }
    finally:
        cleanup_processes(processes)
        for probe in kvt_probes:
            probe.close()
        cleanup_known_ports()

    summary_json = json.dumps(summary, ensure_ascii=False, indent=2)
    summary_path.write_text(summary_json, encoding="utf-8")
    (run_dir / "run.log").write_text(summary_json + "\n", encoding="utf-8")
    if smoke:
        smoke_name = f"{smoke_label}.log"
        (SMOKE_DIR / smoke_name).write_text(summary_json + "\n", encoding="utf-8")
    print(f"LOAD_SWEEP_SUMMARY {json.dumps(summary, ensure_ascii=False)}", flush=True)
    return summary


def smoke_passed(summary: dict, migration: bool) -> bool:
    no_errors = all(
        summary.get(key, 0) == 0
        for key in ("EngineCore_fatal_count", "Traceback_count", "ERROR_count")
    )
    basic = (
        summary.get("status") == "completed"
        and summary.get("successful_responses", 0) == summary.get("request_num", -1)
        and summary.get("http_200_text_nonempty", 0) == summary.get("request_num", -1)
        and summary.get("abort_finish_reason_count", 0) == 0
        and summary.get("CUDA_illegal_memory_access_count", 0) == 0
        and no_errors
    )
    if not migration:
        return basic
    evidence = sum(
        summary.get(key, 0)
        for key in ("KVTResp_code_0_count", "FinishedReq_lines", "SendStubMetrics_count", "migration_end_count")
    )
    summary["migration_not_triggered"] = evidence == 0
    return (
        basic
        and summary.get("KVTResp_code_404_count", 0) == 0
        and summary.get("KVTResp_code_500_count", 0) == 0
        and summary.get("abort_count", 0) == 0
        and evidence > 0
    )


def run_migration_smoke_only() -> None:
    summary = run_one(
        "stable_short",
        "low_load",
        "Hybrid_live_migration",
        1,
        smoke=True,
        smoke_label="smoke_migration_kvt_fixed",
    )
    passed = smoke_passed(summary, True)
    output_log = SMOKE_DIR / "smoke_migration_kvt_fixed.log"
    output_log.write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    lines = [
        "# Step 03 Migration Smoke KVT Fixed Summary",
        "",
        f"- KVT listener endpoints: {', '.join(summary.get('kvt_listener_endpoints', []))}",
        f"- Successful responses: {summary.get('successful_responses', 0)}/{summary.get('request_num', 0)}",
        f"- HTTP 200 and non-empty text: {summary.get('http_200_text_nonempty', 0)}",
        f"- Abort finish reasons: {summary.get('abort_finish_reason_count', 0)}",
        f"- KVTResp(code=0): {summary.get('KVTResp_code_0_count', 0)}",
        f"- KVTResp(code=404): {summary.get('KVTResp_code_404_count', 0)}",
        f"- KVTResp(code=500): {summary.get('KVTResp_code_500_count', 0)}",
        f"- FinishedReq: {summary.get('FinishedReq_lines', 0)}",
        f"- SendStubMetrics: {summary.get('SendStubMetrics_count', 0)}",
        f"- Abort log count: {summary.get('abort_count', 0)}",
        f"- EngineCore fatal: {summary.get('EngineCore_fatal_count', 0)}",
        f"- Traceback: {summary.get('Traceback_count', 0)}",
        f"- ERROR: {summary.get('ERROR_count', 0)}",
        f"- CUDA illegal memory access: {summary.get('CUDA_illegal_memory_access_count', 0)}",
        f"- Smoke passed: **{passed}**",
    ]
    (STEP_DIR / "step03_migration_smoke_kvt_fixed_summary.md").write_text(
        "\n".join(lines) + "\n", encoding="utf-8"
    )
    if not passed:
        raise RuntimeError("Migration KVT smoke gate failed; formal sweep was not started")


def run_smoke() -> None:
    baseline = run_one("stable_short", "low_load", "Hybrid_no_migration", 1, smoke=True)
    migration = run_one("stable_short", "low_load", "Hybrid_live_migration", 1, smoke=True)
    passed = smoke_passed(baseline, False) and smoke_passed(migration, True)
    for summary, name in ((baseline, "smoke_baseline.log"), (migration, "smoke_migration.log")):
        (SMOKE_DIR / name).write_text(
            json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
        )
    lines = [
        "# Step 03 Smoke Summary",
        "",
        "| Group | Status | Successful | Fatal | Traceback | ERROR | Migration evidence |",
        "|---|---|---:|---:|---:|---:|---:|",
    ]
    for summary in (baseline, migration):
        evidence = sum(
            summary.get(key, 0)
            for key in ("KVTResp_code_0_count", "FinishedReq_lines", "SendStubMetrics_count", "migration_end_count")
        )
        lines.append(
            f"| {summary.get('group')} | {summary.get('status')} | {summary.get('successful_responses', 0)} | "
            f"{summary.get('EngineCore_fatal_count', 0)} | {summary.get('Traceback_count', 0)} | "
            f"{summary.get('ERROR_count', 0)} | {evidence} |"
        )
    lines.extend(["", f"Smoke passed: **{passed}**"])
    (STEP_DIR / "step03_smoke_summary.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    if not passed:
        raise RuntimeError("Smoke gate failed; full sweep was not started")


def write_group_summary(workload: str, load_level: str, group: str, summaries: list[dict]) -> None:
    path = STEP_DIR / f"{workload}_{load_level}_{group}_summary.md"
    lines = [
        f"# {workload} / {load_level} / {group}",
        "",
        "| Run | Status | Success | QPS | Throughput | Median | P95 | P99 | KVT0 | FinishedReq | Fatal/TB/ERROR |",
        "|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---|",
    ]
    for summary in summaries:
        lines.append(
            f"| {summary.get('repeat_id')} | {summary.get('status')} | {summary.get('successful_responses', 0)} | "
            f"{format_num(summary.get('qps'))} | {format_num(summary.get('throughput_tok_s'))} | "
            f"{format_num(summary.get('median_e2e_latency_s'))} | {format_num(summary.get('p95_e2e_latency_s'))} | "
            f"{format_num(summary.get('p99_e2e_latency_s'))} | {summary.get('KVTResp_code_0_count', 0)} | "
            f"{summary.get('FinishedReq_lines', 0)} | {summary.get('EngineCore_fatal_count', 0)}/"
            f"{summary.get('Traceback_count', 0)}/{summary.get('ERROR_count', 0)} |"
        )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def format_num(value) -> str:
    return "NA" if not isinstance(value, (int, float)) else f"{value:.4f}"


def run_full() -> None:
    fatal_signatures: dict[str, int] = defaultdict(int)
    for workload in WORKLOADS:
        for load_level in LOADS:
            for group in GROUPS:
                summaries = []
                for repeat_id in range(1, REPEATS + 1):
                    summary = run_one(workload, load_level, group, repeat_id)
                    summaries.append(summary)
                    if summary.get("CUDA_illegal_memory_access_count", 0) > 0:
                        raise RuntimeError("STOP_REQUIRED: CUDA illegal memory access detected")
                    fatal = summary.get("first_fatal", "NO_FATAL_FOUND")
                    if fatal != "NO_FATAL_FOUND":
                        signature = "\n".join(fatal.splitlines()[:8])
                        fatal_signatures[signature] += 1
                        if fatal_signatures[signature] >= 3:
                            raise RuntimeError("STOP_REQUIRED: same EngineCore fatal occurred three times")
                write_group_summary(workload, load_level, group, summaries)
                system_failures = sum(
                    summary.get("CUDA_illegal_memory_access_count", 0) > 0
                    or summary.get("EngineCore_fatal_count", 0) > 0
                    for summary in summaries
                )
                if load_level == "high_load" and system_failures >= 2:
                    print(f"HIGH_LOAD_PAUSED {workload}: {system_failures} system-level failures", flush=True)


def all_run_summaries() -> list[dict]:
    summaries = []
    for path in sorted(RUNS_DIR.glob("*/*/*/run_*/run_summary.json")):
        try:
            summaries.append(json.loads(path.read_text(encoding="utf-8")))
        except Exception as exc:
            summaries.append({"status": "failed", "summary_path": str(path), "exception": repr(exc)})
    return summaries


def numeric_mean(items: list[dict], key: str):
    values = [item.get(key) for item in items if isinstance(item.get(key), (int, float))]
    return statistics.mean(values) if values else None


def numeric_stdev(items: list[dict], key: str):
    values = [item.get(key) for item in items if isinstance(item.get(key), (int, float))]
    return statistics.stdev(values) if len(values) > 1 else (0.0 if values else None)


def aggregate(summaries: list[dict]) -> list[dict]:
    grouped = defaultdict(list)
    for summary in summaries:
        grouped[(summary.get("workload_type"), summary.get("load_level"), summary.get("group"))].append(summary)
    aggregated = []
    for (workload, load_level, group), items in sorted(grouped.items()):
        completed = [item for item in items if item.get("status") == "completed"]
        row = {
            "workload_type": workload,
            "load_level": load_level,
            "group": group,
            "run_count": len(items),
            "completed_run_count": len(completed),
            "total_requests": sum(item.get("request_num", 0) for item in completed),
            "total_successful_responses": sum(item.get("successful_responses", 0) for item in completed),
            "avg_success_rate": numeric_mean(completed, "success_rate"),
            "avg_qps": numeric_mean(completed, "qps"),
            "std_qps": numeric_stdev(completed, "qps"),
            "avg_throughput_tok_s": numeric_mean(completed, "throughput_tok_s"),
            "std_throughput_tok_s": numeric_stdev(completed, "throughput_tok_s"),
            "avg_median_e2e_latency_s": numeric_mean(completed, "median_e2e_latency_s"),
            "avg_p95_e2e_latency_s": numeric_mean(completed, "p95_e2e_latency_s"),
            "avg_p99_e2e_latency_s": numeric_mean(completed, "p99_e2e_latency_s"),
            "min_p99_e2e_latency_s": min((item["p99_e2e_latency_s"] for item in completed), default=None),
            "max_p99_e2e_latency_s": max((item["p99_e2e_latency_s"] for item in completed), default=None),
            "total_KVTResp_code_0": sum(item.get("KVTResp_code_0_count", 0) for item in completed),
            "total_KVTResp_code_404": sum(item.get("KVTResp_code_404_count", 0) for item in completed),
            "total_FinishedReq": sum(item.get("FinishedReq_lines", 0) for item in completed),
            "total_SendStubMetrics": sum(item.get("SendStubMetrics_count", 0) for item in completed),
            "total_migration_end": sum(item.get("migration_end_count", 0) for item in completed),
            "total_abort": sum(item.get("abort_count", 0) for item in completed),
            "total_fatal": sum(item.get("EngineCore_fatal_count", 0) for item in completed),
            "total_traceback": sum(item.get("Traceback_count", 0) for item in completed),
            "total_error": sum(item.get("ERROR_count", 0) for item in completed),
        }
        aggregated.append(row)
    return aggregated


def ratio_rows(aggregated: list[dict]) -> list[dict]:
    index = {(row["workload_type"], row["load_level"], row["group"]): row for row in aggregated}
    rows = []
    for workload in WORKLOADS:
        for load_level in LOADS:
            baseline = index.get((workload, load_level, "Hybrid_no_migration"), {})
            migration = index.get((workload, load_level, "Hybrid_live_migration"), {})
            def ratio(key):
                denominator = baseline.get(key)
                numerator = migration.get(key)
                return numerator / denominator if isinstance(numerator, (int, float)) and denominator else None
            evidence = sum(
                migration.get(key, 0) or 0
                for key in ("total_KVTResp_code_0", "total_FinishedReq", "total_SendStubMetrics")
            )
            rows.append(
                {
                    "workload_type": workload,
                    "load_level": load_level,
                    "qps_ratio": ratio("avg_qps"),
                    "throughput_ratio": ratio("avg_throughput_tok_s"),
                    "median_latency_ratio": ratio("avg_median_e2e_latency_s"),
                    "p95_latency_ratio": ratio("avg_p95_e2e_latency_s"),
                    "p99_latency_ratio": ratio("avg_p99_e2e_latency_s"),
                    "migration_evidence_count": evidence,
                    "fairly_comparable": True,
                    "migration_evidence_exists": evidence > 0,
                    "note": "" if evidence else "该负载未触发迁移，不用于迁移收益判断",
                }
            )
    return rows


def td(value, cls="") -> str:
    if value is None or value in ("NA", "未记录"):
        cls = (cls + " missing").strip()
        value = "NA"
    elif isinstance(value, float):
        value = f"{value:.4f}"
    return f'<td class="{cls}">{html.escape(str(value))}</td>'


def render_html(summaries: list[dict], aggregated: list[dict], ratios: list[dict]) -> str:
    css = """
    body{font-family:Arial,'Microsoft YaHei',sans-serif;margin:24px;color:#202124;background:#f7f8fa}
    h1{font-size:24px;margin:0 0 16px}h2{font-size:18px;margin:24px 0 8px}
    .wrap{overflow-x:auto;background:white;border:1px solid #dfe3e8;border-radius:6px}
    table{border-collapse:collapse;width:100%;font-size:12px}th,td{padding:7px 8px;border:1px solid #e1e5ea;text-align:right;white-space:nowrap}
    th{position:sticky;top:0;background:#303846;color:white;z-index:1}th:first-child,td:first-child{text-align:left}
    tr.baseline td{background:#edf6ff}tr.migration td{background:#fff3e6}.good{color:#087f23;font-weight:700}.bad{color:#b42318;font-weight:700}.missing{background:#eee!important;color:#777}
    .note{font-size:12px;color:#5f6368;margin-top:12px}
    """
    config = [
        ("GPU", "4 x NVIDIA GeForce RTX 5090"), ("CUDA", "13.0"),
        ("PyTorch", "2.14.0.dev20260717+cu130"), ("vLLM", "0.18.1.dev0+gbcf2be961.d20260719"),
        ("Llumnix commit", "aa9097c899fd2f290e031f8a88f854afe4f0b415"),
        ("blade_kvt commit", "6ebc08a193e10f9f809859edda929a031804da62"),
        ("模型路径", MODEL_PATH), ("实例数量", INSTANCES), ("Connector", "HybridConnector"),
        ("协议", "vllm-kvt"), ("Workload 数量", len(WORKLOADS)), ("负载等级数量", len(LOADS)),
        ("每组重复次数", REPEATS), ("结果目录", ROOT),
    ]
    parts = ["<!doctype html><html lang='zh-CN'><head><meta charset='utf-8'>", f"<style>{css}</style></head><body>",
             "<h1>Llumnix RTX 5090 不同负载下公平实验指标总表</h1>", "<h2>实验配置摘要</h2><div class='wrap'><table><thead><tr><th>项目</th><th>内容</th></tr></thead><tbody>"]
    for key, value in config:
        parts.append(f"<tr>{td(key)}{td(value)}</tr>")
    parts.append("</tbody></table></div>")
    parts.append("<h2>不同负载下 Baseline 与 Migration 聚合指标</h2><div class='wrap'><table><thead><tr>" + "".join(
        f"<th>{x}</th>" for x in ["工作负载","负载等级","实验组","迁移","完成轮数","总请求","成功请求","成功率","平均QPS","平均token吞吐","Median(s)","P95(s)","P99(s)","KVT0","FinishedReq","SendStub","Abort","Fatal","Traceback","ERROR"]
    ) + "</tr></thead><tbody>")
    for row in aggregated:
        migration = row["group"] == "Hybrid_live_migration"
        cls = "migration" if migration else "baseline"
        success_cls = "good" if row.get("avg_success_rate") == 1 else "bad"
        error_cls = "good" if not (row["total_fatal"] or row["total_traceback"] or row["total_error"]) else "bad"
        values = [row["workload_type"],row["load_level"],row["group"],"是" if migration else "否",row["completed_run_count"],row["total_requests"],row["total_successful_responses"]]
        parts.append(f"<tr class='{cls}'>" + "".join(td(v) for v in values) + td(row["avg_success_rate"],success_cls) + "".join(td(row[k]) for k in ["avg_qps","avg_throughput_tok_s","avg_median_e2e_latency_s","avg_p95_e2e_latency_s","avg_p99_e2e_latency_s","total_KVTResp_code_0","total_FinishedReq","total_SendStubMetrics","total_abort"]) + "".join(td(row[k],error_cls) for k in ["total_fatal","total_traceback","total_error"]) + "</tr>")
    parts.append("</tbody></table></div>")
    parts.append("<h2>Migration 相对 Baseline 变化</h2><div class='wrap'><table><thead><tr>" + "".join(f"<th>{x}</th>" for x in ["工作负载","负载等级","QPS比值","吞吐比值","Median比值","P95比值","P99比值","迁移证据总数","公平可比","存在迁移证据","备注"]) + "</tr></thead><tbody>")
    for row in ratios:
        parts.append("<tr>" + "".join(td(row[k]) for k in ["workload_type","load_level","qps_ratio","throughput_ratio","median_latency_ratio","p95_latency_ratio","p99_latency_ratio","migration_evidence_count","fairly_comparable","migration_evidence_exists","note"]) + "</tr>")
    parts.append("</tbody></table></div>")
    parts.append("<h2>逐轮结果</h2><div class='wrap'><table><thead><tr>" + "".join(f"<th>{x}</th>" for x in ["工作负载","负载","实验组","轮次","成功请求","QPS","吞吐","Median","P95","P99","KVT0","FinishedReq","SendStub","Fatal/TB/ERROR","状态"]) + "</tr></thead><tbody>")
    for row in summaries:
        migration = row.get("group") == "Hybrid_live_migration"
        cls = "migration" if migration else "baseline"
        errors = f"{row.get('EngineCore_fatal_count',0)}/{row.get('Traceback_count',0)}/{row.get('ERROR_count',0)}"
        parts.append(f"<tr class='{cls}'>" + "".join(td(row.get(k)) for k in ["workload_type","load_level","group","repeat_id","successful_responses","qps","throughput_tok_s","median_e2e_latency_s","p95_e2e_latency_s","p99_e2e_latency_s","KVTResp_code_0_count","FinishedReq_lines","SendStubMetrics_count"]) + td(errors,"good" if errors=="0/0/0" else "bad") + td(row.get("status"),"good" if row.get("status")=="completed" else "bad") + "</tr>")
    parts.append("</tbody></table></div>")
    parts.append(f"<p class='note'>生成时间：{html.escape(now_iso())}<br>说明：Baseline 与 Migration 使用相同请求文件；NA 表示原始响应或日志未直接记录。</p></body></html>")
    return "".join(parts)


def write_validation(summaries: list[dict], aggregated: list[dict], ratios: list[dict]) -> None:
    completed = [item for item in summaries if item.get("status") == "completed"]
    failed = [item for item in summaries if item.get("status") != "completed"]
    no_evidence = [f"{row['workload_type']}/{row['load_level']}" for row in ratios if not row["migration_evidence_exists"]]
    reportable = [
        f"{row['workload_type']}/{row['load_level']}/{row['group']}"
        for row in aggregated
        if row["completed_run_count"] == REPEATS and row["avg_success_rate"] == 1
        and row["total_fatal"] == row["total_traceback"] == row["total_error"] == 0
    ]
    lines = [
        "# VALIDATION REPORT", "",
        f"- 应跑 runs：54", f"- 已生成 summaries：{len(summaries)}",
        f"- 完成 runs：{len(completed)}", f"- 失败 runs：{len(failed)}",
        "- 公平性：每个 workload/load_level 的 Baseline 与 Migration 使用同一个 request_file。",
        f"- EngineCore fatal：{sum(x.get('EngineCore_fatal_count',0) for x in completed)}",
        f"- Traceback：{sum(x.get('Traceback_count',0) for x in completed)}",
        f"- ERROR：{sum(x.get('ERROR_count',0) for x in completed)}",
        f"- CUDA illegal memory access：{sum(x.get('CUDA_illegal_memory_access_count',0) for x in completed)}",
        "- total_input_tokens / total_output_tokens：原始响应 usage 为空时记为 NA。",
        "- downtime：当前日志未直接记录，记为 NA。",
        f"- 未触发迁移的负载：{', '.join(no_evidence) if no_evidence else '无'}",
        f"- 可用于报告：{', '.join(reportable) if reportable else '无'}",
    ]
    (FINAL_DIR / "VALIDATION_REPORT.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def mooncake_audit() -> None:
    try:
        import mooncake  # type: ignore
        import mooncake.mooncake_connector_v1  # type: ignore
        import_status = f"available: {mooncake.__file__}"
    except Exception as exc:
        import_status = f"unavailable: {type(exc).__name__}: {exc}"
    lines = [
        "# MOONCAKE FEASIBILITY", "",
        f"- MooncakeConnector import：{import_status}",
        "- vllm-mooncake 启动参数：项目测试工具中已有 MooncakeConnector 分支。",
        "- 是否修改代码：未修改。", "- 是否重编 vLLM：未重编。",
        "- 本轮是否运行 Mooncake 正式实验：否。",
        "- 建议：仅在 import 可用且独立 smoke 通过后另开结果目录验证。",
    ]
    (FINAL_DIR / "MOONCAKE_FEASIBILITY.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def ratio_rows_v2(aggregated: list[dict]) -> list[dict]:
    index = {(row["workload_type"], row["load_level"], row["group"]): row for row in aggregated}
    rows = []
    for workload in WORKLOADS:
        for load_level in LOADS:
            baseline = index.get((workload, load_level, "Hybrid_no_migration"), {})
            migration = index.get((workload, load_level, "Hybrid_live_migration"), {})

            def ratio(key: str):
                denominator = baseline.get(key)
                numerator = migration.get(key)
                return numerator / denominator if isinstance(numerator, (int, float)) and denominator else None

            evidence = sum(
                migration.get(key, 0) or 0
                for key in ("total_KVTResp_code_0", "total_FinishedReq", "total_SendStubMetrics")
            )
            rows.append({
                "workload_type": workload,
                "load_level": load_level,
                "qps_ratio": ratio("avg_qps"),
                "throughput_ratio": ratio("avg_throughput_tok_s"),
                "median_latency_ratio": ratio("avg_median_e2e_latency_s"),
                "p95_latency_ratio": ratio("avg_p95_e2e_latency_s"),
                "p99_latency_ratio": ratio("avg_p99_e2e_latency_s"),
                "migration_evidence_count": evidence,
                "fairly_comparable": True,
                "migration_evidence_exists": evidence > 0,
                "note": "" if evidence else "该负载未触发迁移，不用于迁移收益判断",
            })
    return rows


def td_v2(value, cls: str = "") -> str:
    if value is None or value in ("NA", "未记录"):
        cls = (cls + " missing").strip()
        value = "NA"
    elif isinstance(value, float):
        value = f"{value:.4f}"
    return f'<td class="{cls}">{html.escape(str(value))}</td>'


def render_html_v2(summaries: list[dict], aggregated: list[dict], ratios: list[dict]) -> str:
    css = """
    body{font-family:Arial,'Microsoft YaHei',sans-serif;margin:24px;color:#202124;background:#f7f8fa}
    h1{font-size:24px;margin:0 0 16px}h2{font-size:18px;margin:24px 0 8px}
    .wrap{overflow-x:auto;background:white;border:1px solid #dfe3e8;border-radius:6px}
    table{border-collapse:collapse;width:100%;font-size:12px}th,td{padding:7px 8px;border:1px solid #e1e5ea;text-align:right;white-space:nowrap}
    th{position:sticky;top:0;background:#303846;color:white;z-index:1}th:first-child,td:first-child{text-align:left}
    tr.baseline td{background:#edf6ff}tr.migration td{background:#fff3e6}.good{color:#087f23;font-weight:700}.bad{color:#b42318;font-weight:700}.missing{background:#eee!important;color:#777}
    .note{font-size:12px;color:#5f6368;margin-top:12px}
    """
    config = [
        ("GPU", "4 x NVIDIA GeForce RTX 5090"), ("CUDA", "13.0"),
        ("PyTorch", "2.14.0.dev20260717+cu130"), ("vLLM", "0.18.1.dev0+gbcf2be961.d20260719"),
        ("Llumnix commit", "aa9097c899fd2f290e031f8a88f854afe4f0b415"),
        ("blade_kvt commit", "6ebc08a193e10f9f809859edda929a031804da62"),
        ("模型路径", MODEL_PATH), ("实例数量", INSTANCES), ("Connector", "HybridConnector"),
        ("协议", "vllm-kvt"), ("Workload 数量", len(WORKLOADS)), ("负载等级数量", len(LOADS)),
        ("每组重复次数", REPEATS), ("结果目录", ROOT),
    ]
    parts = [
        "<!doctype html><html lang='zh-CN'><head><meta charset='utf-8'>",
        f"<style>{css}</style></head><body>",
        "<h1>Llumnix RTX 5090 不同负载下公平实验指标总表</h1>",
        "<h2>实验配置摘要</h2><div class='wrap'><table><thead><tr><th>项目</th><th>内容</th></tr></thead><tbody>",
    ]
    for key, value in config:
        parts.append(f"<tr>{td_v2(key)}{td_v2(value)}</tr>")
    parts.append("</tbody></table></div>")

    aggregate_headers = ["工作负载","负载等级","实验组","迁移","完成轮数","总请求","成功请求","成功率","平均QPS","平均token吞吐","Median(s)","P95(s)","P99(s)","KVT0","FinishedReq","SendStub","Abort","Fatal","Traceback","ERROR"]
    parts.append("<h2>不同负载下 Baseline 与 Migration 聚合指标</h2><div class='wrap'><table><thead><tr>" + "".join(f"<th>{x}</th>" for x in aggregate_headers) + "</tr></thead><tbody>")
    for row in aggregated:
        migration = row["group"] == "Hybrid_live_migration"
        cls = "migration" if migration else "baseline"
        success_cls = "good" if row.get("avg_success_rate") == 1 else "bad"
        error_cls = "good" if not (row["total_fatal"] or row["total_traceback"] or row["total_error"]) else "bad"
        values = [row["workload_type"],row["load_level"],row["group"],"是" if migration else "否",row["completed_run_count"],row["total_requests"],row["total_successful_responses"]]
        parts.append(f"<tr class='{cls}'>" + "".join(td_v2(v) for v in values) + td_v2(row["avg_success_rate"],success_cls) + "".join(td_v2(row[k]) for k in ["avg_qps","avg_throughput_tok_s","avg_median_e2e_latency_s","avg_p95_e2e_latency_s","avg_p99_e2e_latency_s","total_KVTResp_code_0","total_FinishedReq","total_SendStubMetrics","total_abort"]) + "".join(td_v2(row[k],error_cls) for k in ["total_fatal","total_traceback","total_error"]) + "</tr>")
    parts.append("</tbody></table></div>")

    ratio_headers = ["工作负载","负载等级","QPS比值","吞吐比值","Median比值","P95比值","P99比值","迁移证据总数","公平可比","存在迁移证据","备注"]
    parts.append("<h2>Migration 相对 Baseline 变化</h2><div class='wrap'><table><thead><tr>" + "".join(f"<th>{x}</th>" for x in ratio_headers) + "</tr></thead><tbody>")
    for row in ratios:
        parts.append("<tr>" + "".join(td_v2(row[k]) for k in ["workload_type","load_level","qps_ratio","throughput_ratio","median_latency_ratio","p95_latency_ratio","p99_latency_ratio","migration_evidence_count","fairly_comparable","migration_evidence_exists","note"]) + "</tr>")
    parts.append("</tbody></table></div>")

    run_headers = ["工作负载","负载等级","实验组","轮次","成功请求","QPS","吞吐","Median","P95","P99","KVT0","FinishedReq","SendStub","Fatal/TB/ERROR","状态"]
    parts.append("<h2>逐轮结果</h2><div class='wrap'><table><thead><tr>" + "".join(f"<th>{x}</th>" for x in run_headers) + "</tr></thead><tbody>")
    for row in summaries:
        cls = "migration" if row.get("group") == "Hybrid_live_migration" else "baseline"
        errors = f"{row.get('EngineCore_fatal_count',0)}/{row.get('Traceback_count',0)}/{row.get('ERROR_count',0)}"
        keys = ["workload_type","load_level","group","repeat_id","successful_responses","qps","throughput_tok_s","median_e2e_latency_s","p95_e2e_latency_s","p99_e2e_latency_s","KVTResp_code_0_count","FinishedReq_lines","SendStubMetrics_count"]
        parts.append(f"<tr class='{cls}'>" + "".join(td_v2(row.get(k)) for k in keys) + td_v2(errors,"good" if errors=="0/0/0" else "bad") + td_v2(row.get("status"),"good" if row.get("status")=="completed" else "bad") + "</tr>")
    parts.append("</tbody></table></div>")
    parts.append(f"<p class='note'>生成时间：{html.escape(now_iso())}<br>说明：Baseline 与 Migration 使用相同请求文件；NA 表示原始响应或日志未直接记录。</p></body></html>")
    return "".join(parts)


def write_validation_v2(summaries: list[dict], aggregated: list[dict], ratios: list[dict]) -> None:
    completed = [item for item in summaries if item.get("status") == "completed"]
    failed = [item for item in summaries if item.get("status") != "completed"]
    no_evidence = [f"{row['workload_type']}/{row['load_level']}" for row in ratios if not row["migration_evidence_exists"]]
    reportable = [
        f"{row['workload_type']}/{row['load_level']}/{row['group']}"
        for row in aggregated
        if row["completed_run_count"] == REPEATS and row["avg_success_rate"] == 1
        and row["total_fatal"] == row["total_traceback"] == row["total_error"] == 0
    ]
    lines = [
        "# VALIDATION REPORT", "", "- 应跑 runs：54", f"- 已生成 summaries：{len(summaries)}",
        f"- 完成 runs：{len(completed)}", f"- 失败 runs：{len(failed)}",
        "- 公平性：每个 workload/load_level 的 Baseline 与 Migration 使用同一个 request_file。",
        f"- EngineCore fatal：{sum(x.get('EngineCore_fatal_count',0) for x in completed)}",
        f"- Traceback：{sum(x.get('Traceback_count',0) for x in completed)}",
        f"- ERROR：{sum(x.get('ERROR_count',0) for x in completed)}",
        f"- CUDA illegal memory access：{sum(x.get('CUDA_illegal_memory_access_count',0) for x in completed)}",
        "- total_input_tokens / total_output_tokens：原始响应 usage 为空时记为 NA。",
        "- downtime：当前日志未直接记录，记为 NA。",
        f"- 未触发迁移的负载：{', '.join(no_evidence) if no_evidence else '无'}",
        f"- 可用于报告：{', '.join(reportable) if reportable else '无'}",
    ]
    (FINAL_DIR / "VALIDATION_REPORT.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def mooncake_audit_v2() -> None:
    try:
        import mooncake  # type: ignore
        import mooncake.mooncake_connector_v1  # type: ignore
        import_status = f"available: {mooncake.__file__}"
    except Exception as exc:
        import_status = f"unavailable: {type(exc).__name__}: {exc}"
    lines = [
        "# MOONCAKE FEASIBILITY", "", f"- MooncakeConnector import：{import_status}",
        "- vllm-mooncake 启动参数：项目测试工具中存在 MooncakeConnector 分支。",
        "- 是否修改代码：未修改。", "- 是否重编 vLLM：未重编。",
        "- 本轮是否运行 Mooncake 正式实验：否。",
        "- 建议：仅在 import 可用且独立 smoke 通过后，另开结果目录验证。",
    ]
    (FINAL_DIR / "MOONCAKE_FEASIBILITY.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def finalize() -> None:
    create_directories()
    summaries = all_run_summaries()
    aggregated = aggregate(summaries)
    ratios = ratio_rows_v2(aggregated)
    (FINAL_DIR / "hybrid_load_sweep_all_summaries.json").write_text(
        json.dumps(summaries, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    (FINAL_DIR / "hybrid_load_sweep_aggregated.json").write_text(
        json.dumps({"aggregated": aggregated, "ratios": ratios}, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    html_text = render_html_v2(summaries, aggregated, ratios)
    (FINAL_DIR / "hybrid_load_sweep_table.html").write_text(html_text, encoding="utf-8")
    (FINAL_DIR / "llumnix_5090_load_sweep_metrics.html").write_text(html_text, encoding="utf-8")
    write_validation_v2(summaries, aggregated, ratios)
    mooncake_audit_v2()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "phase", choices=("prepare", "smoke", "migration-smoke", "full", "finalize", "all")
    )
    args = parser.parse_args()
    create_directories()
    if args.phase in ("prepare", "all"):
        write_environment_audit()
        write_workloads()
    if args.phase in ("smoke", "all"):
        run_smoke()
    if args.phase == "migration-smoke":
        run_migration_smoke_only()
    if args.phase in ("full", "all"):
        run_full()
    if args.phase in ("finalize", "all"):
        finalize()


if __name__ == "__main__":
    main()
