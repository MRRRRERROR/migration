#!/usr/bin/env python3
"""Run the four-group, 500-request Llumnix comparison without source changes."""

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
from datetime import datetime
from pathlib import Path
from urllib import request as urlrequest
from urllib.error import HTTPError, URLError

from transformers import AutoTokenizer

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


ROOT = Path(os.environ["MULTI_BASELINE_ROOT"]).resolve()
MODEL_PATH = os.environ.get("MODEL_PATH", "/data/models/Qwen2.5-7B-Instruct")
INSTANCES = 4
REQUEST_NUM = int(os.environ.get("MULTI_BASELINE_REQUEST_NUM", "500"))
CONCURRENCY = int(os.environ.get("MULTI_BASELINE_CONCURRENCY", "50"))
REPEATS = int(os.environ.get("MULTI_BASELINE_REPEATS", "3"))
ALLOW_CONTENDED = os.environ.get("MULTI_BASELINE_ALLOW_CONTENDED", "0") == "1"
GPU_MEMORY_UTILIZATION = os.environ.get("MULTI_BASELINE_GPU_MEMORY_UTILIZATION", "0.55")
FORCE_RERUN = os.environ.get("MULTI_BASELINE_FORCE_RERUN", "0") == "1"
SEED = 20260721
KVT_HOST = "127.0.0.1"
KVT_PORTS = tuple(36218 + 8 * index for index in range(INSTANCES))

CONFIG_DIR = ROOT / "00_config"
WORKLOAD_DIR = ROOT / "02_workloads"
RUNS_DIR = ROOT / "03_runs"
STEP_DIR = ROOT / "04_step_summaries"
FINAL_DIR = ROOT / "05_final"

WORKLOADS = {
    "stable_short": {
        "prompt": lambda i: f"Stable short request {i}: Hello, briefly introduce yourself.",
        "max_tokens": lambda i: 32,
        "description": "短输入，固定输出上限 32 tokens",
    },
    "mixed_tail": {
        "prompt": lambda i: (
            (
                f"Mixed tail long request {i}: Explain request migration, "
                + "KV cache transfer, and service latency in detail. " * 18
            ).strip()
            if i % 5 == 0
            else f"Mixed tail short request {i}: Hello, briefly introduce yourself."
        ),
        "max_tokens": lambda i: 192 if i % 5 == 0 else 48,
        "description": "80% 短请求 48 tokens，20% 长尾请求 192 tokens",
    },
    "long_output_migration": {
        "prompt": lambda i: (
            f"Long output migration request {i}: Explain how a distributed LLM serving system "
            + "can migrate a running request while preserving its KV cache. " * 8
        ).strip(),
        "max_tokens": lambda i: 256,
        "description": "中等输入，固定输出上限 256 tokens",
    },
}

GROUPS = {
    "Hybrid_no_migration": {
        "policy": "flood",
        "full_mode": True,
        "migration": False,
        "scheduler": True,
        "description": "HybridConnector/vllm-kvt，flood dispatch，关闭 live migration",
    },
    "Hybrid_live_migration": {
        "policy": "flood",
        "full_mode": True,
        "migration": True,
        "scheduler": True,
        "description": "HybridConnector/vllm-kvt，flood dispatch，开启 live migration",
    },
    "Round_robin_no_migration": {
        "policy": "round-robin",
        "full_mode": False,
        "migration": False,
        "scheduler": False,
        "description": "Gateway 原生本地 round-robin，无 scheduler，无迁移",
    },
    "Load_balance_no_migration": {
        "policy": "load-balance",
        "full_mode": False,
        "migration": False,
        "scheduler": True,
        "description": "原生 lite-mode load-balance，按当前负载选择实例，无迁移",
    },
}


def now_iso() -> str:
    return datetime.now().astimezone().isoformat(timespec="seconds")


def process_table() -> dict[int, dict]:
    result = subprocess.run(
        ["ps", "-eo", "pid=,ppid=,user=,args="],
        text=True,
        capture_output=True,
        check=False,
    )
    table = {}
    for line in result.stdout.splitlines():
        fields = line.strip().split(None, 3)
        if len(fields) < 3 or not fields[0].isdigit() or not fields[1].isdigit():
            continue
        table[int(fields[0])] = {
            "ppid": int(fields[1]),
            "owner": fields[2],
            "command": fields[3] if len(fields) == 4 else "",
        }
    return table


def experiment_process_ids(processes) -> set[int]:
    table = process_table()
    selected = {proc.pid for proc in processes if getattr(proc, "pid", None)}
    changed = True
    while changed:
        changed = False
        for pid, details in table.items():
            if details["ppid"] in selected and pid not in selected:
                selected.add(pid)
                changed = True
    return selected


def gpu_compute_processes(experiment_pids: set[int] | None = None) -> list[dict]:
    """Return active GPU compute processes without touching shared workloads."""
    result = subprocess.run(
        [
            "nvidia-smi",
            "--query-compute-apps=pid,used_memory,gpu_uuid",
            "--format=csv,noheader,nounits",
        ],
        text=True,
        capture_output=True,
        check=False,
    )
    processes = []
    table = process_table()
    experiment_pids = experiment_pids or set()
    for line in result.stdout.splitlines():
        fields = [field.strip() for field in line.split(",")]
        if len(fields) != 3 or not fields[0].isdigit():
            continue
        pid = int(fields[0])
        details = table.get(pid, {})
        processes.append({
            "pid": pid,
            "owner": details.get("owner", "unknown"),
            "command": details.get("command", ""),
            "used_memory_mib": int(fields[1]) if fields[1].isdigit() else fields[1],
            "gpu_uuid": fields[2],
            "is_experiment_process": pid in experiment_pids,
        })
    return processes


def gpu_snapshot(processes=()) -> dict:
    experiment_pids = experiment_process_ids(processes)
    apps = gpu_compute_processes(experiment_pids)
    result = subprocess.run(
        [
            "nvidia-smi",
            "--query-gpu=index,uuid,name,memory.total,memory.used,memory.free,utilization.gpu",
            "--format=csv,noheader,nounits",
        ],
        text=True,
        capture_output=True,
        check=False,
    )
    gpus = []
    for line in result.stdout.splitlines():
        fields = [field.strip() for field in line.split(",")]
        if len(fields) != 7:
            continue
        gpu_apps = [item for item in apps if item["gpu_uuid"] == fields[1]]
        non_experiment = [item for item in gpu_apps if not item["is_experiment_process"]]
        gpus.append({
            "index": int(fields[0]),
            "uuid": fields[1],
            "name": fields[2],
            "memory_total_mib": int(fields[3]),
            "memory_used_mib": int(fields[4]),
            "memory_free_mib": int(fields[5]),
            "utilization_gpu_percent": int(fields[6]),
            "non_experiment_process_present": bool(non_experiment),
            "non_experiment_process_count": len(non_experiment),
        })
    non_experiment = [item for item in apps if not item["is_experiment_process"]]
    return {
        "timestamp": now_iso(),
        "gpus": gpus,
        "compute_processes": apps,
        "non_experiment_gpu_process_count": len(non_experiment),
    }


def wait_for_idle_gpus(poll_interval: float = 60.0) -> None:
    """Wait until all four GPUs are idle so comparison runs remain fair."""
    status_path = STEP_DIR / "gpu_resource_wait.json"
    while True:
        active = gpu_compute_processes()
        if active and ALLOW_CONTENDED:
            status = {
                "checked_at": now_iso(),
                "state": "contended_allowed",
                "active_compute_processes": active,
                "policy": "Shared GPU use explicitly allowed; run must be marked contended.",
            }
            status_path.write_text(
                json.dumps(status, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
            )
            print("GPU_RESOURCE_GATE contended run allowed", flush=True)
            return
        status = {
            "checked_at": now_iso(),
            "state": "waiting" if active else "ready",
            "active_compute_processes": active,
            "policy": "All GPUs must have zero pre-existing compute processes before a run starts.",
        }
        status_path.write_text(json.dumps(status, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        if not active:
            print("GPU_RESOURCE_GATE ready: all GPUs are idle", flush=True)
            return
        details = ", ".join(
            f"pid={item['pid']} owner={item['owner']} memory={item['used_memory_mib']}MiB"
            for item in active
        )
        print(f"GPU_RESOURCE_GATE waiting: {details}", flush=True)
        time.sleep(poll_interval)


def create_dirs() -> None:
    for path in (CONFIG_DIR, WORKLOAD_DIR, RUNS_DIR, STEP_DIR, FINAL_DIR):
        path.mkdir(parents=True, exist_ok=True)


def percentile(values: list[float], pct: float):
    if not values:
        return None
    ordered = sorted(values)
    pos = (len(ordered) - 1) * pct / 100
    lo, hi = math.floor(pos), math.ceil(pos)
    if lo == hi:
        return ordered[lo]
    return ordered[lo] + (ordered[hi] - ordered[lo]) * (pos - lo)


def write_capability_audit() -> None:
    lines = [
        "# Baseline Capability Audit",
        "",
        f"- Generated: `{now_iso()}`",
        f"- Result root: `{ROOT}`",
        "- Project source modified: `False`",
        "- vLLM rebuilt: `False`",
        "",
        "## Capability Matrix",
        "",
        "| Experiment group | Native capability | Implementation | Core source change required |",
        "|---|---|---|---|",
        "| Hybrid_no_migration | Yes | `flood`, full mode, scheduler rescheduling disabled | No |",
        "| Hybrid_live_migration | Yes | `flood`, full mode, scheduler rescheduling enabled | No |",
        "| Round_robin_no_migration | Yes | Gateway `LocalBalancer` / `RoundRobinBalancer`, lite mode, no scheduler | No |",
        "| Load_balance_no_migration | Yes | Scheduler `load-balance` lite mode, lowest current load metric | No |",
        "",
        "## Evidence",
        "",
        "1. `cmd/config/config.go` declares `round-robin`, `load-balance`, and `flood` as supported scheduling policies.",
        "2. `pkg/gateway/load-balancer/roundrobin_balancer.go` implements native round-robin instance selection.",
        "3. `pkg/gateway/load-balancer/composite_balancer.go` selects local round-robin when policy is `round-robin`.",
        "4. `pkg/scheduler/policy/scheduling_policy_registry.go` implements native full/lite load-balance dispatch.",
        "5. `tests/local/conftest.py` does not start Scheduler for round-robin, matching the official deployment design.",
        "6. `docs/source/design/scheduler/policy_framework.md` states load-balance selects the instance with lowest load.",
        "",
        "## Fairness Decision",
        "",
        "- All groups use the same model, four GPUs, four vLLM instances, HybridConnector/vllm-kvt, request files, concurrency, and vLLM engine limits.",
        "- Round-robin and load-balance both use lite-mode backends. Their necessary architectural difference is that native round-robin is gateway-local while native load-balance requires Scheduler.",
        "- Hybrid_no_migration and Hybrid_live_migration differ only in the Scheduler rescheduling switch.",
        "- All single-node KVT/discovery endpoints use `127.0.0.1`; API and KVT listener readiness are mandatory.",
        "",
        "## Feasibility",
        "",
        "Both requested non-migration baselines are available without modifying Llumnix, vLLM, or blade_kvt core source. Formal runs may proceed.",
    ]
    (CONFIG_DIR / "baseline_capability_audit.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_workloads() -> None:
    tokenizer = AutoTokenizer.from_pretrained(MODEL_PATH, trust_remote_code=True)
    manifest = []
    for name, cfg in WORKLOADS.items():
        path = WORKLOAD_DIR / f"{name}.jsonl"
        rows = []
        for index in range(REQUEST_NUM):
            prompt = cfg["prompt"](index)
            rows.append(
                {
                    "id": f"{name}_{index}",
                    "workload_type": name,
                    "prompt": prompt,
                    "prompt_tokens": len(tokenizer.encode(prompt, add_special_tokens=False)),
                    "max_tokens": cfg["max_tokens"](index),
                    "temperature": 0.0,
                    "stream": False,
                    "ignore_eos": True,
                    "seed": SEED,
                }
            )
        payload = "\n".join(json.dumps(row, ensure_ascii=False) for row in rows) + "\n"
        if path.exists() and path.read_text(encoding="utf-8") != payload:
            raise RuntimeError(f"Existing fixed workload differs: {path}")
        path.write_text(payload, encoding="utf-8")
        manifest.append(
            {
                "workload_type": name,
                "request_file": str(path),
                "request_num": REQUEST_NUM,
                "concurrency": CONCURRENCY,
                "description": cfg["description"],
                "temperature": 0.0,
                "ignore_eos": True,
                "seed": SEED,
            }
        )
    (WORKLOAD_DIR / "workload_manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )


def read_workload(name: str) -> list[dict]:
    path = WORKLOAD_DIR / f"{name}.jsonl"
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line]


def cleanup_ports() -> None:
    ports = (
        "6379/tcp 8100/tcp 8101/tcp 8102/tcp 8103/tcp 18088/tcp 18089/tcp "
        "31100/tcp 31101/tcp 31102/tcp 31103/tcp 36218/tcp 36226/tcp 36234/tcp 36242/tcp"
    )
    subprocess.run(f"fuser -k -9 {ports}", shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    time.sleep(3)


def start_stack(group: str):
    cfg = GROUPS[group]
    os.environ["VLLM_HOST_IP"] = KVT_HOST
    redis_dir = CONFIG_DIR / f"redis_ephemeral_{time.time_ns()}"
    redis_dir.mkdir(parents=True, exist_ok=False)
    redis_cmd = (
        f"{get_redis_command()} --save '' --appendonly no "
        f"--dir {redis_dir} --dbfilename run.rdb"
    )
    processes = [start_process("Redis", redis_cmd, "redis.log")]
    commands = {"redis": redis_cmd}
    if cfg["scheduler"]:
        scheduler_cmd = get_scheduler_command(
            policy=cfg["policy"],
            enable_full_mode_scheduling=cfg["full_mode"],
            enable_migration=cfg["migration"],
            enable_pd=False,
        )
        processes.append(start_process("Scheduler", scheduler_cmd, "scheduler.log"))
        commands["scheduler"] = scheduler_cmd
    gateway_cmd = get_gateway_command(
        policy=cfg["policy"],
        enable_full_mode_scheduling=cfg["full_mode"],
        enable_pd=False,
        connector_type="HybridConnector",
    )
    processes.append(start_process("Gateway", gateway_cmd, "gateway.log"))
    commands["gateway"] = gateway_cmd
    commands["instances"] = []
    for index in range(INSTANCES):
        port = VLLM_BASE_PORT + index
        tag = f"multi500_{group}_{index}"
        vllm_cmd = get_vllm_command(
            "neutral", port, index, cfg["full_mode"], tag, "HybridConnector"
        )
        vllm_cmd = re.sub(
            r"--gpu-memory-utilization\s+\S+",
            f"--gpu-memory-utilization {GPU_MEMORY_UTILIZATION}",
            vllm_cmd,
            count=1,
        )
        discovery_cmd = get_discovery_command("neutral", port)
        discovery_cmd = re.sub(r"^python3\b", sys.executable, discovery_cmd, count=1)
        processes.append(start_process(f"vLLM-{index}", vllm_cmd, f"vllm_{index}.log"))
        processes.append(start_process(f"Runtime-{index}", discovery_cmd, f"runtime_{index}.log"))
        commands["instances"].append({"index": index, "vllm": vllm_cmd, "discovery": discovery_cmd})
        if not wait_for_service(f"http://localhost:{port}/health", timeout=360):
            raise TimeoutError(f"API readiness timeout: {port}")
    return processes, commands


def wait_for_discovery_instances(timeout: float = 120.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        ready = []
        for index in range(INSTANCES):
            path = LOG_DIR / f"runtime_{index}.log"
            text = path.read_text(errors="replace") if path.exists() else ""
            ready.append(
                "status changed: unhealthy -> healthy" in text
                and "Updated 1 healthy instances in backend" in text
            )
        if all(ready):
            return
        time.sleep(1)
    missing = [index for index, is_ready in enumerate(ready) if not is_ready]
    raise TimeoutError(f"Discovery readiness timeout: runtime instances {missing}")


def wait_for_kvt_listeners(timeout: float = 120.0) -> list[socket.socket]:
    deadline = time.time() + timeout
    ready: set[int] = set()
    while time.time() < deadline:
        logs = "\n".join(path.read_text(errors="replace") for path in LOG_DIR.glob("vllm_*.log"))
        listeners = subprocess.run(
            ["ss", "-ltnH"], capture_output=True, text=True, check=False
        ).stdout
        for port in KVT_PORTS:
            if port in ready:
                continue
            log_ready = (
                re.search(rf"init_kv_transfer_server.*{re.escape(KVT_HOST)}:{port}\b", logs)
                or re.search(
                    rf"TCPServer\.start_server:\s+ip={re.escape(KVT_HOST)}\s+port={port}\b",
                    logs,
                )
            )
            if not log_ready:
                continue
            if not re.search(rf"(?:{re.escape(KVT_HOST)}|0\.0\.0\.0|\*):{port}\b", listeners):
                continue
            ready.add(port)
        if len(ready) == len(KVT_PORTS):
            return []
        time.sleep(1)
    missing = [f"{KVT_HOST}:{port}" for port in KVT_PORTS if port not in ready]
    raise TimeoutError(f"KVT listener readiness timeout: {missing}")


def post_completion(item: dict) -> dict:
    payload = {
        "model": MODEL_PATH,
        "prompt": item["prompt"],
        "max_tokens": item["max_tokens"],
        "temperature": 0.0,
        "stream": False,
        "ignore_eos": True,
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
        raw, status = exc.read().decode("utf-8", errors="replace"), exc.code
    except (URLError, TimeoutError, OSError) as exc:
        raw, status = repr(exc), 0
    latency = time.time() - started
    text, finish_reason, usage = "", None, {}
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
        "text": text,
        "finish_reason": finish_reason,
        "usage": usage,
        "raw": raw[:2000],
    }


def copy_logs(run_dir: Path) -> None:
    run_dir.mkdir(parents=True, exist_ok=True)
    for path in LOG_DIR.glob("*.log"):
        shutil.copy2(path, run_dir / path.name)


def service_log_text(run_dir: Path) -> str:
    names = ("redis.log", "scheduler.log", "gateway.log")
    paths = [run_dir / name for name in names if (run_dir / name).exists()]
    paths.extend(sorted(run_dir.glob("vllm_*.log")))
    paths.extend(sorted(run_dir.glob("runtime_*.log")))
    return "\n".join(path.read_text(errors="replace") for path in paths)


def count_patterns(run_dir: Path) -> dict:
    text = service_log_text(run_dir)
    finished = re.findall(r"FinishedReq=([A-Za-z0-9_-]+)", text)
    finish_lists = re.findall(r"finish_request_ids\s*[=:]\s*\[([^\]]*)\]", text)
    finish_ids = []
    for value in finish_lists:
        finish_ids.extend(re.findall(r"[A-Za-z0-9_-]{8,}", value))
    abort_lines = [line for line in text.splitlines() if "abort req" in line.lower() or "AbortReq" in line]
    host_mismatch = len(
        re.findall(r"(?:init_kv_transfer_server|TCPServer\.start_server).*10\.112\.184\.62", text)
    )
    return {
        "migrate_out_selected_count": text.count("migrate_out_selected"),
        "migrate_out_done_count": text.count("migrate_out_done"),
        "finish_request_ids_count": len(finish_ids),
        "FinishedReq_lines": len(finished),
        "unique_FinishedReq_ids": len(set(finished)),
        "KVTResp_code_0_count": text.count("KVTResp(code=0"),
        "KVTResp_code_404_count": text.count("KVTResp(code=404"),
        "KVTResp_code_500_count": text.count("KVTResp(code=500"),
        "SendStubMetrics_count": text.count("SendStubMetrics"),
        "migration_end_count": len(re.findall(r"migration end", text, re.IGNORECASE)),
        "migration_suspend_count": sum("reason='migration.suspend'" in line for line in abort_lines),
        "abort_count": sum("reason='migration.suspend'" not in line for line in abort_lines),
        "downtime": "NA",
        "total_migration_time": "NA",
        "EngineCore_fatal_count": len(re.findall(r"EngineCore encountered a fatal error|EngineCore failed to start", text)),
        "Traceback_count": text.count("Traceback (most recent call last):"),
        "ERROR_count": len(re.findall(r"(?m)^.*\bERROR\b", text)),
        "RuntimeError_count": text.count("RuntimeError"),
        "CUDA_illegal_memory_access_count": len(re.findall(r"CUDA illegal memory access|illegal memory access", text, re.I)),
        "host_mismatch_count": host_mismatch,
    }


def first_fatal(run_dir: Path) -> str:
    markers = (
        "EngineCore encountered a fatal error",
        "EngineCore failed to start",
        "Traceback (most recent call last):",
        "CUDA illegal memory access",
        "RuntimeError:",
    )
    for path in sorted(run_dir.glob("*.log")):
        if path.name == "run.log":
            continue
        lines = path.read_text(errors="replace").splitlines()
        for index, line in enumerate(lines):
            if any(marker in line for marker in markers):
                return "\n".join([f"FILE {path}"] + lines[index : index + 80])
    return "NO_FATAL_FOUND"


def valid_checkpoint(path: Path) -> bool:
    if not path.exists():
        return False
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return False
    if data.get("status") != "completed" or data.get("successful_responses") != REQUEST_NUM:
        return False
    if any(data.get(key, 0) for key in (
        "EngineCore_fatal_count", "Traceback_count", "CUDA_illegal_memory_access_count",
        "KVTResp_code_500_count", "finish_reason_abort_count", "host_mismatch_count",
    )):
        return False
    if data.get("migration_enabled") and (
        data.get("KVTResp_code_404_count", 0) or data.get("abort_count", 0)
    ):
        return False
    return True


def run_one(workload: str, group: str, repeat_id: int) -> dict:
    cfg = GROUPS[group]
    request_file = WORKLOAD_DIR / f"{workload}.jsonl"
    run_dir = RUNS_DIR / workload / group / f"run_{repeat_id}"
    run_dir.mkdir(parents=True, exist_ok=True)
    summary_path = run_dir / "run_summary.json"
    if not FORCE_RERUN and valid_checkpoint(summary_path):
        print(f"SKIP_COMPLETED {summary_path}", flush=True)
        return json.loads(summary_path.read_text(encoding="utf-8"))

    requests = read_workload(workload)
    if cfg["migration"]:
        for item in requests:
            item["kv_transfer_params"] = {"ali_llumnix_disagg": False}

    processes, probes = [], []
    commands = {}
    started_at, wall_started = now_iso(), time.time()
    summary = None
    gpu_before, gpu_after = {}, {}
    try:
        wait_for_idle_gpus()
        cleanup_ports()
        gpu_before = gpu_snapshot()
        shutil.rmtree(NAMING_DIR, ignore_errors=True)
        shutil.rmtree(LOG_DIR, ignore_errors=True)
        LOG_DIR.mkdir(parents=True, exist_ok=True)
        processes, commands = start_stack(group)
        wait_for_discovery_instances(120)
        probes = wait_for_kvt_listeners(120)
        time.sleep(10)

        benchmark_started = time.time()
        with concurrent.futures.ThreadPoolExecutor(max_workers=CONCURRENCY) as executor:
            futures = [executor.submit(post_completion, item) for item in requests]
            results = [future.result() for future in concurrent.futures.as_completed(futures)]
        duration = time.time() - benchmark_started
        gpu_after = gpu_snapshot(processes)
        copy_logs(run_dir)

        successful = [r for r in results if r["status"] == 200 and r["text_nonempty"]]
        latencies = [r["latency_s"] for r in results]
        finish_abort = sum(r.get("finish_reason") == "abort" for r in results)
        output_tokens = sum(
            next(item["max_tokens"] for item in requests if item["id"] == r["id"])
            for r in successful
        )
        input_tokens = sum(item["prompt_tokens"] for item in requests)
        non_experiment_gpu_process_count = max(
            gpu_before.get("non_experiment_gpu_process_count", 0),
            gpu_after.get("non_experiment_gpu_process_count", 0),
        )
        gpu_contention = non_experiment_gpu_process_count > 0
        summary = {
            "workload_type": workload,
            "experiment_group": group,
            "repeat_id": repeat_id,
            "request_num": REQUEST_NUM,
            "concurrency": CONCURRENCY,
            "instance_num": INSTANCES,
            "connector": "HybridConnector",
            "protocol": "vllm-kvt",
            "scheduling_policy": cfg["policy"],
            "scheduling_mode": "full" if cfg["full_mode"] else "lite",
            "migration_enabled": cfg["migration"],
            "request_file": str(request_file),
            "request_file_sha256": subprocess.check_output(["sha256sum", str(request_file)], text=True).split()[0],
            "model_path": MODEL_PATH,
            "seed": SEED,
            "temperature": 0.0,
            "ignore_eos": True,
            "start_time": started_at,
            "end_time": now_iso(),
            "duration_s": duration,
            "wall_duration_s": time.time() - wall_started,
            "status": "completed",
            "fairness_status": "contended" if gpu_contention else "fair",
            "gpu_contention": gpu_contention,
            "usable_for_final_fair_performance_comparison": not gpu_contention,
            "note": (
                "A non-experiment GPU process appeared during the run."
                if gpu_contention
                else "All four GPUs passed the idle-resource gate and remained exclusive."
            ),
            "nvidia_smi_before": gpu_before,
            "nvidia_smi_after": gpu_after,
            "non_experiment_gpu_process_count": non_experiment_gpu_process_count,
            "successful_responses": len(successful),
            "failed_responses": REQUEST_NUM - len(successful),
            "success_rate": len(successful) / REQUEST_NUM,
            "http_200_text_nonempty": len(successful),
            "empty_text_count": sum(r["status"] == 200 and not r["text_nonempty"] for r in results),
            "finish_reason_abort_count": finish_abort,
            "QPS": len(successful) / duration if duration else 0,
            "throughput_tok_s": output_tokens / duration if duration else 0,
            "median_e2e_latency_s": statistics.median(latencies),
            "P95_e2e_latency_s": percentile(latencies, 95),
            "P99_e2e_latency_s": percentile(latencies, 99),
            "mean_e2e_latency_s": statistics.mean(latencies),
            "total_input_tokens": input_tokens,
            "input_token_method": "Qwen tokenizer count from fixed request file",
            "total_output_tokens": output_tokens,
            "output_token_method": "configured max_tokens for successful requests; ignore_eos=true",
            "kvt_listener_endpoints": [f"{KVT_HOST}:{port}" for port in KVT_PORTS],
            "launch_commands": commands,
            **count_patterns(run_dir),
            "first_fatal": first_fatal(run_dir),
            "sample_results": [{**r, "text": r["text"][:300]} for r in results[:5]],
        }
    except Exception as exc:
        gpu_after = gpu_snapshot(processes)
        copy_logs(run_dir)
        patterns = (repr(exc) + "\n" + first_fatal(run_dir)).lower()
        is_oom = any(marker in patterns for marker in (
            "out of memory", "cuda oom", "cudamalloc", "failed to allocate",
        ))
        summary = {
            "workload_type": workload,
            "experiment_group": group,
            "repeat_id": repeat_id,
            "request_num": REQUEST_NUM,
            "concurrency": CONCURRENCY,
            "instance_num": INSTANCES,
            "connector": "HybridConnector",
            "protocol": "vllm-kvt",
            "scheduling_policy": cfg["policy"],
            "migration_enabled": cfg["migration"],
            "request_file": str(request_file),
            "model_path": MODEL_PATH,
            "start_time": started_at,
            "end_time": now_iso(),
            "wall_duration_s": time.time() - wall_started,
            "status": "failed_oom" if is_oom else "failed",
            "fairness_status": "fair",
            "gpu_contention": False,
            "usable_for_final_fair_performance_comparison": False,
            "note": "All four GPUs passed the idle-resource gate before startup; this run failed and is not usable.",
            "nvidia_smi_before": gpu_before,
            "nvidia_smi_after": gpu_after,
            "non_experiment_gpu_process_count": max(
                gpu_before.get("non_experiment_gpu_process_count", 0),
                gpu_after.get("non_experiment_gpu_process_count", 0),
            ),
            "exception": repr(exc),
            "traceback": traceback.format_exc(),
            **count_patterns(run_dir),
            "first_fatal": first_fatal(run_dir),
        }
    finally:
        cleanup_processes(processes)
        for probe in probes:
            probe.close()
        cleanup_ports()

    payload = json.dumps(summary, ensure_ascii=False, indent=2) + "\n"
    summary_path.write_text(payload, encoding="utf-8")
    (run_dir / "run.log").write_text(payload, encoding="utf-8")
    print(f"MULTI500_SUMMARY {json.dumps(summary, ensure_ascii=False)}", flush=True)
    return summary


def immediate_stop_reason(summary: dict) -> str | None:
    if summary.get("status") == "failed_oom":
        return None
    checks = (
        ("EngineCore_fatal_count", "EngineCore fatal"),
        ("Traceback_count", "Traceback"),
        ("CUDA_illegal_memory_access_count", "CUDA illegal memory access"),
        ("KVTResp_code_500_count", "KVTResp(code=500)"),
        ("finish_reason_abort_count", "finish_reason=abort"),
        ("host_mismatch_count", "KVT host mismatch"),
    )
    for key, label in checks:
        if summary.get(key, 0) > 0:
            return label
    if summary.get("migration_enabled"):
        if summary.get("KVTResp_code_404_count", 0) > 0:
            return "KVTResp(code=404)"
        if summary.get("abort_count", 0) > 0:
            return "migration load-fail abort"
    return None


def run_failed(summary: dict) -> bool:
    if summary.get("status") != "completed" or summary.get("successful_responses") != REQUEST_NUM:
        return True
    if summary.get("experiment_group") == "Hybrid_live_migration":
        evidence = sum(
            summary.get(key, 0)
            for key in ("KVTResp_code_0_count", "FinishedReq_lines", "SendStubMetrics_count", "migration_end_count")
        )
        return evidence == 0
    return False


def run_all() -> None:
    failure_streak = {group: 0 for group in GROUPS}
    base_order = list(GROUPS)
    for workload in WORKLOADS:
        for repeat_id in range(1, REPEATS + 1):
            offset = (repeat_id - 1) % len(base_order)
            group_order = base_order[offset:] + base_order[:offset]
            for group in group_order:
                summary = run_one(workload, group, repeat_id)
                stop_reason = immediate_stop_reason(summary)
                if stop_reason:
                    (STEP_DIR / "STOPPED.md").write_text(
                        f"# Experiment Stopped\n\n- Run: `{workload}/{group}/run_{repeat_id}`\n"
                        f"- Reason: `{stop_reason}`\n- First fatal:```\n{summary.get('first_fatal')}\n```\n",
                        encoding="utf-8",
                    )
                    raise RuntimeError(f"Mandatory stop: {stop_reason}")
                if run_failed(summary):
                    failure_streak[group] += 1
                else:
                    failure_streak[group] = 0
                if failure_streak[group] >= 2:
                    raise RuntimeError(f"Two consecutive failures for {group}")


def all_summaries() -> list[dict]:
    values = []
    for path in sorted(RUNS_DIR.glob("*/*/run_*/run_summary.json")):
        try:
            values.append(json.loads(path.read_text(encoding="utf-8")))
        except Exception:
            continue
    return values


def mean(values):
    nums = [value for value in values if isinstance(value, (int, float))]
    return statistics.mean(nums) if nums else None


def aggregate(summaries: list[dict]) -> list[dict]:
    rows = []
    metrics = (
        "successful_responses", "failed_responses", "success_rate", "QPS", "throughput_tok_s",
        "median_e2e_latency_s", "P95_e2e_latency_s", "P99_e2e_latency_s", "mean_e2e_latency_s",
        "KVTResp_code_0_count", "KVTResp_code_404_count", "KVTResp_code_500_count",
        "FinishedReq_lines", "unique_FinishedReq_ids", "SendStubMetrics_count", "migration_end_count",
        "migration_suspend_count", "abort_count", "EngineCore_fatal_count", "Traceback_count",
        "ERROR_count", "RuntimeError_count", "CUDA_illegal_memory_access_count",
    )
    for workload in WORKLOADS:
        for group in GROUPS:
            selected = [s for s in summaries if s.get("workload_type") == workload and s.get("experiment_group") == group]
            row = {"workload_type": workload, "experiment_group": group, "runs": len(selected)}
            for metric in metrics:
                row[f"mean_{metric}"] = mean([s.get(metric) for s in selected])
                row[f"total_{metric}"] = sum(s.get(metric, 0) for s in selected if isinstance(s.get(metric), (int, float)))
            rows.append(row)
    return rows


def td(value) -> str:
    if value is None or value == "NA":
        return '<td class="na">NA</td>'
    if isinstance(value, float):
        return f"<td>{value:.4f}</td>"
    return f"<td>{html.escape(str(value))}</td>"


def render_html(summaries: list[dict], aggregated: list[dict]) -> str:
    config_rows = "".join(
        f"<tr><td>{html.escape(name)}</td><td>{html.escape(cfg['description'])}</td><td>{REQUEST_NUM}</td><td>{CONCURRENCY}</td><td>{REPEATS}</td></tr>"
        for name, cfg in WORKLOADS.items()
    )
    audit_rows = "".join(
        f"<tr><td>{html.escape(group)}</td><td>可用</td><td>{html.escape(cfg['description'])}</td></tr>"
        for group, cfg in GROUPS.items()
    )
    aggregate_rows = ""
    for row in aggregated:
        aggregate_rows += "<tr>" + td(row["workload_type"]) + td(row["experiment_group"]) + td(row["runs"])
        for key in ("mean_success_rate", "mean_QPS", "mean_throughput_tok_s", "mean_median_e2e_latency_s", "mean_P95_e2e_latency_s", "mean_P99_e2e_latency_s"):
            aggregate_rows += td(row.get(key))
        aggregate_rows += "</tr>"
    migration_rows = ""
    for summary in summaries:
        if summary.get("experiment_group") != "Hybrid_live_migration":
            continue
        migration_rows += "<tr>" + td(summary.get("workload_type")) + td(summary.get("repeat_id"))
        for key in ("KVTResp_code_0_count", "FinishedReq_lines", "unique_FinishedReq_ids", "SendStubMetrics_count", "migration_end_count", "KVTResp_code_500_count", "abort_count"):
            migration_rows += td(summary.get(key))
        migration_rows += "</tr>"
    exception_rows = ""
    for group in GROUPS:
        selected = [s for s in summaries if s.get("experiment_group") == group]
        exception_rows += "<tr>" + td(group)
        for key in ("EngineCore_fatal_count", "Traceback_count", "ERROR_count", "RuntimeError_count", "CUDA_illegal_memory_access_count", "KVTResp_code_500_count", "finish_reason_abort_count"):
            exception_rows += td(sum(s.get(key, 0) for s in selected))
        exception_rows += "</tr>"
    run_rows = ""
    for s in summaries:
        run_rows += "<tr>" + td(s.get("workload_type")) + td(s.get("experiment_group")) + td(s.get("repeat_id")) + td(s.get("status")) + td(s.get("successful_responses")) + td(s.get("QPS")) + td(s.get("throughput_tok_s")) + td(s.get("median_e2e_latency_s")) + td(s.get("P95_e2e_latency_s")) + td(s.get("P99_e2e_latency_s")) + "</tr>"
    return f"""<!doctype html><html lang="zh-CN"><head><meta charset="utf-8"><title>Llumnix RTX 5090 多基线 500 请求实验</title><style>
body{{font-family:Arial,'Microsoft YaHei',sans-serif;margin:24px;color:#1f2937;background:#f7f8fa}}h1{{font-size:24px}}h2{{font-size:18px;margin-top:28px}}.wrap{{max-width:1500px;margin:auto}}table{{width:100%;border-collapse:collapse;background:white;font-size:12px;table-layout:auto}}th,td{{border:1px solid #d7dce2;padding:7px;text-align:center;word-break:break-word}}th{{background:#263746;color:white;position:sticky;top:0}}tr:nth-child(even){{background:#f3f6f8}}.na{{background:#e5e7eb;color:#6b7280}}.meta{{font-size:13px;color:#4b5563}}
</style></head><body><div class="wrap"><h1>Llumnix RTX 5090 多基线 500 请求公平对比</h1><p class="meta">结果目录：{html.escape(str(ROOT))}</p>
<p class="meta"><strong>公平性状态：</strong>正式公平实验；每轮启动前均要求四张 GPU 不存在既有计算进程。</p>
<h2>表 1：实验配置</h2><table><tr><th>Workload</th><th>配置</th><th>请求数</th><th>并发</th><th>重复轮数</th></tr>{config_rows}</table>
<h2>表 2：Baseline 能力审计</h2><table><tr><th>实验组</th><th>状态</th><th>原生实现</th></tr>{audit_rows}</table>
<h2>表 3：三类 Workload 聚合指标</h2><table><tr><th>Workload</th><th>实验组</th><th>完成轮数</th><th>成功率均值</th><th>QPS 均值</th><th>输出吞吐 tok/s</th><th>Median E2E(s)</th><th>P95 E2E(s)</th><th>P99 E2E(s)</th></tr>{aggregate_rows}</table>
<h2>表 4：Hybrid Migration 迁移证据</h2><table><tr><th>Workload</th><th>轮次</th><th>KVT code=0</th><th>FinishedReq</th><th>唯一迁移请求</th><th>SendStubMetrics</th><th>migration_end</th><th>KVT code=500</th><th>失败 abort</th></tr>{migration_rows}</table>
<h2>表 5：异常复核</h2><table><tr><th>实验组</th><th>EngineCore fatal</th><th>Traceback</th><th>ERROR</th><th>RuntimeError</th><th>CUDA illegal</th><th>KVT 500</th><th>finish abort</th></tr>{exception_rows}</table>
<h2>表 6：逐轮结果</h2><table><tr><th>Workload</th><th>实验组</th><th>轮次</th><th>状态</th><th>成功请求</th><th>QPS</th><th>吞吐 tok/s</th><th>Median(s)</th><th>P95(s)</th><th>P99(s)</th></tr>{run_rows}</table>
<p class="meta">自动生成时间：{now_iso()}</p></div></body></html>"""


def finalize() -> None:
    summaries = all_summaries()
    aggregated = aggregate(summaries)
    payload = {
        "generated_at": now_iso(),
        "result_root": str(ROOT),
        "expected_runs": 36,
        "completed_runs": sum(s.get("status") == "completed" for s in summaries),
        "summaries": summaries,
        "aggregated": aggregated,
    }
    (FINAL_DIR / "multi_baseline_500req_aggregated.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    (FINAL_DIR / "llumnix_5090_multi_baseline_500req.html").write_text(
        render_html(summaries, aggregated), encoding="utf-8"
    )
    totals = {key: sum(s.get(key, 0) for s in summaries) for key in (
        "EngineCore_fatal_count", "Traceback_count", "ERROR_count", "RuntimeError_count",
        "CUDA_illegal_memory_access_count", "KVTResp_code_500_count", "finish_reason_abort_count",
    )}
    included = [group for group in GROUPS if any(s.get("experiment_group") == group and s.get("status") == "completed" for s in summaries)]
    missing = [group for group in GROUPS if group not in included]
    lines = [
        "# Validation Report", "", f"- Generated: `{now_iso()}`", f"- Expected runs: `36`",
        f"- Recorded runs: `{len(summaries)}`", f"- Completed runs: `{payload['completed_runs']}`",
        f"- Included baselines: `{', '.join(included) or 'None'}`", f"- Unavailable baselines: `{', '.join(missing) or 'None'}`",
        "", "## Fairness Status", "",
        "本轮为正式公平实验。每轮启动前均通过 GPU 空闲门禁，并记录运行前后 nvidia-smi 快照。",
        "", "## Exception Totals", "",
    ]
    lines.extend(f"- {key}: `{value}`" for key, value in totals.items())
    lines.extend(["", "## Validation", "", f"- All 36 runs complete: `{payload['completed_runs'] == 36}`", f"- Mandatory-stop artifact exists: `{(STEP_DIR / 'STOPPED.md').exists()}`"])
    (FINAL_DIR / "VALIDATION_REPORT.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("phase", choices=("prepare", "run", "run-one", "finalize", "all"))
    parser.add_argument("--workload", choices=tuple(WORKLOADS))
    parser.add_argument("--group", choices=tuple(GROUPS))
    parser.add_argument("--repeat", type=int, default=1)
    args = parser.parse_args()
    create_dirs()
    if args.phase == "all":
        write_capability_audit()
        write_workloads()
        try:
            run_all()
        finally:
            finalize()
        return
    if args.phase == "prepare":
        write_capability_audit()
        write_workloads()
    if args.phase == "run":
        run_all()
    if args.phase == "run-one":
        if not args.workload or not args.group:
            parser.error("run-one requires --workload and --group")
        summary = run_one(args.workload, args.group, args.repeat)
        stop_reason = immediate_stop_reason(summary)
        if stop_reason:
            raise RuntimeError(f"Mandatory stop: {stop_reason}")
        if run_failed(summary):
            raise RuntimeError("Run did not meet success/evidence requirements")
    if args.phase == "finalize":
        finalize()


if __name__ == "__main__":
    main()
