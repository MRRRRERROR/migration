#!/usr/bin/env python3
import concurrent.futures
import json
import os
import re
import shutil
import statistics
import time
import traceback
from pathlib import Path
from urllib import request as urlrequest
from urllib.error import HTTPError, URLError

from tests.local.utils import (
    LOG_DIR,
    NAMING_DIR,
    cleanup_processes,
    get_discovery_command,
    get_gateway_command,
    get_redis_command,
    get_scheduler_command,
    get_vllm_command,
    start_process,
    wait_for_service,
    VLLM_BASE_PORT,
)


ROOT = Path(os.environ["BASELINE_ROOT"])
WORKLOAD_DIR = ROOT / "workloads"
MODEL_PATH = os.environ.get("MODEL_PATH", "/data/models/Qwen2.5-7B-Instruct")
INSTANCES = int(os.environ.get("FAIR_INSTANCES", "4"))
WORKERS = int(os.environ.get("FAIR_WORKERS", "20"))
REPEATS = int(os.environ.get("FAIR_REPEATS", "3"))


WORKLOADS = {
    "stable_short": {
        "label": "稳定短请求 workload",
        "request_num": 50,
        "max_tokens": 32,
        "prompt_fn": lambda i: f"Stable short request {i}: Hello, briefly introduce yourself.",
    },
    "long_output_migration": {
        "label": "长输出迁移 workload",
        "request_num": 50,
        "max_tokens": 256,
        "prompt_fn": lambda i: (f"Long migration request {i}: Hello, my name is " * 20).strip(),
    },
    "mixed_tail": {
        "label": "长尾混合 workload",
        "request_num": 50,
        "max_tokens": 192,
        "prompt_fn": lambda i: (
            (f"Tail long request {i}: Please continue this introduction with details. " * 25).strip()
            if i % 10 in {0, 1}
            else f"Tail short request {i}: Hello, briefly introduce yourself."
        ),
    },
}


def percentile(values, pct):
    if not values:
        return None
    ordered = sorted(values)
    idx = min(len(ordered) - 1, max(0, round((pct / 100) * (len(ordered) - 1))))
    return ordered[idx]


def write_workloads():
    WORKLOAD_DIR.mkdir(parents=True, exist_ok=True)
    manifest = []
    for name, cfg in WORKLOADS.items():
        path = WORKLOAD_DIR / f"{name}.jsonl"
        rows = []
        for i in range(cfg["request_num"]):
            rows.append({
                "id": f"{name}_{i}",
                "prompt": cfg["prompt_fn"](i),
                "max_tokens": cfg["max_tokens"],
                "temperature": 0.0,
                "stream": False,
                "ignore_eos": True,
            })
        path.write_text("\n".join(json.dumps(x, ensure_ascii=False) for x in rows), encoding="utf-8")
        manifest.append({
            "workload": name,
            "label": cfg["label"],
            "path": str(path),
            "request_num": cfg["request_num"],
            "max_tokens": cfg["max_tokens"],
        })
    (WORKLOAD_DIR / "manifest.json").write_text(json.dumps(manifest, indent=2, ensure_ascii=False), encoding="utf-8")
    return manifest


def load_workload(name):
    path = WORKLOAD_DIR / f"{name}.jsonl"
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def post_completion(item):
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
    start = time.time()
    try:
        with urlrequest.urlopen(req, timeout=900) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
            status = resp.status
    except HTTPError as e:
        raw = e.read().decode("utf-8", errors="replace")
        status = e.code
    except URLError as e:
        raw = repr(e)
        status = 0
    latency = time.time() - start
    text = ""
    finish_reason = None
    usage = {}
    output_tokens = None
    try:
        data = json.loads(raw)
        choice = (data.get("choices") or [{}])[0]
        text = choice.get("text", "")
        finish_reason = choice.get("finish_reason")
        usage = data.get("usage") or {}
        output_tokens = usage.get("completion_tokens")
    except Exception:
        pass
    return {
        "id": item["id"],
        "status": status,
        "latency": latency,
        "text": text,
        "finish_reason": finish_reason,
        "usage": usage,
        "output_tokens": output_tokens,
        "raw": raw[:2000],
    }


def copy_logs(stage_dir):
    stage_dir.mkdir(parents=True, exist_ok=True)
    if LOG_DIR.exists():
        for p in LOG_DIR.glob("*.log"):
            shutil.copy2(p, stage_dir / p.name)


def count_patterns(stage_dir):
    texts = []
    for p in stage_dir.glob("*.log"):
        texts.append(p.read_text(errors="replace"))
    text = "\n".join(texts)
    finished = re.findall(r"FinishedReq=([A-Za-z0-9_-]+)", text)
    return {
        "migrate_out_selected_count": len(re.findall(r"migrate_out_selected", text)),
        "migrate_out_begin_count": len(re.findall(r"migrate out|Migrate out|migration transfer kv and suspend req", text)),
        "migrate_out_done_count": len(re.findall(r"migrate_out_done", text)),
        "finish_request_ids_count": len(re.findall(r"finish_request_ids=\[[^\]]+\]|finish_request_ids: \[[^\]]+\]", text)),
        "unique_finish_request_ids_count": 0,
        "finished_req_lines": len(finished),
        "unique_finished_req_ids": len(set(finished)),
        "kvt_code0": text.count("KVTResp(code=0"),
        "kvt_code404": text.count("KVTResp(code=404"),
        "kvt_code500": text.count("KVTResp(code=500"),
        "send_stub_metrics": len(re.findall(r"SendStubMetrics", text)),
        "migration_end_count": len(re.findall(r"migration end", text)),
        "abort_count": len(re.findall(r"abort req|AbortReq|migration\\.suspend", text)),
        "downtime": "未记录",
        "total_migration_time": "未记录",
        "enginecore_fatal_count": len(re.findall(r"EngineCore encountered a fatal error|EngineCore failed to start", text)),
        "traceback_count": len(re.findall(r"Traceback", text)),
        "error_count": len(re.findall(r"\bERROR\b", text)),
        "runtime_error_count": len(re.findall(r"RuntimeError", text)),
        "cuda_illegal_memory_access_count": len(re.findall(r"CUDA illegal memory access|illegal memory access", text)),
        "mooncake_transfer_success": 0,
    }


def first_fatal(stage_dir):
    markers = (
        "EngineCore encountered a fatal error",
        "EngineCore failed to start",
        "Traceback (most recent call last)",
        "RuntimeError:",
        "TypeError:",
        "AttributeError:",
        "ModuleNotFoundError:",
    )
    for path in sorted(stage_dir.glob("*.log")):
        lines = path.read_text(errors="replace").splitlines()
        for idx, line in enumerate(lines):
            if any(marker in line for marker in markers):
                return "\n".join([f"FILE {path}"] + lines[idx:idx + 80])
    return "NO_FATAL_FOUND"


def cleanup_ports():
    os.system("pkill -f 'redis-server.*6379' || true")
    os.system("pkill -f './bin/gateway' || true")
    os.system("pkill -f './bin/scheduler' || true")
    os.system("pkill -f 'vllm serve' || true")
    os.system("pkill -f 'discovery.discovery' || true")
    os.system("pkill -f 'benchmark_serving.py' || true")
    os.system("fuser -k -9 6379/tcp 8100/tcp 8101/tcp 8102/tcp 8103/tcp 18088/tcp 18089/tcp 31100/tcp 31101/tcp 31102/tcp 31103/tcp 36218/tcp 36226/tcp 36234/tcp 36242/tcp >/dev/null 2>&1 || true")
    time.sleep(5)


def start_stack(group):
    enable_migration = group == "hybrid_migration"
    processes = []
    processes.append(start_process("Redis", get_redis_command(), "redis.log"))
    processes.append(start_process(
        "Scheduler",
        get_scheduler_command(
            policy="flood",
            enable_full_mode_scheduling=True,
            enable_migration=enable_migration,
            enable_pd=False,
        ),
        "scheduler.log",
    ))
    processes.append(start_process(
        "Gateway",
        get_gateway_command(
            policy="flood",
            enable_full_mode_scheduling=True,
            enable_pd=False,
            connector_type="HybridConnector",
        ),
        "gateway.log",
    ))
    for i in range(INSTANCES):
        port = VLLM_BASE_PORT + i
        processes.append(start_process(
            f"vLLM-{i}",
            get_vllm_command(
                "neutral",
                port,
                i,
                True,
                f"{group}_neutral_{i}",
                "HybridConnector",
            ),
            f"vllm_{i}.log",
        ))
        processes.append(start_process(
            f"Runtime-{i}",
            get_discovery_command("neutral", port),
            f"runtime_{i}.log",
        ))
    return processes


def run_one(workload_name, group, repeat):
    cleanup_ports()
    shutil.rmtree(NAMING_DIR, ignore_errors=True)
    shutil.rmtree(LOG_DIR, ignore_errors=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    stage_dir = ROOT / "hybrid_fair" / workload_name / group / f"run{repeat}"
    summary_path = stage_dir / "summary.json"
    processes = []
    try:
        workload = load_workload(workload_name)
        if group == "hybrid_migration":
            for item in workload:
                item["kv_transfer_params"] = {"ali_llumnix_disagg": False}
        processes = start_stack(group)
        for i in range(INSTANCES):
            port = VLLM_BASE_PORT + i
            if not wait_for_service(f"http://localhost:{port}/health", timeout=360):
                raise TimeoutError(f"Service startup timeout for port {port}")
        time.sleep(20)
        start = time.time()
        with concurrent.futures.ThreadPoolExecutor(max_workers=WORKERS) as executor:
            futures = [executor.submit(post_completion, item) for item in workload]
            results = [f.result() for f in concurrent.futures.as_completed(futures)]
        duration = time.time() - start
        copy_logs(stage_dir)
        latencies = [r["latency"] for r in results]
        ok = [r for r in results if r["status"] == 200 and r["text"]]
        max_tokens_sum = sum(item["max_tokens"] for item in workload if any(r["id"] == item["id"] and r["status"] == 200 and r["text"] for r in results))
        pat = count_patterns(stage_dir)
        summary = {
            "workload": workload_name,
            "group": group,
            "connector_protocol": "HybridConnector / vllm-kvt",
            "migration_enabled": group == "hybrid_migration",
            "run": repeat,
            "instances": INSTANCES,
            "requests": len(workload),
            "workers": WORKERS,
            "max_tokens_mode": "per-workload-file",
            "successful_responses": len(ok),
            "failed_responses": len(workload) - len(ok),
            "success_rate": len(ok) / len(workload) if workload else 0,
            "http_200_nonempty": len(ok),
            "duration_s": duration,
            "qps": len(ok) / duration if duration > 0 else 0,
            "throughput_tok_s": max_tokens_sum / duration if duration > 0 else 0,
            "median_e2e_latency": statistics.median(latencies) if latencies else None,
            "p95_e2e_latency": percentile(latencies, 95),
            "p99_e2e_latency": percentile(latencies, 99),
            "mean_e2e_latency": statistics.mean(latencies) if latencies else None,
            "total_input_tokens": "未记录",
            "total_output_tokens": "未记录",
            **pat,
            "first_fatal": first_fatal(stage_dir),
            "sample_results": results[:5],
            "summary_path": str(summary_path),
        }
        summary_path.write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")
        print(f"FAIR_RUN_SUMMARY {json.dumps(summary, ensure_ascii=False)}", flush=True)
        return summary
    except Exception as exc:
        copy_logs(stage_dir)
        summary = {
            "workload": workload_name,
            "group": group,
            "run": repeat,
            "failed": True,
            "exception": repr(exc),
            "traceback": traceback.format_exc(),
            "first_fatal": first_fatal(stage_dir),
        }
        stage_dir.mkdir(parents=True, exist_ok=True)
        summary_path.write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")
        print(f"FAIR_RUN_FAILED {json.dumps(summary, ensure_ascii=False)}", flush=True)
        return summary
    finally:
        copy_logs(stage_dir / "final_before_cleanup")
        cleanup_processes(processes)
        cleanup_ports()


def main():
    manifest = write_workloads()
    all_summaries = []
    for workload in manifest:
        for group in ["hybrid_no_migration", "hybrid_migration"]:
            for repeat in range(1, REPEATS + 1):
                all_summaries.append(run_one(workload["workload"], group, repeat))
    (ROOT / "hybrid_fair" / "all_summaries.json").parent.mkdir(parents=True, exist_ok=True)
    (ROOT / "hybrid_fair" / "all_summaries.json").write_text(
        json.dumps(all_summaries, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )


if __name__ == "__main__":
    main()
