#!/usr/bin/env python3
"""Figure 12 v2 runner-only instrumentation and forced-fragmentation experiment."""

from __future__ import annotations

import argparse
import concurrent.futures
import html
import json
import math
import os
import re
import statistics
import shutil
import sys
import threading
import time
import traceback
from datetime import datetime
from pathlib import Path
from urllib import request as urlrequest
from urllib.error import HTTPError, URLError

from transformers import AutoTokenizer

sys.path.insert(0, os.getcwd())
os.environ.setdefault(
    "PAPER_SUPPLEMENT_ROOT",
    "/data/home/zylu/llumnix-v018-main/results/5090_paper_supplement_20260821_170651",
)
import run_paper_supplement_experiments as paper

ORIGINAL_CONFIGURE_BASE = paper.configure_base
ORIGINAL_CLEANUP_PROCESSES = paper.base.cleanup_processes


ROOT = Path(os.environ["PAPER_SUPPLEMENT_ROOT"]).resolve()
V2_DIR = ROOT / "02_fig12_fragmentation_v2"
STEP_DIR = ROOT / "04_step_summaries"
FINAL_DIR = ROOT / "05_final"
MODEL_PATH = os.environ.get("MODEL_PATH", "/data/models/Qwen2.5-7B-Instruct")
SEED = 20260823
BLOCK_SIZE_TOKENS = 16
LONG_INPUT_THRESHOLD = 2048
KV_CACHE_MEMORY_BYTES = int(os.environ.get("FIG12_V2_KV_CACHE_BYTES", str(256 * 1024 * 1024)))
MAX_MODEL_LEN = 4608
POLICIES = ["no_migration", "load_sr", "tpot_tail", "composite_proposed"]
LABELS = {
    "no_migration": "不迁移",
    "load_sr": "Load-SR（KV 小优先）",
    "tpot_tail": "TPOT 尾延迟感知",
    "composite_proposed": "综合策略 Proposed",
}

ACTIVE_SAMPLER = None
ALL_RESULTS: list[dict] = []
METRICS_BEFORE = {}
METRICS_AFTER = {}
INJECTION_INFO = {}
ACTIVE_RUN_DIR: Path | None = None
REQUEST_TIMEOUT_SECONDS = int(os.environ.get("FIG12_V2_REQUEST_TIMEOUT_SECONDS", "600"))
GATEWAY_SSE_READ_TIMEOUT_SECONDS = 300

PRESSURE_CONFIGS = {
    "config_a": {
        "label": "A-light", "request_num": 200, "concurrency": 20,
        "occupancy_count": 120, "occupancy_max_tokens": 192,
        "long_prompt_len": 2048, "long_max_tokens": 32,
    },
    "config_b": {
        "label": "B-medium", "request_num": 300, "concurrency": 30,
        "occupancy_count": 180, "occupancy_max_tokens": 256,
        "long_prompt_len": 2048, "long_max_tokens": 48,
    },
    "config_c": {
        "label": "C-high", "request_num": 400, "concurrency": 40,
        "occupancy_count": 240, "occupancy_max_tokens": 256,
        "long_prompt_len": 3072, "long_max_tokens": 48,
    },
    "config_d": {
        "label": "D-original-pressure", "request_num": 500, "concurrency": 50,
        "occupancy_count": 300, "occupancy_max_tokens": 1024,
        "long_prompt_len": 4096, "long_max_tokens": 64,
    },
}


def percentile(values, pct):
    values = sorted(v for v in values if v is not None)
    if not values:
        return None
    pos = (len(values) - 1) * pct / 100
    lo, hi = int(pos), min(int(pos) + 1, len(values) - 1)
    return values[lo] + (values[hi] - values[lo]) * (pos - lo)


def metric_stats(values):
    return {
        "mean": statistics.mean(values) if values else None,
        "p95": percentile(values, 95),
        "p99": percentile(values, 99),
    }


def true_error_count(run_dir: Path):
    count = 0
    for pattern in ("redis.log", "scheduler.log", "runtime_*.log", "vllm_*.log"):
        for path in run_dir.glob(pattern):
            for line in path.read_text(errors="replace").splitlines():
                is_error = re.search(r"(?:^|\))\s*ERROR(?:\s|:)", line) or re.match(r"^ERROR:", line)
                if is_error and "ERROR:blade_kvt.hybrid_connector.kvtbackend:disagg kvt done." not in line:
                    count += 1
    return count


def scrape_metrics():
    result = {}
    wanted = {
        "vllm:request_queue_time_seconds",
        "vllm:request_prefill_time_seconds",
        "vllm:time_to_first_token_seconds",
    }
    for port in range(8100, 8104):
        try:
            text = urlrequest.urlopen(f"http://127.0.0.1:{port}/metrics", timeout=5).read().decode()
        except Exception:
            continue
        for line in text.splitlines():
            if not line or line.startswith("#"):
                continue
            match = re.match(r"([^\s{]+)(?:\{([^}]*)\})?\s+([+\-0-9.eE]+)$", line)
            if not match:
                continue
            name, labels, raw = match.groups()
            family = next((metric for metric in wanted if name.startswith(metric + "_")), None)
            if family is None:
                continue
            entry = result.setdefault(family, {"sum": 0.0, "count": 0.0, "buckets": {}})
            value = float(raw)
            if name.endswith("_sum"):
                entry["sum"] += value
            elif name.endswith("_count"):
                entry["count"] += value
            elif name.endswith("_bucket"):
                le_match = re.search(r'le="([^"]+)"', labels or "")
                if le_match:
                    le = float("inf") if le_match.group(1) == "+Inf" else float(le_match.group(1))
                    entry["buckets"][le] = entry["buckets"].get(le, 0.0) + value
    return result


def histogram_delta(before, after, family):
    b, a = before.get(family, {}), after.get(family, {})
    count = (a.get("count", 0.0) - b.get("count", 0.0))
    total = (a.get("sum", 0.0) - b.get("sum", 0.0))
    buckets = {}
    for le, value in a.get("buckets", {}).items():
        buckets[le] = value - b.get("buckets", {}).get(le, 0.0)
    def q(p):
        if count <= 0:
            return None
        target = count * p
        for le in sorted(buckets):
            if buckets[le] >= target:
                return None if math.isinf(le) else le
        return None
    return {"mean": total / count if count > 0 else None, "p95": q(0.95), "p99": q(0.99), "count": int(count)}


class V2StatusSampler:
    def __init__(self, output_path: Path, interval_s: float = 0.1):
        global ACTIVE_SAMPLER
        self.output_path = output_path
        self.interval_s = interval_s
        self.samples = []
        self.stop_event = threading.Event()
        self.fragmentation_ready = threading.Event()
        self.long_injected = threading.Event()
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.demand_blocks = math.ceil(4096 / BLOCK_SIZE_TOKENS)
        ACTIVE_SAMPLER = self

    def start(self):
        self.thread.start()

    def stop(self):
        self.stop_event.set()
        self.thread.join(timeout=5)
        self.output_path.write_text(json.dumps(self.samples, ensure_ascii=False, indent=2))
        return self.samples

    def _run(self):
        import redis
        from llumnix.cms.proto.cms_pb2 import InstanceStatus
        client = redis.Redis(host="127.0.0.1", port=6379, decode_responses=False)
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
                    free_tokens = max(0, total_tokens - used_tokens)
                    instances.append({
                        "instance_key": key.decode(errors="replace"),
                        "num_total_gpu_tokens": total_tokens,
                        "num_used_gpu_tokens": used_tokens,
                        "num_free_gpu_tokens": free_tokens,
                        "total_blocks": total_tokens // BLOCK_SIZE_TOKENS,
                        "used_blocks": math.ceil(used_tokens / BLOCK_SIZE_TOKENS),
                        "free_blocks": free_tokens // BLOCK_SIZE_TOKENS,
                        "num_running_reqs": int(fields.get("num_running_reqs", fields.get("num_running_requests", 0))),
                        "num_waiting_reqs": int(fields.get("num_waiting_reqs", fields.get("num_waiting_requests", 0))),
                    })
                valid = [row for row in instances if row["total_blocks"] > 0]
                free_blocks = [row["free_blocks"] for row in valid]
                total_free = sum(free_blocks)
                cluster_total = sum(row["total_blocks"] for row in valid)
                max_local = max(free_blocks, default=0)
                fragmented = len(valid) == 4 and total_free >= self.demand_blocks and max_local < self.demand_blocks
                if fragmented:
                    self.fragmentation_ready.set()
                injected = self.long_injected.is_set()
                demand_per_instance = [
                    self.demand_blocks if injected and row["num_waiting_reqs"] > 0 else 0
                    for row in valid
                ]
                blocked_hol = sum(
                    row["num_waiting_reqs"] for row in valid
                    if injected and row["free_blocks"] < self.demand_blocks
                )
                fragmentation_blocks = max(0, total_free - max_local) if fragmented else 0
                proportion = fragmentation_blocks / cluster_total if cluster_total else None
                sample = {
                    "timestamp_ms": time.time() * 1000,
                    "block_size_tokens": BLOCK_SIZE_TOKENS,
                    "metric_scope": "estimated demand mapping; exact exported free-token capacity converted to blocks",
                    "long_input_injected": injected,
                    "instances": instances,
                    "free_blocks_per_instance": free_blocks,
                    "total_free_blocks": total_free,
                    "cluster_total_blocks": cluster_total,
                    "head_of_line_request_demand_blocks_per_instance": demand_per_instance,
                    "blocked_HOL_request_count": blocked_hol,
                    "fragmentation_blocks": fragmentation_blocks,
                    "fragmentation_proportion": proportion,
                    "fragmentation_condition": fragmented,
                }
            except Exception as exc:
                sample = {"timestamp_ms": time.time() * 1000, "sampler_error": repr(exc)}
            self.samples.append(sample)
            self.stop_event.wait(self.interval_s)


def stream_completion(item):
    payload = {
        "model": MODEL_PATH,
        "prompt": item["prompt"],
        "max_tokens": item["max_tokens"],
        "temperature": 0.0,
        "stream": True,
        "ignore_eos": True,
        "kv_transfer_params": {"ali_llumnix_disagg": False},
    }
    if item["phase"] == "occupancy":
        request_index = int(item["id"].rsplit("_", 1)[-1])
        target_endpoint = f"http://127.0.0.1:{8100 + request_index % 4}/v1/completions"
    else:
        target_endpoint = "http://127.0.0.1:18089/v1/completions"
    req = urlrequest.Request(
        target_endpoint,
        data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json", "X-Request-Id": item["id"]}, method="POST",
    )
    start_ts = time.time()
    start_ns = time.perf_counter_ns()
    first_token_ns = None
    first_token_ts = None
    chunks, finish_reason, response_id, status = [], None, None, 0
    raw_error = ""
    try:
        with urlrequest.urlopen(req, timeout=item.get("timeout_seconds", REQUEST_TIMEOUT_SECONDS)) as response:
            status = response.status
            for raw_line in response:
                line = raw_line.decode(errors="replace").strip()
                if not line.startswith("data:"):
                    continue
                body = line[5:].strip()
                if body == "[DONE]":
                    break
                try:
                    data = json.loads(body)
                except Exception:
                    continue
                response_id = response_id or data.get("id")
                choice = (data.get("choices") or [{}])[0]
                text = choice.get("text") or ""
                if text:
                    if first_token_ns is None:
                        first_token_ns = time.perf_counter_ns()
                        first_token_ts = time.time()
                    chunks.append(text)
                finish_reason = choice.get("finish_reason") or finish_reason
    except HTTPError as exc:
        status, raw_error = exc.code, exc.read().decode(errors="replace")[:2000]
    except (URLError, TimeoutError, OSError) as exc:
        raw_error = repr(exc)
    end_ns = time.perf_counter_ns()
    finish_ts = time.time()
    text = "".join(chunks)
    timeout_error = bool(re.search(r"(?:read\s+timed?\s*out|timed?\s*out|timeout)", raw_error, re.I))
    if status == 200 and text and finish_reason != "abort":
        request_status = "success"
    elif timeout_error:
        request_status = "timeout"
    elif finish_reason == "abort":
        request_status = "abort"
    else:
        request_status = "failed"
    return {
        "id": item["id"], "request_id": item["id"], "response_id": response_id,
        "http_status": status, "status": status, "request_status": request_status,
        "latency_s": (end_ns - start_ns) / 1e9,
        "ttft_s": (first_token_ns - start_ns) / 1e9 if first_token_ns else None,
        "start_ts": start_ts, "first_token_ts": first_token_ts, "finish_ts": finish_ts,
        "text_nonempty": bool(text), "text": text[:2000], "finish_reason": finish_reason,
        "raw": raw_error, "phase": item["phase"], "prompt_len": item["prompt_len"],
        "expected_input_len": item["expected_input_len"], "max_tokens": item["max_tokens"],
        "configured_max_tokens": item["max_tokens"], "long_input": item["long_input"],
        "target_endpoint": target_endpoint,
        "timeout_seconds": item.get("timeout_seconds", REQUEST_TIMEOUT_SECONDS),
        "sse_read_timeout": timeout_error,
    }


def submit_forced(requests, concurrency, benchmark_start):
    global ALL_RESULTS, METRICS_BEFORE, METRICS_AFTER, INJECTION_INFO
    phase_a = [row for row in requests if row["phase"] == "occupancy"]
    phase_b = [row for row in requests if row["phase"] == "large_arrival"]
    a_workers = int(concurrency * 0.8)
    b_workers = concurrency - a_workers
    METRICS_BEFORE = scrape_metrics()
    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=a_workers) as ex_a, \
            concurrent.futures.ThreadPoolExecutor(max_workers=b_workers) as ex_b:
        futures = [ex_a.submit(stream_completion, row) for row in phase_a]
        reached = ACTIVE_SAMPLER.fragmentation_ready.wait(timeout=180)
        INJECTION_INFO = {
            "fragmentation_condition_reached_before_injection": reached,
            "injection_timestamp_ms": time.time() * 1000,
            "occupancy_workers": a_workers, "long_input_workers": b_workers,
        }
        ACTIVE_SAMPLER.long_injected.set()
        futures.extend(ex_b.submit(stream_completion, row) for row in phase_b)
        for future in concurrent.futures.as_completed(futures):
            results.append(future.result())
    METRICS_AFTER = scrape_metrics()
    ALL_RESULTS = results
    return results


def make_workload(config, namespace):
    tokenizer = AutoTokenizer.from_pretrained(MODEL_PATH, trust_remote_code=True)
    total = config["request_num"]
    occupancy_count = config["occupancy_count"]
    rows = []
    for index in range(total):
        if index < occupancy_count:
            target = 64 + (index * 17 % 65)
            prompt, actual = paper.token_exact_prompt(tokenizer, target, f"Figure12-v2 occupancy {index}")
            max_tokens, phase = config["occupancy_max_tokens"], "occupancy"
        else:
            target = config["long_prompt_len"]
            prompt, actual = paper.token_exact_prompt(tokenizer, target, f"Figure12-v2 long-input {index}")
            max_tokens, phase = config["long_max_tokens"], "large_arrival"
        rows.append({
            "id": f"fig12v2_{namespace}_{phase}_{index}",
            "prompt": prompt, "prompt_len": actual, "prompt_tokens": actual,
            "expected_input_len": target, "max_tokens": max_tokens, "phase": phase,
            "long_input": actual >= LONG_INPUT_THRESHOLD,
            "temperature": 0.0, "ignore_eos": True,
            "timeout_seconds": config.get("timeout_seconds", REQUEST_TIMEOUT_SECONDS),
            "kv_transfer_params": {"ali_llumnix_disagg": False},
        })
    return rows


def configure_v2(request_num, concurrency, run_root, workload_dir, event_path):
    paper.RUNS_DIR = run_root
    paper.WORKLOAD_DIR = workload_dir
    ORIGINAL_CONFIGURE_BASE(4, request_num, concurrency, run_root, workload_dir,
                            MAX_MODEL_LEN, MAX_MODEL_LEN, event_path)
    original = base_original = paper.base._paper_original_get_vllm_command
    def wrapped(*args, **kwargs):
        command = original(*args, **kwargs)
        command = re.sub(r"--max-model-len\s+\S+", f"--max-model-len {MAX_MODEL_LEN}", command, count=1)
        command = re.sub(r"--max-num-batched-tokens\s+\S+", f"--max-num-batched-tokens {MAX_MODEL_LEN}", command, count=1)
        command = re.sub(r"\s+--kv-cache-memory-bytes\s+\S+", "", command)
        command += f" --kv-cache-memory-bytes {KV_CACHE_MEMORY_BYTES}"
        return command
    paper.base.get_vllm_command = wrapped
    # run_custom normally configures the base a second time. The v2 wrapper has
    # already frozen all values, including the explicit KV-cache cap.
    paper.configure_base = lambda *args, **kwargs: None
    def v2_stop_reason(summary):
        checks = (
            "EngineCore_fatal_count", "Scheduler_fatal_count", "Traceback_count",
            "KVTResp_code_500_count",
            "CUDA_illegal_memory_access_count",
        )
        failures = [f"{key}={summary.get(key)}" for key in checks if (summary.get(key) or 0) > 0]
        return ", ".join(failures) if failures else None
    paper.base.immediate_stop_reason = v2_stop_reason

    def ordered_cleanup(processes):
        # Preserve runtime logs first, then stop serving processes before Redis.
        groups = (processes[3:], processes[1:3], processes[:1])
        for group in groups:
            if group:
                ORIGINAL_CLEANUP_PROCESSES(group)
        if ACTIVE_RUN_DIR is not None:
            cleanup_dir = ACTIVE_RUN_DIR / "cleanup_logs"
            cleanup_dir.mkdir(parents=True, exist_ok=True)
            for path in (Path.cwd() / "logs").glob("*.log"):
                shutil.copy2(path, cleanup_dir / path.name)

    paper.base.cleanup_processes = ordered_cleanup


def enrich_summary(summary, run_dir: Path, config: dict, policy: str):
    long_ttft = [row["ttft_s"] for row in ALL_RESULTS if row["long_input"] and row["ttft_s"] is not None]
    queue = histogram_delta(METRICS_BEFORE, METRICS_AFTER, "vllm:request_queue_time_seconds")
    prefill = histogram_delta(METRICS_BEFORE, METRICS_AFTER, "vllm:request_prefill_time_seconds")
    ttft_hist = histogram_delta(METRICS_BEFORE, METRICS_AFTER, "vllm:time_to_first_token_seconds")
    samples_path = run_dir / "instance_status_timeseries.json"
    samples = json.loads(samples_path.read_text()) if samples_path.exists() else []
    proportions = [s.get("fragmentation_proportion") for s in samples if s.get("fragmentation_proportion") is not None]
    nonzero = [value for value in proportions if value > 0]
    timeout_count = sum(row.get("sse_read_timeout", False) for row in ALL_RESULTS)
    cleanup_tracebacks = 0
    cleanup_dir = run_dir / "cleanup_logs"
    if cleanup_dir.exists():
        cleanup_tracebacks = sum(
            path.read_text(errors="replace").count("Traceback (most recent call last)")
            for path in cleanup_dir.glob("*.log")
        )
    successful = sum(
        row.get("request_status") == "success" and row.get("text_nonempty") for row in ALL_RESULTS
    )
    summary.update({
        "experiment_policy": policy,
        "request_num": config["request_num"],
        "concurrency": config["concurrency"],
        "timeout_seconds": config.get("timeout_seconds", REQUEST_TIMEOUT_SECONDS),
        "gateway_sse_read_timeout_seconds": GATEWAY_SSE_READ_TIMEOUT_SECONDS,
        "successful_responses": successful,
        "http_200_text_nonempty": successful,
        "failed_responses": config["request_num"] - successful,
        "sse_read_timeout_count": timeout_count,
        "cleanup_traceback_count": cleanup_tracebacks,
        "runtime_traceback_count": summary.get("Traceback_count", 0),
        "sample_results": ALL_RESULTS[:5],
        "long_input_request_count": sum(row["long_input"] for row in ALL_RESULTS),
        "long_input_TTFT_mean_s": statistics.mean(long_ttft) if long_ttft else None,
        "long_input_TTFT_P95_s": percentile(long_ttft, 95),
        "long_input_TTFT_P99_s": percentile(long_ttft, 99),
        "queue_delay_mean_s": queue["mean"], "queue_delay_P95_s": queue["p95"], "queue_delay_P99_s": queue["p99"],
        "prefill_latency_mean_s": prefill["mean"], "prefill_latency_P95_s": prefill["p95"], "prefill_latency_P99_s": prefill["p99"],
        "service_TTFT_histogram": ttft_hist,
        "fragmentation_proportion_mean": statistics.mean(proportions) if proportions else None,
        "fragmentation_proportion_P95": percentile(proportions, 95),
        "fragmentation_proportion_P99": percentile(proportions, 99),
        "fragmentation_nonzero_sample_count": len(nonzero),
        "blocked_HOL_request_count": max((s.get("blocked_HOL_request_count", 0) for s in samples), default=0),
        "blocked_HOL_sample_count": sum(s.get("blocked_HOL_request_count", 0) > 0 for s in samples),
        "injection": INJECTION_INFO,
        "actual_service_ERROR_count": true_error_count(run_dir),
        "metrics_scope": "long-input TTFT from runner SSE first non-empty token; queue/prefill from vLLM Prometheus histogram deltas; fragmentation demand mapping estimated",
    })
    if timeout_count:
        summary.update({
            "status": "invalid_timeout",
            "validity": "invalid_timeout",
            "usable_for_main_conclusion": False,
        })
    elif successful < config["request_num"]:
        summary.update({
            "validity": "invalid_incomplete",
            "usable_for_main_conclusion": False,
        })
    else:
        summary.update({"validity": "candidate_valid", "usable_for_main_conclusion": True})
    (run_dir / "request_metrics.json").write_text(json.dumps(ALL_RESULTS, ensure_ascii=False, indent=2))
    (run_dir / "run_summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2))
    (run_dir / "run.log").write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n")
    return summary


def run_config(policy, repeat_id, config: dict, run_group: str, experiment: str, case: str):
    global ALL_RESULTS, METRICS_BEFORE, METRICS_AFTER, INJECTION_INFO, ACTIVE_RUN_DIR
    ALL_RESULTS, METRICS_BEFORE, METRICS_AFTER, INJECTION_INFO = [], {}, {}, {}
    total, concurrency = config["request_num"], config["concurrency"]
    requests = make_workload(config, f"{run_group}_{case}")
    run_root = V2_DIR / run_group
    workload_dir = V2_DIR / "workloads" / run_group
    run_dir = run_root / experiment / case / policy / f"run_{repeat_id}"
    ACTIVE_RUN_DIR = run_dir
    event_path = run_dir / "migration_efficiency_events.jsonl"
    configure_v2(total, concurrency, run_root, workload_dir, event_path)
    paper.RedisStatusSampler = V2StatusSampler
    extra = {
        "figure12_v2": True, "kv_cache_memory_bytes": KV_CACHE_MEMORY_BYTES,
        "block_size_tokens": BLOCK_SIZE_TOKENS, "long_input_threshold": LONG_INPUT_THRESHOLD,
        "metric_design": "forced fragmentation: occupancy then long-input burst",
        "pressure_config": config,
    }
    summary = None
    try:
        summary = paper.run_custom(
            experiment, case, policy, repeat_id, requests,
            concurrency, MAX_MODEL_LEN, submit_forced, False, config["long_prompt_len"], extra,
        )
    except RuntimeError as exc:
        summary_path = run_dir / "run_summary.json"
        summary = json.loads(summary_path.read_text()) if summary_path.exists() else {
            "status": "failed", "first_fatal": repr(exc), "successful_responses": 0,
        }
    finally:
        ACTIVE_RUN_DIR = None
    return enrich_summary(summary, run_dir, config, policy)


def run_one(policy, repeat_id, smoke):
    config = dict(PRESSURE_CONFIGS["config_a"] if smoke else PRESSURE_CONFIGS["config_d"])
    config["concurrency"] = config["concurrency"]
    group = "smoke" if smoke else "formal"
    return run_config(policy, repeat_id, config, group, "forced_fragmentation", "two_phase")


def write_v1_audit():
    agg_path = FINAL_DIR / "fig12_fragmentation_aggregated.json"
    data = json.loads(agg_path.read_text())
    lines = [
        "# Figure 12 v1 有效性审计", "",
        f"- 生成时间：{datetime.now().astimezone().isoformat(timespec='seconds')}",
        "- 结论：`completed-but-not-valid`，不得作为论文主结论。", "",
        "## 三轮完成与请求结果", "",
        "| 策略 | 完成轮数 | 成功请求 | 非空文本 | load fail / abort | failed migration |",
        "|---|---:|---:|---:|---:|---:|",
    ]
    for key, row in data["policies"].items():
        lines.append(f"| {LABELS[key]} | {row['run_count']}/3 | {row['successful_responses_total']}/1500 | {row['http_200_text_nonempty_total']}/1500 | {row['abort_total']} | {row['failed_migration_total']} |")
    lines += [
        "", "## 碎片化比例为何全部为 0", "",
        "1. Redis 采样确实获得了每实例 `num_total_gpu_tokens` 与 `num_used_gpu_tokens`，并非 free capacity 未采集。",
        "2. parser 只在 `cluster total free >= demand` 且 `max local free < demand` 时计碎片化；没有缺失值默认填 0 的分支。",
        "3. v1 每实例约有 40640 个 KV token，而并发 50 的 occupancy 请求远不足以把每实例局部空闲压到 1536 token 以下。",
        "4. `submit_phased` 先向同一个固定大小 executor 提交全部 occupancy 请求，再提交 long-input；large-arrival 常常要等前序 futures 腾出 worker，两个阶段重叠不足。",
        "5. 因此 v1 更接近普通混合负载，没有稳定制造 HOL blocking；0 反映场景未触发，而不是证明策略消除了碎片化。",
        "", "## long-input TTFT 缺失原因", "",
        "- benchmark 使用 `stream=False`，只记录完整响应 E2E，无法得到首 token 时间。",
        "- workload 有 `prompt_tokens` 和 `phase=large_arrival`，所以并非完全无法区分长输入。",
        "- runner 结果未持久化逐请求 TTFT，也没有实现 request-level TTFT parser；不能用 large-arrival E2E 冒充 TTFT。",
        "", "## 可保留的数据", "",
        "- no_migration 的 1500/1500 功能稳定性结果。",
        "- Load-SR 的 13 次 load-fail abort 作为稳定性边界。",
        "- tpot_tail 与 composite_proposed 的吞吐/E2E趋势，只作为探索性结果。",
        "", "## 不可写入论文主结论", "",
        "- 四策略对碎片化的优劣排序。",
        "- long-input TTFT、queue delay、prefill 改善结论。",
        "- 将 Load-SR 的最低 E2E P99 表述为严格最优。",
    ]
    path = STEP_DIR / "fig12_v1_validity_audit.md"
    path.write_text("\n".join(lines) + "\n")
    print(path)


def write_timeout_overload_audit():
    failed_dir = V2_DIR / "formal/forced_fragmentation/two_phase/load_sr/run_1"
    summary_path = failed_dir / "run_summary.json"
    summary = json.loads(summary_path.read_text())
    backup = summary_path.with_name("run_summary.pre_overload_audit.json")
    if not backup.exists():
        shutil.copy2(summary_path, backup)
    summary.update({
        "execution_status": summary.get("status", "completed"),
        "status": "invalid_overload_run",
        "validity": "invalid_overload_run",
        "usable_for_main_conclusion": False,
        "timeout_seconds": GATEWAY_SSE_READ_TIMEOUT_SECONDS,
        "sse_read_timeout_count": 152,
        "invalid_reason": "forced fragmentation overload caused gateway SSE read timeouts",
    })
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2))
    lines = [
        "# Figure 12 v2 超时/过载审计", "",
        f"- 生成时间：{datetime.now().astimezone().isoformat(timespec='seconds')}",
        "- 审计轮次：`load_sr/run_1`", "- 有效性：`invalid_overload_run`，不计入正式结果。", "",
        "## 超时层级与时间线", "",
        "1. benchmark Python 客户端原始 read timeout 为 900 秒；真正先触发的是 gateway SSE reader 的固定 `ReadTimeout = 5 * time.Minute`，即 300 秒。",
        "2. 第一个失败请求在 gateway 内等待 `300011 ms` 后返回 HTTP 400，TTFT 为 `300003 ms`，input_tokens=2048，output_tokens=0。",
        "3. 第一批 gateway 长输入请求约在 21:21:43 进入；第一条 timeout 在 21:28:23.343，距 gateway 阶段开始约 400.24 秒。具体超时请求约在 21:23:23 进入，恰好等待 300 秒。",
        "4. 第一条 timeout 时，gateway 已接收 58 个长输入请求，完成 48 个，outstanding 10 个；occupancy 请求直接发往四个 vLLM API，不经过 gateway。",
        "5. 本轮最终 297/500 成功，gateway 共记录 152 条 SSE read timeout；剩余请求不能据此轮作为有效性能样本。", "",
        "## 迁移与服务状态", "",
        f"- triggered_migration_count：{summary.get('triggered_migration_count', 'NA')}",
        f"- successful_migration_count：{summary.get('successful_migration_count', 'NA')}",
        f"- failed_migration_count：{summary.get('failed_migration_count', 'NA')}",
        f"- KVTResp(code=500)：{summary.get('KVTResp_code_500_count', 0)}",
        f"- finish_reason_abort：{summary.get('finish_reason_abort_count', 0)}",
        f"- EngineCore fatal：{summary.get('EngineCore_fatal_count', 0)}",
        f"- Traceback（归档 runtime 日志）：{summary.get('Traceback_count', 0)}",
        f"- CUDA illegal memory access：{summary.get('CUDA_illegal_memory_access_count', 0)}",
        "- `_infly_kvt` AssertionError：0。",
        "- timeout 后仍有源端请求继续完成；gateway timeout 释放的是该流式转发请求，不能视为整个 EngineCore 已停止。", "",
        "## Redis 次生错误", "",
        "Redis `Connection refused` 出现在服务清理阶段，是 Redis/CMS 先停、状态采样线程仍读取 Redis 导致的次生 Traceback；它不是本轮第一故障。runner 已改为先停 API/vLLM/worker，再停 gateway/scheduler，最后停 Redis，并将这类日志单列为 `cleanup_traceback_count`。", "",
        "## 无效原因", "",
        "该轮存在大量 SSE timeout、仅 297/500 成功，并伴随 KVT500/abort 计数；它不满足 Figure 12 的完整请求、无超时、无 KVT500 验收要求，因此只保留为过载边界案例。",
        "原始日志未覆盖；原 summary 备份为 `run_summary.pre_overload_audit.json`。",
    ]
    path = STEP_DIR / "fig12_v2_timeout_overload_audit.md"
    path.write_text("\n".join(lines) + "\n")
    print(path)


def pressure_reasons(summary):
    reasons = []
    request_num = summary.get("request_num", 0)
    success = summary.get("successful_responses", 0)
    if request_num <= 0 or success / request_num < 0.95:
        reasons.append(f"success={success}/{request_num}")
    for key in ("sse_read_timeout_count", "EngineCore_fatal_count", "Scheduler_fatal_count",
                "runtime_traceback_count", "KVTResp_code_500_count",
                "CUDA_illegal_memory_access_count"):
        if (summary.get(key) or 0) > 0:
            reasons.append(f"{key}={summary.get(key)}")
    if (summary.get("fragmentation_nonzero_sample_count") or 0) == 0:
        reasons.append("fragmentation_nonzero_sample_count=0")
    if (summary.get("blocked_HOL_sample_count") or 0) == 0:
        reasons.append("blocked_HOL_sample_count=0")
    if summary.get("long_input_TTFT_P99_s") is None:
        reasons.append("long_input_TTFT unavailable")
    return reasons


def run_pressure_sweep():
    for config_name, base_config in PRESSURE_CONFIGS.items():
        config = dict(base_config)
        config["timeout_seconds"] = 600
        for policy in ("no_migration", "load_sr"):
            path = V2_DIR / "pressure_sweep/pressure_sweep" / config_name / policy / "run_1/run_summary.json"
            if path.exists():
                existing = json.loads(path.read_text())
                if not pressure_reasons(existing):
                    print(f"SKIP valid {config_name}/{policy}")
                    continue
            summary = run_config(policy, 1, config, "pressure_sweep", "pressure_sweep", config_name)
            summary["pressure_validation_reasons"] = pressure_reasons(summary)
            path.write_text(json.dumps(summary, ensure_ascii=False, indent=2))
            print(json.dumps({"config": config_name, "policy": policy,
                              "reasons": summary["pressure_validation_reasons"]}, ensure_ascii=False))


def write_pressure_report():
    rows = []
    for config_name, config in PRESSURE_CONFIGS.items():
        for policy in ("no_migration", "load_sr"):
            path = V2_DIR / "pressure_sweep/pressure_sweep" / config_name / policy / "run_1/run_summary.json"
            if not path.exists():
                rows.append({"config": config_name, "policy": policy, "missing": True})
                continue
            row = json.loads(path.read_text())
            row.update({"config": config_name, "policy": policy,
                        "pressure_validation_reasons": pressure_reasons(row)})
            rows.append(row)
    valid_configs = []
    for config_name in PRESSURE_CONFIGS:
        pair = [row for row in rows if row.get("config") == config_name]
        if len(pair) == 2 and all(not row.get("missing") and not row["pressure_validation_reasons"] for row in pair):
            valid_configs.append(config_name)
    recommendation = next((name for name in ("config_d", "config_c", "config_b", "config_a")
                           if name in valid_configs), None)
    lines = ["# Figure 12 v2 压力窗口扫描", "",
             f"- 生成时间：{datetime.now().astimezone().isoformat(timespec='seconds')}",
             "- 选择原则：能稳定制造非零碎片化和 HOL 阻塞、无 SSE timeout/运行时 fatal/KVT500，且成功率最高的最强配置。", "",
             "| 配置 | 策略 | 成功请求 | 碎片化非零采样 | HOL 最大值 | 长输入 TTFT P99(s) | SSE timeout | KVT500 | 是否有效 | 备注 |",
             "|---|---|---:|---:|---:|---:|---:|---:|---|---|"]
    for row in rows:
        if row.get("missing"):
            lines.append(f"| {row['config']} | {row['policy']} | Missing | - | - | - | - | - | 否 | 未运行 |")
            continue
        valid = not row["pressure_validation_reasons"]
        lines.append(
            f"| {row['config']} | {row['policy']} | {row.get('successful_responses', 0)}/{row.get('request_num', 0)} | "
            f"{row.get('fragmentation_nonzero_sample_count', 0)} | {row.get('blocked_HOL_request_count', 0)} | "
            f"{row.get('long_input_TTFT_P99_s', 'NA')} | {row.get('sse_read_timeout_count', 0)} | "
            f"{row.get('KVTResp_code_500_count', 0)} | {'是' if valid else '否'} | "
            f"{row.get('invalid_reason', '') or ('通过' if valid else '; '.join(row['pressure_validation_reasons']))} |"
        )
    lines += ["", f"## 推荐正式配置", "", f"- 推荐：`{recommendation or '尚无有效配置'}`"]
    if recommendation:
        lines.append(f"- 参数：`{json.dumps(PRESSURE_CONFIGS[recommendation], ensure_ascii=False)}`")
    path = STEP_DIR / "fig12_v2_pressure_sweep.md"
    path.write_text("\n".join(lines) + "\n")
    json_path = STEP_DIR / "fig12_v2_pressure_sweep.json"
    json_path.write_text(json.dumps({"rows": rows, "valid_configs": valid_configs,
                                     "recommended_config": recommendation}, ensure_ascii=False, indent=2))
    print(json.dumps({"report": str(path), "recommended_config": recommendation}, ensure_ascii=False))
    return recommendation


def strict_reason(summary, smoke=False):
    request_num = summary.get("request_num", 200 if smoke else 500)
    required = math.ceil(request_num * 0.95) if smoke else request_num
    reasons = []
    if summary.get("successful_responses", 0) < required:
        reasons.append(f"successful_responses={summary.get('successful_responses')}")
    if summary.get("http_200_text_nonempty", 0) < required:
        reasons.append(f"http_200_text_nonempty={summary.get('http_200_text_nonempty')}")
    for key in ("EngineCore_fatal_count", "Scheduler_fatal_count", "Traceback_count",
                "KVTResp_code_500_count", "CUDA_illegal_memory_access_count",
                "actual_service_ERROR_count"):
        if (summary.get(key) or 0) != 0:
            reasons.append(f"{key}={summary.get(key)}")
    if smoke:
        if (summary.get("fragmentation_nonzero_sample_count") or 0) == 0:
            reasons.append("fragmentation never became nonzero")
        if (summary.get("blocked_HOL_sample_count") or 0) == 0:
            reasons.append("blocked HOL never observed")
        if summary.get("long_input_TTFT_P99_s") is None:
            reasons.append("long-input TTFT unavailable")
    return reasons


def aggregate():
    sweep_path = STEP_DIR / "fig12_v2_pressure_sweep.json"
    recommended = None
    if sweep_path.exists():
        recommended = json.loads(sweep_path.read_text()).get("recommended_config")
    selected_base = (V2_DIR / "formal_selected/forced_fragmentation" / recommended
                     if recommended else V2_DIR / "formal/forced_fragmentation/two_phase")
    policies = {}
    all_reasons = []
    for policy in POLICIES:
        rows = []
        for repeat in range(1, 4):
            path = selected_base / policy / f"run_{repeat}/run_summary.json"
            if not path.exists():
                all_reasons.append(f"missing {path}")
                continue
            row = json.loads(path.read_text())
            reasons = strict_reason(row)
            row["strict_validation_reasons"] = reasons
            all_reasons.extend(f"{policy}/run_{repeat}: {reason}" for reason in reasons)
            rows.append(row)
        def avg(key):
            vals = [r.get(key) for r in rows if isinstance(r.get(key), (int, float))]
            return statistics.mean(vals) if vals else None
        def total(key): return sum((r.get(key) or 0) for r in rows)
        policies[policy] = {
            "label": LABELS[policy], "run_count": len(rows),
            "request_num_per_run": rows[0].get("request_num") if rows else None,
            "successful_responses_total": total("successful_responses"),
            "QPS_mean": avg("QPS"), "throughput_tok_s_mean": avg("output_throughput_tok_s"),
            "E2E_P99_mean_s": avg("P99_e2e_latency_s"),
            "long_input_TTFT_P99_mean_s": avg("long_input_TTFT_P99_s"),
            "prefill_P99_mean_s": avg("prefill_latency_P99_s"),
            "queue_delay_P99_mean_s": avg("queue_delay_P99_s"),
            "fragmentation_proportion_mean": avg("fragmentation_proportion_mean"),
            "fragmentation_proportion_P99_mean": avg("fragmentation_proportion_P99"),
            "blocked_HOL_sample_count_total": total("blocked_HOL_sample_count"),
            "migration_count_total": total("triggered_migration_count"),
            "successful_migration_total": total("successful_migration_count"),
            "failed_migration_total": total("failed_migration_count"),
            "load_fail_abort_total": total("abort_count"),
            "KVTResp_code_0_total": total("KVTResp_code_0_count"),
            "KVTResp_code_404_total": total("KVTResp_code_404_count"),
            "KVTResp_code_500_total": total("KVTResp_code_500_count"),
            "actual_service_ERROR_total": total("actual_service_ERROR_count"),
            "stability_limited": all((r.get("abort_count") or 0) > 0 for r in rows) if rows else None,
            "runs": rows,
        }
    eligible = [p for p in POLICIES if len(policies[p]["runs"]) == 3]
    def best(key):
        vals = [(policies[p].get(key), p) for p in eligible if policies[p].get(key) is not None]
        return min(vals)[1] if vals else None
    result = {
        "experiment": "Figure 12 fragmentation v2", "generated_at": datetime.now().isoformat(),
        "recommended_pressure_config": recommended,
        "all_runs_completed": all(policies[p]["run_count"] == 3 for p in POLICIES),
        "strict_validation_passed": not all_reasons and all(policies[p]["run_count"] == 3 for p in POLICIES),
        "validation_reasons": all_reasons,
        "best": {"fragmentation": best("fragmentation_proportion_mean"),
                 "long_input_TTFT_P99": best("long_input_TTFT_P99_mean_s"),
                 "prefill_P99": best("prefill_P99_mean_s"), "E2E_P99": best("E2E_P99_mean_s"),
                 "fewest_failed_migrations": best("failed_migration_total")},
        "policies": policies,
    }
    FINAL_DIR.mkdir(parents=True, exist_ok=True)
    STEP_DIR.mkdir(parents=True, exist_ok=True)
    json_path = FINAL_DIR / "fig12_fragmentation_v2_aggregated.json"
    json_path.write_text(json.dumps(result, ensure_ascii=False, indent=2))
    def f(v): return "NA" if v is None else (f"{v:.4f}" if isinstance(v, float) else str(v))
    md = ["# Figure 12 v2 碎片化实验汇总", "",
          f"- 12 轮完成：{result['all_runs_completed']}", f"- 严格验收：{result['strict_validation_passed']}", "",
          "| 策略 | 成功请求 | QPS | E2E P99 | 长输入 TTFT P99 | Prefill P99 | 碎片化比例 | 成功/失败迁移 | load fail |",
          "|---|---:|---:|---:|---:|---:|---:|---:|---:|"]
    for p in POLICIES:
        a = policies[p]
        md.append(f"| {a['label']} | {a['successful_responses_total']}/1500 | {f(a['QPS_mean'])} | {f(a['E2E_P99_mean_s'])} | {f(a['long_input_TTFT_P99_mean_s'])} | {f(a['prefill_P99_mean_s'])} | {f(a['fragmentation_proportion_mean'])} | {a['successful_migration_total']}/{a['failed_migration_total']} | {a['load_fail_abort_total']} |")
    md += ["", "## 回答", "", f"1. no_migration 是否出现碎片化：{policies['no_migration']['fragmentation_proportion_mean'] not in (None, 0)}",
           f"2. 碎片化最低：{LABELS.get(result['best']['fragmentation'], 'NA')}",
           f"3. long-input TTFT P99 最低：{LABELS.get(result['best']['long_input_TTFT_P99'], 'NA')}",
           f"4. prefill P99 最低：{LABELS.get(result['best']['prefill_P99'], 'NA')}",
           f"5. E2E P99 最低：{LABELS.get(result['best']['E2E_P99'], 'NA')}",
           f"6. 迁移失败最少：{LABELS.get(result['best']['fewest_failed_migrations'], 'NA')}",
           f"7. Load-SR 是否仍有 load fail：{policies['load_sr']['load_fail_abort_total'] > 0}",
           f"8. 可用于论文主结论：{result['strict_validation_passed']}",
           f"9. 可以启动 Figure 13：{result['strict_validation_passed']}", "", "## 验收问题", "",
           *(f"- {x}" for x in (all_reasons or ["无"]))]
    md_path = STEP_DIR / "fig12_fragmentation_v2_summary.md"
    md_path.write_text("\n".join(md) + "\n")
    rows_html = "".join(
        f"<tr><td>{html.escape(policies[p]['label'])}</td><td>{policies[p]['successful_responses_total']}/1500</td><td>{f(policies[p]['QPS_mean'])}</td><td>{f(policies[p]['E2E_P99_mean_s'])}</td><td>{f(policies[p]['long_input_TTFT_P99_mean_s'])}</td><td>{f(policies[p]['prefill_P99_mean_s'])}</td><td>{f(policies[p]['fragmentation_proportion_mean'])}</td><td>{policies[p]['successful_migration_total']}/{policies[p]['failed_migration_total']}</td><td>{policies[p]['load_fail_abort_total']}</td></tr>"
        for p in POLICIES)
    doc = f"""<!doctype html><html lang='zh-CN'><meta charset='utf-8'><title>Figure 12 v2</title><style>body{{font-family:Arial,'Microsoft YaHei';margin:24px;background:#f7f9fc;color:#182230}}h1{{font-size:24px}}.box{{background:white;border:1px solid #d8e0ea;padding:16px;border-radius:6px}}table{{width:100%;border-collapse:collapse;table-layout:fixed;font-size:13px}}th,td{{border:1px solid #d8e0ea;padding:8px;text-align:center}}th{{background:#e9eff6}}td:first-child{{font-weight:700;text-align:left}}</style><body><h1>Llumnix RTX 5090 Figure 12 v2 碎片化实验</h1><div class='box'><p>12 轮完成：{result['all_runs_completed']}；严格验收：{result['strict_validation_passed']}</p><table><tr><th>策略</th><th>成功请求</th><th>QPS</th><th>E2E P99</th><th>长输入 TTFT P99</th><th>Prefill P99</th><th>碎片化比例</th><th>成功/失败迁移</th><th>load fail</th></tr>{rows_html}</table></div></body></html>"""
    html_path = FINAL_DIR / "fig12_fragmentation_v2_table.html"
    html_path.write_text(doc)
    print(json.dumps({"summary": str(md_path), "html": str(html_path), "json": str(json_path), "result": result["best"], "valid": result["strict_validation_passed"]}, ensure_ascii=False, indent=2))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--phase", choices=["audit", "timeout-audit", "smoke", "formal",
                                             "pressure-sweep", "pressure-report",
                                             "formal-selected", "aggregate"], required=True)
    args = parser.parse_args()
    paper.configure_policies()
    if args.phase == "audit":
        write_v1_audit()
    elif args.phase == "timeout-audit":
        write_timeout_overload_audit()
    elif args.phase == "smoke":
        summary = run_one("no_migration", 1, True)
        reasons = strict_reason(summary, smoke=True)
        print(json.dumps({"summary": summary, "validation_reasons": reasons}, ensure_ascii=False, indent=2))
        if reasons:
            raise SystemExit(2)
    elif args.phase == "formal":
        for policy in POLICIES:
            for repeat in range(1, 4):
                path = V2_DIR / "formal/forced_fragmentation/two_phase" / policy / f"run_{repeat}/run_summary.json"
                if path.exists():
                    existing = json.loads(path.read_text())
                    if existing.get("status") == "completed" and not strict_reason(existing):
                        print(f"SKIP valid {policy}/run_{repeat}")
                        continue
                summary = run_one(policy, repeat, False)
                reasons = strict_reason(summary)
                print(json.dumps({"policy": policy, "repeat": repeat, "reasons": reasons}, ensure_ascii=False))
                if reasons:
                    raise SystemExit(3)
    elif args.phase == "pressure-sweep":
        run_pressure_sweep()
        write_pressure_report()
    elif args.phase == "pressure-report":
        write_pressure_report()
    elif args.phase == "formal-selected":
        recommendation = write_pressure_report()
        if not recommendation:
            raise SystemExit("No valid pressure configuration; formal experiment not started")
        config = dict(PRESSURE_CONFIGS[recommendation])
        config["timeout_seconds"] = 600
        for policy in POLICIES:
            for repeat in range(1, 4):
                path = V2_DIR / "formal_selected/forced_fragmentation" / recommendation / policy / f"run_{repeat}/run_summary.json"
                if path.exists():
                    existing = json.loads(path.read_text())
                    if existing.get("usable_for_main_conclusion") and not strict_reason(existing):
                        print(f"SKIP valid {policy}/run_{repeat}")
                        continue
                summary = run_config(policy, repeat, config, "formal_selected",
                                     "forced_fragmentation", recommendation)
                reasons = strict_reason(summary)
                summary["strict_validation_reasons"] = reasons
                path.write_text(json.dumps(summary, ensure_ascii=False, indent=2))
                print(json.dumps({"policy": policy, "repeat": repeat, "reasons": reasons}, ensure_ascii=False))
                if reasons:
                    raise SystemExit(4)
    else:
        aggregate()


if __name__ == "__main__":
    main()
