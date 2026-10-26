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
        headers={"Content-Type": "application/json"}, method="POST",
    )
    start_ns = time.perf_counter_ns()
    first_token_ns = None
    chunks, finish_reason, response_id, status = [], None, None, 0
    raw_error = ""
    try:
        with urlrequest.urlopen(req, timeout=900) as response:
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
                    chunks.append(text)
                finish_reason = choice.get("finish_reason") or finish_reason
    except HTTPError as exc:
        status, raw_error = exc.code, exc.read().decode(errors="replace")[:2000]
    except (URLError, TimeoutError, OSError) as exc:
        raw_error = repr(exc)
    end_ns = time.perf_counter_ns()
    text = "".join(chunks)
    return {
        "id": item["id"], "response_id": response_id, "status": status,
        "latency_s": (end_ns - start_ns) / 1e9,
        "ttft_s": (first_token_ns - start_ns) / 1e9 if first_token_ns else None,
        "text_nonempty": bool(text), "text": text[:2000], "finish_reason": finish_reason,
        "raw": raw_error, "phase": item["phase"], "prompt_len": item["prompt_len"],
        "expected_input_len": item["expected_input_len"], "max_tokens": item["max_tokens"],
        "configured_max_tokens": item["max_tokens"], "long_input": item["long_input"],
        "target_endpoint": target_endpoint,
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


def make_workload(total, formal=False):
    tokenizer = AutoTokenizer.from_pretrained(MODEL_PATH, trust_remote_code=True)
    occupancy_count = 300 if formal else 120
    rows = []
    for index in range(total):
        if index < occupancy_count:
            target = 64 + (index * 17 % 65)
            prompt, actual = paper.token_exact_prompt(tokenizer, target, f"Figure12-v2 occupancy {index}")
            max_tokens, phase = 1024, "occupancy"
        else:
            target = 4096 if index % 2 else 2048
            prompt, actual = paper.token_exact_prompt(tokenizer, target, f"Figure12-v2 long-input {index}")
            max_tokens, phase = 64, "large_arrival"
        rows.append({
            "id": f"fig12v2_{'formal' if formal else 'smoke'}_{phase}_{index}",
            "prompt": prompt, "prompt_len": actual, "prompt_tokens": actual,
            "expected_input_len": target, "max_tokens": max_tokens, "phase": phase,
            "long_input": actual >= LONG_INPUT_THRESHOLD,
            "temperature": 0.0, "ignore_eos": True,
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


def run_one(policy, repeat_id, smoke):
    global ALL_RESULTS, METRICS_BEFORE, METRICS_AFTER, INJECTION_INFO
    ALL_RESULTS, METRICS_BEFORE, METRICS_AFTER, INJECTION_INFO = [], {}, {}, {}
    total, concurrency = (200, 40) if smoke else (500, 50)
    requests = make_workload(total, formal=not smoke)
    run_group = "smoke" if smoke else "formal"
    run_root = V2_DIR / run_group
    workload_dir = V2_DIR / "workloads" / run_group
    event_path = run_root / policy / f"run_{repeat_id}" / "migration_efficiency_events.jsonl"
    configure_v2(total, concurrency, run_root, workload_dir, event_path)
    paper.RedisStatusSampler = V2StatusSampler
    summary = paper.run_custom(
        "forced_fragmentation", "two_phase", policy, repeat_id, requests,
        concurrency, MAX_MODEL_LEN, submit_forced, False, 4096,
        {"figure12_v2": True, "kv_cache_memory_bytes": KV_CACHE_MEMORY_BYTES,
         "block_size_tokens": BLOCK_SIZE_TOKENS, "long_input_threshold": LONG_INPUT_THRESHOLD,
         "metric_design": "forced fragmentation: occupancy then long-input burst"},
    )
    run_dir = run_root / "forced_fragmentation" / "two_phase" / policy / f"run_{repeat_id}"
    long_ttft = [row["ttft_s"] for row in ALL_RESULTS if row["long_input"] and row["ttft_s"] is not None]
    queue = histogram_delta(METRICS_BEFORE, METRICS_AFTER, "vllm:request_queue_time_seconds")
    prefill = histogram_delta(METRICS_BEFORE, METRICS_AFTER, "vllm:request_prefill_time_seconds")
    ttft_hist = histogram_delta(METRICS_BEFORE, METRICS_AFTER, "vllm:time_to_first_token_seconds")
    samples_path = run_dir / "instance_status_timeseries.json"
    samples = json.loads(samples_path.read_text()) if samples_path.exists() else []
    proportions = [s.get("fragmentation_proportion") for s in samples if s.get("fragmentation_proportion") is not None]
    nonzero = [v for v in proportions if v > 0]
    summary.update({
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
    (run_dir / "request_metrics.json").write_text(json.dumps(ALL_RESULTS, ensure_ascii=False, indent=2))
    (run_dir / "run_summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2))
    (run_dir / "run.log").write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n")
    return summary


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


def strict_reason(summary, smoke=False):
    required = 190 if smoke else 500
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
    policies = {}
    all_reasons = []
    for policy in POLICIES:
        rows = []
        for repeat in range(1, 4):
            path = V2_DIR / "formal/forced_fragmentation/two_phase" / policy / f"run_{repeat}/run_summary.json"
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
    parser.add_argument("--phase", choices=["audit", "smoke", "formal", "aggregate"], required=True)
    args = parser.parse_args()
    paper.configure_policies()
    if args.phase == "audit":
        write_v1_audit()
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
    else:
        aggregate()


if __name__ == "__main__":
    main()
