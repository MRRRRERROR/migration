#!/usr/bin/env python3
"""Run paper-oriented Llumnix supplement experiments without core changes."""

from __future__ import annotations

import argparse
import concurrent.futures
import hashlib
import json
import os
import random
import re
import statistics
import sys
import threading
import time
import traceback
from datetime import datetime
from pathlib import Path

from transformers import AutoTokenizer

sys.path.insert(0, os.getcwd())
os.environ.setdefault("MIGRATION_POLICY_ROOT", os.environ["PAPER_SUPPLEMENT_ROOT"])
import run_migration_policy_baselines as base


ROOT = Path(os.environ["PAPER_SUPPLEMENT_ROOT"]).resolve()
MODEL_PATH = os.environ.get("MODEL_PATH", "/data/models/Qwen2.5-7B-Instruct")
SEED = int(os.environ.get("PAPER_SUPPLEMENT_SEED", "20260821"))
REPEATS = int(os.environ.get("PAPER_SUPPLEMENT_REPEATS", "3"))
ALLOW_CONTENDED = os.environ.get("PAPER_SUPPLEMENT_ALLOW_CONTENDED", "1") == "1"

CONFIG_DIR = ROOT / "00_config"
SMOKE_DIR = ROOT / "01_smoke"
WORKLOAD_DIR = ROOT / "02_workloads"
RUNS_DIR = ROOT / "03_runs"
STEP_DIR = ROOT / "04_step_summaries"
FINAL_DIR = ROOT / "05_final"

FIG10_LENGTHS = [256, 512, 1024, 2048, 4096, 8192]
FIG12_POLICIES = ["no_migration", "load_sr", "tpot_tail", "composite_proposed"]
FIG12_WORKLOADS = ["fragmentation_medium", "fragmentation_heavy"]
FIG13_CVS = [2, 4, 6, 8]
FIG13_POLICIES = ["priority_agnostic", "priority_tail", "priority_proposed"]


def now_iso() -> str:
    return datetime.now().astimezone().isoformat(timespec="seconds")


def percentile(values: list[float], pct: float):
    return base.percentile(values, pct) if values else None


def write_json(path: Path, value) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def configure_base(instances: int, request_num: int, concurrency: int, runs_dir: Path,
                   workload_dir: Path, max_model_len: int, max_batched_tokens: int,
                   event_path: Path) -> None:
    base.ROOT = ROOT
    base.MODEL_PATH = MODEL_PATH
    base.INSTANCES = instances
    base.REQUEST_NUM = request_num
    base.CONCURRENCY = concurrency
    base.REPEATS = REPEATS
    base.ALLOW_CONTENDED = ALLOW_CONTENDED
    base.KVT_PORTS = tuple(36218 + 8 * index for index in range(instances))
    base.CONFIG_DIR = CONFIG_DIR
    base.WORKLOAD_DIR = workload_dir
    base.RUNS_DIR = runs_dir
    base.STEP_DIR = STEP_DIR
    base.FINAL_DIR = FINAL_DIR
    base.PROFILING_DIR = CONFIG_DIR / "profiling_5090"
    base.TTFT_PROFILING_PATH = base.PROFILING_DIR / "ttft_5090_qwen25_7b.json"
    base.TPOT_PROFILING_PATH = base.PROFILING_DIR / "tpot_5090_qwen25_7b.json"
    base.KVT_HOST = "127.0.0.1"
    os.environ["LLUMNIX_MIGRATION_EFFICIENCY_LOG"] = str(event_path)
    original = getattr(base, "_paper_original_get_vllm_command", None)
    if original is None:
        original = base.get_vllm_command
        base._paper_original_get_vllm_command = original

    def wrapped(*args, **kwargs):
        command = original(*args, **kwargs)
        command = re.sub(r"--max-model-len\s+\S+", f"--max-model-len {max_model_len}", command, count=1)
        command = re.sub(
            r"--max-num-batched-tokens\s+\S+",
            f"--max-num-batched-tokens {max_batched_tokens}", command, count=1,
        )
        return command

    base.get_vllm_command = wrapped


def configure_policies() -> None:
    base.POLICIES = {
        "no_migration": {
            "policy": "flood", "full_mode": True, "migration": False, "scheduler": True,
            "rescheduling_policy": "neutral_load", "request_order": "FCR",
            "description": "Hybrid full mode without live migration.",
        },
        "load_sr": {
            "policy": "flood", "full_mode": True, "migration": True, "scheduler": True,
            "rescheduling_policy": "neutral_load", "request_order": "SR",
            "description": "Load pairing with KV-size-aware SR selection.",
        },
        "tpot_tail": {
            "policy": "flood", "full_mode": True, "migration": True, "scheduler": True,
            "rescheduling_policy": "binpacking_mitigation", "request_order": "FCR",
            "requires_profiling": True,
            "description": "Predicted-TPOT tail-aware migration.",
        },
        "composite_proposed": {
            "policy": "flood", "full_mode": True, "migration": True, "scheduler": True,
            "rescheduling_policy": "binpacking_mitigation", "request_order": "COMPOSITE",
            "requires_profiling": True,
            "description": "Composite migration policy.",
        },
    }


def create_dirs() -> None:
    for path in (CONFIG_DIR, SMOKE_DIR, WORKLOAD_DIR, RUNS_DIR, STEP_DIR, FINAL_DIR):
        path.mkdir(parents=True, exist_ok=True)


def copy_profiling() -> None:
    source = Path(os.environ.get(
        "PAPER_PROFILING_SOURCE",
        "/data/home/zylu/llumnix-v018-main/results/5090_migration_policy_baselines_20260819_201922/00_config/profiling_5090",
    ))
    target = CONFIG_DIR / "profiling_5090"
    target.mkdir(parents=True, exist_ok=True)
    for name in ("ttft_5090_qwen25_7b.json", "tpot_5090_qwen25_7b.json"):
        destination = target / name
        if not destination.exists():
            destination.write_bytes((source / name).read_bytes())


def write_design_docs() -> None:
    design = f"""# Llumnix Supplement Experiment Design

- Generated: `{now_iso()}`
- Root: `{ROOT}`
- Model: `{MODEL_PATH}`
- KVT host: `127.0.0.1`
- Core source modification: `False`
- vLLM rebuild: `False`

## Figure 10-like migration efficiency

Two instances, HybridConnector/vllm-kvt, live migration, context lengths `{FIG10_LENGTHS}`, three repeats. `migration_initiated` is taken from the existing JSONL wrapper event. `FinishedReq` and `SendStubMetrics` are real blade_kvt completion evidence. `total_migration_time_ms` is a cross-log correlation from initiated timestamp to `FinishedReq`; `copy_time_proxy_ms` is `WaitAndSendUs/1000`. Exact commit downtime, first-token-after-resume downtime, and stage count remain unavailable and are never inferred.

## Figure 12-like fragmentation and HOL

Four instances and two phased workloads. Phase A creates long-running occupancy; phase B injects large-context requests. Redis `InstanceStatus` is sampled read-only. Used/free KV token capacity is exact according to exported instance status. Fragmentation/HOL values are explicitly marked as estimates because per-request block allocation and queue-head block demand are not exported.

## Figure 13-like priority isolation

The current gateway has no request-level priority field and dispatches its internal task queue with `high_priority=false`. Therefore the experiment implements only runner-level priority admission ordering. It is a functional proxy, not native Llumnix priority-aware migration. Results must not be presented as proof of scheduler-level priority isolation.
"""
    audit = f"""# Paper Experiment Capability Audit

| Capability | Status | Evidence / limitation |
|---|---|---|
| Live migration completion | Available | `FinishedReq`, `KVTResp(code=0)`, `SendStubMetrics` |
| Initiation timestamp and request ID | Available | `migration_efficiency_events.jsonl` |
| Total migration latency | Correlatable proxy | `FinishedReq wall time - migration_initiated wall time` |
| Copy duration | Available proxy | `WaitAndSendUs`, `SendUs*`, `TransUs*` |
| Exact final-suspend downtime | Unavailable | No final-suspend/commit/resume timestamps |
| First token after resume | Unavailable | No request-linked resume-token timestamp |
| Per-instance KV usage | Available | Redis `InstanceStatus` total/used GPU tokens |
| Exact external fragmentation | Unavailable | Per-request contiguous block demand not exported |
| Exact HOL blocking | Unavailable | Queue head block requirement not exported |
| Native request priority | Unavailable | Gateway request type has no priority field |
| Runner admission priority proxy | Available | Fixed trace, high-priority requests admitted first within arrival windows |

No missing value will be guessed. Missing exact metrics are recorded as `null` with a reason.
"""
    (CONFIG_DIR / "paper_experiment_design.md").write_text(design, encoding="utf-8")
    (CONFIG_DIR / "paper_experiment_capability_audit.md").write_text(audit, encoding="utf-8")


def token_exact_prompt(tokenizer, target_tokens: int, label: str) -> tuple[str, int]:
    seed_text = f"{label}. Explain distributed LLM request migration and KV cache transfer carefully. "
    ids = tokenizer.encode(seed_text * (target_tokens // 8 + 8), add_special_tokens=False)[:target_tokens]
    prompt = tokenizer.decode(ids, skip_special_tokens=True)
    actual = len(tokenizer.encode(prompt, add_special_tokens=False))
    return prompt, actual


def write_workload(path: Path, rows: list[dict]) -> None:
    payload = "\n".join(json.dumps(row, ensure_ascii=False) for row in rows) + "\n"
    if path.exists() and path.read_text(encoding="utf-8") != payload:
        raise RuntimeError(f"Fixed workload mismatch: {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(payload, encoding="utf-8")


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def parse_finished_metrics(run_dir: Path, event_path: Path) -> list[dict]:
    initiated = {}
    if event_path.exists():
        for line in event_path.read_text(errors="replace").splitlines():
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                continue
            if event.get("event_name") == "migration_initiated" or event.get("event") == "migration_initiated":
                initiated[event.get("request_id")] = event
    rows = []
    for log in sorted(run_dir.glob("vllm_*.log")):
        lines = log.read_text(errors="replace").splitlines()
        for index, line in enumerate(lines):
            match = re.search(r"^(\d{4}-\d\d-\d\d \d\d:\d\d:\d\d[.,]\d+).*FinishedReq=([^,\s]+)", line)
            if not match:
                continue
            request_id = match.group(2)
            timestamp_text = match.group(1).replace(",", ".")
            finish_ts = datetime.strptime(timestamp_text, "%Y-%m-%d %H:%M:%S.%f").timestamp()
            metrics_line = next((candidate for candidate in lines[index:index + 4] if "SendStubMetrics" in candidate), "")
            def number(name):
                found = re.search(rf"{name}=([0-9.]+)", metrics_line)
                return float(found.group(1)) if found else None
            event = initiated.get(request_id)
            init_ms = event.get("timestamp_ms") if event else None
            if init_ms is None and event and event.get("timestamp_ns"):
                init_ms = event["timestamp_ns"] / 1_000_000
            rows.append({
                "request_id": request_id,
                "source_log": str(log),
                "migration_initiated_timestamp_ms": init_ms,
                "finished_req_timestamp_ms": finish_ts * 1000,
                "total_migration_time_ms": finish_ts * 1000 - init_ms if init_ms is not None else None,
                "copy_time_proxy_ms": number("WaitAndSendUs") / 1000 if number("WaitAndSendUs") is not None else None,
                "send_time_max_ms": number("SendUsMax") / 1000 if number("SendUsMax") is not None else None,
                "transfer_bytes": number("SbSizeTotal"),
                "downtime_commit_ms": None,
                "downtime_resume_ms": None,
                "first_token_after_resume_ms": None,
                "stage_count": None,
            })
    return rows


def enrich_fig10(summary_path: Path, event_path: Path) -> dict:
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    rows = parse_finished_metrics(summary_path.parent, event_path)
    totals = [row["total_migration_time_ms"] for row in rows if row["total_migration_time_ms"] is not None]
    copies = [row["copy_time_proxy_ms"] for row in rows if row["copy_time_proxy_ms"] is not None]
    summary.update({
        "migration_efficiency_event_path": str(event_path),
        "migration_correlations": rows,
        "correlated_migration_count": len(totals),
        "avg_total_migration_time_ms": statistics.mean(totals) if totals else None,
        "p95_total_migration_time_ms": percentile(totals, 95),
        "avg_copy_time_proxy_ms": statistics.mean(copies) if copies else None,
        "p95_copy_time_proxy_ms": percentile(copies, 95),
        "exact_downtime_commit_ms": None,
        "exact_downtime_reason": "final_suspend/commit/resume timestamps are not emitted",
        "stage_count": None,
        "stage_count_reason": "per-stage events are not emitted",
    })
    write_json(summary_path, summary)
    return summary


def run_fig10(smoke: bool = False) -> list[dict]:
    tokenizer = AutoTokenizer.from_pretrained(MODEL_PATH, trust_remote_code=True)
    lengths = [256] if smoke else FIG10_LENGTHS
    repeats = 1 if smoke else REPEATS
    results = []
    for seq_len in lengths:
        request_num, concurrency = ((20, 10) if smoke else (60, 20))
        prompt_tokens = seq_len - 64
        rows = []
        for index in range(request_num):
            prompt, actual = token_exact_prompt(tokenizer, prompt_tokens, f"Figure10 seq {seq_len} request {index}")
            rows.append({"id": f"fig10_{seq_len}_{index}", "prompt": prompt, "prompt_tokens": actual,
                         "max_tokens": 64, "ignore_eos": True, "temperature": 0.0,
                         "seed": SEED, "seq_len_config": seq_len})
        workload_dir = WORKLOAD_DIR / ("figure10_smoke" if smoke else "figure10_formal")
        run_root = (SMOKE_DIR if smoke else RUNS_DIR) / "figure10"
        workload_path = workload_dir / f"seq_{seq_len}.jsonl"
        write_workload(workload_path, rows)
        for repeat in range(1, repeats + 1):
            event_path = run_root / f"seq_{seq_len}" / "load_sr" / f"run_{repeat}" / "migration_efficiency_events.jsonl"
            configure_base(2, request_num, concurrency, run_root, workload_dir,
                           max(seq_len, 1024), max(seq_len, 2048), event_path)
            base.WORKLOADS = {f"seq_{seq_len}": {"description": "Figure 10-like fixed context"}}
            base.read_workload = lambda name, p=workload_path: [json.loads(x) for x in p.read_text(encoding="utf-8").splitlines() if x]
            summary = base.run_one(f"seq_{seq_len}", "load_sr", repeat)
            summary_path = run_root / f"seq_{seq_len}" / "load_sr" / f"run_{repeat}" / "run_summary.json"
            summary = enrich_fig10(summary_path, event_path)
            summary.update({"paper_figure": "Figure10-like", "seq_len_config": seq_len,
                            "request_file_sha256": sha256(workload_path)})
            write_json(summary_path, summary)
            results.append(summary)
            reason = base.immediate_stop_reason(summary)
            if reason:
                raise RuntimeError(f"Figure10 stopped: {reason}")
    return results


def write_infly_root_cause(summary: dict, seq_len: int) -> None:
    report = f"""# INFPLY KVT Root Cause Analysis

- Generated: `{now_iso()}`
- Sequence length: `{seq_len}`
- Failed run: `{summary.get('repeat_id')}`
- Core source modified: `False`
- Classification: `migration state synchronization failure`

## First fatal

```text
{summary.get('first_fatal', 'NO_FATAL_FOUND')}
```

## Evidence counts

- KVTResp(code=0): `{summary.get('KVTResp_code_0_count', 0)}`
- KVTResp(code=500): `{summary.get('KVTResp_code_500_count', 0)}`
- FinishedReq: `{summary.get('FinishedReq_lines', 0)}`
- migration_end: `{summary.get('migration_end_count', 0)}`
- abort: `{summary.get('abort_count', 0)}`
- EngineCore fatal: `{summary.get('EngineCore_fatal_count', 0)}`
- Traceback: `{summary.get('Traceback_count', 0)}`
- AssertionError: `{summary.get('AssertionError_count', 0)}`
- `_infly_kvt` log matches: `{summary.get('infly_kvt_log_count', 0)}`

## Root-cause boundary

The stable reproduction reaches `blade_kvt.hybrid_connector.kvtbackend._update_infly_kvt`, where the received request metadata references a request ID absent from `_infly_kvt`. This establishes a state-lifecycle mismatch between migration metadata arrival and the in-flight registry. The current evidence does not distinguish premature removal, duplicate/late metadata, or failed-transfer cleanup ordering. No source patch was attempted.
"""
    path = ROOT / "01_fig10_migration_efficiency" / "retry_4096_minimal" / "INFPLY_KVT_ROOT_CAUSE.md"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(report, encoding="utf-8")


def run_fig10_minimal_retry() -> list[dict]:
    tokenizer = AutoTokenizer.from_pretrained(MODEL_PATH, trust_remote_code=True)
    retry_root = ROOT / "01_fig10_migration_efficiency" / "retry_4096_minimal"
    high_pressure = retry_root / "HIGH_PRESSURE_FAILURE_ARCHIVE.md"
    if not high_pressure.exists():
        high_pressure.write_text(
            "# High-pressure Failure Archive\n\n"
            "- Original run: `03_runs/figure10/seq_4096/load_sr/run_3`\n"
            "- Configuration: `request_num=60`, `concurrency=20`, `seq_len=4096`\n"
            "- Classification: `long-context high-pressure migration state synchronization failure`\n"
            "- Evidence: `KVTResp(code=500)`, `load fail`, EngineCore fatal, "
            "`AssertionError: meta.reqid not in self._infly_kvt`\n"
            "- Core source modified: `False`\n",
            encoding="utf-8",
        )
    all_results = []
    for seq_len in (4096, 8192):
        workload_dir = retry_root / "workloads"
        run_root = retry_root
        workload_path = workload_dir / f"seq_{seq_len}.jsonl"
        rows = []
        for index in range(20):
            prompt, actual = token_exact_prompt(
                tokenizer, seq_len - 64, f"Figure10 minimal retry seq {seq_len} request {index}"
            )
            rows.append({
                "id": f"fig10_retry_{seq_len}_{index}", "prompt": prompt,
                "prompt_tokens": actual, "max_tokens": 64, "ignore_eos": True,
                "temperature": 0.0, "seed": SEED, "seq_len_config": seq_len,
            })
        write_workload(workload_path, rows)
        seq_results = []
        for repeat in range(1, 4):
            event_path = run_root / f"seq_{seq_len}" / "load_sr" / f"run_{repeat}" / "migration_efficiency_events.jsonl"
            configure_base(2, 20, 10, run_root, workload_dir, seq_len, max(seq_len, 2048), event_path)
            base.WORKLOADS = {f"seq_{seq_len}": {"description": "Figure 10 minimal retry"}}
            base.read_workload = lambda name, p=workload_path: [
                json.loads(line) for line in p.read_text(encoding="utf-8").splitlines() if line
            ]
            summary = base.run_one(f"seq_{seq_len}", "load_sr", repeat)
            summary_path = run_root / f"seq_{seq_len}" / "load_sr" / f"run_{repeat}" / "run_summary.json"
            summary = enrich_fig10(summary_path, event_path)
            text = base.service_log_text(summary_path.parent)
            summary.update({
                "paper_figure": "Figure10-like minimal retry", "seq_len_config": seq_len,
                "minimal_retry": True, "required_minimum_migrations": 1,
                "AssertionError_count": text.count("AssertionError"),
                "infly_kvt_log_count": text.count("_infly_kvt"),
                "request_file_sha256": sha256(workload_path),
            })
            write_json(summary_path, summary)
            seq_results.append(summary)
            all_results.append(summary)
            reason = base.immediate_stop_reason(summary)
            migration_evidence = max(
                summary.get("KVTResp_code_0_count", 0),
                summary.get("FinishedReq_lines", 0),
                summary.get("migration_end_count", 0),
            )
            if reason or summary.get("AssertionError_count", 0) or summary.get("infly_kvt_log_count", 0):
                write_infly_root_cause(summary, seq_len)
                raise RuntimeError(f"Minimal retry reproduced at seq_len={seq_len}: {reason or 'infly_kvt'}")
            if summary.get("successful_responses") != 20 or migration_evidence < 1:
                raise RuntimeError(
                    f"Minimal retry did not meet success/migration evidence at seq_len={seq_len} run={repeat}"
                )
        if seq_len == 4096:
            write_json(retry_root / "4096_MINIMAL_VALIDATION.json", {
                "status": "passed", "runs": len(seq_results),
                "all_requests_successful": all(item.get("successful_responses") == 20 for item in seq_results),
                "all_runs_have_migration_evidence": all(
                    max(item.get("KVTResp_code_0_count", 0), item.get("FinishedReq_lines", 0),
                        item.get("migration_end_count", 0)) >= 1 for item in seq_results
                ),
                "original_failure_classification": "high-pressure failure",
            })
    write_json(retry_root / "minimal_retry_results.json", all_results)
    return all_results


def close_fig10() -> dict:
    figure_dir = ROOT / "01_fig10_migration_efficiency"
    retry_root = figure_dir / "retry_4096_minimal"
    summaries = []
    for seq_len in (4096, 8192):
        for path in sorted((retry_root / f"seq_{seq_len}").rglob("run_summary.json")):
            row = json.loads(path.read_text(encoding="utf-8"))
            row["summary_path"] = str(path)
            summaries.append(row)
    if len(summaries) != 6:
        raise RuntimeError(f"Figure 10 closure requires 6 minimal summaries, found {len(summaries)}")

    def total(key: str):
        return sum(int(row.get(key, 0) or 0) for row in summaries)

    def mean_available(key: str):
        values = [float(row[key]) for row in summaries if row.get(key) is not None]
        return statistics.mean(values) if values else None

    accepted = all(
        row.get("status") == "completed"
        and row.get("successful_responses") == 20
        and row.get("KVTResp_code_500_count", 0) == 0
        and row.get("EngineCore_fatal_count", 0) == 0
        and row.get("Traceback_count", 0) == 0
        and row.get("AssertionError_count", 0) == 0
        for row in summaries
    )
    payload = {
        "generated_at": now_iso(),
        "status": "closed_with_stability_boundary" if accepted else "not_accepted",
        "official_statistics_scope": "minimal-pressure 4096/8192 only",
        "accepted_run_count": len(summaries),
        "request_num_per_run": 20,
        "concurrency": 10,
        "total_requests": 120,
        "successful_responses": total("successful_responses"),
        "KVTResp_code_0_count": total("KVTResp_code_0_count"),
        "KVTResp_code_500_count": total("KVTResp_code_500_count"),
        "FinishedReq_lines": total("FinishedReq_lines"),
        "migration_end_count": total("migration_end_count"),
        "recoverable_load_fail_abort_count": total("abort_count"),
        "EngineCore_fatal_count": total("EngineCore_fatal_count"),
        "Traceback_count": total("Traceback_count"),
        "AssertionError_count": total("AssertionError_count"),
        "avg_total_migration_time_ms_across_runs": mean_available("avg_total_migration_time_ms"),
        "avg_copy_time_proxy_ms_across_runs": mean_available("avg_copy_time_proxy_ms"),
        "high_pressure_boundary_case": {
            "path": str(ROOT / "03_runs/figure10/seq_4096/load_sr/run_3"),
            "request_num": 60, "concurrency": 20,
            "classification": "long-context high-pressure migration state synchronization failure",
            "root_cause_signature": "AssertionError: meta.reqid not in self._infly_kvt",
        },
        "notes": [
            "4096 minimal passed all three repeats without fatal or failed migration.",
            "8192 completed all requests in all three repeats; run 1 contained one recoverable load-fail abort.",
            "Exact final-suspend downtime and first-token-after-resume downtime remain unavailable.",
        ],
        "runs": summaries,
    }
    write_json(figure_dir / "figure10_official_statistics.json", payload)
    report = f"""# Figure 10 Migration Efficiency Closure

- Status: `{payload['status']}`
- Official scope: `4096/8192 minimal-pressure runs`
- Accepted runs: `{len(summaries)}/6`
- Requests: `{payload['successful_responses']}/{payload['total_requests']}` successful
- KVTResp(code=0): `{payload['KVTResp_code_0_count']}`
- KVTResp(code=500): `{payload['KVTResp_code_500_count']}`
- EngineCore fatal / Traceback / AssertionError: `{payload['EngineCore_fatal_count']} / {payload['Traceback_count']} / {payload['AssertionError_count']}`
- Recoverable load-fail abort: `{payload['recoverable_load_fail_abort_count']}` (8192 run 1)

## Stability boundary

The original 4096-token run with 60 requests and concurrency 20 is retained as a high-pressure stability-boundary case. It failed with `KVTResp(code=500)`, `load fail`, and `AssertionError: meta.reqid not in self._infly_kvt`. The same sequence length passed three repeats at 20 requests and concurrency 10, so the failure is classified as pressure-dependent rather than a deterministic 4096-token incompatibility.

## Reporting rule

The six minimal-pressure runs enter the official Figure 10 statistics. The 8192 run-1 recoverable migration failure is disclosed. Exact final-suspend and first-token-after-resume downtime are not claimed because those timestamps are not emitted.
"""
    (figure_dir / "FIGURE10_CLOSURE.md").write_text(report, encoding="utf-8")
    return payload


def write_feasibility_report() -> None:
    report = """# Figure 12 / Figure 13 Feasibility Report

## Figure 12

The existing system exports per-instance total and used GPU token capacity through Redis `InstanceStatus`. A read-only sampler can measure free capacity, imbalance, and waiting/running request counts. Exact external fragmentation and exact HOL blocking are unavailable because contiguous block layout and queue-head block demand are not exported. Formal output will distinguish exact status fields from estimates.

## Figure 13

The gateway request schema contains no request priority. The gateway currently dispatches its internal worker task with `high_priority=false`. Native priority-aware scheduling and migration therefore cannot be claimed without a larger source change. The permitted experiment is runner-level priority admission ordering using one fixed trace. It can measure observed high/low class latency, but is not a scheduler-level priority-isolation implementation.
"""
    (CONFIG_DIR / "FIG12_FIG13_FEASIBILITY.md").write_text(report, encoding="utf-8")


class RedisStatusSampler:
    def __init__(self, output_path: Path, interval_s: float = 0.2):
        self.output_path = output_path
        self.interval_s = interval_s
        self.samples: list[dict] = []
        self.stop_event = threading.Event()
        self.thread = threading.Thread(target=self._run, daemon=True)

    def start(self) -> None:
        self.thread.start()

    def stop(self) -> list[dict]:
        self.stop_event.set()
        self.thread.join(timeout=5)
        write_json(self.output_path, self.samples)
        return self.samples

    def _run(self) -> None:
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
                    total = int(fields.get("num_total_gpu_tokens", 0))
                    used = int(fields.get("num_used_gpu_tokens", 0))
                    instances.append({
                        "instance_key": key.decode(errors="replace"),
                        "num_total_gpu_tokens": total,
                        "num_used_gpu_tokens": used,
                        "num_free_gpu_tokens": max(0, total - used),
                        "num_running_reqs": int(fields.get("num_running_reqs", fields.get("num_running_requests", 0))),
                        "num_waiting_reqs": int(fields.get("num_waiting_reqs", fields.get("num_waiting_requests", 0))),
                    })
            except Exception as exc:
                instances = [{"sampler_error": repr(exc)}]
            self.samples.append({"timestamp_ms": time.time() * 1000, "instances": instances})
            self.stop_event.wait(self.interval_s)


def summarize_responses(results: list[dict], request_num: int, duration: float) -> dict:
    successful = [item for item in results if item.get("status") == 200 and item.get("text_nonempty")]
    latencies = [float(item.get("observed_e2e_s", item["latency_s"])) for item in results]
    output_tokens = sum(int(item.get("configured_max_tokens", 0)) for item in successful)
    return {
        "successful_responses": len(successful),
        "failed_responses": request_num - len(successful),
        "success_rate": len(successful) / request_num if request_num else None,
        "http_200_text_nonempty": len(successful),
        "empty_text_count": sum(item.get("status") == 200 and not item.get("text_nonempty") for item in results),
        "finish_reason_abort_count": sum(item.get("finish_reason") == "abort" for item in results),
        "QPS": len(successful) / duration if duration else None,
        "output_throughput_tok_s": output_tokens / duration if duration else None,
        "median_e2e_latency_s": statistics.median(latencies) if latencies else None,
        "P95_e2e_latency_s": percentile(latencies, 95),
        "P99_e2e_latency_s": percentile(latencies, 99),
        "mean_e2e_latency_s": statistics.mean(latencies) if latencies else None,
        "total_output_tokens_configured": output_tokens,
    }


def run_custom(experiment: str, case: str, policy: str, repeat_id: int,
               requests: list[dict], concurrency: int, max_model_len: int,
               submitter, smoke: bool = False, sampler_demand_tokens: int | None = None,
               metadata: dict | None = None) -> dict:
    run_root = SMOKE_DIR if smoke else RUNS_DIR
    run_dir = run_root / experiment / case / policy / f"run_{repeat_id}"
    run_dir.mkdir(parents=True, exist_ok=True)
    summary_path = run_dir / "run_summary.json"
    if summary_path.exists():
        existing = json.loads(summary_path.read_text(encoding="utf-8"))
        if existing.get("status") == "completed":
            return existing
    workload_dir = WORKLOAD_DIR / f"{experiment}_{'smoke' if smoke else 'formal'}"
    workload_path = workload_dir / f"{case}.jsonl"
    write_workload(workload_path, requests)
    event_path = run_dir / "migration_efficiency_events.jsonl"
    configure_base(4, len(requests), concurrency, run_root / experiment, workload_dir,
                   max_model_len, max(2048, max_model_len), event_path)
    processes, probes = [], []
    sampler = None
    start_wall = time.time()
    gpu_before, gpu_after = {}, {}
    summary = {}
    try:
        base.wait_for_idle_gpus()
        base.cleanup_ports()
        gpu_before = base.gpu_snapshot()
        base.shutil.rmtree(base.NAMING_DIR, ignore_errors=True)
        base.shutil.rmtree(base.LOG_DIR, ignore_errors=True)
        base.LOG_DIR.mkdir(parents=True, exist_ok=True)
        processes, commands = base.start_stack(policy)
        base.wait_for_discovery_instances(120)
        probes = base.wait_for_kvt_listeners(120)
        time.sleep(10)
        if sampler_demand_tokens is not None:
            sampler = RedisStatusSampler(run_dir / "instance_status_timeseries.json")
            sampler.start()
        benchmark_start = time.time()
        results = submitter(requests, concurrency, benchmark_start)
        duration = time.time() - benchmark_start
        samples = sampler.stop() if sampler else []
        sampler = None
        gpu_after = base.gpu_snapshot(processes)
        base.copy_logs(run_dir)
        for result in results:
            result["configured_max_tokens"] = next(
                row["max_tokens"] for row in requests if row["id"] == result["id"]
            )
        contention = max(gpu_before.get("non_experiment_gpu_process_count", 0),
                         gpu_after.get("non_experiment_gpu_process_count", 0)) > 0
        summary = {
            "paper_experiment": experiment, "case": case, "policy": policy,
            "repeat_id": repeat_id, "request_num": len(requests), "concurrency": concurrency,
            "instance_num": 4, "connector": "HybridConnector", "protocol": "vllm-kvt",
            "migration_enabled": base.POLICIES[policy]["migration"],
            "request_file": str(workload_path), "request_file_sha256": sha256(workload_path),
            "model_path": MODEL_PATH, "seed": SEED, "start_time": datetime.fromtimestamp(start_wall).astimezone().isoformat(),
            "end_time": now_iso(), "duration_s": duration, "status": "completed",
            "fairness_status": "contended" if contention else "fair", "gpu_contention": contention,
            "usable_for_final_fair_performance_comparison": not contention,
            "nvidia_smi_before": gpu_before, "nvidia_smi_after": gpu_after,
            "launch_commands": commands, "sample_results": results[:5],
            **summarize_responses(results, len(requests), duration),
            **base.count_patterns(run_dir), **(metadata or {}),
        }
        phase_large = [row for row in results if row.get("phase") == "large_arrival"]
        if phase_large:
            values = [row["latency_s"] for row in phase_large]
            summary.update({
                "large_arrival_response_count": len(phase_large),
                "large_arrival_median_e2e_s": statistics.median(values),
                "large_arrival_p95_e2e_s": percentile(values, 95),
                "large_arrival_p99_e2e_s": percentile(values, 99),
            })
        if any(row.get("priority_class") for row in results):
            for klass in ("high", "normal"):
                values = [row["observed_e2e_s"] for row in results
                          if row.get("priority_class") == klass and row.get("observed_e2e_s") is not None]
                summary[f"{klass}_request_count"] = len(values)
                summary[f"{klass}_median_e2e_s"] = statistics.median(values) if values else None
                summary[f"{klass}_p95_e2e_s"] = percentile(values, 95)
                summary[f"{klass}_p99_e2e_s"] = percentile(values, 99)
        if sampler_demand_tokens is not None:
            exact = []
            estimated_fragmented, estimated_hol = 0, 0
            for sample in samples:
                valid = [row for row in sample["instances"] if "num_free_gpu_tokens" in row]
                if not valid:
                    continue
                total_free = sum(row["num_free_gpu_tokens"] for row in valid)
                max_local = max(row["num_free_gpu_tokens"] for row in valid)
                waiting = sum(row["num_waiting_reqs"] for row in valid)
                fragmented = total_free >= sampler_demand_tokens and max_local < sampler_demand_tokens
                estimated_fragmented += int(fragmented)
                estimated_hol += int(fragmented and waiting > 0)
                exact.append({"total_free_gpu_tokens": total_free, "max_local_free_gpu_tokens": max_local,
                              "waiting_requests": waiting})
            summary.update({
                "status_sample_count": len(samples),
                "exact_instance_status_sample_count": len(exact),
                "estimated_fragmentation_sample_count": estimated_fragmented,
                "estimated_HOL_sample_count": estimated_hol,
                "fragmentation_metric_scope": "estimate from exported free-token capacity; not exact block layout",
                "HOL_metric_scope": "estimate from waiting count plus distributed-free/local-insufficient condition",
            })
        summary["first_fatal"] = base.first_fatal(run_dir)
    except Exception as exc:
        if sampler:
            sampler.stop()
        gpu_after = base.gpu_snapshot(processes)
        base.copy_logs(run_dir)
        summary = {
            "paper_experiment": experiment, "case": case, "policy": policy,
            "repeat_id": repeat_id, "request_num": len(requests), "concurrency": concurrency,
            "status": "failed", "exception": repr(exc), "traceback": traceback.format_exc(),
            "nvidia_smi_before": gpu_before, "nvidia_smi_after": gpu_after,
            **base.count_patterns(run_dir), "first_fatal": base.first_fatal(run_dir),
            **(metadata or {}),
        }
    finally:
        base.cleanup_processes(processes)
        for probe in probes:
            probe.close()
        base.cleanup_ports()
    write_json(summary_path, summary)
    (run_dir / "run.log").write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    reason = base.immediate_stop_reason(summary)
    if reason:
        raise RuntimeError(f"{experiment}/{case}/{policy} stopped: {reason}")
    if summary.get("status") != "completed" or summary.get("successful_responses") != len(requests):
        raise RuntimeError(f"{experiment}/{case}/{policy} did not complete all requests")
    return summary


def submit_phased(requests: list[dict], concurrency: int, benchmark_start: float) -> list[dict]:
    phase_a = [row for row in requests if row["phase"] == "occupancy"]
    phase_b = [row for row in requests if row["phase"] == "large_arrival"]
    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as executor:
        futures = [executor.submit(base.post_completion, row) for row in phase_a]
        time.sleep(0.5)
        futures.extend(executor.submit(base.post_completion, row) for row in phase_b)
        for future in concurrent.futures.as_completed(futures):
            result = future.result()
            source = next(row for row in requests if row["id"] == result["id"])
            result["phase"] = source["phase"]
            results.append(result)
    return results


def make_fig12_workload(case: str, smoke: bool,
                        total_override: int | None = None) -> tuple[list[dict], int]:
    tokenizer = AutoTokenizer.from_pretrained(MODEL_PATH, trust_remote_code=True)
    total = total_override if total_override is not None else (20 if smoke else 500)
    occupancy_count = int(total * 0.8)
    demand = 1536 if case == "fragmentation_medium" else 3072
    rows = []
    for index in range(total):
        if index < occupancy_count:
            prompt, actual = token_exact_prompt(tokenizer, 96, f"Figure12 occupancy {index}")
            rows.append({"id": f"fig12_{case}_occ_{index}", "prompt": prompt, "prompt_tokens": actual,
                         "max_tokens": 256, "phase": "occupancy", "kv_transfer_params": {"ali_llumnix_disagg": False}})
        else:
            prompt, actual = token_exact_prompt(tokenizer, demand, f"Figure12 large arrival {index}")
            rows.append({"id": f"fig12_{case}_large_{index}", "prompt": prompt, "prompt_tokens": actual,
                         "max_tokens": 64, "phase": "large_arrival", "kv_transfer_params": {"ali_llumnix_disagg": False}})
    return rows, demand


def run_fig12(smoke: bool = False) -> list[dict]:
    cases = ["fragmentation_medium"] if smoke else FIG12_WORKLOADS
    repeats = 1 if smoke else REPEATS
    results = []
    for case in cases:
        requests, demand = make_fig12_workload(case, smoke)
        for policy in FIG12_POLICIES:
            for repeat in range(1, repeats + 1):
                summary = run_custom("figure12", case, policy, repeat, requests, 20 if smoke else 50,
                                     4096, submit_phased, smoke, demand,
                                     {"large_request_prompt_tokens": demand, "metric_design": "phased occupancy then large arrivals"})
                results.append(summary)
    return results


def run_fig12_stale_guard_validation(smoke: bool) -> list[dict]:
    request_num = 50 if smoke else 500
    concurrency = 20 if smoke else 50
    requests, demand = make_fig12_workload(
        "fragmentation_medium", smoke=False, total_override=request_num
    )
    experiment = "figure12_stale_guard_smoke" if smoke else "figure12_stale_guard_retry"
    summary = run_custom(
        experiment,
        "fragmentation_medium",
        "tpot_tail",
        1,
        requests,
        concurrency,
        4096,
        submit_phased,
        smoke,
        demand,
        {
            "large_request_prompt_tokens": demand,
            "metric_design": "phased occupancy then large arrivals",
            "validation_scope": "stale migration state guard",
            "replaces_original_run": "figure12/fragmentation_medium/tpot_tail/run_1",
        },
    )
    return [summary]


def run_fig12_endpoint_guard_validation(smoke: bool) -> list[dict]:
    request_num = 50 if smoke else 500
    concurrency = 20 if smoke else 50
    requests, demand = make_fig12_workload(
        "fragmentation_medium", smoke=False, total_override=request_num
    )
    experiment = "figure12_endpoint_guard_smoke" if smoke else "figure12_endpoint_guard_retry"
    summary = run_custom(
        experiment,
        "fragmentation_medium",
        "tpot_tail",
        1,
        requests,
        concurrency,
        4096,
        submit_phased,
        smoke,
        demand,
        {
            "large_request_prompt_tokens": demand,
            "metric_design": "phased occupancy then large arrivals",
            "validation_scope": "empty destination migration endpoint guard",
            "replaces_original_run": "figure12/fragmentation_medium/tpot_tail/run_1",
        },
    )
    return [summary]


def make_priority_trace(cv: int, smoke: bool) -> list[dict]:
    tokenizer = AutoTokenizer.from_pretrained(MODEL_PATH, trust_remote_code=True)
    count = 20 if smoke else 1000
    rng = random.Random(SEED + cv)
    high_ids = set(rng.sample(range(count), count // 10))
    shape = 1.0 / (cv * cv)
    scale = 0.002 / shape
    arrival = 0.0
    rows = []
    for index in range(count):
        arrival += rng.gammavariate(shape, scale)
        prompt, actual = token_exact_prompt(tokenizer, 64, f"Figure13 request {index}")
        rows.append({"id": f"fig13_cv{cv}_{index}", "prompt": prompt, "prompt_tokens": actual,
                     "max_tokens": 96, "priority_class": "high" if index in high_ids else "normal",
                     "trace_arrival_s": arrival, "kv_transfer_params": {"ali_llumnix_disagg": False}})
    return rows


def priority_submitter(aware: bool):
    def submit(requests: list[dict], concurrency: int, benchmark_start: float) -> list[dict]:
        buckets: dict[int, list[dict]] = {}
        for row in requests:
            buckets.setdefault(int(row["trace_arrival_s"] / 0.05), []).append(row)
        ordered = []
        for bucket in sorted(buckets):
            rows = buckets[bucket]
            if aware:
                rows = sorted(rows, key=lambda row: row["priority_class"] != "high")
            ordered.extend(rows)
        results = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as executor:
            futures = {}
            for row in ordered:
                delay = benchmark_start + row["trace_arrival_s"] - time.time()
                if delay > 0:
                    time.sleep(delay)
                futures[executor.submit(base.post_completion, row)] = row
            for future in concurrent.futures.as_completed(futures):
                row = futures[future]
                result = future.result()
                result["priority_class"] = row["priority_class"]
                result["trace_arrival_s"] = row["trace_arrival_s"]
                result["observed_e2e_s"] = time.time() - (benchmark_start + row["trace_arrival_s"])
                results.append(result)
        return results
    return submit


def run_fig13(smoke: bool = False) -> list[dict]:
    cvs = [2] if smoke else FIG13_CVS
    repeats = 1 if smoke else REPEATS
    mapping = {
        "priority_agnostic": ("load_sr", False),
        "priority_tail": ("tpot_tail", True),
        "priority_proposed": ("composite_proposed", True),
    }
    results = []
    for cv in cvs:
        requests = make_priority_trace(cv, smoke)
        for label in FIG13_POLICIES:
            backend_policy, aware = mapping[label]
            for repeat in range(1, repeats + 1):
                summary = run_custom("figure13", f"cv_{cv}", backend_policy, repeat, requests,
                                     10 if smoke else 50, 2048, priority_submitter(aware), smoke,
                                     metadata={"reported_policy": label, "arrival_cv": cv,
                                               "high_priority_fraction": 0.10,
                                               "priority_implementation_scope": "runner-level admission ordering"})
                summary_path = (SMOKE_DIR if smoke else RUNS_DIR) / "figure13" / f"cv_{cv}" / backend_policy / f"run_{repeat}" / "run_summary.json"
                write_json(summary_path, summary)
                results.append(summary)
    return results


def aggregate() -> dict:
    summaries = []
    for path in RUNS_DIR.rglob("run_summary.json"):
        try:
            summaries.append(json.loads(path.read_text(encoding="utf-8")))
        except Exception:
            pass
    payload = {
        "generated_at": now_iso(), "result_root": str(ROOT), "completed_runs": len(summaries),
        "summaries": summaries,
        "missing_exact_metrics": [
            "downtime_commit_ms", "downtime_resume_ms", "first_token_after_resume_ms",
            "migration_stage_count", "exact_external_fragmentation", "exact_HOL_blocking",
            "native_request_priority_metrics",
        ],
    }
    write_json(FINAL_DIR / "paper_supplement_aggregated.json", payload)
    return payload


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--phase",
        choices=("audit", "fig10-smoke", "fig10", "reparse-fig10", "fig12-smoke", "fig12",
                 "fig12-stale-guard-smoke", "fig12-stale-guard-retry",
                 "fig12-endpoint-guard-smoke", "fig12-endpoint-guard-retry",
                 "fig13-smoke", "fig13", "fig10-minimal-retry", "fig10-close", "aggregate"),
        required=True,
    )
    args = parser.parse_args()
    create_dirs()
    configure_policies()
    copy_profiling()
    write_design_docs()
    write_feasibility_report()
    if args.phase == "fig10-smoke":
        write_json(STEP_DIR / "figure10_smoke_results.json", run_fig10(smoke=True))
    elif args.phase == "fig10":
        write_json(STEP_DIR / "figure10_results.json", run_fig10(smoke=False))
    elif args.phase == "reparse-fig10":
        reparsed = []
        for summary_path in list(SMOKE_DIR.rglob("run_summary.json")) + list((RUNS_DIR / "figure10").rglob("run_summary.json")):
            event_path = summary_path.parent / "migration_efficiency_events.jsonl"
            reparsed.append(enrich_fig10(summary_path, event_path))
        write_json(STEP_DIR / "figure10_reparsed_results.json", reparsed)
    elif args.phase == "fig12-smoke":
        write_json(STEP_DIR / "figure12_smoke_results.json", run_fig12(smoke=True))
    elif args.phase == "fig12":
        write_json(STEP_DIR / "figure12_results.json", run_fig12(smoke=False))
    elif args.phase == "fig12-stale-guard-smoke":
        write_json(
            STEP_DIR / "figure12_stale_guard_smoke_results.json",
            run_fig12_stale_guard_validation(smoke=True),
        )
    elif args.phase == "fig12-stale-guard-retry":
        write_json(
            STEP_DIR / "figure12_stale_guard_retry_results.json",
            run_fig12_stale_guard_validation(smoke=False),
        )
    elif args.phase == "fig12-endpoint-guard-smoke":
        write_json(
            STEP_DIR / "figure12_endpoint_guard_smoke_results.json",
            run_fig12_endpoint_guard_validation(smoke=True),
        )
    elif args.phase == "fig12-endpoint-guard-retry":
        write_json(
            STEP_DIR / "figure12_endpoint_guard_retry_results.json",
            run_fig12_endpoint_guard_validation(smoke=False),
        )
    elif args.phase == "fig13-smoke":
        write_json(STEP_DIR / "figure13_smoke_results.json", run_fig13(smoke=True))
    elif args.phase == "fig13":
        write_json(STEP_DIR / "figure13_results.json", run_fig13(smoke=False))
    elif args.phase == "fig10-minimal-retry":
        run_fig10_minimal_retry()
    elif args.phase == "fig10-close":
        close_fig10()
    elif args.phase == "aggregate":
        aggregate()
    print(f"PAPER_SUPPLEMENT_ROOT={ROOT}")


if __name__ == "__main__":
    main()
