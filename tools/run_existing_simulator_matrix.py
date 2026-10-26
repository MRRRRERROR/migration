#!/usr/bin/env python3
"""Run existing Llumnix simulator dispatch and migration matrices.

This runner composes only existing Llumnix CLI mechanisms. It does not patch
simulator internals or add new scheduling algorithms.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
import signal
import statistics
import subprocess
import sys
import time
from pathlib import Path
from urllib.error import URLError
from urllib.request import urlopen

import aiohttp


REPO = Path("/mnt/d/llumnix-ray-main")
PROFILE = Path(os.environ.get(
    "SIM_PROFILE_PATH",
    str(REPO / "_simulator_profile_test/simulator_approx_profile_5090/qwen2p5_7b_5090_profile_approx_numpy126.pkl"),
))
MODEL = REPO / "_simulator_model_stub/Qwen2.5-7B-Instruct"


WORKLOADS = {
    "stable_short": {
        "max_tokens": lambda i: 32,
        "prompt": lambda i: f"Stable short request {i}: Hello, briefly introduce yourself.",
    },
    "mixed_tail": {
        "max_tokens": lambda i: 160 if i % 5 == 0 else 48,
        "prompt": lambda i: (
            (f"Mixed tail long request {i}: Explain request migration and KV cache transfer. " * 16).strip()
            if i % 5 == 0
            else f"Mixed tail short request {i}: Hello, briefly introduce yourself."
        ),
    },
    "long_output_migration": {
        "max_tokens": lambda i: 192,
        "prompt": lambda i: (
            f"Long output migration request {i}: Explain how distributed LLM serving migrates "
            + "a running request while preserving KV cache state. " * 10
        ).strip(),
    },
}


def pct(values: list[float], percentile: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    rank = (len(ordered) - 1) * percentile / 100.0
    low = int(rank)
    high = min(low + 1, len(ordered) - 1)
    frac = rank - low
    return ordered[low] * (1 - frac) + ordered[high] * frac


async def one_request(session: aiohttp.ClientSession, index: int, workload: str) -> dict:
    spec = WORKLOADS[workload]
    payload = {
        "prompt": spec["prompt"](index),
        "stream": False,
        "temperature": 0.0,
        "top_p": 1.0,
        "max_tokens": spec["max_tokens"](index),
        "ignore_eos": True,
    }
    start = time.perf_counter()
    try:
        async with session.post("http://127.0.0.1:1234/generate_benchmark", json=payload) as response:
            raw = await response.text()
            elapsed = time.perf_counter() - start
            try:
                body = json.loads(raw)
            except json.JSONDecodeError:
                body = {"raw": raw}
            return {
                "index": index,
                "status": response.status,
                "elapsed_s": elapsed,
                "ok": response.status == 200 and bool(body.get("generated_text")),
                "request_id": body.get("request_id"),
                "output_tokens": int(body.get("num_output_tokens_cf") or 0),
                "max_tokens": payload["max_tokens"],
                "body": body,
            }
    except Exception as exc:  # noqa: BLE001
        return {
            "index": index,
            "status": None,
            "elapsed_s": time.perf_counter() - start,
            "ok": False,
            "request_id": None,
            "output_tokens": 0,
            "max_tokens": payload["max_tokens"],
            "error": repr(exc),
        }


async def run_client(requests: int, concurrency: int, workload: str) -> dict:
    semaphore = asyncio.Semaphore(concurrency)
    timeout = aiohttp.ClientTimeout(total=900)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        async def guarded(index: int) -> dict:
            async with semaphore:
                return await one_request(session, index, workload)

        start = time.perf_counter()
        responses = await asyncio.gather(*(guarded(i) for i in range(requests)))
        duration = time.perf_counter() - start
    successful = [item for item in responses if item["ok"]]
    latencies = [item["elapsed_s"] for item in successful]
    output_tokens = sum(item["output_tokens"] for item in successful)
    return {
        "requests": requests,
        "concurrency": concurrency,
        "workload": workload,
        "duration_s": duration,
        "success_count": len(successful),
        "failure_count": requests - len(successful),
        "qps": len(successful) / duration if duration else 0.0,
        "throughput_tok_s": output_tokens / duration if duration else 0.0,
        "median_e2e_s": statistics.median(latencies) if latencies else None,
        "p95_e2e_s": pct(latencies, 95),
        "p99_e2e_s": pct(latencies, 99),
        "output_tokens": output_tokens,
        "responses": responses,
    }


def run_cmd(command: list[str], **kwargs) -> subprocess.CompletedProcess:
    return subprocess.run(command, text=True, check=False, **kwargs)


def stop_runtime() -> None:
    run_cmd(["ray", "stop", "--force"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def start_server(
    out_dir: Path,
    dispatch_policy: str,
    migration: bool,
    pair_policy: str,
    request_policy: str,
    migrate_out_threshold: str = "3300",
    polling_interval: str | None = None,
) -> subprocess.Popen:
    command = [
        sys.executable, "-u", "-m", "llumnix.entrypoints.vllm.api_server",
        "--config-file", "configs/vllm.yml",
        "--port", "1234",
        "--ray-cluster-port", "6389",
        "--initial-instances", "4",
        "--model", str(MODEL),
        "--trust-remote-code",
        "--max-model-len", "1024",
        "--max-num-batched-tokens", "2048",
        "--gpu-memory-utilization", "0.5",
        "--worker-use-ray",
        "--device", "cpu",
        "--dispatch-policy", dispatch_policy,
        "--topk-random-dispatch", "1",
        "--simulator-mode",
        "--profiling-result-file-path", str(PROFILE),
        "--disable-async-output-proc",
        "--log-instance-info",
        "--log-filename", str(out_dir / "wsl_sim_instance.log"),
    ]
    if polling_interval is not None:
        command += ["--polling-interval", polling_interval]
    if migration:
        command += [
            "--enable-routine-migration",
            "--pair-migration-frequency", "1",
            "--pair-migration-policy", pair_policy,
            "--migrate-out-threshold", migrate_out_threshold,
            "--migration-load-metric", "remaining_steps",
            "--request-migration-policy", request_policy,
            "--migration-backend", "rayrpc",
            "--migration-buffer-blocks", "32",
        ]
    (out_dir / "server_command.json").write_text(json.dumps(command, indent=2), encoding="utf-8")
    log = (out_dir / "server_console.log").open("w", encoding="utf-8")
    return subprocess.Popen(command, cwd=REPO, stdout=log, stderr=subprocess.STDOUT, text=True)


def wait_ready(process: subprocess.Popen, timeout_s: float = 180.0) -> bool:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        if process.poll() is not None:
            return False
        try:
            with urlopen("http://127.0.0.1:1234/is_ready", timeout=2) as resp:
                if resp.status == 200:
                    return True
        except (URLError, TimeoutError, OSError):
            pass
        time.sleep(2)
    return False


def terminate_process(process: subprocess.Popen) -> None:
    if process.poll() is not None:
        return
    process.terminate()
    try:
        process.wait(timeout=10)
    except subprocess.TimeoutExpired:
        process.send_signal(signal.SIGKILL)
        process.wait(timeout=10)


def parse_dispatch(server_text: str) -> dict[str, str]:
    pattern = r"global_scheduler.py:\d+\] dispatch request ([0-9a-f]{8,}) to instance ([0-9a-f]{8,})"
    return dict(re.findall(pattern, server_text))


def parse_migration(server_text: str) -> dict:
    begin_pattern = r"([0-9a-f]{8,})->([0-9a-f]{8,}) begin migrate out ([0-9a-f]{8,})"
    done_pattern = (
        r"Instance ([0-9a-f]{8,})->([0-9a-f]{8,}) migrate done, .*?migrate request (\[[^\]]*\]), "
        r"migration status: MigrationStatus\.([A-Z_]+), len: ([0-9]+) blocks, cost: ([0-9.]+) ms"
    )
    begins = re.findall(begin_pattern, server_text)
    dones = re.findall(done_pattern, server_text)
    success = [item for item in dones if item[3] == "FINISHED" and item[2] != "[]"]
    failed = [item for item in dones if item[3] != "FINISHED" or item[2] == "[]"]
    pairs: dict[str, int] = {}
    for src, dst, *_ in begins:
        pairs[f"{src}->{dst}"] = pairs.get(f"{src}->{dst}", 0) + 1
    for src, dst, *_ in dones:
        pairs[f"{src}->{dst}"] = pairs.get(f"{src}->{dst}", 0) + 1
    return {
        "triggered_migration_count": len(begins),
        "successful_migration_count": len(success),
        "failed_migration_count": len(failed),
        "migration_suspend_count": len(re.findall(r"suspend|remove_running_request|last stage migration", server_text, re.I)),
        "migration_end_count": len(dones),
        "send_cache_call_count": "未直接记录",
        "source_destination_pairs": pairs,
        "done_records": [
            {
                "src": src,
                "dst": dst,
                "request_list": reqs,
                "status": status,
                "blocks": int(blocks),
                "cost_ms": float(cost),
            }
            for src, dst, reqs, status, blocks, cost in dones
        ],
    }


def summarize_run(out_dir: Path, client_result: dict) -> dict:
    server_text = (out_dir / "server_console.log").read_text(errors="replace")
    request_to_instance = parse_dispatch(server_text)
    instance_request_counts: dict[str, int] = {}
    instance_token_counts: dict[str, int] = {}
    for item in client_result["responses"]:
        request_id = item.get("request_id")
        instance_id = request_to_instance.get(request_id)
        if not instance_id:
            continue
        instance_request_counts[instance_id] = instance_request_counts.get(instance_id, 0) + 1
        instance_token_counts[instance_id] = instance_token_counts.get(instance_id, 0) + int(item.get("output_tokens") or 0)
    fatal_like = []
    for line in server_text.splitlines():
        if re.search(r"Traceback|ERROR|RuntimeError|ActorDiedError|Error in engine loop|EngineCore", line):
            if "libcuda.so.1" not in line and "Failed to import from vllm._C" not in line:
                fatal_like.append(line)
    summary = {
        **{k: v for k, v in client_result.items() if k != "responses"},
        "instance_request_counts": instance_request_counts,
        "instance_token_counts": instance_token_counts,
        "dispatch_record_count": len(request_to_instance),
        "fatal_runtime_count": len(fatal_like),
        "first_fatal_runtime": fatal_like[:3],
        "migration": parse_migration(server_text),
    }
    (out_dir / "summary.json").write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")
    return summary


def run_case(root: Path, name: str, workload: str, requests: int, concurrency: int,
             dispatch_policy: str, migration: bool = False, pair_policy: str = "balanced",
             request_policy: str = "SR", migrate_out_threshold: str = "3300",
             polling_interval: str | None = None) -> dict:
    out_dir = root / name
    out_dir.mkdir(parents=True, exist_ok=True)
    print(f"RUN {name}", flush=True)
    stop_runtime()
    process = start_server(
        out_dir,
        dispatch_policy,
        migration,
        pair_policy,
        request_policy,
        migrate_out_threshold=migrate_out_threshold,
        polling_interval=polling_interval,
    )
    (out_dir / "server.pid").write_text(str(process.pid), encoding="utf-8")
    ready = wait_ready(process)
    (out_dir / "run_status.txt").write_text(f"ready={ready}\n", encoding="utf-8")
    if not ready:
        terminate_process(process)
        stop_runtime()
        summary = {"name": name, "ready": False}
        (out_dir / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
        return summary
    client_result = asyncio.run(run_client(requests, concurrency, workload))
    (out_dir / "client_result.json").write_text(json.dumps(client_result, indent=2, ensure_ascii=False), encoding="utf-8")
    summary = summarize_run(out_dir, client_result)
    terminate_process(process)
    stop_runtime()
    print(
        f"DONE {name}: success={summary['success_count']}/{summary['requests']} "
        f"qps={summary['qps']:.4f} migrations={summary['migration']['successful_migration_count']}",
        flush=True,
    )
    return summary


def uniform_label(counts: dict[str, int], total: int, rr: bool) -> str:
    if len(counts) < 4:
        return "不均匀"
    values = list(counts.values())
    if rr:
        return "接近 25/25/25/25" if max(abs(v - total / 4) for v in values) <= 5 else "不接近 25/25/25/25"
    return "较均匀" if max(values) / max(1, total) <= 0.40 else "不均匀"


def write_dispatch_matrix(root: Path, rows: list[tuple[str, dict]]) -> None:
    lines = [
        "# SIM_EXISTING_DISPATCH_MATRIX",
        "",
        "- 机制: no migration, existing dispatch-policy",
        "- 配置: 4 instances, 100 requests, concurrency=20",
        "",
        "| workload | dispatch-policy | success | QPS | throughput tok/s | median s | P95 s | P99 s | instance 请求数 | instance token 数 | 分布判断 |",
        "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- | --- |",
    ]
    for name, s in rows:
        workload, policy = name.split("__", 1)
        counts = s.get("instance_request_counts", {})
        tokens = s.get("instance_token_counts", {})
        lines.append(
            "| {} | {} | {}/{} | {:.4f} | {:.4f} | {} | {} | {} | {} | {} | {} |".format(
                workload,
                policy,
                s.get("success_count", 0),
                s.get("requests", 0),
                s.get("qps", 0.0),
                s.get("throughput_tok_s", 0.0),
                s.get("median_e2e_s"),
                s.get("p95_e2e_s"),
                s.get("p99_e2e_s"),
                "`" + json.dumps(counts, ensure_ascii=False) + "`",
                "`" + json.dumps(tokens, ensure_ascii=False) + "`",
                uniform_label(counts, s.get("requests", 0), policy == "rr"),
            )
        )
    (root / "SIM_EXISTING_DISPATCH_MATRIX.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_migration_trigger(root: Path, rows: list[tuple[str, dict]]) -> None:
    lines = [
        "# SIM_EXISTING_MIGRATION_TRIGGER",
        "",
        "- 机制: enable-routine-migration, pair-migration-policy=balanced, request-migration-policy=SR",
        "- 配置: 4 instances, 120 requests, concurrency=40",
        "",
        "| workload | dispatch-policy | success | triggered | successful | failed | suspend | migration_end | send_cache | pairs | instance 请求数 | instance token 数 |",
        "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- | --- | --- |",
    ]
    for name, s in rows:
        workload, policy = name.split("__", 1)
        m = s.get("migration", {})
        lines.append(
            "| {} | {} | {}/{} | {} | {} | {} | {} | {} | {} | `{}` | `{}` | `{}` |".format(
                workload,
                policy,
                s.get("success_count", 0),
                s.get("requests", 0),
                m.get("triggered_migration_count", 0),
                m.get("successful_migration_count", 0),
                m.get("failed_migration_count", 0),
                m.get("migration_suspend_count", 0),
                m.get("migration_end_count", 0),
                m.get("send_cache_call_count", "未直接记录"),
                json.dumps(m.get("source_destination_pairs", {}), ensure_ascii=False),
                json.dumps(s.get("instance_request_counts", {}), ensure_ascii=False),
                json.dumps(s.get("instance_token_counts", {}), ensure_ascii=False),
            )
        )
    total_success = sum(s.get("migration", {}).get("successful_migration_count", 0) for _, s in rows)
    lines += ["", f"- successful_migration_count 合计: {total_success}"]
    (root / "SIM_EXISTING_MIGRATION_TRIGGER.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_policy_matrix(root: Path, rows: list[tuple[str, dict]]) -> None:
    lines = [
        "# SIM_EXISTING_POLICY_MATRIX",
        "",
        "- 机制: existing request-migration-policy",
        "- 配置: 4 instances, 200 requests, concurrency=40, pair-migration-policy=balanced",
        "",
        "| workload | request-migration-policy | success | QPS | throughput tok/s | triggered | successful | failed | migration_end | pairs | instance 请求数 | instance token 数 |",
        "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- | --- |",
    ]
    for name, s in rows:
        workload, request_policy = name.split("__", 1)
        m = s.get("migration", {})
        lines.append(
            "| {} | {} | {}/{} | {:.4f} | {:.4f} | {} | {} | {} | {} | `{}` | `{}` | `{}` |".format(
                workload,
                request_policy,
                s.get("success_count", 0),
                s.get("requests", 0),
                s.get("qps", 0.0),
                s.get("throughput_tok_s", 0.0),
                m.get("triggered_migration_count", 0),
                m.get("successful_migration_count", 0),
                m.get("failed_migration_count", 0),
                m.get("migration_end_count", 0),
                json.dumps(m.get("source_destination_pairs", {}), ensure_ascii=False),
                json.dumps(s.get("instance_request_counts", {}), ensure_ascii=False),
                json.dumps(s.get("instance_token_counts", {}), ensure_ascii=False),
            )
        )
    (root / "SIM_EXISTING_POLICY_MATRIX.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, default=REPO / "results" / f"wsl_sim_existing_{time.strftime('%Y%m%d_%H%M%S')}")
    args = parser.parse_args()
    os.chdir(REPO)
    args.root.mkdir(parents=True, exist_ok=True)

    dispatch_rows = []
    for workload in ("stable_short", "mixed_tail", "long_output_migration"):
        for policy in ("rr", "balanced", "load", "queue"):
            name = f"{workload}__{policy}"
            summary = run_case(args.root / "dispatch_runs", name, workload, 100, 20, policy)
            dispatch_rows.append((name, summary))
    write_dispatch_matrix(args.root, dispatch_rows)

    trigger_rows = []
    for workload in ("mixed_tail", "long_output_migration"):
        name = f"{workload}__rr"
        summary = run_case(args.root / "migration_trigger_runs", name, workload, 120, 40, "rr", True, "balanced", "SR")
        trigger_rows.append((name, summary))
    write_migration_trigger(args.root, trigger_rows)

    total_migration_success = sum(
        row[1].get("migration", {}).get("successful_migration_count", 0)
        for row in trigger_rows
    )
    policy_rows = []
    if total_migration_success > 0:
        for workload in ("mixed_tail", "long_output_migration"):
            for request_policy in ("LCR", "SR", "LR", "FCW", "FCWSR"):
                name = f"{workload}__{request_policy}"
                summary = run_case(
                    args.root / "policy_runs",
                    name,
                    workload,
                    200,
                    40,
                    "rr",
                    True,
                    "balanced",
                    request_policy,
                )
                policy_rows.append((name, summary))
        write_policy_matrix(args.root, policy_rows)
    else:
        (args.root / "SIM_EXISTING_POLICY_MATRIX.md").write_text(
            "# SIM_EXISTING_POLICY_MATRIX\n\n"
            "原生迁移触发验证的 successful_migration_count 为 0，本轮按要求未进入策略小矩阵。\n",
            encoding="utf-8",
        )

    final = {
        "root": str(args.root),
        "dispatch_report": str(args.root / "SIM_EXISTING_DISPATCH_MATRIX.md"),
        "migration_trigger_report": str(args.root / "SIM_EXISTING_MIGRATION_TRIGGER.md"),
        "policy_matrix_report": str(args.root / "SIM_EXISTING_POLICY_MATRIX.md"),
        "trigger_successful_migrations": total_migration_success,
    }
    (args.root / "run_manifest.json").write_text(json.dumps(final, indent=2, ensure_ascii=False), encoding="utf-8")
    print(json.dumps(final, indent=2, ensure_ascii=False), flush=True)


if __name__ == "__main__":
    main()
