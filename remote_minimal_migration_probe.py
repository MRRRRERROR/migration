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


RESULT_DIR = Path(os.environ["RESULT_DIR"])
HANDOFF = Path("AI_HANDOFF.md")
NUM_INSTANCES = int(os.environ.get("MIGRATION_INSTANCES", "4"))
REQUESTS = int(os.environ.get("MIGRATION_REQUESTS", "12"))
WORKERS = int(os.environ.get("MIGRATION_WORKERS", "8"))
MAX_TOKENS = int(os.environ.get("MIGRATION_MAX_TOKENS", "256"))


def post_completion(i):
    prompt = (f"Migration probe request {i}: Hello, my name is " * 20).strip()
    payload = {
        "model": os.environ.get("MODEL_PATH", "/data/models/Qwen2.5-7B-Instruct"),
        "prompt": prompt,
        "max_tokens": MAX_TOKENS,
        "stream": False,
        "temperature": 0.0,
        "ignore_eos": True,
        "kv_transfer_params": {"ali_llumnix_disagg": False},
    }
    req = urlrequest.Request(
        "http://localhost:18089/v1/completions",
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    start = time.time()
    try:
        with urlrequest.urlopen(req, timeout=600) as resp:
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
    out_tokens = None
    try:
        data = json.loads(raw)
        choice = (data.get("choices") or [{}])[0]
        text = choice.get("text", "")
        finish_reason = choice.get("finish_reason")
        usage = data.get("usage") or {}
        out_tokens = usage.get("completion_tokens")
    except Exception:
        pass
    return {
        "i": i,
        "status": status,
        "raw": raw,
        "latency": latency,
        "text": text,
        "finish_reason": finish_reason,
        "usage": usage,
        "output_tokens": out_tokens,
    }


def copy_logs(stage_dir):
    stage_dir.mkdir(parents=True, exist_ok=True)
    if LOG_DIR.exists():
        for p in LOG_DIR.glob("*.log"):
            shutil.copy2(p, stage_dir / p.name)
    if HANDOFF.exists():
        shutil.copy2(HANDOFF, stage_dir / HANDOFF.name)


def first_fatal(stage_dir):
    paths = sorted(stage_dir.glob("*.log"))
    markers = (
        "EngineCore encountered a fatal error",
        "EngineCore failed to start",
        "Traceback (most recent call last)",
        "RuntimeError:",
        "TypeError:",
        "AttributeError:",
        "ModuleNotFoundError:",
    )
    for path in paths:
        lines = path.read_text(errors="replace").splitlines()
        for idx, line in enumerate(lines):
            if any(marker in line for marker in markers):
                return "\n".join(
                    [f"FILE {path}"]
                    + [f"{j + 1}: {lines[j]}" for j in range(idx, min(idx + 100, len(lines)))]
                )
    return "NO_ENGINECORE_FATAL_FOUND"


def collect_migration_events(stage_dir):
    patterns = {
        "migrate_out_selected": r"migrate_out_selected|rescheduling.*selected",
        "migrate_out_begin": r"migrate out|Migrate out|migration transfer kv and suspend req",
        "migrate_in_begin": r"migrate in|migration begin|start migrate",
        "migrate_in_done": r"migration end|update success migrate in request",
        "finish_request_ids": r"finish_request_ids=\[[^\]]+\]|finish_request_ids: \[[^\]]+\]",
        "traceback": r"Traceback",
    }
    event_lines = []
    counts = {k: 0 for k in patterns}
    for path in sorted(stage_dir.glob("*.log")):
        lines = path.read_text(errors="replace").splitlines()
        for line in lines:
            if re.search(r"migrat|MIGRAT|resched|finish_request_ids", line):
                event_lines.append(f"{path.name}: {line}")
            for key, pattern in patterns.items():
                if re.search(pattern, line):
                    counts[key] += 1
    (stage_dir / "migration_events_extracted.log").write_text(
        "\n".join(event_lines), encoding="utf-8"
    )
    return counts, event_lines[-200:]


def percentile(values, pct):
    if not values:
        return None
    ordered = sorted(values)
    idx = min(len(ordered) - 1, max(0, round((pct / 100) * (len(ordered) - 1))))
    return ordered[idx]


def main():
    shutil.rmtree(NAMING_DIR, ignore_errors=True)
    shutil.rmtree(LOG_DIR, ignore_errors=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    processes = []
    stage_dir = RESULT_DIR / "minimal_migration_probe"
    summary_path = stage_dir / "summary.json"
    try:
        processes.append(start_process("Redis", get_redis_command(), "redis_migration.log"))
        processes.append(
            start_process(
                "Scheduler",
                get_scheduler_command(
                    policy="flood",
                    enable_full_mode_scheduling=True,
                    enable_migration=True,
                    enable_pd=False,
                ),
                "scheduler_migration.log",
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
                "gateway_migration.log",
            )
        )
        for i in range(NUM_INSTANCES):
            port = VLLM_BASE_PORT + i
            processes.append(
                start_process(
                    f"vLLM-{i}",
                    get_vllm_command(
                        "neutral",
                        port,
                        i,
                        True,
                        f"migration_neutral_{i}",
                        "HybridConnector",
                    ),
                    f"vllm_{i}_migration.log",
                )
            )
            processes.append(
                start_process(
                    f"Runtime-{i}",
                    get_discovery_command("neutral", port),
                    f"runtime_{i}_migration.log",
                )
            )

        for i in range(NUM_INSTANCES):
            port = VLLM_BASE_PORT + i
            if not wait_for_service(f"http://localhost:{port}/health", timeout=300):
                raise TimeoutError(f"Service startup timeout for port {port}")
        time.sleep(25)

        start = time.time()
        with concurrent.futures.ThreadPoolExecutor(max_workers=WORKERS) as executor:
            futures = [executor.submit(post_completion, i) for i in range(REQUESTS)]
            results = [future.result() for future in concurrent.futures.as_completed(futures)]
        duration = time.time() - start
        copy_logs(stage_dir)

        latencies = [r["latency"] for r in results]
        ok_results = [r for r in results if r["status"] == 200 and r["text"]]
        fatal = first_fatal(stage_dir)
        event_counts, event_tail = collect_migration_events(stage_dir)
        total_output_tokens = sum(
            r["output_tokens"] if isinstance(r["output_tokens"], int) else MAX_TOKENS
            for r in ok_results
        )
        summary = {
            "instances": NUM_INSTANCES,
            "requests": REQUESTS,
            "workers": WORKERS,
            "successful_responses": len(ok_results),
            "http_200": sum(1 for r in results if r["status"] == 200),
            "nonempty_text": sum(1 for r in results if r["text"]),
            "error_rate": 1 - (len(ok_results) / REQUESTS),
            "duration_s": duration,
            "qps": len(ok_results) / duration if duration > 0 else 0,
            "throughput_tok_s": total_output_tokens / duration if duration > 0 else 0,
            "median_e2e_latency": statistics.median(latencies) if latencies else None,
            "p95_latency": percentile(latencies, 95),
            "p99_latency": percentile(latencies, 99),
            "migration_event_counts": event_counts,
            "first_fatal": fatal,
            "sample_results": results[:5],
            "migration_event_tail": event_tail,
        }
        stage_dir.mkdir(parents=True, exist_ok=True)
        summary_path.write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")
        print(f"MIGRATION_PROBE_SUMMARY {json.dumps(summary, ensure_ascii=False)}")
    except Exception:
        copy_logs(stage_dir)
        print("MIGRATION_PROBE_RUNNER_EXCEPTION")
        traceback.print_exc()
        fatal = first_fatal(stage_dir)
        print("MIGRATION_PROBE_FATAL")
        print(fatal)
    finally:
        copy_logs(stage_dir / "final_before_cleanup")
        with HANDOFF.open("a", encoding="utf-8") as f:
            f.write("\n## 0721 minimal migration probe\n")
            f.write("- 当前阶段: 第四阶段，四实例最小真实迁移验证\n")
            f.write("- 运行命令: python remote_minimal_migration_probe.py\n")
            f.write(f"- 结果目录: {RESULT_DIR}\n")
            if summary_path.exists():
                f.write("- benchmark summary:\n")
                f.write(summary_path.read_text(encoding="utf-8") + "\n")
            f.write("- 下一步计划: 基于第一条 fatal 或迁移事件缺失原因继续处理\n")
        copy_logs(stage_dir / "final_with_handoff")
        cleanup_processes(processes)


if __name__ == "__main__":
    main()
