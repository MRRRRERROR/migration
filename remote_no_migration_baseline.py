import concurrent.futures
import json
import os
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
    get_vllm_command,
    start_process,
    wait_for_service,
    VLLM_BASE_PORT,
)


RESULT_DIR = Path(os.environ["RESULT_DIR"])
HANDOFF = Path("AI_HANDOFF.md")
COUNTS = [int(x) for x in os.environ.get("BASELINE_INSTANCES", "2,4").split(",")]
REQUESTS = int(os.environ.get("BASELINE_REQUESTS", "50"))
WORKERS = int(os.environ.get("BASELINE_WORKERS", "20"))
MAX_TOKENS = int(os.environ.get("BASELINE_MAX_TOKENS", "32"))


def post_completion(i):
    payload = {
        "model": os.environ.get("MODEL_PATH", "/data/models/Qwen2.5-7B-Instruct"),
        "prompt": f"Baseline request {i}: Hello, briefly introduce yourself.",
        "max_tokens": MAX_TOKENS,
        "stream": False,
        "temperature": 0.0,
    }
    req = urlrequest.Request(
        "http://localhost:18089/v1/completions",
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    start = time.time()
    try:
        with urlrequest.urlopen(req, timeout=240) as resp:
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


def first_fatal(log_dir):
    paths = sorted(log_dir.glob("vllm_*.log")) + [log_dir / "gateway_baseline.log"]
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
        if not path.exists():
            continue
        lines = path.read_text(errors="replace").splitlines()
        for idx, line in enumerate(lines):
            if any(marker in line for marker in markers):
                return "\n".join(
                    [f"FILE {path}"]
                    + [f"{j + 1}: {lines[j]}" for j in range(idx, min(idx + 90, len(lines)))]
                )
    return "NO_ENGINECORE_FATAL_FOUND"


def copy_logs(stage_dir):
    stage_dir.mkdir(parents=True, exist_ok=True)
    if LOG_DIR.exists():
        for p in LOG_DIR.glob("*.log"):
            shutil.copy2(p, stage_dir / p.name)
    if HANDOFF.exists():
        shutil.copy2(HANDOFF, stage_dir / HANDOFF.name)


def instance_distribution(stage_dir, num_instances):
    dist = {}
    for i in range(num_instances):
        path = stage_dir / f"vllm_{i}_baseline.log"
        count = 0
        if path.exists():
            for line in path.read_text(errors="replace").splitlines():
                if "Added request" in line:
                    count += 1
        dist[f"vllm_{i}"] = count
    return dist


def percentile(values, pct):
    if not values:
        return None
    ordered = sorted(values)
    idx = min(len(ordered) - 1, max(0, round((pct / 100) * (len(ordered) - 1))))
    return ordered[idx]


def run_case(num_instances):
    shutil.rmtree(NAMING_DIR, ignore_errors=True)
    shutil.rmtree(LOG_DIR, ignore_errors=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    processes = []
    stage_dir = RESULT_DIR / f"{num_instances}inst_no_migration"
    summary_path = stage_dir / "summary.json"
    try:
        processes.append(start_process("Redis", get_redis_command(), "redis_baseline.log"))
        processes.append(
            start_process(
                "Gateway",
                get_gateway_command(policy="round-robin", enable_full_mode_scheduling=False),
                "gateway_baseline.log",
            )
        )
        for i in range(num_instances):
            port = VLLM_BASE_PORT + i
            processes.append(
                start_process(
                    f"vLLM-{i}",
                    get_vllm_command(
                        "neutral",
                        port,
                        i,
                        False,
                        f"baseline_neutral_{i}",
                        "HybridConnector",
                    ),
                    f"vllm_{i}_baseline.log",
                )
            )
            processes.append(
                start_process(
                    f"Runtime-{i}",
                    get_discovery_command("neutral", port),
                    f"runtime_{i}_baseline.log",
                )
            )

        for i in range(num_instances):
            port = VLLM_BASE_PORT + i
            if not wait_for_service(f"http://localhost:{port}/health", timeout=300):
                raise TimeoutError(f"Service startup timeout for port {port}")
        time.sleep(15)

        start = time.time()
        with concurrent.futures.ThreadPoolExecutor(max_workers=WORKERS) as executor:
            futures = [executor.submit(post_completion, i) for i in range(REQUESTS)]
            results = [future.result() for future in concurrent.futures.as_completed(futures)]
        duration = time.time() - start
        copy_logs(stage_dir)

        latencies = [r["latency"] for r in results]
        ok_results = [r for r in results if r["status"] == 200 and r["text"]]
        fatal = first_fatal(stage_dir)
        dist = instance_distribution(stage_dir, num_instances)
        total_output_tokens = sum(
            r["output_tokens"] if isinstance(r["output_tokens"], int) else MAX_TOKENS
            for r in ok_results
        )
        summary = {
            "instances": num_instances,
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
            "instance_distribution": dist,
            "first_fatal": fatal,
            "sample_results": results[:5],
        }
        stage_dir.mkdir(parents=True, exist_ok=True)
        summary_path.write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")

        print(f"BASELINE_{num_instances}_SUMMARY {json.dumps(summary, ensure_ascii=False)}")
        return summary
    except Exception:
        copy_logs(stage_dir)
        print(f"BASELINE_{num_instances}_RUNNER_EXCEPTION")
        traceback.print_exc()
        fatal = first_fatal(stage_dir)
        summary = {
            "instances": num_instances,
            "requests": REQUESTS,
            "successful_responses": 0,
            "error_rate": 1,
            "first_fatal": fatal,
        }
        stage_dir.mkdir(parents=True, exist_ok=True)
        summary_path.write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")
        print(f"BASELINE_{num_instances}_FATAL")
        print(fatal)
        return summary
    finally:
        copy_logs(stage_dir / "final_before_cleanup")
        cleanup_processes(processes)


def main():
    summaries = []
    for count in COUNTS:
        summaries.append(run_case(count))
        fatal = summaries[-1].get("first_fatal")
        passed = (
            summaries[-1].get("successful_responses") == REQUESTS
            and fatal == "NO_ENGINECORE_FATAL_FOUND"
        )
        if not passed:
            break

    with HANDOFF.open("a", encoding="utf-8") as f:
        f.write("\n## 0721 no-migration baseline\n")
        f.write("- 当前阶段: 第三阶段，双实例/四实例无迁移 baseline\n")
        f.write("- 运行命令: python remote_no_migration_baseline.py\n")
        f.write(f"- 结果目录: {RESULT_DIR}\n")
        f.write("- benchmark summary:\n")
        f.write(json.dumps(summaries, indent=2, ensure_ascii=False) + "\n")
        f.write("- 是否已经产出可用实验数据: 是，普通无迁移 baseline 数据\n")
        f.write("- 下一步计划: 四实例无迁移通过后进入最小真实迁移验证\n")


if __name__ == "__main__":
    main()
