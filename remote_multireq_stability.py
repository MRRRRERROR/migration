import json
import os
import shutil
import traceback
from pathlib import Path
from urllib import request as urlrequest
from urllib.error import HTTPError, URLError
import time

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
)


RESULT_DIR = Path(os.environ["RESULT_DIR"])
HANDOFF = Path("AI_HANDOFF.md")


def post_completion(prompt, max_tokens=8):
    payload = {
        "model": os.environ.get("MODEL_PATH", "/data/models/Qwen2.5-7B-Instruct"),
        "prompt": prompt,
        "max_tokens": max_tokens,
        "stream": False,
        "temperature": 0.0,
    }
    req = urlrequest.Request(
        "http://localhost:18089/v1/completions",
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urlrequest.urlopen(req, timeout=180) as resp:
            return resp.status, resp.read().decode("utf-8", errors="replace")
    except HTTPError as e:
        return e.code, e.read().decode("utf-8", errors="replace")
    except URLError as e:
        return 0, repr(e)


def parse_result(raw):
    try:
        data = json.loads(raw)
    except Exception:
        return "", None, {}, None
    choice = (data.get("choices") or [{}])[0]
    text = choice.get("text", "")
    finish_reason = choice.get("finish_reason")
    usage = data.get("usage") or {}
    return text, finish_reason, usage, usage.get("completion_tokens")


def first_fatal():
    paths = [
        Path("logs/vllm_0_minimal.log"),
        Path("logs/gateway_minimal.log"),
        Path("logs/runtime_0_minimal.log"),
    ]
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
                    + [f"{j + 1}: {lines[j]}" for j in range(idx, min(idx + 80, len(lines)))]
                )
    return "NO_ENGINECORE_FATAL_FOUND"


def save_logs(stage):
    stage_dir = RESULT_DIR / stage
    stage_dir.mkdir(parents=True, exist_ok=True)
    for p in [
        Path("logs/vllm_0_minimal.log"),
        Path("logs/gateway_minimal.log"),
        Path("logs/runtime_0_minimal.log"),
        HANDOFF,
    ]:
        if p.exists():
            shutil.copy2(p, stage_dir / p.name)


def print_result(prefix, status, raw):
    text, finish_reason, usage, out_tokens = parse_result(raw)
    print(f"{prefix}_STATUS {status}")
    print(f"{prefix}_RAW_JSON {raw}")
    print(f"{prefix}_TEXT {text!r}")
    print(f"{prefix}_FINISH_REASON {finish_reason}")
    print(f"{prefix}_USAGE {usage}")
    print(f"{prefix}_OUTPUT_TOKENS {out_tokens}")
    return text, finish_reason, usage, out_tokens


def run_batch(count):
    ok = 0
    nonempty = 0
    stage = f"BATCH{count}"
    for i in range(count):
        status, raw = post_completion(
            f"Say hello in one short sentence. request {i} of {count}",
            max_tokens=8,
        )
        text, _, _, _ = print_result(f"{stage}_{i}", status, raw)
        ok += int(status == 200)
        nonempty += int(status == 200 and bool(text))
    time.sleep(3)
    fatal = first_fatal()
    print(f"{stage}_OK {ok} TOTAL {count}")
    print(f"{stage}_NONEMPTY {nonempty} TOTAL {count}")
    print(f"{stage}_FIRST_FATAL")
    print(fatal)
    save_logs(stage.lower())
    return ok == count and nonempty == count and fatal == "NO_ENGINECORE_FATAL_FOUND"


def main():
    os.environ.setdefault("MODEL_NAME", "Qwen/Qwen2.5-7B-Instruct")
    os.environ.setdefault("MODEL_PATH", "/data/models/Qwen2.5-7B-Instruct")
    shutil.rmtree(NAMING_DIR, ignore_errors=True)
    shutil.rmtree(LOG_DIR, ignore_errors=True)
    processes = []
    completed = []
    latest_fatal = "NOT_RUN"
    try:
        processes.append(start_process("Redis", get_redis_command()))
        processes.append(start_process("Gateway", get_gateway_command()))
        processes.append(
            start_process(
                "vLLM-0",
                get_vllm_command(
                    "neutral", 8100, 0, False, "minimal_neutral_0", "HybridConnector"
                ),
                "vllm_0_minimal.log",
            )
        )
        processes.append(
            start_process(
                "Runtime-0",
                get_discovery_command("neutral", 8100, 1),
                "runtime_0_minimal.log",
            )
        )

        wait_for_service("http://localhost:8100/health", timeout=240)
        time.sleep(10)

        for count in (10, 20, 50):
            passed = run_batch(count)
            completed.append((count, passed))
            latest_fatal = first_fatal()
            if not passed:
                break
    except Exception:
        print("RUNNER_EXCEPTION")
        traceback.print_exc()
        latest_fatal = first_fatal()
        save_logs("runner_exception")
    finally:
        save_logs("final_before_cleanup")
        with HANDOFF.open("a", encoding="utf-8") as f:
            f.write("\n## 0721 ordinary multi-request stability\n")
            f.write("- 当前阶段: 第二阶段，普通多请求稳定\n")
            f.write("- 运行命令: python remote_multireq_stability.py\n")
            f.write(f"- 结果目录: {RESULT_DIR}\n")
            f.write(f"- benchmark summary: {completed}\n")
            f.write("- 第一条 fatal traceback:\n")
            f.write(latest_fatal + "\n")
            f.write("- 是否已经产出可用实验数据: 否，当前为普通链路稳定性验证\n")
            f.write("- 下一步计划: 通过后进入 2 实例/4 实例无迁移 baseline\n")
        save_logs("final_with_handoff")
        cleanup_processes(processes)


if __name__ == "__main__":
    main()
