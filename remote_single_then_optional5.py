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


def post_completion(prompt, max_tokens=5):
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
    p = Path("logs/vllm_0_minimal.log")
    if not p.exists():
        return "logs/vllm_0_minimal.log missing"
    lines = p.read_text(errors="replace").splitlines()
    markers = (
        "EngineCore encountered a fatal error",
        "EngineCore failed to start",
        "Traceback (most recent call last)",
    )
    for idx, line in enumerate(lines):
        if any(marker in line for marker in markers):
            return "\n".join(
                f"{j + 1}: {lines[j]}" for j in range(idx, min(idx + 70, len(lines)))
            )
    return "NO_ENGINECORE_FATAL_FOUND"


def save_logs():
    RESULT_DIR.mkdir(parents=True, exist_ok=True)
    for p in [
        Path("logs/vllm_0_minimal.log"),
        Path("logs/gateway_minimal.log"),
        Path("logs/runtime_0_minimal.log"),
        HANDOFF,
    ]:
        if p.exists():
            shutil.copy2(p, RESULT_DIR / p.name)


def print_result(prefix, status, raw):
    text, finish_reason, usage, out_tokens = parse_result(raw)
    print(f"{prefix}_STATUS {status}")
    print(f"{prefix}_RAW_JSON {raw}")
    print(f"{prefix}_TEXT {text!r}")
    print(f"{prefix}_FINISH_REASON {finish_reason}")
    print(f"{prefix}_USAGE {usage}")
    print(f"{prefix}_OUTPUT_TOKENS {out_tokens}")
    return text


def main():
    os.environ.setdefault("MODEL_NAME", "Qwen/Qwen2.5-7B-Instruct")
    os.environ.setdefault("MODEL_PATH", "/data/models/Qwen2.5-7B-Instruct")
    shutil.rmtree(NAMING_DIR, ignore_errors=True)
    shutil.rmtree(LOG_DIR, ignore_errors=True)
    processes = []
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

        status, raw = post_completion("Hello, briefly introduce yourself.", max_tokens=5)
        text = print_result("SINGLE", status, raw)
        time.sleep(3)
        fatal = first_fatal()
        print("SINGLE_FIRST_FATAL")
        print(fatal)
        save_logs()

        if status == 200 and text and fatal == "NO_ENGINECORE_FATAL_FOUND":
            ok = 0
            for i in range(5):
                batch_status, batch_raw = post_completion(
                    f"Say hello in one short sentence. #{i}", max_tokens=5
                )
                batch_text = print_result(f"BATCH5_{i}", batch_status, batch_raw)
                ok += int(batch_status == 200 and bool(batch_text))
            time.sleep(3)
            fatal2 = first_fatal()
            print(f"BATCH5_OK {ok} TOTAL 5")
            print("BATCH5_FIRST_FATAL")
            print(fatal2)
            save_logs()
    except Exception:
        print("RUNNER_EXCEPTION")
        traceback.print_exc()
        save_logs()
    finally:
        save_logs()
        cleanup_processes(processes)


if __name__ == "__main__":
    main()
