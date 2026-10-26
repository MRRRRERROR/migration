import os
import shutil
import time
import traceback
from pathlib import Path

from tests.local.utils import (
    LOG_DIR,
    NAMING_DIR,
    cleanup_processes,
    get_discovery_command,
    get_gateway_command,
    get_redis_command,
    get_vllm_command,
    send_request,
    start_process,
    wait_for_service,
)


def main():
    handoff = Path("AI_HANDOFF.md")
    processes = []
    result = "unknown"
    latest_traceback = ""

    os.environ.setdefault("MODEL_NAME", "Qwen/Qwen2.5-7B-Instruct")
    os.environ.setdefault("MODEL_PATH", "/data/models/Qwen2.5-7B-Instruct")

    shutil.rmtree(NAMING_DIR, ignore_errors=True)
    shutil.rmtree(LOG_DIR, ignore_errors=True)
    LOG_DIR.mkdir(exist_ok=True)
    NAMING_DIR.mkdir(parents=True, exist_ok=True)

    try:
        processes.append(start_process("Redis", get_redis_command(), "redis_minimal.log"))
        time.sleep(2)

        processes.append(
            start_process(
                "Gateway",
                get_gateway_command(
                    policy="round-robin",
                    enable_pd=False,
                    enable_full_mode_scheduling=False,
                    separate_pd_scheduling=False,
                    connector_type="HybridConnector",
                ),
                "gateway_minimal.log",
            )
        )

        processes.append(
            start_process(
                "vLLM-0",
                get_vllm_command(
                    instance_type="neutral",
                    port=8100,
                    cuda=0,
                    enable_full_mode_scheduling=False,
                    tag="minimal_neutral_0",
                    connector_type="HybridConnector",
                ),
                "vllm_0_minimal.log",
            )
        )
        processes.append(
            start_process(
                "Runtime-0",
                get_discovery_command("neutral", 8100),
                "runtime_0_minimal.log",
            )
        )

        if not wait_for_service("http://localhost:8100/health", timeout=120):
            raise TimeoutError("vLLM-0 health timeout")

        time.sleep(10)

        payload = {
            "prompt": "Hello, my name is",
            "max_tokens": 5,
            "stream": False,
            "temperature": 0.0,
        }
        text = send_request(payload, endpoint_type="completions")
        result = f"success text={text!r}"
        print("MINIMAL_SINGLE_REQUEST_RESULT", result)
    except Exception:
        latest_traceback = traceback.format_exc()
        result = "failed"
        print(latest_traceback)
        raise
    finally:
        with handoff.open("a", encoding="utf-8") as f:
            f.write("\n## 0720 blade_kvt 回写后最小单请求测试\n")
            f.write("- 测试命令: python remote_minimal_single_request.py\n")
            f.write("- 服务组合: redis + gateway + vLLM-0 + runtime discovery\n")
            f.write("- 迁移: 关闭 full-mode scheduling, round-robin, 单实例普通请求\n")
            f.write(f"- 测试结果: {result}\n")
            if latest_traceback:
                f.write("- 最新 traceback:\n")
                f.write("```text\n")
                f.write(latest_traceback)
                f.write("\n```\n")
        cleanup_processes(processes)


if __name__ == "__main__":
    main()
