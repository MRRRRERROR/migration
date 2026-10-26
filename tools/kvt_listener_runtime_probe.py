#!/usr/bin/env python3
"""Start the migration stack without requests for KVT listener inspection."""

from __future__ import annotations

import importlib.util
import os
import shutil
import sys
import time
from pathlib import Path


ROOT = Path(os.environ["LOAD_SWEEP_ROOT"]).resolve()
REPO = Path("/data/home/zylu/llumnix-v018-main")
RUNNER = Path("/tmp/run_llumnix_load_sweep.py")
STOP = Path("/tmp/stop_kvt_listener_runtime_probe")
DESTINATION = ROOT / "01_smoke" / "kvt_listener_runtime_check"

os.chdir(REPO)
sys.path.insert(0, str(REPO))

spec = importlib.util.spec_from_file_location("load_sweep_runner", RUNNER)
runner = importlib.util.module_from_spec(spec)
assert spec.loader is not None
spec.loader.exec_module(runner)


def main() -> None:
    processes = []
    STOP.unlink(missing_ok=True)
    try:
        runner.cleanup_known_ports()
        shutil.rmtree(runner.NAMING_DIR, ignore_errors=True)
        shutil.rmtree(runner.LOG_DIR, ignore_errors=True)
        runner.LOG_DIR.mkdir(parents=True, exist_ok=True)
        processes = runner.start_stack("Hybrid_live_migration")
        for index in range(runner.INSTANCES):
            port = runner.VLLM_BASE_PORT + index
            if not runner.wait_for_service(f"http://localhost:{port}/health", timeout=360):
                raise TimeoutError(f"API readiness failed for port {port}")
        print("KVT_LISTENER_PROBE_API_READY", flush=True)
        deadline = time.time() + 600
        while time.time() < deadline and not STOP.exists():
            time.sleep(1)
    finally:
        runner.copy_logs(DESTINATION)
        runner.cleanup_processes(processes)
        runner.cleanup_known_ports()
        print("KVT_LISTENER_PROBE_CLEANED", flush=True)


if __name__ == "__main__":
    main()
