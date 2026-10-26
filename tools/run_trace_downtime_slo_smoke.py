#!/usr/bin/env python3
"""Run two small four-instance trace validation cases."""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))
os.environ.setdefault("MIGRATION_POLICY_ROOT", str(REPO / "results" / "_trace_smoke_bootstrap"))
os.environ.setdefault("PAPER_SUPPLEMENT_ROOT", str(REPO / "results" / "_trace_smoke_bootstrap"))
os.environ.setdefault("PAPER_SUPPLEMENT_ALLOW_CONTENDED", "1")

import run_migration_policy_baselines as base
import run_paper_supplement_experiments as paper


def run_case(root: Path, policy: str, request_num: int, concurrency: int) -> dict:
    run_dir = root / policy
    trace_dir = run_dir / "trace"
    event_path = run_dir / "migration_events.jsonl"
    run_dir.mkdir(parents=True, exist_ok=True)
    paper.ROOT = root
    paper.CONFIG_DIR = root / "00_config"
    paper.STEP_DIR = root / "04_step_summaries"
    paper.FINAL_DIR = root / "05_final"
    for path in (paper.CONFIG_DIR, paper.STEP_DIR, paper.FINAL_DIR, root / "workloads"):
        path.mkdir(parents=True, exist_ok=True)
    paper.configure_policies()
    paper.configure_base(
        4, request_num, concurrency, root / "runs", root / "workloads",
        1024, 2048, event_path,
    )
    base.ALLOW_CONTENDED = True
    os.environ["LLUMNIX_ENABLE_STRUCTURED_MIGRATION_LOG"] = "1"
    processes = []
    probes = []
    started = time.time()
    try:
        occupied = base.gpu_compute_processes()
        (run_dir / "gpu_processes_before.json").write_text(
            json.dumps(occupied, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        base.shutil.rmtree(base.NAMING_DIR, ignore_errors=True)
        base.shutil.rmtree(base.LOG_DIR, ignore_errors=True)
        base.LOG_DIR.mkdir(parents=True, exist_ok=True)
        processes, commands = base.start_stack(policy)
        base.wait_for_discovery_instances(120)
        probes = base.wait_for_kvt_listeners(120)
        time.sleep(5)
        command = [
            sys.executable, str(REPO / "tools" / "trace_benchmark_client.py"),
            "--url", "http://127.0.0.1:18089/v1/completions",
            "--model", str(base.MODEL_PATH),
            "--request-num", str(request_num),
            "--concurrency", str(concurrency),
            "--max-tokens", "32",
            "--enable-token-trace",
            "--token-trace-output-dir", str(trace_dir),
            "--token-trace-mode", "client_stream",
            "--experiment-group", "migration_on" if base.POLICIES[policy]["migration"] else "no_migration",
            "--strategy", policy,
        ]
        completed = subprocess.run(command, cwd=REPO, text=True, capture_output=True, check=False)
        (run_dir / "trace_client.stdout.log").write_text(completed.stdout, encoding="utf-8")
        (run_dir / "trace_client.stderr.log").write_text(completed.stderr, encoding="utf-8")
        base.copy_logs(run_dir)
        trace_path = trace_dir / "request_token_trace.jsonl"
        if not event_path.exists():
            event_path.write_text("", encoding="utf-8")
        analysis_dir = run_dir / "analysis"
        analysis = subprocess.run([
            sys.executable, str(REPO / "tools" / "analyze_downtime_slo.py"),
            "--request-token-trace", str(trace_path),
            "--migration-log", str(event_path),
            "--output-dir", str(analysis_dir),
        ], cwd=REPO, text=True, capture_output=True, check=False)
        (run_dir / "analysis.stdout.log").write_text(analysis.stdout, encoding="utf-8")
        (run_dir / "analysis.stderr.log").write_text(analysis.stderr, encoding="utf-8")
        summary = json.loads((analysis_dir / "downtime_slo_summary.json").read_text(encoding="utf-8"))
        summary.update({
            "policy": policy,
            "request_num": request_num,
            "concurrency": concurrency,
            "client_exit_code": completed.returncode,
            "analysis_exit_code": analysis.returncode,
            "duration_s": time.time() - started,
            "launch_commands": commands,
        })
        (run_dir / "smoke_summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
        return summary
    finally:
        base.cleanup_processes(processes)
        for probe in probes:
            probe.close()
        if base.LOG_DIR.exists():
            archive = run_dir / "logs_after_cleanup"
            archive.mkdir(exist_ok=True)
            for path in base.LOG_DIR.glob("*.log"):
                shutil.copy2(path, archive / path.name)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-root", type=Path)
    parser.add_argument("--request-num", type=int, default=50)
    parser.add_argument("--concurrency", type=int, default=20)
    args = parser.parse_args()
    root = args.output_root or REPO / "results" / f"trace_downtime_slo_smoke_{datetime.now():%Y%m%d_%H%M%S}"
    root.mkdir(parents=True, exist_ok=False)
    summaries = {}
    for policy in ("no_migration", "load_sr"):
        summaries[policy] = run_case(root, policy, args.request_num, args.concurrency)
    (root / "smoke_all.json").write_text(json.dumps(summaries, ensure_ascii=False, indent=2), encoding="utf-8")
    print(root)


if __name__ == "__main__":
    main()
