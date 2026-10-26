#!/usr/bin/env python3
"""Scan migrate-out-threshold values with the measured RTX 5090 simulator profile."""

from __future__ import annotations

import json
import os
import re
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import run_existing_simulator_matrix as sim  # noqa: E402


THRESHOLDS = ("64", "128", "256", "512", "1024", "2048", "4096", "8192")


def main() -> None:
    root = sim.REPO / "results" / f"wsl_sim_measured_migration_threshold_scan_{time.strftime('%Y%m%d_%H%M%S')}"
    root.mkdir(parents=True, exist_ok=True)

    measured_profile = sim.REPO / "profiles/5090_qwen25_7b_measured/qwen2p5_7b_5090_profile_measured.pkl"
    os.environ["SIM_PROFILE_PATH"] = str(measured_profile)
    sim.PROFILE = measured_profile
    sim.WORKLOADS["long_output_migration"]["max_tokens"] = lambda _i: 512

    rows = []
    stopped = False
    for threshold in THRESHOLDS:
        name = f"flood_long_sr_threshold_{threshold}"
        summary = sim.run_case(
            root / "runs",
            name,
            "long_output_migration",
            100,
            100,
            "flood",
            migration=True,
            pair_policy="balanced",
            request_policy="SR",
            migrate_out_threshold=threshold,
            polling_interval="0.01",
        )
        run_dir = root / "runs" / name
        server_text = (run_dir / "server_console.log").read_text(errors="replace")
        begin_count = len(re.findall(r"begin migrate out", server_text, re.I))
        send_cache_count = len(re.findall(r"send_cache", server_text, re.I))
        migration = summary.get("migration", {})
        row = {
            "threshold": threshold,
            "run_dir": str(run_dir),
            "success": f"{summary.get('success_count', 0)}/{summary.get('requests', 0)}",
            "qps": summary.get("qps"),
            "throughput_tok_s": summary.get("throughput_tok_s"),
            "triggered_migration_count": migration.get("triggered_migration_count", 0),
            "begin_migrate_out_log_count": begin_count,
            "successful_migration_count": migration.get("successful_migration_count", 0),
            "failed_migration_count": migration.get("failed_migration_count", 0),
            "migration_end_count": migration.get("migration_end_count", 0),
            "send_cache_log_count": send_cache_count,
            "pairs": migration.get("source_destination_pairs", {}),
            "instance_request_counts": summary.get("instance_request_counts", {}),
            "fatal_runtime_count": summary.get("fatal_runtime_count"),
            "first_fatal_runtime": summary.get("first_fatal_runtime", []),
        }
        rows.append(row)
        (root / "scan_rows.json").write_text(json.dumps(rows, indent=2, ensure_ascii=False), encoding="utf-8")
        if row["triggered_migration_count"] > 0 or begin_count > 0:
            stopped = True
            break

    lines = [
        "# SIM_MEASURED_MIGRATION_THRESHOLD_SCAN",
        "",
        f"- profile: `{measured_profile}`",
        "- instances: 4",
        "- workload: long_output_migration",
        "- dispatch-policy: flood",
        "- request-migration-policy: SR",
        "- pair-migration-policy: balanced",
        "- polling-interval: 0.01",
        "- request_num: 100",
        "- concurrency: 100",
        "- max_tokens: 512",
        f"- stopped_early: {stopped}",
        "",
        "| threshold | success | QPS | throughput tok/s | triggered | begin migrate out | successful | failed | migration_end | send_cache logs | pairs | instance 请求数 | fatal |",
        "| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- | ---: |",
    ]
    for row in rows:
        lines.append(
            "| {threshold} | {success} | {qps:.4f} | {throughput_tok_s:.4f} | {triggered_migration_count} | {begin_migrate_out_log_count} | {successful_migration_count} | {failed_migration_count} | {migration_end_count} | {send_cache_log_count} | `{pairs}` | `{counts}` | {fatal_runtime_count} |".format(
                threshold=row["threshold"],
                success=row["success"],
                qps=row["qps"] or 0.0,
                throughput_tok_s=row["throughput_tok_s"] or 0.0,
                triggered_migration_count=row["triggered_migration_count"],
                begin_migrate_out_log_count=row["begin_migrate_out_log_count"],
                successful_migration_count=row["successful_migration_count"],
                failed_migration_count=row["failed_migration_count"],
                migration_end_count=row["migration_end_count"],
                send_cache_log_count=row["send_cache_log_count"],
                pairs=json.dumps(row["pairs"], ensure_ascii=False),
                counts=json.dumps(row["instance_request_counts"], ensure_ascii=False),
                fatal_runtime_count=row["fatal_runtime_count"],
            )
        )
    (root / "SIM_MEASURED_MIGRATION_THRESHOLD_SCAN.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    manifest = {
        "root": str(root),
        "report": str(root / "SIM_MEASURED_MIGRATION_THRESHOLD_SCAN.md"),
        "stopped_early": stopped,
        "last_row": rows[-1] if rows else None,
    }
    (root / "run_manifest.json").write_text(json.dumps(manifest, indent=2, ensure_ascii=False), encoding="utf-8")
    print(json.dumps(manifest, indent=2, ensure_ascii=False), flush=True)


if __name__ == "__main__":
    main()
