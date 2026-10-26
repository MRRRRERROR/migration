#!/usr/bin/env python3
"""Run one extreme existing-simulator migration trigger test."""

from __future__ import annotations

import csv
import json
import re
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import run_existing_simulator_matrix as sim  # noqa: E402


def main() -> None:
    root = sim.REPO / "results" / f"wsl_sim_extreme_migration_trigger_{time.strftime('%Y%m%d_%H%M%S')}"
    root.mkdir(parents=True, exist_ok=True)
    sim.WORKLOADS["long_output_migration"]["max_tokens"] = lambda _i: 512
    summary = sim.run_case(
        root,
        "extreme_flood_long_output_migration",
        "long_output_migration",
        100,
        100,
        "flood",
        migration=True,
        pair_policy="balanced",
        request_policy="SR",
        migrate_out_threshold="0.0",
        polling_interval="0.01",
    )
    run_dir = root / "extreme_flood_long_output_migration"
    server_text = (run_dir / "server_console.log").read_text(errors="replace")
    pair_lines = [
        line for line in server_text.splitlines()
        if re.search(r"pair migration|pair_migration|migrate_out_threshold|enable_routine_migration|begin migrate", line, re.I)
    ]
    load_lines = [
        line for line in server_text.splitlines()
        if re.search(r"migration_load_metric|remaining_steps|dispatch request to .*load", line)
    ]
    state_stats: dict[str, dict[str, int]] = {}
    csvs = list(run_dir.glob("*.log_instance.csv"))
    if csvs:
        with csvs[0].open(newline="", encoding="utf-8") as f:
            rows = list(csv.DictReader(f))
        for row in rows:
            inst = row.get("instance_id", "unknown")
            stats = state_stats.setdefault(inst, {"max_running": 0, "max_waiting": 0, "samples": 0})
            stats["samples"] += 1
            for src, dst in (("num_running_requests", "max_running"), ("num_waiting_requests", "max_waiting")):
                try:
                    stats[dst] = max(stats[dst], int(float(row.get(src) or 0)))
                except ValueError:
                    pass
    migration = summary.get("migration", {})
    report = [
        "# SIM_EXTREME_MIGRATION_TRIGGER",
        "",
        "- instances: 4",
        "- dispatch-policy: flood",
        "- enable-routine-migration: true",
        "- pair-migration-policy: balanced",
        "- request-migration-policy: SR",
        "- polling-interval: 0.01",
        "- pair-migration-frequency: 1",
        "- migrate-out-threshold: 0.0",
        "- workload: long_output_migration",
        "- request_num: 100",
        "- concurrency: 100",
        "- max_tokens: 512",
        "",
        "## 结果",
        "",
        f"- success count: {summary.get('success_count')}/{summary.get('requests')}",
        f"- QPS: {summary.get('qps'):.4f}",
        f"- throughput: {summary.get('throughput_tok_s'):.4f} tok/s",
        f"- median E2E: {summary.get('median_e2e_s')}",
        f"- P95 E2E: {summary.get('p95_e2e_s')}",
        f"- P99 E2E: {summary.get('p99_e2e_s')}",
        f"- instance request counts: `{json.dumps(summary.get('instance_request_counts', {}), ensure_ascii=False)}`",
        f"- instance token counts: `{json.dumps(summary.get('instance_token_counts', {}), ensure_ascii=False)}`",
        f"- instance running/waiting max: `{json.dumps(state_stats, ensure_ascii=False)}`",
        f"- triggered_migration_count: {migration.get('triggered_migration_count', 0)}",
        f"- successful_migration_count: {migration.get('successful_migration_count', 0)}",
        f"- failed_migration_count: {migration.get('failed_migration_count', 0)}",
        f"- begin migrate out count: {migration.get('triggered_migration_count', 0)}",
        f"- send_cache calls: {migration.get('send_cache_call_count')}",
        f"- manager pair migration related log lines: {len(pair_lines)}",
        f"- source/destination pairs: `{json.dumps(migration.get('source_destination_pairs', {}), ensure_ascii=False)}`",
        f"- fatal/runtime count: {summary.get('fatal_runtime_count')}",
        "",
        "## 负载日志样例",
        "",
    ]
    report.extend(f"- `{line[:500]}`" for line in load_lines[:30])
    report += ["", "## pair/migration 日志样例", ""]
    report.extend(f"- `{line[:500]}`" for line in pair_lines[:40])
    (root / "SIM_EXTREME_MIGRATION_TRIGGER.md").write_text("\n".join(report) + "\n", encoding="utf-8")
    manifest = {
        "root": str(root),
        "extreme_report": str(root / "SIM_EXTREME_MIGRATION_TRIGGER.md"),
        "triggered": migration.get("triggered_migration_count", 0),
        "successful": migration.get("successful_migration_count", 0),
    }
    (root / "run_manifest.json").write_text(json.dumps(manifest, indent=2, ensure_ascii=False), encoding="utf-8")
    print(json.dumps(manifest, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
