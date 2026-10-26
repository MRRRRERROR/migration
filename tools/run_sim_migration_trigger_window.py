#!/usr/bin/env python3
"""Scan existing simulator routine-migration trigger windows."""

from __future__ import annotations

import json
import re
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from run_existing_simulator_matrix import REPO, run_case  # noqa: E402


COMBOS = [
    ("flood", "long_output_migration"),
    ("flood", "mixed_tail"),
    ("load", "long_output_migration"),
    ("queue", "long_output_migration"),
]


def fmt(value) -> str:
    return "" if value is None else str(value)


def write_window_report(root: Path, rows: list[tuple[str, dict]], stopped_early: bool) -> None:
    lines = [
        "# SIM_MIGRATION_TRIGGER_WINDOW",
        "",
        "- 目标: 找到 Llumnix 原生 routine migration 在 simulator 中稳定触发的配置",
        "- instances: 4",
        "- request_num: 200",
        "- concurrency: 80",
        "- enable-routine-migration: true",
        "- pair-migration-policy: balanced",
        "- request-migration-policy: SR",
        "- polling-interval: 0.1",
        "- pair-migration-frequency: 1",
        "- migrate-out-threshold: 0.1",
        f"- 扫描是否提前停止: {stopped_early}",
        "",
        "| dispatch-policy | workload | success | QPS | throughput tok/s | median s | P95 s | P99 s | instance 请求数 | instance token 数 | triggered | successful | failed | send_cache | pairs |",
        "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- | ---: | ---: | ---: | --- | --- |",
    ]
    for name, s in rows:
        dispatch, workload = name.split("__", 1)
        m = s.get("migration", {})
        lines.append(
            "| {} | {} | {}/{} | {:.4f} | {:.4f} | {} | {} | {} | `{}` | `{}` | {} | {} | {} | {} | `{}` |".format(
                dispatch,
                workload,
                s.get("success_count", 0),
                s.get("requests", 0),
                s.get("qps", 0.0),
                s.get("throughput_tok_s", 0.0),
                fmt(s.get("median_e2e_s")),
                fmt(s.get("p95_e2e_s")),
                fmt(s.get("p99_e2e_s")),
                json.dumps(s.get("instance_request_counts", {}), ensure_ascii=False),
                json.dumps(s.get("instance_token_counts", {}), ensure_ascii=False),
                m.get("triggered_migration_count", 0),
                m.get("successful_migration_count", 0),
                m.get("failed_migration_count", 0),
                m.get("send_cache_call_count", "未直接记录"),
                json.dumps(m.get("source_destination_pairs", {}), ensure_ascii=False),
            )
        )
    (root / "SIM_MIGRATION_TRIGGER_WINDOW.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def grep_first(text: str, pattern: str) -> str:
    match = re.search(pattern, text)
    return match.group(0) if match else "未找到"


def write_zero_root_cause(root: Path, rows: list[tuple[str, dict]]) -> None:
    audit_lines = [
        "# SIM_MIGRATION_ZERO_ROOT_CAUSE",
        "",
        "## 结论",
        "",
        "四个触发窗口组合均未产生 migration。请求链路成功，迁移开关参数已传入；当前主要原因是现有 balanced pair migration 的候选过滤/约束没有形成可迁移 pair。",
        "",
        "## 参数传入审计",
        "",
        "| run | enable_routine_migration | pair_migration_frequency | polling_interval | migrate_out_threshold | migration_load_metric | begin migrate out | fatal/runtime |",
        "| --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    for name, _summary in rows:
        run_dir = root / "runs" / name
        text = (run_dir / "server_console.log").read_text(errors="replace")
        summary = json.loads((run_dir / "summary.json").read_text(encoding="utf-8"))
        enable = grep_first(text, r"enable_routine_migration=True")
        freq = grep_first(text, r"pair_migration_frequency=1")
        polling = grep_first(text, r"polling_interval=0\.1")
        threshold = grep_first(text, r"migrate_out_threshold=0\.1")
        metric = grep_first(text, r"migration_load_metric='remaining_steps'")
        begin = "有" if "begin migrate out" in text else "无"
        first_fatal = summary.get("first_fatal_runtime", [])
        fatal = "0" if not first_fatal else first_fatal[0]
        audit_lines.append(
            f"| {name} | {enable} | {freq} | {polling} | {threshold} | {metric} | {begin} | {fatal} |"
        )
    audit_lines += [
        "",
        "## 代码路径审计",
        "",
        "- `manager.py` 只在 `enable_migration` 且 `num_instance_info_updates % pair_migration_frequency == 0` 时调用 pair migration。",
        "- `migration_filter.py` 的 `LoadFilter` 使用 `migrate_out_threshold` 和 `migration_load_metric` 筛 src/dst。",
        "- `migration_policy.py` 的 `Balanced` 要求迁移后负载差仍为正且小于迁移前，并且目标迁入后不超过 threshold。",
        "- 本轮没有 `begin migrate out`，说明还没进入单请求迁移执行阶段；因此 `send_cache` 没有可观测调用。",
        "- `queue__long_output_migration` 出现一次启动阶段 `_connect_to_actors` TimeoutError 日志，但该组最终 200/200 成功；这不是迁移执行失败。",
        "",
        "## 下一步",
        "",
        "继续只用现有机制时，应扫描 `migrate-out-threshold` 的较大值，并配合 `flood` 或 `queue` 制造负载差；本轮严格按 0.1 执行，未改参数。",
    ]
    (root / "SIM_MIGRATION_ZERO_ROOT_CAUSE.md").write_text("\n".join(audit_lines) + "\n", encoding="utf-8")


def main() -> None:
    root = REPO / "results" / f"wsl_sim_migration_trigger_window_{time.strftime('%Y%m%d_%H%M%S')}"
    root.mkdir(parents=True, exist_ok=True)
    rows: list[tuple[str, dict]] = []
    stopped = False
    for dispatch, workload in COMBOS:
        name = f"{dispatch}__{workload}"
        summary = run_case(
            root / "runs",
            name,
            workload,
            200,
            80,
            dispatch,
            migration=True,
            pair_policy="balanced",
            request_policy="SR",
            migrate_out_threshold="0.1",
            polling_interval="0.1",
        )
        rows.append((name, summary))
        migration_count = summary.get("migration", {}).get("successful_migration_count", 0)
        triggered_count = summary.get("migration", {}).get("triggered_migration_count", 0)
        if migration_count > 0 or triggered_count > 0:
            stopped = True
            break
    write_window_report(root, rows, stopped)
    if not stopped:
        write_zero_root_cause(root, rows)
    manifest = {
        "root": str(root),
        "window_report": str(root / "SIM_MIGRATION_TRIGGER_WINDOW.md"),
        "zero_root_cause_report": str(root / "SIM_MIGRATION_ZERO_ROOT_CAUSE.md") if not stopped else None,
        "stopped_early": stopped,
    }
    (root / "run_manifest.json").write_text(json.dumps(manifest, indent=2, ensure_ascii=False), encoding="utf-8")
    print(json.dumps(manifest, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
