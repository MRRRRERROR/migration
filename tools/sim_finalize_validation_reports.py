#!/usr/bin/env python3
"""Recompute simulator validation summaries from existing local result logs."""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path


def dispatch_counts(server_log: Path) -> dict[str, int]:
    text = server_log.read_text(errors="replace")
    pattern = r"global_scheduler.py:\d+\] dispatch request [0-9a-f]{8,} to instance ([0-9a-f]{8,})"
    counts: dict[str, int] = {}
    for instance_id in re.findall(pattern, text):
        counts[instance_id] = counts.get(instance_id, 0) + 1
    return counts


def error_summary(out_dir: Path) -> dict[str, int | list[str]]:
    paths = [out_dir / "server_console.log", out_dir / "client_console.log"]
    lines: list[str] = []
    for path in paths:
        if not path.exists():
            continue
        for line in path.read_text(errors="replace").splitlines():
            if re.search(r"Traceback|ERROR|RuntimeError|ActorDiedError|Error in engine loop|EngineCore", line):
                lines.append(line)
    fatal_lines = [
        line for line in lines
        if "libcuda.so.1" not in line and "Failed to import from vllm._C" not in line
    ]
    return {
        "raw_error_like_lines": len(lines),
        "fatal_or_runtime_lines": len(fatal_lines),
        "first_fatal_or_runtime_lines": fatal_lines[:5],
    }


def refresh_distribution_report(out_dir: Path) -> None:
    counts = dispatch_counts(out_dir / "server_console.log")
    data = {
        "dispatch_counts": counts,
        "total_dispatch_records": sum(counts.values()),
        "instance_count_seen": len(counts),
    }
    (out_dir / "instance_distribution.json").write_text(json.dumps(data, indent=2), encoding="utf-8")
    bench = json.loads((out_dir / "concurrent_benchmark.json").read_text(encoding="utf-8"))
    errors = error_summary(out_dir)
    lines = [
        "# SIM_4INST_DISTRIBUTION",
        "",
        f"- 结果目录: `{out_dir}`",
        "- 配置: 4 instances, 100 requests, concurrency=20, stable_short, no_migration",
        "- 调度策略: `load`",
        f"- 成功请求: {bench['successful_responses']}/{bench['requests']}",
        f"- QPS: {bench['qps']:.4f}",
        f"- 吞吐: {bench['throughput_tok_s']:.4f} tok/s",
        f"- median E2E: {bench['median_e2e_s']}",
        f"- fatal/runtime 行数: {errors['fatal_or_runtime_lines']}",
        "",
        "## 实例分布",
        "",
        "| instance_id | 请求数 |",
        "| --- | ---: |",
    ]
    for key, value in sorted(counts.items()):
        lines.append(f"| `{key}` | {value} |")
    max_count = max(counts.values(), default=0)
    dominant_ratio = max_count / max(1, sum(counts.values()))
    judgement = (
        f"仍然高度集中，主实例占比 {dominant_ratio:.2%}。"
        if dominant_ratio >= 0.80
        else f"未集中到单实例，主实例占比 {dominant_ratio:.2%}。"
    )
    lines += ["", "## 判断", "", judgement]
    (out_dir / "SIM_4INST_DISTRIBUTION.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def policy_status(policy_dir: Path) -> dict:
    bench_path = policy_dir / "concurrent_benchmark.json"
    counts = dispatch_counts(policy_dir / "server_console.log") if (policy_dir / "server_console.log").exists() else {}
    distribution = {
        "dispatch_counts": counts,
        "total_dispatch_records": sum(counts.values()),
        "instance_count_seen": len(counts),
    }
    (policy_dir / "instance_distribution.json").write_text(json.dumps(distribution, indent=2), encoding="utf-8")
    errors = error_summary(policy_dir)
    if bench_path.exists():
        bench = json.loads(bench_path.read_text(encoding="utf-8"))
        return {
            "status": "完成" if bench["successful_responses"] == bench["requests"] else "请求未全成功",
            "summary": (
                f"成功 {bench['successful_responses']}/{bench['requests']}，"
                f"QPS {bench['qps']:.4f}，吞吐 {bench['throughput_tok_s']:.4f} tok/s，"
                f"参与实例 {len(counts)} 个"
            ),
            "errors": errors,
            "migration_lines": migration_line_count(policy_dir),
        }
    run_status = (policy_dir / "run_status.txt").read_text(errors="replace").strip()
    return {
        "status": "未运行",
        "summary": run_status,
        "errors": errors,
        "migration_lines": migration_line_count(policy_dir),
    }


def migration_line_count(out_dir: Path) -> int:
    total = 0
    for name in ("server_console.log", "client_console.log", "run_status.txt"):
        path = out_dir / name
        if path.exists():
            total += sum(1 for line in path.read_text(errors="replace").splitlines() if "migrat" in line.lower())
    return total


def refresh_policy_summary(root: Path) -> None:
    summary = root / "SIM_POLICY_SMOKE_SUMMARY.md"
    lines = [
        "# SIM_POLICY_SMOKE_SUMMARY",
        "",
        f"- 根目录: `{root}`",
        "- 配置: 4 instances, 100 requests, concurrency=20, stable_short",
        "- 说明: TPOT-tail 与 composite_proposed 不在当前本地 vLLM simulator entrypoint 暴露的 CLI 策略中；本轮不伪造等价策略。",
    ]
    for policy in ("no_migration", "load_sr", "tpot_tail", "composite_proposed"):
        policy_dir = root / f"policy_{policy}"
        status = policy_status(policy_dir)
        lines += [
            "",
            f"## {policy}",
            "",
            f"- 状态: {status['status']}",
            f"- 结果目录: `{policy_dir}`",
            f"- 摘要: {status['summary']}",
            f"- migration 关键词行数: {status['migration_lines']}",
            f"- fatal/runtime 行数: {status['errors']['fatal_or_runtime_lines']}",
        ]
        first = status["errors"]["first_fatal_or_runtime_lines"]
        if first:
            lines.append(f"- 首条 fatal/runtime: `{first[0]}`")
    summary.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", required=True, type=Path)
    args = parser.parse_args()
    refresh_distribution_report(args.root / "distribution_no_migration")
    refresh_policy_summary(args.root)
    print(args.root / "distribution_no_migration" / "SIM_4INST_DISTRIBUTION.md")
    print(args.root / "SIM_POLICY_SMOKE_SUMMARY.md")


if __name__ == "__main__":
    main()
