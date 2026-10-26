#!/usr/bin/env python3
"""Join token traces with migration events and compute downtime and SLO metrics."""

from __future__ import annotations

import argparse
import csv
import html
import json
import math
import re
import statistics
from collections import defaultdict
from pathlib import Path
from typing import Any, Iterable


EVENT_MAP = {
    "migration_initiated": "migration_start",
    "migration_start": "migration_start",
    "final_suspend_begin": "migration_suspend",
    "migration_suspend": "migration_suspend",
    "dst_resume": "migration_resume",
    "migration_resume": "migration_resume",
    "migration_done": "migration_end",
    "migration_end": "migration_end",
    "migration_fail": "migration_fail",
    "invalid_migration_endpoint": "migration_fail",
    "stale_migration_update": "migration_fail",
    "migration_abort": "migration_abort",
}


def percentile(values: Iterable[float], q: float) -> float | None:
    clean = sorted(float(v) for v in values if v is not None and math.isfinite(float(v)))
    if not clean:
        return None
    position = (len(clean) - 1) * q / 100.0
    low, high = math.floor(position), math.ceil(position)
    if low == high:
        return clean[low]
    return clean[low] + (clean[high] - clean[low]) * (position - low)


def stats(values: Iterable[float]) -> dict[str, float | None]:
    clean = [float(v) for v in values if v is not None and math.isfinite(float(v))]
    return {
        "mean": statistics.mean(clean) if clean else None,
        "median": statistics.median(clean) if clean else None,
        "p95": percentile(clean, 95),
        "p99": percentile(clean, 99),
        "max": max(clean) if clean else None,
        "count": len(clean),
    }


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    rows = []
    for line_number, line in enumerate(path.read_text(encoding="utf-8", errors="replace").splitlines(), 1):
        line = line.strip()
        if not line:
            continue
        try:
            rows.append(json.loads(line))
        except json.JSONDecodeError as exc:
            raise ValueError(f"invalid JSONL at {path}:{line_number}: {exc}") from exc
    return rows


def read_migration_events(path: Path) -> list[dict[str, Any]]:
    events = []
    marker = re.compile(r"LLUMNIX_MIGRATION_EVENT\s+(\{.*\})")
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        candidate = line.strip()
        match = marker.search(candidate)
        if match:
            candidate = match.group(1)
        elif not candidate.startswith("{"):
            continue
        try:
            event = json.loads(candidate)
        except json.JSONDecodeError:
            continue
        raw_name = event.get("event") or event.get("event_name")
        normalized = EVENT_MAP.get(raw_name)
        if not normalized or not event.get("request_id"):
            continue
        timestamp_ms = event.get("timestamp_ms")
        if timestamp_ms is None and event.get("timestamp") is not None:
            timestamp_ms = float(event["timestamp"]) * 1000.0
        if timestamp_ms is None and event.get("timestamp_ns") is not None:
            timestamp_ms = float(event["timestamp_ns"]) / 1_000_000.0
        if timestamp_ms is None:
            continue
        event = dict(event)
        event["event"] = normalized
        event["timestamp_ms"] = float(timestamp_ms)
        events.append(event)
    return events


def migration_windows(events: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for event in events:
        grouped[str(event["request_id"])].append(event)
    result: dict[str, list[dict[str, Any]]] = {}
    for request_id, request_events in grouped.items():
        request_events.sort(key=lambda row: row["timestamp_ms"])
        open_windows: list[dict[str, Any]] = []
        complete: list[dict[str, Any]] = []
        for event in request_events:
            if event["event"] == "migration_start":
                window = {"start": event, "suspend": None, "resume": None, "end": None}
                open_windows.append(window)
            elif event["event"] in ("migration_suspend", "migration_resume") and open_windows:
                open_windows[-1][event["event"].removeprefix("migration_")] = event
            elif event["event"] in ("migration_end", "migration_fail", "migration_abort") and open_windows:
                window = open_windows.pop(0)
                window["end"] = event
                window["outcome"] = event["event"]
                complete.append(window)
        result[request_id] = complete
    return result


def windows_for_trace(
    trace: dict[str, Any], windows: dict[str, list[dict[str, Any]]]
) -> list[dict[str, Any]]:
    request_id = str(trace.get("request_id"))
    if request_id in windows:
        return windows[request_id]
    # vLLM EngineCore appends a numeric DP-rank suffix to the OpenAI response
    # ID. Accept it only when the normalized match is unique.
    candidates = [value for key, value in windows.items() if re.fullmatch(re.escape(request_id) + r"-\d+", key)]
    return candidates[0] if len(candidates) == 1 else []


def _slo_fields(ttft: float | None, p99_tpot: float | None, row: dict[str, Any], ttft_limit: float, tpot_limit: float, prefix: str) -> dict[str, bool]:
    ttft_bad = ttft is None or ttft > ttft_limit
    tpot_bad = p99_tpot is None or p99_tpot > tpot_limit
    request_bad = ttft_bad or tpot_bad or bool(row.get("aborted")) or bool(row.get("timeout")) or not bool(row.get("finished"))
    return {
        f"{prefix}ttft_slo_violated": ttft_bad,
        f"{prefix}tpot_slo_violated": tpot_bad,
        f"{prefix}request_slo_violated": request_bad,
    }


def analyze_request(trace: dict[str, Any], windows: list[dict[str, Any]], args: argparse.Namespace) -> dict[str, Any]:
    rel_tokens = [float(v) for v in trace.get("token_timestamps_ms") or []]
    intervals = [float(v) for v in trace.get("token_intervals_ms") or []]
    start_wall = trace.get("request_start_wall_time_ms")
    alignment_ok = start_wall is not None and bool(trace.get("strict_per_token_trace"))
    absolute_tokens = [float(start_wall) + v for v in rel_tokens] if alignment_ok else []
    internal_times: list[float] = []
    server_downtime_times: list[float] = []
    visible_times: list[float] = []
    extra_times: list[float] = []
    pre_tpots: list[float] = []
    last_pre_rel = None
    first_post_rel = None
    details = []
    for window in windows:
        start_ms = window["start"]["timestamp_ms"]
        end_ms = window["end"]["timestamp_ms"]
        internal = max(0.0, end_ms - start_ms)
        internal_times.append(internal)
        suspend = window.get("suspend")
        resume = window.get("resume")
        server_downtime = None
        server_downtime_unavailable_reason = None
        if window.get("outcome") != "migration_end":
            server_downtime_unavailable_reason = "migration did not complete successfully"
        elif suspend is None:
            server_downtime_unavailable_reason = "missing final_suspend_begin"
        elif resume is None:
            server_downtime_unavailable_reason = "missing dst_resume"
        else:
            suspend_ms = float(suspend["timestamp_ms"])
            resume_ms = float(resume["timestamp_ms"])
            if resume_ms < suspend_ms:
                server_downtime_unavailable_reason = "dst_resume precedes final_suspend_begin"
            else:
                server_downtime = resume_ms - suspend_ms
                server_downtime_times.append(server_downtime)
        pre_indices = [i for i, value in enumerate(absolute_tokens) if value <= start_ms]
        post_indices = [i for i, value in enumerate(absolute_tokens) if value >= end_ms]
        visible = pre_tpot = extra = None
        if pre_indices and post_indices:
            pre_index, post_index = pre_indices[-1], post_indices[0]
            visible = absolute_tokens[post_index] - absolute_tokens[pre_index]
            interval_end = max(0, pre_index)
            candidates = intervals[max(0, interval_end - args.pre_migration_tpot_window):interval_end]
            pre_tpot = statistics.mean(candidates) if candidates else None
            extra = max(0.0, visible - pre_tpot) if pre_tpot is not None else None
            last_pre_rel = rel_tokens[pre_index]
            first_post_rel = rel_tokens[post_index]
            visible_times.append(visible)
            if pre_tpot is not None:
                pre_tpots.append(pre_tpot)
            if extra is not None:
                extra_times.append(extra)
        details.append({
            "migration_start_wall_ms": start_ms,
            "final_suspend_begin_wall_ms": suspend.get("timestamp_ms") if suspend else None,
            "destination_resume_wall_ms": resume.get("timestamp_ms") if resume else None,
            "migration_end_wall_ms": end_ms,
            "migration_internal_time_ms": internal,
            "server_downtime_ms": server_downtime,
            "server_downtime_unavailable_reason": server_downtime_unavailable_reason,
            "visible_downtime_ms": visible,
            "extra_downtime_ms": extra,
            "pre_migration_tpot_ms": pre_tpot,
            "outcome": window.get("outcome"),
        })
    ttft = trace.get("ttft_ms")
    mean_tpot = statistics.mean(intervals) if intervals else None
    p95_tpot, p99_tpot = percentile(intervals, 95), percentile(intervals, 99)
    result = {
        "request_id": trace.get("request_id"),
        "request_type": trace.get("request_type", ""),
        "priority": trace.get("priority", ""),
        "migrated": bool(windows),
        "migration_count": len(windows),
        "ttft_ms": ttft,
        "mean_tpot_ms": mean_tpot,
        "p95_tpot_ms": p95_tpot,
        "p99_tpot_ms": p99_tpot,
        "e2e_latency_ms": trace.get("e2e_latency_ms"),
        "migration_internal_time_ms": sum(internal_times) if internal_times else None,
        "server_downtime_ms": (
            sum(server_downtime_times)
            if windows and len(server_downtime_times) == len(windows)
            else None
        ),
        "visible_downtime_ms": sum(visible_times) if len(visible_times) == len(windows) and windows else None,
        "extra_downtime_ms": sum(extra_times) if len(extra_times) == len(windows) and windows else None,
        "pre_migration_last_token_ts_ms": last_pre_rel,
        "post_migration_first_token_ts_ms": first_post_rel,
        "pre_migration_tpot_ms": statistics.mean(pre_tpots) if pre_tpots else None,
        "finished": bool(trace.get("finished")),
        "aborted": bool(trace.get("aborted")),
        "timeout": bool(trace.get("timeout")),
        "clock_alignment_valid": alignment_ok,
        "downtime_unavailable_reason": None if not windows or (alignment_ok and len(visible_times) == len(windows)) else "missing clock alignment or bracketing tokens",
        "server_downtime_unavailable_reason": (
            None
            if not windows or len(server_downtime_times) == len(windows)
            else "one or more migrations lack ordered final_suspend_begin/dst_resume events"
        ),
        "migration_details": details,
    }
    result.update(_slo_fields(ttft, p99_tpot, result, args.ttft_slo_ms, args.tpot_slo_ms, ""))
    result.update(_slo_fields(ttft, p99_tpot, result, args.strict_ttft_slo_ms, args.strict_tpot_slo_ms, "strict_"))
    result.update(_slo_fields(ttft, p99_tpot, result, args.loose_ttft_slo_ms, args.loose_tpot_slo_ms, "loose_"))
    result["token_intervals_ms"] = intervals
    return result


def rate(rows: list[dict[str, Any]], key: str) -> float | None:
    return sum(bool(row.get(key)) for row in rows) / len(rows) if rows else None


def group_summary(rows: list[dict[str, Any]], slo_prefix: str = "") -> dict[str, Any]:
    return {
        "count": len(rows),
        "ttft_p99_ms": percentile((row.get("ttft_ms") for row in rows), 99),
        "e2e_p99_ms": percentile((row.get("e2e_latency_ms") for row in rows), 99),
        "mean_tpot_ms": stats(row.get("mean_tpot_ms") for row in rows)["mean"],
        "p99_tpot_ms": percentile((row.get("p99_tpot_ms") for row in rows), 99),
        "slo_violation_rate": rate(rows, f"{slo_prefix}request_slo_violated"),
        "migrated_count": sum(bool(row.get("migrated")) for row in rows),
        "migrated_ratio": rate(rows, "migrated"),
        "finish_rate": rate(rows, "finished"),
    }


def build_summary(rows: list[dict[str, Any]], event_count: int, args: argparse.Namespace) -> dict[str, Any]:
    migrated = [row for row in rows if row["migrated"]]
    non_migrated = [row for row in rows if not row["migrated"]]
    server_downtime_events = [
        detail["server_downtime_ms"]
        for row in migrated
        for detail in row.get("migration_details", [])
        if detail.get("server_downtime_ms") is not None
    ]
    summary: dict[str, Any] = {
        "total_requests": len(rows),
        "finished_requests": sum(row["finished"] for row in rows),
        "aborted_requests": sum(row["aborted"] for row in rows),
        "timeout_requests": sum(row["timeout"] for row in rows),
        "success_rate": rate(rows, "finished"),
        "migrated_request_count": len(migrated),
        "migration_count": sum(row["migration_count"] for row in rows),
        "parsed_migration_event_count": event_count,
        "visible_downtime_ms": stats(row.get("visible_downtime_ms") for row in migrated),
        # Paper-style downtime is an event-level metric: one sample for each
        # successful migration, even if one request migrates more than once.
        "server_downtime_ms": stats(server_downtime_events),
        "server_downtime_per_request_ms": stats(
            row.get("server_downtime_ms") for row in migrated
        ),
        "extra_downtime_ms": stats(row.get("extra_downtime_ms") for row in migrated),
        "migration_internal_time_ms": stats(row.get("migration_internal_time_ms") for row in migrated),
        "downtime_computable_request_count": sum(row.get("visible_downtime_ms") is not None for row in migrated),
        "server_downtime_computable_migration_count": len(server_downtime_events),
        "server_downtime_computable_request_count": sum(
            row.get("server_downtime_ms") is not None for row in migrated
        ),
        "server_downtime_definition": "dst_resume - final_suspend_begin",
    }
    all_intervals = [value for row in rows for value in row.get("token_intervals_ms", [])]
    limits = {
        "strict": args.strict_tpot_slo_ms,
        "normal": args.tpot_slo_ms,
        "loose": args.loose_tpot_slo_ms,
    }
    for name, prefix in (("strict", "strict_"), ("normal", ""), ("loose", "loose_")):
        threshold_key = f"{prefix}tpot_slo_violated"
        threshold = limits[name]
        summary[f"{name}_ttft_violation_rate"] = rate(rows, f"{prefix}ttft_slo_violated")
        summary[f"{name}_tpot_interval_violation_rate"] = (
            sum(value > threshold for value in all_intervals) / len(all_intervals) if all_intervals else None
        )
        summary[f"{name}_request_tpot_violation_rate"] = rate(rows, threshold_key)
        summary[f"{name}_request_slo_violation_rate"] = rate(rows, f"{prefix}request_slo_violated")
    summary["migrated_requests"] = group_summary(migrated)
    summary["non_migrated_requests"] = group_summary(non_migrated)
    for label, subset in (("migrated", migrated), ("non_migrated", non_migrated)):
        summary[f"{label}_request_slo_violation_rate"] = rate(subset, "request_slo_violated")
    by_type: dict[str, list[dict[str, Any]]] = defaultdict(list)
    by_priority: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        by_type[str(row.get("request_type") or "unspecified")].append(row)
        by_priority[str(row.get("priority") or "unspecified")].append(row)
    summary["by_request_type"] = {key: group_summary(value) for key, value in sorted(by_type.items())}
    summary["by_priority"] = {key: group_summary(value) for key, value in sorted(by_priority.items())}
    return summary


def flatten(prefix: str, value: Any, output: list[tuple[str, Any]]) -> None:
    if isinstance(value, dict):
        for key, child in value.items():
            flatten(f"{prefix}.{key}" if prefix else key, child, output)
    elif not isinstance(value, list):
        output.append((prefix, value))


def write_outputs(rows: list[dict[str, Any]], summary: dict[str, Any], output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    with (output_dir / "downtime_slo_per_request.jsonl").open("w", encoding="utf-8") as handle:
        for row in rows:
            stored = dict(row)
            stored.pop("token_intervals_ms", None)
            handle.write(json.dumps(stored, ensure_ascii=False) + "\n")
    (output_dir / "downtime_slo_summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    flat: list[tuple[str, Any]] = []
    flatten("", summary, flat)
    with (output_dir / "downtime_slo_summary.csv").open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(("metric", "value"))
        writer.writerows(flat)
    markdown = ["# 停机时间与 SLO 汇总", "", "| 指标 | 值 |", "|---|---:|"]
    markdown.extend(f"| `{key}` | {value} |" for key, value in flat)
    (output_dir / "downtime_slo_summary.md").write_text("\n".join(markdown) + "\n", encoding="utf-8")
    table_rows = "".join(f"<tr><td>{html.escape(key)}</td><td>{html.escape(str(value))}</td></tr>" for key, value in flat)
    document = f"""<!doctype html><html lang=\"zh-CN\"><head><meta charset=\"utf-8\"><title>停机时间与 SLO</title>
<style>body{{font-family:Arial,'Microsoft YaHei',sans-serif;margin:32px;color:#182230}}table{{border-collapse:collapse;width:100%}}th,td{{padding:9px 12px;border:1px solid #d8dee8;text-align:left}}th{{background:#26384d;color:white}}tr:nth-child(even){{background:#f5f7f9}}</style></head>
<body><h1>迁移停机时间与 SLO 汇总</h1><p>服务端停机时间按 <code>dst_resume - final_suspend_begin</code> 计算；用户可见停顿是迁移前后相邻 token 的客户端到达间隔；内部迁移时长是完整迁移事件窗口。三者分别统计，不互相替代。</p><table><thead><tr><th>指标</th><th>值</th></tr></thead><tbody>{table_rows}</tbody></table></body></html>"""
    (output_dir / "downtime_slo_table.html").write_text(document, encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--request-token-trace", type=Path, required=True)
    parser.add_argument("--migration-log", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--ttft-slo-ms", type=float, default=1000.0)
    parser.add_argument("--tpot-slo-ms", type=float, default=25.0)
    parser.add_argument("--strict-ttft-slo-ms", type=float, default=500.0)
    parser.add_argument("--strict-tpot-slo-ms", type=float, default=14.5)
    parser.add_argument("--loose-ttft-slo-ms", type=float, default=2000.0)
    parser.add_argument("--loose-tpot-slo-ms", type=float, default=50.0)
    parser.add_argument("--pre-migration-tpot-window", type=int, default=8)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    traces = read_jsonl(args.request_token_trace)
    events = read_migration_events(args.migration_log)
    windows = migration_windows(events)
    rows = [analyze_request(trace, windows_for_trace(trace, windows), args) for trace in traces]
    summary = build_summary(rows, len(events), args)
    summary["slo_thresholds_ms"] = {
        "strict": {"ttft": args.strict_ttft_slo_ms, "tpot": args.strict_tpot_slo_ms},
        "normal": {"ttft": args.ttft_slo_ms, "tpot": args.tpot_slo_ms},
        "loose": {"ttft": args.loose_ttft_slo_ms, "tpot": args.loose_tpot_slo_ms},
    }
    write_outputs(rows, summary, args.output_dir)
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
