#!/usr/bin/env python3
"""Parse 5090 Llumnix migration experiment logs into CSV summaries.

The script is intentionally log-driven: it does not import Llumnix, so it can
run after experiments without requiring a live Ray/vLLM service.
"""

from __future__ import annotations

import argparse
import ast
import csv
import math
import re
from collections import defaultdict
from pathlib import Path
from statistics import mean, median


EVENT_RE = re.compile(
    r"MIGRATION_EVENT\s+event=(?P<event>\S+)\s+"
    r"request_id=(?P<request_id>\S+)\s+"
    r"instance_id=(?P<instance_id>\S+)\s+"
    r"dst_instance_id=(?P<dst_instance_id>\S+)\s+"
    r"timestamp=(?P<timestamp>[0-9.]+)\s*(?P<extra>.*)"
)
KEY_VALUE_RE = re.compile(r"(\w+)=([^ ]+)")
BENCHMARK_NAME_RE = re.compile(
    r"benchmark(?:_4inst)?(?:_50req)?(?:_len(?P<len>\d+))?(?:_qps(?P<qps>[0-9.]+))?\.log$"
)


def percentile(values: list[float], q: float) -> float | str:
    if not values:
        return ""
    values = sorted(values)
    idx = int(math.ceil(q * len(values))) - 1
    return values[max(0, min(idx, len(values) - 1))]


def number_or_blank(value: float | str) -> float | str:
    if value == "":
        return ""
    return value


def stats(prefix: str, values: list[float]) -> dict[str, float | int | str]:
    values = [v for v in values if isinstance(v, (int, float)) and math.isfinite(v)]
    if not values:
        return {
            f"{prefix}_count": 0,
            f"{prefix}_mean": "",
            f"{prefix}_p50": "",
            f"{prefix}_p95": "",
            f"{prefix}_p99": "",
            f"{prefix}_min": "",
            f"{prefix}_max": "",
        }
    return {
        f"{prefix}_count": len(values),
        f"{prefix}_mean": mean(values),
        f"{prefix}_p50": median(values),
        f"{prefix}_p95": percentile(values, 0.95),
        f"{prefix}_p99": percentile(values, 0.99),
        f"{prefix}_min": min(values),
        f"{prefix}_max": max(values),
    }


def parse_python_list(text: str, name: str) -> list:
    match = re.search(rf"{re.escape(name)}=(\[.*?\])", text, re.S)
    if not match:
        return []
    payload = match.group(1)
    payload = re.sub(r"np\.float64\(([^)]+)\)", r"\1", payload)
    try:
        return ast.literal_eval(payload)
    except Exception:
        return []


def parse_benchmark_file(path: Path) -> dict[str, object]:
    text = path.read_text(encoding="utf-8", errors="replace")
    row: dict[str, object] = {"benchmark_log": path.name}

    name_match = BENCHMARK_NAME_RE.search(path.name)
    if name_match:
        row["seq_len"] = int(name_match.group("len")) if name_match.group("len") else ""
        row["target_qps"] = float(name_match.group("qps")) if name_match.group("qps") else ""

    match = re.search(
        r"backend\s+\S+\s+dur_s\s+([0-9.]+)\s+tokens_per_s\s+([0-9.]+)\s+qps\s+([0-9.]+)",
        text,
    )
    if match:
        row["duration_s"] = float(match.group(1))
        row["throughput_tok_s"] = float(match.group(2))
        row["actual_qps"] = float(match.group(3))

    match = re.search(
        r"successful_responses\s+(\d+)\s+prompt_token_count\s+(\d+)\s+response_token_count\s+(\d+)",
        text,
    )
    if match:
        row["successful_responses"] = int(match.group(1))
        row["prompt_token_count"] = int(match.group(2))
        row["response_token_count"] = int(match.group(3))

    match = re.search(
        r"median_token_latency=([0-9.nan-]+),\s+median_e2e_latency=([0-9.nan-]+),\s+"
        r"median_inference_latency=([0-9.nan-]+)",
        text,
    )
    if match:
        row["median_token_latency_s"] = float(match.group(1))
        row["median_e2e_latency_s"] = float(match.group(2))
        row["median_inference_latency_s"] = float(match.group(3))

    e2e = [float(v) for v in parse_python_list(text, "all_e2e_latencies")]
    response_lens = [int(v) for v in parse_python_list(text, "all_response_lens")]
    row.update(stats("e2e_latency_s", e2e))
    if response_lens:
        row["response_len_min"] = min(response_lens)
        row["response_len_max"] = max(response_lens)
        row["response_len_mean"] = mean(response_lens)
    return row


def parse_migration_events(paths: list[Path]) -> list[dict[str, object]]:
    events: list[dict[str, object]] = []
    for path in paths:
        for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
            match = EVENT_RE.search(line)
            if not match:
                continue
            extra = dict(KEY_VALUE_RE.findall(match.group("extra")))
            event = {
                "event_log": path.name,
                "event": match.group("event"),
                "request_id": match.group("request_id"),
                "instance_id": match.group("instance_id"),
                "dst_instance_id": match.group("dst_instance_id"),
                "timestamp": float(match.group("timestamp")),
            }
            event.update(extra)
            events.append(event)
    events.sort(key=lambda item: float(item["timestamp"]))
    return events


def infer_seq_len_from_name(path_name: str) -> int | str:
    match = re.search(r"(?:len|events_)(\d+)", path_name)
    if match:
        return int(match.group(1))
    return ""


def build_migration_rows(events: list[dict[str, object]]) -> list[dict[str, object]]:
    grouped: dict[str, list[dict[str, object]]] = defaultdict(list)
    for event in events:
        grouped[str(event["request_id"])].append(event)

    rows: list[dict[str, object]] = []
    for request_id, req_events in grouped.items():
        starts = [e for e in req_events if e["event"] == "migrate_out_stage_a"]
        stops = [e for e in req_events if e["event"] == "migrate_out_stage_0"]
        commits = [e for e in req_events if e["event"] == "migrate_in"]
        if not (starts and stops and commits):
            continue
        for start in starts:
            start_t = float(start["timestamp"])
            stop = next((e for e in stops if float(e["timestamp"]) >= start_t), None)
            if stop is None:
                continue
            stop_t = float(stop["timestamp"])
            commit = next((e for e in commits if float(e["timestamp"]) >= stop_t), None)
            if commit is None:
                continue
            commit_t = float(commit["timestamp"])
            rows.append(
                {
                    "seq_len": infer_seq_len_from_name(str(start["event_log"])),
                    "request_id": request_id,
                    "src_instance_id": start["instance_id"],
                    "dst_instance_id": start["dst_instance_id"],
                    "migrate_out_stage_a_ts": start_t,
                    "migrate_out_stage_0_ts": stop_t,
                    "migrate_in_ts": commit_t,
                    "stage_a_to_stage_0_ms": (stop_t - start_t) * 1000,
                    "downtime_ms": (commit_t - stop_t) * 1000,
                    "total_migration_ms": (commit_t - start_t) * 1000,
                    "stage_block_num": stop.get("stage_block_num", ""),
                    "pre_stage_num_blocks": stop.get("pre_stage_num_blocks", ""),
                    "event_log": start["event_log"],
                }
            )
            break
    return rows


def summarize_migrations(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    by_len: dict[object, list[dict[str, object]]] = defaultdict(list)
    for row in rows:
        by_len[row["seq_len"]].append(row)

    summary = []
    for seq_len, group in sorted(by_len.items(), key=lambda item: str(item[0])):
        downtime = [float(r["downtime_ms"]) for r in group]
        total = [float(r["total_migration_ms"]) for r in group]
        prep = [float(r["stage_a_to_stage_0_ms"]) for r in group]
        pairs = {f"{r['src_instance_id']}->{r['dst_instance_id']}" for r in group}
        instances = set()
        for row in group:
            instances.add(str(row["src_instance_id"]))
            instances.add(str(row["dst_instance_id"]))
        out = {
            "seq_len": seq_len,
            "complete_migrations": len(group),
            "src_dst_pair_count": len(pairs),
            "src_dst_pairs": ";".join(sorted(pairs)),
            "participating_instance_count": len([i for i in instances if i and i != "None"]),
        }
        out.update(stats("downtime_ms", downtime))
        out.update(stats("total_migration_ms", total))
        out.update(stats("stage_a_to_stage_0_ms", prep))
        summary.append(out)
    return summary


def write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fieldnames = list(rows[0].keys())
    for row in rows[1:]:
        for key in row:
            if key not in fieldnames:
                fieldnames.append(key)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--exp-dir", required=True, type=Path)
    parser.add_argument(
        "--server-log",
        action="append",
        type=Path,
        default=[],
        help="Optional server log(s) containing MIGRATION_EVENT lines.",
    )
    args = parser.parse_args()

    exp_dir = args.exp_dir
    benchmark_logs = sorted(exp_dir.glob("benchmark*.log"))
    event_logs = sorted(exp_dir.glob("migration_events*.log")) + args.server_log

    benchmark_rows = [parse_benchmark_file(path) for path in benchmark_logs]
    events = parse_migration_events([path for path in event_logs if path.exists()])
    migration_rows = build_migration_rows(events)
    migration_summary = summarize_migrations(migration_rows)

    write_csv(exp_dir / "benchmark_summary.csv", benchmark_rows)
    write_csv(exp_dir / "migration_raw.csv", migration_rows)
    write_csv(exp_dir / "migration_summary.csv", migration_summary)
    write_csv(exp_dir / "migration_events_raw.csv", events)

    print("wrote", exp_dir / "benchmark_summary.csv")
    print("wrote", exp_dir / "migration_raw.csv")
    print("wrote", exp_dir / "migration_summary.csv")
    print("wrote", exp_dir / "migration_events_raw.csv")
    print("benchmark_logs", len(benchmark_logs))
    print("migration_events", len(events))
    print("complete_migrations", len(migration_rows))


if __name__ == "__main__":
    main()
