#!/usr/bin/env python3
"""Generate and validate the fixed Config E complex mixed workload trace."""

from __future__ import annotations

import argparse
import json
import math
import random
import statistics
from collections import Counter, defaultdict
from pathlib import Path


SEED = 20260905
REQUEST_NUM = 1200
CONCURRENCY = 40
ARRIVAL_CV = 4.0
MEAN_INTERARRIVAL_S = 0.02

REQUEST_TYPES = {
    "short_chat": ((64, 256), (32, 64)),
    "medium_mixed": ((512, 1024), (128, 256)),
    "prefill_heavy": ((2048, 4096), (32, 64)),
    "decode_kv_heavy": ((64, 256), (256, 512)),
    "heavy_long_long": ((2048, 4096), (256, 512)),
}

OVERALL_RATIOS = {
    "short_chat": 0.30,
    "medium_mixed": 0.10,
    "prefill_heavy": 0.25,
    "decode_kv_heavy": 0.25,
    "heavy_long_long": 0.10,
}

PHASES = [
    ("phase_0_warmup", 120, {
        "short_chat": 0.60, "medium_mixed": 0.30, "prefill_heavy": 0.05,
        "decode_kv_heavy": 0.05, "heavy_long_long": 0.00,
    }),
    ("phase_1_kv_buildup", 300, {
        "short_chat": 0.20, "medium_mixed": 0.10, "prefill_heavy": 0.05,
        "decode_kv_heavy": 0.55, "heavy_long_long": 0.10,
    }),
    ("phase_2_long_input_burst", 300, {
        "short_chat": 0.15, "medium_mixed": 0.10, "prefill_heavy": 0.60,
        "decode_kv_heavy": 0.05, "heavy_long_long": 0.10,
    }),
    ("phase_3_mixed_high_pressure", 360, {
        "short_chat": 0.25, "medium_mixed": 0.15, "prefill_heavy": 0.25,
        "decode_kv_heavy": 0.25, "heavy_long_long": 0.10,
    }),
    ("phase_4_tail_cleanup", 120, {
        "short_chat": 0.10, "medium_mixed": 0.20, "prefill_heavy": 0.10,
        "decode_kv_heavy": 0.20, "heavy_long_long": 0.40,
    }),
]


def exact_counts(total: int, ratios: dict[str, float]) -> dict[str, int]:
    raw = {name: total * ratio for name, ratio in ratios.items()}
    counts = {name: math.floor(value) for name, value in raw.items()}
    remaining = total - sum(counts.values())
    order = sorted(raw, key=lambda name: (raw[name] - counts[name], name), reverse=True)
    for name in order[:remaining]:
        counts[name] += 1
    return counts


def coefficient_of_variation(values: list[float]) -> float | None:
    if len(values) < 2:
        return None
    mean = statistics.mean(values)
    return statistics.pstdev(values) / mean if mean else None


def generate() -> list[dict]:
    rng = random.Random(SEED)
    rows = []
    arrival = 0.0
    gamma_shape = 1.0 / (ARRIVAL_CV ** 2)
    gamma_scale = MEAN_INTERARRIVAL_S / gamma_shape
    index = 0
    for phase, phase_total, ratios in PHASES:
        request_types = []
        for request_type, count in exact_counts(phase_total, ratios).items():
            request_types.extend([request_type] * count)
        rng.shuffle(request_types)
        for request_type in request_types:
            input_range, output_range = REQUEST_TYPES[request_type]
            arrival += rng.gammavariate(gamma_shape, gamma_scale)
            input_len = rng.randint(*input_range)
            output_len = rng.randint(*output_range)
            rows.append({
                "request_id": f"config_e_{index:04d}",
                "arrival_time": round(arrival, 9),
                "input_len": input_len,
                "output_len": output_len,
                "max_tokens": output_len,
                "request_type": request_type,
                "phase": phase,
                "priority": "normal",
                "seed": SEED,
            })
            index += 1
    return rows


def validate(rows: list[dict]) -> tuple[list[str], dict]:
    errors = []
    required = {
        "request_id", "arrival_time", "input_len", "output_len", "max_tokens",
        "request_type", "phase", "priority", "seed",
    }
    if len(rows) != REQUEST_NUM:
        errors.append(f"request_num expected {REQUEST_NUM}, got {len(rows)}")
    if any(required - row.keys() for row in rows):
        errors.append("one or more requests are missing required fields")
    if len({row["request_id"] for row in rows}) != len(rows):
        errors.append("request_id values are not unique")
    if any(row["max_tokens"] != row["output_len"] for row in rows):
        errors.append("max_tokens and output_len differ")
    if any(rows[i]["arrival_time"] > rows[i + 1]["arrival_time"] for i in range(len(rows) - 1)):
        errors.append("arrival_time is not monotonic")

    overall = Counter(row["request_type"] for row in rows)
    target_overall = exact_counts(REQUEST_NUM, OVERALL_RATIOS)

    by_phase = defaultdict(Counter)
    for row in rows:
        by_phase[row["phase"]][row["request_type"]] += 1
    for phase, phase_total, ratios in PHASES:
        expected = exact_counts(phase_total, ratios)
        actual = {name: by_phase[phase][name] for name in REQUEST_TYPES}
        if actual != expected:
            errors.append(f"{phase} counts mismatch: actual={actual}, expected={expected}")

    intervals = [rows[i]["arrival_time"] - rows[i - 1]["arrival_time"] for i in range(1, len(rows))]
    stats = {
        "overall_counts": dict(overall),
        "target_overall_counts": target_overall,
        "phase_counts": {
            phase: {name: by_phase[phase][name] for name in REQUEST_TYPES}
            for phase, _, _ in PHASES
        },
        "arrival": {
            "min_s": min(row["arrival_time"] for row in rows),
            "max_s": max(row["arrival_time"] for row in rows),
            "mean_interarrival_s": statistics.mean(intervals),
            "sample_cv": coefficient_of_variation(intervals),
            "target_cv": ARRIVAL_CV,
        },
    }
    return errors, stats


def range_stats(rows: list[dict], field: str) -> str:
    values = [int(row[field]) for row in rows]
    return f"min={min(values)}, mean={statistics.mean(values):.2f}, max={max(values)}"


def write_design(path: Path, rows: list[dict], errors: list[str], stats: dict) -> None:
    phase_lines = []
    for phase, total, _ in PHASES:
        phase_lines.append(f"| {phase} | {total} | `{json.dumps(stats['phase_counts'][phase], ensure_ascii=False)}` |")
    type_lines = []
    for request_type in REQUEST_TYPES:
        subset = [row for row in rows if row["request_type"] == request_type]
        type_lines.append(
            f"| {request_type} | {len(subset)} | {len(subset)/len(rows):.2%} | "
            f"{range_stats(subset, 'input_len')} | {range_stats(subset, 'output_len')} |"
        )
    error_lines = "\n".join(f"- {error}" for error in errors) or "- 无"
    status = "FAIL（禁止启动实验）" if errors else "PASS"
    path.write_text(f"""# complex_mixed_high_load Config E 设计与校验

- 数据集：`complex_mixed_high_load_config_e`
- 总请求数：{REQUEST_NUM}
- 并发：{CONCURRENCY}
- 随机种子：{SEED}
- 到达间隔目标 CV：{ARRIVAL_CV}
- 到达间隔均值：{MEAN_INTERARRIVAL_S} 秒（请求中未给定，生成器显式固定）
- 比例口径：以五阶段比例为准（用户于 2026-09-05 确认）
- 校验状态：**{status}**

## Phase 计数

| Phase | 请求数 | 各类型计数 |
|---|---:|---|
{chr(10).join(phase_lines)}

## 请求类型统计

| request_type | 数量 | 实际占比 | input_len | output_len |
|---|---:|---:|---|---|
{chr(10).join(type_lines)}

## Arrival 统计

- 最早到达：{stats['arrival']['min_s']:.6f} 秒
- 最晚到达：{stats['arrival']['max_s']:.6f} 秒
- 样本平均到达间隔：{stats['arrival']['mean_interarrival_s']:.6f} 秒
- 样本 CV：{stats['arrival']['sample_cv']:.4f}

## 整体比例校验

- Phase 加权实际计数：`{json.dumps(stats['overall_counts'], ensure_ascii=False)}`
- 原始总体比例对应计数：`{json.dumps(stats['target_overall_counts'], ensure_ascii=False)}`
- 说明：原始总体比例与阶段比例不相容，本实验按确认后的五阶段比例执行；该差异不再视为校验失败。

## 校验问题

{error_lines}
""", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--output-dir", type=Path,
        default=Path("results/complex_mixed_high_load/config_e"),
    )
    args = parser.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    rows = generate()
    errors, stats = validate(rows)
    trace_path = args.output_dir / "complex_mixed_high_load_config_e.jsonl"
    with trace_path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")
    design_path = args.output_dir / "COMPLEX_WORKLOAD_CONFIG_E_DESIGN.md"
    write_design(design_path, rows, errors, stats)
    print(json.dumps({
        "trace": str(trace_path), "design": str(design_path),
        "valid": not errors, "errors": errors,
    }, ensure_ascii=False, indent=2))
    return 0 if not errors else 2


if __name__ == "__main__":
    raise SystemExit(main())

