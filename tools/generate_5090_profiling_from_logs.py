#!/usr/bin/env python3
"""Build scheduler profiling JSON from existing RTX 5090 gateway logs."""

from __future__ import annotations

import argparse
import json
import math
import re
import statistics
from datetime import datetime, timezone
from pathlib import Path


LINE_RE = re.compile(
    r"status:200,.*?RT:(?P<rt>\d+)ms,TTFT:(?P<ttft>\d+)ms,"
    r"ITL:(?P<itl>\d+)ms\|(?P<itl_tokens>\d+).*?"
    r"input_tokens:(?P<input>\d+),output_tokens:(?P<output>\d+);.*?"
    r"work:(?P<work>\d+);"
)

BATCH_GRID = (1, 2, 4, 8, 16, 32, 50, 64)
TOKENS_PER_REQUEST_GRID = (8, 16, 32, 64, 128, 256, 512, 1024)
PREFILL_GRID = (1, 8, 16, 32, 64, 128, 256, 512, 1024, 2048)


def percentile(values: list[float], q: float) -> float:
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    index = (len(ordered) - 1) * q
    lower = math.floor(index)
    upper = math.ceil(index)
    if lower == upper:
        return ordered[lower]
    return ordered[lower] * (upper - index) + ordered[upper] * (index - lower)


def stats(values: list[float]) -> dict:
    return {
        "mean": statistics.mean(values),
        "std": statistics.pstdev(values),
        "min": min(values),
        "max": max(values),
        "p50": percentile(values, 0.50),
        "p95": percentile(values, 0.95),
        "p99": percentile(values, 0.99),
        "raw_data": values,
    }


def log_distance(a: float, b: float) -> float:
    return abs(math.log2(max(a, 1.0)) - math.log2(max(b, 1.0)))


def nearest(samples: list[dict], key, target, count: int = 100) -> list[dict]:
    return sorted(samples, key=lambda row: key(row, target))[: min(count, len(samples))]


def parse_logs(paths: list[Path]) -> list[dict]:
    samples = []
    for path in paths:
        with path.open(encoding="utf-8", errors="replace") as handle:
            for line in handle:
                match = LINE_RE.search(line)
                if not match:
                    continue
                row = {key: int(value) for key, value in match.groupdict().items()}
                row["batch_size"] = max(1, row["work"])
                row["tokens_per_request"] = max(8, row["input"] + row["output"])
                row["prefill_tokens"] = max(1, row["input"] * row["batch_size"])
                row["source"] = str(path)
                samples.append(row)
    return samples


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-root", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()

    logs = sorted(args.source_root.glob("03_runs/*/Hybrid_no_migration/run_*/gateway.log"))
    samples = parse_logs(logs)
    if not samples:
        raise SystemExit("No request-completion samples found")

    args.output_dir.mkdir(parents=True, exist_ok=True)
    generated_at = datetime.now(timezone.utc).isoformat()
    metadata = {
        "model": "Qwen2.5-7B-Instruct",
        "timestamp": generated_at,
        "description": (
            "Derived from completed requests in 4x RTX 5090 Hybrid_no_migration runs; "
            "each grid point is the median of up to 100 nearest measured samples."
        ),
        "source_root": str(args.source_root),
        "source_logs": [str(path) for path in logs],
        "sample_count": len(samples),
    }

    ttft_results = []
    for tokens in PREFILL_GRID:
        rows = nearest(
            samples,
            lambda row, target: log_distance(row["prefill_tokens"], target),
            tokens,
        )
        values = [float(row["ttft"]) for row in rows]
        ttft_results.append({"tokens_num": tokens, **stats(values)})

    tpot_results = []
    for batch_size in BATCH_GRID:
        for tokens_per_request in TOKENS_PER_REQUEST_GRID:
            rows = nearest(
                samples,
                lambda row, target: (
                    log_distance(row["batch_size"], target[0])
                    + log_distance(row["tokens_per_request"], target[1])
                ),
                (batch_size, tokens_per_request),
            )
            values = [float(row["itl"]) for row in rows]
            tpot_results.append(
                {
                    "batch_size": batch_size,
                    "tokens_per_request": tokens_per_request,
                    **stats(values),
                }
            )

    (args.output_dir / "ttft_5090_qwen25_7b.json").write_text(
        json.dumps({"metadata": metadata, "results": ttft_results}, indent=2) + "\n",
        encoding="utf-8",
    )
    (args.output_dir / "tpot_5090_qwen25_7b.json").write_text(
        json.dumps({"metadata": metadata, "results": tpot_results}, indent=2) + "\n",
        encoding="utf-8",
    )

    low = [row["itl"] for row in samples if row["batch_size"] <= 8]
    high = [row["itl"] for row in samples if row["batch_size"] >= 32]
    audit = [
        "# RTX 5090 Profiling Data Audit",
        "",
        f"- Generated: `{generated_at}`",
        f"- Source logs: `{len(logs)}`",
        f"- Parsed request samples: `{len(samples)}`",
        "- Source condition: `Hybrid_no_migration`, 4x RTX 5090, Qwen2.5-7B-Instruct",
        "- Aggregation: up to 100 nearest measured samples per grid point; p50 feeds the scheduler",
        f"- Low-load ITL p50 (batch <= 8): `{percentile(low, 0.5):.3f} ms`",
        f"- High-load ITL p50 (batch >= 32): `{percentile(high, 0.5):.3f} ms`",
        "- TTFT grid: `" + ", ".join(map(str, PREFILL_GRID)) + "`",
        "- TPOT batch grid: `" + ", ".join(map(str, BATCH_GRID)) + "`",
        "- TPOT context grid: `" + ", ".join(map(str, TOKENS_PER_REQUEST_GRID)) + "`",
        "",
        "This file is derived from real completed requests. It is not an imported H20 profile and does not fabricate migration evidence.",
    ]
    (args.output_dir / "PROFILING_DATA_AUDIT.md").write_text("\n".join(audit) + "\n", encoding="utf-8")
    print(json.dumps({
        "source_logs": len(logs),
        "samples": len(samples),
        "low_load_itl_p50_ms": percentile(low, 0.5),
        "high_load_itl_p50_ms": percentile(high, 0.5),
        "output_dir": str(args.output_dir),
    }, indent=2))


if __name__ == "__main__":
    main()
