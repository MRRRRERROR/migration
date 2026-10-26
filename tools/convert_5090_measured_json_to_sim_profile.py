#!/usr/bin/env python3
"""Convert measured RTX 5090 TTFT/TPOT JSON into Llumnix simulator pkl."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import cloudpickle

from llumnix.backends.profiling import (
    LatencyMemData,
    ProfilingResult,
    SimCacheConfig,
    SimParallelConfig,
)


def load_json(path: Path) -> dict:
    with path.open(encoding="utf-8") as handle:
        return json.load(handle)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-dir", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--model-name", default="Qwen2.5-7B-Instruct")
    parser.add_argument("--migration-bandwidth", type=float, default=1.0)
    parser.add_argument("--swap-bandwidth", type=float, default=32.0)
    parser.add_argument("--num-gpu-blocks-util-05", type=int, default=8192)
    parser.add_argument("--num-gpu-blocks-util-09", type=int, default=16384)
    args = parser.parse_args()

    ttft_path = args.input_dir / "ttft_5090_qwen25_7b.json"
    tpot_path = args.input_dir / "tpot_5090_qwen25_7b.json"
    ttft = load_json(ttft_path)
    tpot = load_json(tpot_path)

    prefill_latency = {
        int(row["tokens_num"]): [float(row["p50"])]
        for row in ttft["results"]
    }
    decode_latency = {
        (int(row["batch_size"]), int(row["tokens_per_request"])): [float(row["p50"])]
        for row in tpot["results"]
    }

    cache_dict = {}
    for utilization, blocks in (
        (0.5, args.num_gpu_blocks_util_05),
        (0.9, args.num_gpu_blocks_util_09),
    ):
        for max_tokens in (2048, 4096, 8000, 8192):
            cache_dict[SimCacheConfig(utilization, 16, max_tokens)] = blocks

    metadata = {
        "source": "measured_5090_qwen25_7b_json",
        "ttft_path": str(ttft_path),
        "tpot_path": str(tpot_path),
        "ttft_metadata": ttft.get("metadata", {}),
        "tpot_metadata": tpot.get("metadata", {}),
        "latency_unit": "ms",
        "prefill_value": "p50 TTFT",
        "decode_value": "p50 TPOT/ITL",
    }

    latency_mem = LatencyMemData(
        prefill_latency=prefill_latency,
        decode_latency=decode_latency,
        cache_dict=cache_dict,
        migration_bandwidth=args.migration_bandwidth,
        swap_bandwidth=args.swap_bandwidth,
        metadata=metadata,
    )

    result = ProfilingResult(args.model_name, {SimParallelConfig(1, 1): latency_mem})
    result.fit_from_database(SimParallelConfig(1, 1))

    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("wb") as handle:
        cloudpickle.dump({args.model_name: result}, handle)

    print(json.dumps({
        "output": str(args.output),
        "model_name": args.model_name,
        "prefill_points": len(prefill_latency),
        "decode_points": len(decode_latency),
        "cache_configs": len(cache_dict),
        "latency_unit": "ms",
    }, indent=2))


if __name__ == "__main__":
    main()
