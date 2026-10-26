#!/usr/bin/env python3
"""Write a simulator profile sanity-check report for the local WSL smoke run."""

from __future__ import annotations

import argparse
import json
import math
import statistics
from pathlib import Path

import cloudpickle

from llumnix.backends.profiling import (
    SimParallelConfig,
    _pad_to_alignment,
    model_decode,
    model_prefill,
)


def pct(values: list[float], percentile: float) -> float:
    if not values:
        return math.nan
    ordered = sorted(values)
    rank = (len(ordered) - 1) * percentile / 100.0
    lo = math.floor(rank)
    hi = math.ceil(rank)
    if lo == hi:
        return ordered[lo]
    return ordered[lo] * (hi - rank) + ordered[hi] * (rank - lo)


def load_latency_info(path: Path) -> dict:
    payload = json.loads(path.read_text())
    if isinstance(payload, list) and payload:
        return payload[0]
    return payload


def profile_latency_ms(latency_mem, input_len: int, output_len: int, decode_batch_size: int) -> dict:
    padded_prefill = _pad_to_alignment(input_len, 8)
    prefill_ms = (
        latency_mem.prefill_latency[padded_prefill][0]
        if padded_prefill in latency_mem.prefill_latency
        else float(model_prefill(padded_prefill, *latency_mem.prefill_model_params))
    )
    per_step_ms = []
    for step in range(output_len):
        total_context = input_len + step
        decode_key = (_pad_to_alignment(decode_batch_size, 8), total_context)
        if decode_key in latency_mem.decode_latency:
            value = latency_mem.decode_latency[decode_key][0]
        else:
            value = float(model_decode(decode_key, *latency_mem.decode_model_params))
        per_step_ms.append(value)
    return {
        "padded_prefill_len": padded_prefill,
        "decode_batch_size": _pad_to_alignment(decode_batch_size, 8),
        "prefill_ms": prefill_ms,
        "decode_total_ms": sum(per_step_ms),
        "decode_avg_ms": statistics.mean(per_step_ms) if per_step_ms else 0.0,
        "theoretical_e2e_ms": prefill_ms + sum(per_step_ms),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--profile", required=True, type=Path)
    parser.add_argument("--result-dir", required=True, type=Path)
    parser.add_argument("--input-len", type=int, default=64)
    parser.add_argument("--output-len", type=int, default=32)
    parser.add_argument("--decode-batch-size", type=int, default=8)
    args = parser.parse_args()

    with args.profile.open("rb") as f:
        db = cloudpickle.load(f)
    profile_result = db["Qwen2.5-7B-Instruct"]
    latency_mem = profile_result.para_dict[SimParallelConfig(1, 1)]
    theoretical = profile_latency_ms(
        latency_mem, args.input_len, args.output_len, args.decode_batch_size
    )

    info = load_latency_info(args.result_dir / "benchmark_4inst_latency_info.json")
    request_latencies_s = info.get("request_latencies", [])
    prefill_ms = info.get("prefill_token_latencies", [])
    decode_ms = info.get("decode_token_latencies", [])
    all_decode_ms = info.get("all_decode_token_latencies", [])

    measured = {
        "successful_requests": len(info.get("request_ids", [])),
        "throughput_tok_s": info.get("throughput"),
        "median_e2e_s": statistics.median(request_latencies_s) if request_latencies_s else math.nan,
        "p95_e2e_s": pct(request_latencies_s, 95),
        "median_ttft_ms": statistics.median(prefill_ms) if prefill_ms else math.nan,
        "median_tpot_ms": statistics.median(decode_ms) if decode_ms else math.nan,
        "median_all_decode_step_ms": statistics.median(all_decode_ms) if all_decode_ms else math.nan,
        "response_token_count": sum(info.get("request_lens", [])),
    }

    report = f"""# SIM_PROFILE_SANITY_CHECK

## 输入文件

- profile: `{args.profile}`
- benchmark result: `{args.result_dir}`
- benchmark latency json: `{args.result_dir / "benchmark_4inst_latency_info.json"}`

## 单位确认

- `llumnix.backends.vllm.sim_executor.SimGPUExecutor.execute_model_async` 从 profile 读取 latency 后执行 `asyncio.sleep(latency / 1000)`。
- 因此 profile 中的 prefill/decode latency 在 simulator 中按 **ms** 解释。
- benchmark 的 `prefill_token_latencies` / `decode_token_latencies` 是 **ms**，`request_latencies` 是 **s**。

## 理论耗时

固定输入长度 `{args.input_len}`，输出长度 `{args.output_len}`，decode batch size `{args.decode_batch_size}`。

| 指标 | 数值 | 单位 |
| --- | ---: | --- |
| padded prefill len | {theoretical["padded_prefill_len"]} | token |
| padded decode batch size | {theoretical["decode_batch_size"]} | request |
| profile prefill | {theoretical["prefill_ms"]:.6f} | ms |
| profile decode total | {theoretical["decode_total_ms"]:.6f} | ms |
| profile decode avg/step | {theoretical["decode_avg_ms"]:.6f} | ms |
| profile theoretical e2e | {theoretical["theoretical_e2e_ms"]:.6f} | ms |

## benchmark 对比

| 指标 | 数值 | 单位 |
| --- | ---: | --- |
| 成功请求数 | {measured["successful_requests"]} | req |
| 吞吐 | {measured["throughput_tok_s"]:.4f} | tok/s |
| median TTFT | {measured["median_ttft_ms"]:.4f} | ms |
| median TPOT/request | {measured["median_tpot_ms"]:.4f} | ms |
| median decode step | {measured["median_all_decode_step_ms"]:.4f} | ms |
| median E2E | {measured["median_e2e_s"]:.6f} | s |
| P95 E2E | {measured["p95_e2e_s"]:.6f} | s |

## 判断

当前本地 simulator smoke 的 median E2E 为 {measured["median_e2e_s"]:.6f}s。

实测 E2E 包含 Python async、Ray actor、HTTP、调度循环和本地 WSL I/O 开销。该结果说明 simulator 管线已使用指定 profile 跑通，但不能直接当作 RTX 5090 真实性能结论。
"""
    out = args.result_dir / "SIM_PROFILE_SANITY_CHECK.md"
    out.write_text(report, encoding="utf-8")
    print(out)


if __name__ == "__main__":
    main()
