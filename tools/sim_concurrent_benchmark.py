#!/usr/bin/env python3
"""Concurrent /generate_benchmark client for simulator smoke runs."""

from __future__ import annotations

import argparse
import asyncio
import json
import statistics
import time
from pathlib import Path

import aiohttp


def pct(values: list[float], percentile: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    rank = (len(ordered) - 1) * percentile / 100.0
    lo = int(rank)
    hi = min(lo + 1, len(ordered) - 1)
    frac = rank - lo
    return ordered[lo] * (1 - frac) + ordered[hi] * frac


async def one_request(session: aiohttp.ClientSession, url: str, index: int, max_tokens: int) -> dict:
    prompt = f"Stable short request {index}: Hello, briefly introduce yourself."
    payload = {
        "prompt": prompt,
        "stream": False,
        "temperature": 0.0,
        "top_p": 1.0,
        "max_tokens": max_tokens,
        "ignore_eos": True,
    }
    start = time.perf_counter()
    try:
        async with session.post(url, json=payload) as resp:
            text = await resp.text()
            elapsed = time.perf_counter() - start
            try:
                body = json.loads(text)
            except json.JSONDecodeError:
                body = {"raw_text": text}
            body_text = body.get("generated_text", "")
            return {
                "index": index,
                "status": resp.status,
                "ok": resp.status == 200 and bool(body_text),
                "elapsed_s": elapsed,
                "generated_text_len": len(body_text),
                "num_output_tokens_cf": body.get("num_output_tokens_cf"),
                "request_id": body.get("request_id"),
                "per_token_latency": body.get("per_token_latency", []),
                "body": body,
            }
    except Exception as exc:  # noqa: BLE001
        return {
            "index": index,
            "status": None,
            "ok": False,
            "elapsed_s": time.perf_counter() - start,
            "error": repr(exc),
        }


async def run(args: argparse.Namespace) -> dict:
    sem = asyncio.Semaphore(args.concurrency)
    timeout = aiohttp.ClientTimeout(total=args.timeout_s)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        async def guarded(i: int) -> dict:
            async with sem:
                return await one_request(session, args.url, i, args.max_tokens)

        start = time.perf_counter()
        responses = await asyncio.gather(*(guarded(i) for i in range(args.requests)))
        duration = time.perf_counter() - start

    ok = [r for r in responses if r.get("ok")]
    latencies = [r["elapsed_s"] for r in ok]
    output_tokens = [
        int(r.get("num_output_tokens_cf") or 0)
        for r in ok
    ]
    return {
        "url": args.url,
        "requests": args.requests,
        "concurrency": args.concurrency,
        "max_tokens": args.max_tokens,
        "duration_s": duration,
        "successful_responses": len(ok),
        "failed_responses": args.requests - len(ok),
        "qps": len(ok) / duration if duration else 0.0,
        "output_tokens": sum(output_tokens),
        "throughput_tok_s": sum(output_tokens) / duration if duration else 0.0,
        "median_e2e_s": statistics.median(latencies) if latencies else None,
        "p95_e2e_s": pct(latencies, 95),
        "p99_e2e_s": pct(latencies, 99),
        "responses": responses,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default="http://127.0.0.1:1234/generate_benchmark")
    parser.add_argument("--requests", type=int, default=100)
    parser.add_argument("--concurrency", type=int, default=20)
    parser.add_argument("--max-tokens", type=int, default=32)
    parser.add_argument("--timeout-s", type=float, default=600)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    result = asyncio.run(run(args))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2), encoding="utf-8")
    print(json.dumps({k: v for k, v in result.items() if k != "responses"}, indent=2))


if __name__ == "__main__":
    main()
