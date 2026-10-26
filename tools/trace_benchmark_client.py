#!/usr/bin/env python3
"""Collect request-level token traces without changing serving behavior."""

from __future__ import annotations

import argparse
import asyncio
import json
import time
import uuid
from pathlib import Path
from typing import Any

import aiohttp


def _intervals(timestamps_ms: list[float]) -> list[float]:
    return [b - a for a, b in zip(timestamps_ms, timestamps_ms[1:])]


async def _client_stream_request(
    session: aiohttp.ClientSession, args: argparse.Namespace, index: int
) -> dict[str, Any]:
    request_id = f"trace-{uuid.uuid4()}-{index}"
    start_wall_ms = time.time() * 1000.0
    start_perf = time.perf_counter()
    timestamps_ms: list[float] = []
    previous_completion_tokens = 0
    uncounted_text_chunk = False
    response_id = None
    finish_reason = None
    final_completion_tokens = None
    error = None
    status = 0
    payload = {
        "model": args.model,
        "prompt": args.prompt,
        "max_tokens": args.max_tokens,
        "temperature": args.temperature,
        "ignore_eos": args.ignore_eos,
        "stream": True,
        "stream_options": {
            "include_usage": True,
            "continuous_usage_stats": True,
        },
    }
    try:
        async with session.post(
            args.url,
            json=payload,
            headers={"X-Request-Id": request_id},
        ) as response:
            status = response.status
            async for raw_line in response.content:
                line = raw_line.decode(errors="replace").strip()
                if not line.startswith("data:"):
                    continue
                body = line[5:].strip()
                if body == "[DONE]":
                    break
                data = json.loads(body)
                response_id = response_id or data.get("id")
                usage = data.get("usage") or {}
                if usage.get("completion_tokens") is not None:
                    final_completion_tokens = int(usage["completion_tokens"])
                choices = data.get("choices") or []
                if choices:
                    text = choices[0].get("text") or ""
                    finish_reason = choices[0].get("finish_reason") or finish_reason
                    if text:
                        received_ms = (time.perf_counter() - start_perf) * 1000.0
                        current_count = usage.get("completion_tokens")
                        if current_count is None:
                            uncounted_text_chunk = True
                        else:
                            current_count = int(current_count)
                            token_delta = current_count - previous_completion_tokens
                            if token_delta <= 0:
                                uncounted_text_chunk = True
                            else:
                                # Tokens carried by one SSE event become visible to
                                # the client together, so they share its receive time.
                                timestamps_ms.extend([received_ms] * token_delta)
                                previous_completion_tokens = current_count
            if status != 200:
                error = f"HTTP {status}"
    except Exception as exc:  # noqa: BLE001 - preserve request failure in trace
        error = repr(exc)
    end_perf = time.perf_counter()

    # A timestamp is accepted as per-token only when the server's final usage
    # proves that exactly one output token was represented by each data chunk.
    strict_token_trace = (
        final_completion_tokens is not None
        and final_completion_tokens == len(timestamps_ms)
        and len(timestamps_ms) > 0
        and not uncounted_text_chunk
    )
    if not strict_token_trace and error is None:
        error = (
            "SSE chunks are not proven one-token chunks: "
            f"chunks={len(timestamps_ms)}, completion_tokens={final_completion_tokens}"
        )
    finished = status == 200 and finish_reason != "abort" and error is None
    return {
        "request_id": response_id or request_id,
        "client_request_id": request_id,
        "workload": args.workload,
        "experiment_group": args.experiment_group,
        "strategy": args.strategy,
        "request_type": args.request_type,
        "priority": args.priority,
        "input_len": args.input_len,
        "expected_output_len": args.max_tokens,
        "num_output_tokens": final_completion_tokens or 0,
        "request_start_wall_time_ms": start_wall_ms,
        "start_perf_counter": start_perf,
        "end_perf_counter": end_perf,
        "token_timestamps_ms": timestamps_ms if strict_token_trace else [],
        "token_intervals_ms": _intervals(timestamps_ms) if strict_token_trace else [],
        "ttft_ms": timestamps_ms[0] if strict_token_trace else None,
        "e2e_latency_ms": (end_perf - start_perf) * 1000.0,
        "finished": finished,
        "aborted": finish_reason == "abort",
        "timeout": isinstance(error, str) and "timeout" in error.lower(),
        "error": error,
        "trace_time_source": "client_stream_receive",
        "strict_per_token_trace": strict_token_trace,
        "clock_alignment": "client time.time sampled immediately before client perf_counter",
    }


async def _server_request(
    session: aiohttp.ClientSession, args: argparse.Namespace, index: int
) -> dict[str, Any]:
    start_perf = time.perf_counter()
    payload = {
        "prompt": args.prompt,
        "max_tokens": args.max_tokens,
        "temperature": args.temperature,
        "ignore_eos": args.ignore_eos,
        "stream": False,
        "enable_token_trace": True,
    }
    try:
        async with session.post(args.url, json=payload) as response:
            body = await response.json()
            pairs = body.get("per_token_latency") or []
            timestamps = body.get("token_timestamps_ms")
            if timestamps is None and pairs:
                elapsed = 0.0
                timestamps = []
                for row in pairs:
                    elapsed += float(row[1])
                    timestamps.append(elapsed)
            timestamps = timestamps or []
            wall_start = body.get("request_start_wall_time_ms")
            strict = len(timestamps) == int(body.get("num_output_tokens_cf") or 0)
            alignment = wall_start is not None
            return {
                "request_id": body.get("request_id"),
                "workload": args.workload,
                "experiment_group": args.experiment_group,
                "strategy": args.strategy,
                "request_type": args.request_type,
                "priority": args.priority,
                "input_len": args.input_len,
                "expected_output_len": args.max_tokens,
                "num_output_tokens": body.get("num_output_tokens_cf") or 0,
                "request_start_wall_time_ms": wall_start,
                "start_perf_counter": start_perf,
                "end_perf_counter": time.perf_counter(),
                "token_timestamps_ms": timestamps if strict else [],
                "token_intervals_ms": _intervals(timestamps) if strict else [],
                "ttft_ms": timestamps[0] if strict and timestamps else None,
                "e2e_latency_ms": (time.perf_counter() - start_perf) * 1000.0,
                "finished": response.status == 200 and strict,
                "aborted": False,
                "timeout": False,
                "error": None if strict else "server updates are not proven per-token",
                "trace_time_source": "server_per_token_latency",
                "strict_per_token_trace": strict,
                "clock_alignment": "server wall start" if alignment else "unavailable",
            }
    except Exception as exc:  # noqa: BLE001
        return {
            "request_id": None,
            "workload": args.workload,
            "experiment_group": args.experiment_group,
            "strategy": args.strategy,
            "request_type": args.request_type,
            "priority": args.priority,
            "input_len": args.input_len,
            "expected_output_len": args.max_tokens,
            "num_output_tokens": 0,
            "request_start_wall_time_ms": None,
            "start_perf_counter": start_perf,
            "end_perf_counter": time.perf_counter(),
            "token_timestamps_ms": [],
            "token_intervals_ms": [],
            "ttft_ms": None,
            "e2e_latency_ms": (time.perf_counter() - start_perf) * 1000.0,
            "finished": False,
            "aborted": False,
            "timeout": "timeout" in repr(exc).lower(),
            "error": repr(exc),
            "trace_time_source": "server_per_token_latency",
            "strict_per_token_trace": False,
            "clock_alignment": "unavailable",
        }


async def run(args: argparse.Namespace) -> list[dict[str, Any]]:
    timeout = aiohttp.ClientTimeout(total=args.timeout_seconds)
    semaphore = asyncio.Semaphore(args.concurrency)
    request_func = _server_request if args.token_trace_mode == "server" else _client_stream_request
    async with aiohttp.ClientSession(timeout=timeout) as session:
        async def guarded(index: int) -> dict[str, Any]:
            async with semaphore:
                return await request_func(session, args, index)
        return await asyncio.gather(*(guarded(i) for i in range(args.request_num)))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", required=True)
    parser.add_argument("--model", default="/data/models/Qwen2.5-7B-Instruct")
    parser.add_argument("--request-num", type=int, default=50)
    parser.add_argument("--concurrency", type=int, default=20)
    parser.add_argument("--max-tokens", type=int, default=32)
    parser.add_argument("--prompt", default="Explain why load balancing matters in one sentence.")
    parser.add_argument("--input-len", type=int, default=0)
    parser.add_argument("--temperature", type=float, default=0.0)
    parser.add_argument("--ignore-eos", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--timeout-seconds", type=float, default=600.0)
    parser.add_argument("--enable-token-trace", action="store_true")
    parser.add_argument("--token-trace-output-dir", type=Path)
    parser.add_argument("--token-trace-mode", choices=("server", "client_stream"), default="server")
    parser.add_argument("--workload", default="stable_short")
    parser.add_argument("--experiment-group", default="trace_smoke")
    parser.add_argument("--strategy", default="no_migration")
    parser.add_argument("--request-type", default="")
    parser.add_argument("--priority", default="normal")
    args = parser.parse_args()
    if args.enable_token_trace and args.token_trace_output_dir is None:
        parser.error("--token-trace-output-dir is required with --enable-token-trace")
    results = asyncio.run(run(args))
    if args.enable_token_trace:
        args.token_trace_output_dir.mkdir(parents=True, exist_ok=True)
        path = args.token_trace_output_dir / "request_token_trace.jsonl"
        with path.open("w", encoding="utf-8") as handle:
            for row in results:
                handle.write(json.dumps(row, ensure_ascii=False) + "\n")
        print(path)
    print(json.dumps({
        "requests": len(results),
        "finished": sum(bool(row["finished"]) for row in results),
        "strict_per_token": sum(bool(row["strict_per_token_trace"]) for row in results),
    }))


if __name__ == "__main__":
    main()
