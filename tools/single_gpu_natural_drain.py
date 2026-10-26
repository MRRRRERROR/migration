#!/usr/bin/env python3
"""Run a single-vLLM natural-drain experiment and build its report."""

from __future__ import annotations

import argparse
import asyncio
import csv
import html
import json
import logging
import math
import random
import statistics
import subprocess
import time
from collections import Counter
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import aiohttp
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import requests
from transformers import AutoTokenizer


SHAREGPT_URL = (
    "https://huggingface.co/datasets/anon8231489123/ShareGPT_Vicuna_unfiltered/"
    "resolve/main/ShareGPT_V3_unfiltered_cleaned_split.json"
)


@dataclass
class RequestRecord:
    request_id: str
    arrival_time: float
    execution_start_time: float | None = None
    finish_time: float | None = None
    input_tokens: int | None = None
    completion_tokens: int | None = None
    final_status: str = "pending"
    http_status: int | None = None
    finish_reason: str | None = None
    error: str | None = None
    server_request_id: str | None = None
    first_token_latency_s: float | None = None
    e2e_latency_s: float | None = None
    state_at_drain: str | None = None


def pct(values: list[float], p: float) -> float | None:
    if not values:
        return None
    vals = sorted(values)
    pos = (len(vals) - 1) * p / 100
    lo, hi = math.floor(pos), math.ceil(pos)
    if lo == hi:
        return vals[lo]
    return vals[lo] * (hi - pos) + vals[hi] * (pos - lo)


def gpu_sample(index: int) -> dict[str, float | None]:
    try:
        raw = subprocess.check_output(
            ["nvidia-smi", f"--id={index}",
             "--query-gpu=utilization.gpu,memory.used,power.draw",
             "--format=csv,noheader,nounits"], text=True, timeout=3
        ).strip().split(",")
        return {"gpu_utilization": float(raw[0]), "gpu_memory_used_mib": float(raw[1]),
                "gpu_power_w": float(raw[2])}
    except Exception:
        return {"gpu_utilization": None, "gpu_memory_used_mib": None, "gpu_power_w": None}


async def fetch_scheduler_metrics(session: aiohttp.ClientSession, base_url: str) -> tuple[int, int]:
    async with session.get(f"{base_url}/metrics", timeout=aiohttp.ClientTimeout(total=3)) as resp:
        text = await resp.text()
    values = {"vllm:num_requests_running": 0.0, "vllm:num_requests_waiting": 0.0}
    for line in text.splitlines():
        if not line or line.startswith("#"):
            continue
        for name in tuple(values):
            if line.startswith(name + "{") or line.startswith(name + " "):
                try:
                    values[name] += float(line.rsplit(" ", 1)[1])
                except ValueError:
                    pass
    return round(values["vllm:num_requests_running"]), round(values["vllm:num_requests_waiting"])


def load_prompts(dataset: Path, tokenizer_path: str, limit: int = 20000) -> list[str]:
    if not dataset.exists():
        dataset.parent.mkdir(parents=True, exist_ok=True)
        logging.info("Downloading ShareGPT from %s", SHAREGPT_URL)
        with requests.get(SHAREGPT_URL, stream=True, timeout=120) as resp:
            resp.raise_for_status()
            with dataset.open("wb") as out:
                for chunk in resp.iter_content(1024 * 1024):
                    out.write(chunk)
    tokenizer = AutoTokenizer.from_pretrained(tokenizer_path, trust_remote_code=True)
    if dataset.suffix == ".jsonl":
        rows = [json.loads(line) for line in dataset.read_text(encoding="utf-8").splitlines() if line.strip()]
    else:
        rows = json.loads(dataset.read_text(encoding="utf-8"))
    prompts: list[str] = []
    for row in rows:
        if row.get("input_tok_ids"):
            prompt = tokenizer.decode(row["input_tok_ids"], skip_special_tokens=True).strip()
            n = len(tokenizer.encode(prompt, add_special_tokens=True))
            if 16 <= n <= 2800:
                prompts.append(prompt)
            if len(prompts) >= limit:
                break
            continue
        conv = row.get("conversations") or []
        if len(conv) < 2:
            continue
        prompt = str(conv[0].get("value", "")).strip()
        if not prompt:
            continue
        n = len(tokenizer.encode(prompt, add_special_tokens=True))
        if 16 <= n <= 2800:
            prompts.append(prompt)
        if len(prompts) >= limit:
            break
    if not prompts:
        raise RuntimeError("No usable ShareGPT prompts")
    logging.info("Loaded %d real ShareGPT prompts", len(prompts))
    return prompts


async def send_request(
    session: aiohttp.ClientSession, base_url: str, model: str, prompt: str,
    rec: RequestRecord, seed: int,
) -> None:
    payload = {
        "model": model,
        "messages": [{"role": "user", "content": prompt}],
        "seed": seed,
        "stream": True,
        "stream_options": {"include_usage": True},
    }
    try:
        async with session.post(
            f"{base_url}/v1/chat/completions", json=payload,
            headers={"X-Request-Id": rec.request_id},
            timeout=aiohttp.ClientTimeout(total=900),
        ) as resp:
            rec.http_status = resp.status
            if resp.status != 200:
                rec.error = (await resp.text())[:1000]
                rec.final_status = "http_error"
                return
            async for raw in resp.content:
                for line in raw.decode("utf-8", errors="replace").splitlines():
                    if not line.startswith("data: ") or line == "data: [DONE]":
                        continue
                    try:
                        event = json.loads(line[6:])
                    except json.JSONDecodeError:
                        continue
                    rec.server_request_id = rec.server_request_id or event.get("id")
                    choices = event.get("choices") or []
                    if choices:
                        delta = choices[0].get("delta") or {}
                        if delta.get("content") and rec.execution_start_time is None:
                            rec.execution_start_time = time.time()
                        if choices[0].get("finish_reason"):
                            rec.finish_reason = choices[0]["finish_reason"]
                    usage = event.get("usage") or {}
                    if usage:
                        rec.input_tokens = usage.get("prompt_tokens")
                        rec.completion_tokens = usage.get("completion_tokens")
            rec.final_status = "finished"
    except Exception as exc:
        rec.error = repr(exc)
        rec.final_status = "exception"
    finally:
        rec.finish_time = time.time()
        if rec.execution_start_time is not None:
            rec.first_token_latency_s = rec.execution_start_time - rec.arrival_time
        rec.e2e_latency_s = rec.finish_time - rec.arrival_time


async def run_once(
    run_dir: Path, prompts: list[str], base_url: str, model: str, gpu: int,
    duration_s: float, qps: float, seed: int, label: str,
    dataset_source: str,
) -> dict[str, Any]:
    run_dir.mkdir(parents=True, exist_ok=True)
    events_path = run_dir / "events.jsonl"
    metrics: list[dict[str, Any]] = []
    records: list[RequestRecord] = []
    tasks: list[asyncio.Task[None]] = []
    start_wall = time.time()
    drain_wall: float | None = None
    drained_wall: float | None = None
    stop_monitor = asyncio.Event()
    rng = random.Random(seed)

    def event(kind: str, **fields: Any) -> None:
        payload = {"event": kind, "timestamp": time.time(), **fields}
        with events_path.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(payload, ensure_ascii=False) + "\n")

    connector = aiohttp.TCPConnector(limit=0)
    async with aiohttp.ClientSession(connector=connector) as session:
        async def monitor() -> None:
            while not stop_monitor.is_set():
                now = time.time()
                try:
                    running, waiting = await fetch_scheduler_metrics(session, base_url)
                except Exception:
                    running, waiting = -1, -1
                unfinished = sum(r.finish_time is None for r in records)
                gpu_row = await asyncio.to_thread(gpu_sample, gpu)
                metrics.append({
                    "timestamp": now,
                    "elapsed_since_start": now - start_wall,
                    "elapsed_since_drain": None if drain_wall is None else now - drain_wall,
                    "num_running_requests": running,
                    "num_waiting_requests": waiting,
                    "num_unfinished_requests": unfinished,
                    **gpu_row,
                })
                await asyncio.sleep(0.5)

        mon = asyncio.create_task(monitor())
        event("run_start", label=label, qps=qps, duration_s=duration_s)
        deadline = asyncio.get_running_loop().time()
        index = 0
        while True:
            now_loop = asyncio.get_running_loop().time()
            if now_loop >= deadline:
                req_id = f"{label}-{index:05d}"
                rec = RequestRecord(request_id=req_id, arrival_time=time.time())
                records.append(rec)
                prompt = prompts[(index + seed) % len(prompts)]
                tasks.append(asyncio.create_task(send_request(
                    session, base_url, model, prompt, rec, seed + index)))
                index += 1
                deadline += rng.expovariate(qps)
            remaining = duration_s - (time.time() - start_wall)
            if remaining <= 0:
                break
            await asyncio.sleep(min(max(0.0, deadline - asyncio.get_running_loop().time()), remaining, 0.2))

        drain_wall = time.time()
        running, waiting = await fetch_scheduler_metrics(session, base_url)
        unfinished = sum(r.finish_time is None for r in records)
        for rec in records:
            if rec.finish_time is None:
                rec.state_at_drain = "running_observed" if rec.execution_start_time is not None else "waiting_or_prefill"
        drain_snapshot = {"running": running, "waiting": waiting, "unfinished": unfinished}
        event("drain_start", **drain_snapshot)
        await asyncio.gather(*tasks)
        while True:
            running, waiting = await fetch_scheduler_metrics(session, base_url)
            unfinished = sum(r.finish_time is None for r in records)
            if running == 0 and waiting == 0 and unfinished == 0:
                drained_wall = time.time()
                event("instance_drained", running=0, waiting=0, unfinished=0)
                break
            await asyncio.sleep(0.2)
        stop_monitor.set()
        await mon

    with (run_dir / "instance_metrics.csv").open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(metrics[0]))
        writer.writeheader(); writer.writerows(metrics)
    with (run_dir / "request_metrics.csv").open("w", newline="", encoding="utf-8") as fh:
        rows = [asdict(r) for r in records]
        writer = csv.DictWriter(fh, fieldnames=list(rows[0]))
        writer.writeheader(); writer.writerows(rows)

    pending = [r for r in records if r.finish_time and r.finish_time > drain_wall]
    remain = [r.finish_time - drain_wall for r in pending if r.finish_time]
    ordered = sorted(pending, key=lambda r: r.finish_time or 0)
    drain_time = (drained_wall or drain_wall) - drain_wall
    half_index = max(0, math.ceil(len(ordered) * 0.5) - 1)
    ninety_index = max(0, math.ceil(len(ordered) * 0.9) - 1)
    last_gap = 0.0
    if len(ordered) >= 2:
        last_gap = (ordered[-1].finish_time or drain_wall) - (ordered[-2].finish_time or drain_wall)
    elif len(ordered) == 1:
        last_gap = drain_time
    completed_reasons = Counter((r.finish_reason or "unknown") for r in records
                                if r.final_status == "finished")
    drain_reasons = Counter((r.finish_reason or "unknown") for r in pending)
    finished_count = sum(completed_reasons.values())
    summary = {
        "label": label, "qps": qps, "duration_s": duration_s,
        "submitted": len(records), "finished": sum(r.final_status == "finished" for r in records),
        "failed": sum(r.final_status != "finished" for r in records),
        "drain_start": drain_wall, "instance_drained": drained_wall,
        "natural_drain_time_s": drain_time, "drain_start_snapshot": drain_snapshot,
        "remaining_completion_s": {
            "p50": pct(remain, 50), "p90": pct(remain, 90),
            "p95": pct(remain, 95), "max": max(remain) if remain else 0.0,
        },
        "time_after_50pct_finished_s": drain_time - ((ordered[half_index].finish_time or drain_wall) - drain_wall) if ordered else 0.0,
        "time_after_90pct_finished_s": drain_time - ((ordered[ninety_index].finish_time or drain_wall) - drain_wall) if ordered else 0.0,
        "last_request_solo_tail_s": last_gap,
        "last_requests": [{
            "request_id": r.request_id, "input_tokens": r.input_tokens,
            "completion_tokens": r.completion_tokens,
            "finish_reason": r.finish_reason or "unknown",
            "remaining_after_drain_s": (r.finish_time or drain_wall) - drain_wall,
        } for r in ordered[-3:]],
        "finish_reason_counts": dict(sorted(completed_reasons.items())),
        "finish_reason_proportions": {
            reason: count / finished_count for reason, count in sorted(completed_reasons.items())
        },
        "drain_start_unfinished_finish_reason_counts": dict(sorted(drain_reasons.items())),
        "no_arrivals_after_drain": all(r.arrival_time <= drain_wall for r in records),
        "drain_tail_source": (
            "waiting" if drain_snapshot["waiting"] > drain_snapshot["running"]
            and drain_snapshot["waiting"] > 0 else "running"
        ),
        "mean_gpu_utilization": statistics.fmean(
            m["gpu_utilization"] for m in metrics if m["gpu_utilization"] is not None),
        "mean_running": statistics.fmean(max(0, m["num_running_requests"]) for m in metrics),
        "p95_waiting": pct([max(0, m["num_waiting_requests"]) for m in metrics], 95),
    }
    (run_dir / "summary.json").write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")
    (run_dir / "config.json").write_text(json.dumps({
        "model": model, "dataset": "ShareGPT-derived real conversations",
        "dataset_source": dataset_source, "arrival_process": "Poisson open-loop",
        "qps": qps, "duration_s": duration_s,
        "request_max_tokens": "omitted", "request_max_completion_tokens": "omitted",
        "request_ignore_eos": "omitted (vLLM default false)",
        "sampling_parameters": "vLLM generation_config=auto defaults",
        "seed": seed, "gpu": gpu,
        "migration": False, "kvt": False, "gateway": False,
    }, indent=2, ensure_ascii=False), encoding="utf-8")
    make_plots(run_dir, metrics, pending, drain_wall, drain_time)
    return summary


def make_plots(run_dir: Path, metrics: list[dict[str, Any]], pending: list[RequestRecord],
               drain: float, drain_time: float) -> None:
    plot_dir = run_dir / "plots"; plot_dir.mkdir(exist_ok=True)
    after = [m for m in metrics if m["timestamp"] >= drain]
    x = [m["timestamp"] - drain for m in after]
    fig, ax = plt.subplots(figsize=(9, 5))
    for key, label in [("num_running_requests", "running"), ("num_waiting_requests", "waiting"),
                       ("num_unfinished_requests", "unfinished")]:
        ax.step(x, [m[key] for m in after], where="post", label=label)
    ax.axvline(0, color="black", linestyle="--", label="drain_start")
    ax.axvline(drain_time, color="red", linestyle="--", label="instance_drained")
    ax.set(xlabel="Seconds since drain_start", ylabel="Requests", title="Request counts during natural drain")
    ax.legend(); ax.grid(alpha=.25); fig.tight_layout(); fig.savefig(plot_dir / "request_counts.png", dpi=160); plt.close(fig)

    ordered = sorted(pending, key=lambda r: (r.finish_time or drain))
    fig, ax = plt.subplots(figsize=(10, max(4, min(14, len(ordered) * .22))))
    for i, rec in enumerate(ordered):
        end = (rec.finish_time or drain) - drain
        color = "tab:blue" if rec.state_at_drain == "running_observed" else "tab:orange"
        ax.hlines(i, 0, end, color=color, linewidth=2)
    ax.set(xlabel="Seconds since drain_start", ylabel="Request (sorted by finish)", title="Requests unfinished at drain_start")
    ax.grid(axis="x", alpha=.25); fig.tight_layout(); fig.savefig(plot_dir / "request_timeline.png", dpi=160); plt.close(fig)

    remain = sorted((r.finish_time or drain) - drain for r in pending)
    fig, ax = plt.subplots(figsize=(8, 5))
    if remain:
        ax.step(remain, [(i + 1) / len(remain) for i in range(len(remain))], where="post")
    ax.set(xlabel="Remaining completion time (s)", ylabel="CDF", title="Post-drain remaining-time distribution")
    ax.grid(alpha=.25); fig.tight_layout(); fig.savefig(plot_dir / "remaining_time_cdf.png", dpi=160); plt.close(fig)


def build_report(root: Path, formal: list[dict[str, Any]], config: dict[str, Any]) -> None:
    def proportion(s: dict[str, Any], reason: str) -> float:
        return 100 * s["finish_reason_proportions"].get(reason, 0.0)

    rows = "".join(
        f"<tr><td>{html.escape(s['label'])}</td><td>{s['natural_drain_time_s']:.3f}</td>"
        f"<td>{s['drain_start_snapshot']['running']}</td><td>{s['drain_start_snapshot']['waiting']}</td>"
        f"<td>{s['drain_start_snapshot']['unfinished']}</td><td>{s['remaining_completion_s']['p50']:.3f}</td>"
        f"<td>{s['remaining_completion_s']['p90']:.3f}</td><td>{s['remaining_completion_s']['p95']:.3f}</td>"
        f"<td>{s['remaining_completion_s']['max']:.3f}</td>"
        f"<td>{proportion(s, 'stop'):.2f}%</td><td>{proportion(s, 'length'):.2f}%</td></tr>" for s in formal)
    sections = []
    for s in formal:
        last = "".join(
            f"<li>{html.escape(r['request_id'])}: input={r['input_tokens']}, completion={r['completion_tokens']}, "
            f"finish_reason={html.escape(r['finish_reason'])}, drain 后 {r['remaining_after_drain_s']:.3f}s 完成</li>"
            for r in s["last_requests"])
        drain_dist = html.escape(json.dumps(
            s["drain_start_unfinished_finish_reason_counts"], ensure_ascii=False))
        sections.append(f"<h2>{html.escape(s['label'])}</h2>"
                        f"<p>全部完成请求：stop={proportion(s, 'stop'):.2f}%，length={proportion(s, 'length'):.2f}%。"
                        f"drain_start 时未完成请求的最终 finish_reason 分布：<code>{drain_dist}</code></p>"
                        f"<h3>最后 1～3 个完成请求</h3><ul>{last}</ul>"
                        f"<img src='{s['label']}/plots/request_counts.png'><img src='{s['label']}/plots/request_timeline.png'>"
                        f"<img src='{s['label']}/plots/remaining_time_cdf.png'>")
    consistent_tail = all(s["last_request_solo_tail_s"] > .2 * s["natural_drain_time_s"] for s in formal)
    conclusion = ("观察到稳定的 tail-drain 现象，少量长尾请求显著延长了实例自然排空时间，值得进一步开展 migration-assisted drain 对照实验。"
                  if consistent_tail else
                  "当前 workload 下未观察到稳定且明显的 tail-drain 现象，现有证据不足以证明运行中请求迁移能够显著加速资源释放。")
    any_length = any(s["finish_reason_counts"].get("length", 0) > 0 for s in formal)
    length_notice = (
        "<p class='warn'><strong>长度限制警告：</strong>检测到 finish_reason=length。请求仍受到输出长度边界限制，"
        "因此本轮不能视为完全自然生成。</p>" if any_length else
        "<p><strong>EOS 检查：</strong>三轮正式实验未检测到 finish_reason=length；所有成功请求均可检查为正常 stop 或其他已列明原因。</p>"
    )
    body = f"""<!doctype html><html lang='zh-CN'><head><meta charset='utf-8'><title>单 GPU 自然排空实验</title>
<style>body{{font-family:Arial,'Microsoft YaHei',sans-serif;max-width:1200px;margin:32px auto;line-height:1.6;color:#172033}}table{{border-collapse:collapse;width:100%}}th,td{{border:1px solid #ccd3df;padding:7px;text-align:right}}th:first-child,td:first-child{{text-align:left}}img{{max-width:100%;margin:12px 0;border:1px solid #ddd}}code{{background:#eef2f7;padding:2px 5px}}.warn{{background:#fff2cc;border-left:5px solid #d08b00;padding:12px}}</style></head><body>
<h1>单 GPU、单实例 LLM Serving 自然排空实验</h1>
<p>目的：测量停止产生新请求后，已进入系统的请求自然完成所需时间，并判断最后少量请求是否形成 tail-drain。</p>
<h2>环境与配置</h2><pre>{html.escape(json.dumps(config, indent=2, ensure_ascii=False))}</pre>
<p>workload：ShareGPT 原始聊天请求；Poisson 持续到达；直接访问单个 vLLM 实例；migration/KVT/gateway 均关闭。running/waiting 来自 vLLM scheduler 原生 Prometheus 指标。</p>
<h2>三轮汇总</h2><table><tr><th>轮次</th><th>Drain(s)</th><th>Running</th><th>Waiting</th><th>Unfinished</th><th>P50</th><th>P90</th><th>P95</th><th>Max</th><th>stop 比例</th><th>length 比例</th></tr>{rows}</table>
{length_notice}<h2>结论</h2><p>{conclusion}</p>
<h2>限制</h2><p>请求级 execution_start_time 使用客户端收到首 token 的时刻近似；drain_start 的 running/waiting 总数来自服务端 scheduler，具有更高可信度。本轮只验证单实例自然排空，不能证明迁移、电力调节或跨域机制的收益。</p>
{''.join(sections)}</body></html>"""
    (root / "report.html").write_text(body, encoding="utf-8")


async def main_async(args: argparse.Namespace) -> None:
    stamp = time.strftime("%Y%m%d_%H%M%S")
    root = Path(args.result_root) / f"single_gpu_natural_drain_{stamp}"
    root.mkdir(parents=True)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s",
                        handlers=[logging.FileHandler(root / "run.log", encoding="utf-8"), logging.StreamHandler()])
    prompts = load_prompts(Path(args.dataset), args.tokenizer)
    short = await run_once(root / "short_test", prompts, args.base_url, args.model, args.gpu,
                           15, .2, args.seed, "short_test", args.dataset)
    if short["failed"] or not short["no_arrivals_after_drain"]:
        raise RuntimeError(f"Short test failed validation: {short}")
    candidates = [.2, .35, .5]
    calibrations = []
    for i, qps in enumerate(candidates):
        calibrations.append(await run_once(root / "calibration" / f"qps_{qps}", prompts,
                                           args.base_url, args.model, args.gpu, 25, qps,
                                           args.seed + 1000 + i, f"cal_qps_{qps}",
                                           args.dataset))
    viable = [s for s in calibrations if s["failed"] == 0 and (s["p95_waiting"] or 0) <= 1]
    chosen = max(viable or calibrations, key=lambda s: (s["mean_gpu_utilization"], -s["failed"]))
    qps = chosen["qps"]
    formal = []
    for i in range(1, 4):
        formal.append(await run_once(root / f"run_{i}", prompts, args.base_url, args.model,
                                     args.gpu, args.formal_duration, qps,
                                     args.seed + i, f"run_{i}", args.dataset))
    aggregate = {"chosen_qps": qps, "short_test": short, "calibration": calibrations, "formal": formal}
    (root / "aggregate_summary.json").write_text(json.dumps(aggregate, indent=2, ensure_ascii=False), encoding="utf-8")
    report_config = {"GPU": "NVIDIA GeForce RTX 5090 (GPU 0)", "model": args.model,
                     "vLLM": "0.24.0", "qps": qps, "duration_per_run_s": args.formal_duration,
                     "request_max_tokens": "omitted", "request_max_completion_tokens": "omitted",
                     "request_ignore_eos": "omitted; vLLM default false",
                     "generation_config_mode": "auto (model generation_config.json loaded)",
                     "generation_config_length_fields": {"max_new_tokens": "absent", "max_length": "absent"},
                     "model_generation_defaults": {"temperature": 0.7, "top_p": 0.8,
                                                   "top_k": 20, "repetition_penalty": 1.05},
                     "server_max_model_len": 4096,
                     "platform_max_output_tokens": "sys.maxsize (no additional CUDA platform cap)",
                     "effective_output_ceiling": "4096 - prompt_tokens (context-window boundary)"}
    build_report(root, formal, report_config)
    print(f"RESULT_ROOT={root}")


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--base-url", default="http://127.0.0.1:18100")
    p.add_argument("--model", default="Qwen2.5-7B-Instruct")
    p.add_argument("--tokenizer", default="/data/models/Qwen2.5-7B-Instruct")
    p.add_argument("--dataset", default="/data/home/zylu/datasets/ShareGPT_V3_unfiltered_cleaned_split.json")
    p.add_argument("--result-root", default="/data/home/zylu/llumnix-v018-main/results")
    p.add_argument("--gpu", type=int, default=0)
    p.add_argument("--formal-duration", type=float, default=180)
    p.add_argument("--seed", type=int, default=20260922)
    args = p.parse_args()
    asyncio.run(main_async(args))


if __name__ == "__main__":
    main()
