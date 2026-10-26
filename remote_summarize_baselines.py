from pathlib import Path
import ast
import json
import re
import statistics

BASE = Path.home() / "llumnix_results" / "baseline_sim_opt-1.3b_20260701_192646"
CASES = ["rr_no_migration", "load_no_migration", "load_migration_defrag"]


def parse_list(text, key):
    m = re.search(rf"{re.escape(key)}=(\[.*?\])", text, re.S)
    return ast.literal_eval(m.group(1)) if m else []


def stats(xs):
    if not xs:
        return None
    xs = sorted(float(x) for x in xs)
    def pct(p):
        return xs[int(round((len(xs) - 1) * p))]
    return {
        "n": len(xs),
        "min": xs[0],
        "p50": statistics.median(xs),
        "mean": statistics.mean(xs),
        "p95": pct(0.95),
        "p99": pct(0.99),
        "max": xs[-1],
    }


def parse_benchmark(path):
    text = path.read_text(errors="ignore")
    out = {}
    for key in ["throughput_tok_s", "successful_responses", "prompt_token_count", "response_token_count"]:
        m = re.search(rf"{key} ([0-9.]+)", text)
        if m:
            out[key] = float(m.group(1)) if "." in m.group(1) else int(m.group(1))
    m = re.search(r"backend .*? dur_s ([0-9.]+) tokens_per_s ([0-9.]+) qps ([0-9.]+)", text)
    if m:
        out["dur_s"] = float(m.group(1))
        out["tokens_per_s"] = float(m.group(2))
        out["qps"] = float(m.group(3))
    for key in ["median_token_latency", "median_e2e_latency", "median_inference_latency"]:
        m = re.search(rf"{key}=([0-9.]+)", text)
        if m:
            out[key] = float(m.group(1))
    out["request_ids"] = parse_list(text, "all_request_ids")
    out["e2e_stats"] = stats(parse_list(text, "all_e2e_latencies"))
    out["per_token_stats"] = stats(parse_list(text, "all_per_token_latencies"))
    out["decode_token_stats"] = stats(parse_list(text, "all_decode_token_latencies"))
    return out


def parse_migration(path, request_ids):
    lines = path.read_text(errors="ignore").splitlines() if path.exists() else []
    ids = set(request_ids)
    records = []
    cur = None
    for line in lines:
        m = re.search(r"begin migrate out ([0-9a-f]+)", line)
        if m:
            cur = {"request_id": m.group(1)}
            continue
        m = re.search(r"migration status: MigrationStatus\.([A-Z_]+), len: (\d+) blocks, cost: ([0-9.]+) ms", line)
        if m and cur:
            cur.update({"status": m.group(1), "blocks": int(m.group(2)), "cost_ms": float(m.group(3))})
            if not ids or cur["request_id"] in ids:
                records.append(cur)
            cur = None
    finished = [r for r in records if r["status"] == "FINISHED"]
    return {
        "attempts": len(records),
        "finished": len(finished),
        "non_finished": len(records) - len(finished),
        "status_counts": {s: sum(1 for r in records if r["status"] == s) for s in sorted(set(r["status"] for r in records))},
        "finished_cost_ms": stats([r["cost_ms"] for r in finished]),
        "finished_blocks": stats([r["blocks"] for r in finished]),
    }


summary = {}
for case in CASES:
    case_dir = BASE / case
    bench = parse_benchmark(case_dir / "benchmark.log")
    mig = parse_migration(case_dir / "migration.log", bench.get("request_ids", []))
    summary[case] = {"benchmark": bench, "migration": mig}

(BASE / "baseline_summary.json").write_text(json.dumps(summary, indent=2, ensure_ascii=False))

lines = []
for case, data in summary.items():
    b = data["benchmark"]
    m = data["migration"]
    e = b.get("e2e_stats") or {}
    lines.append(
        f"{case}: success={b.get('successful_responses')}, qps={b.get('qps')}, "
        f"tokens/s={b.get('tokens_per_s')}, e2e_p50={e.get('p50')}, e2e_p95={e.get('p95')}, "
        f"migrations={m.get('attempts')}, finished={m.get('finished')}"
    )
(BASE / "baseline_summary.txt").write_text("\n".join(lines) + "\n")
print("\n".join(lines))
print("BASE=" + str(BASE))
