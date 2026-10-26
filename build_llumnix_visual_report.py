import ast
import csv
import html
import math
import re
import statistics
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parent
OUT = ROOT / "llumnix_experiment_visual_report.html"
METRICS_CSV = ROOT / "llumnix_experiment_metrics.csv"

EXPS = [
    {
        "id": "rr_no_migration",
        "label": "Baseline 1: Round Robin, no migration",
        "short": "RR no migration",
        "family": "baseline",
        "path": ROOT / "enhanced_sim_opt-1.3b_20260701_212246" / "rr_no_migration",
        "color": "#6B7280",
    },
    {
        "id": "load_no_migration",
        "label": "Baseline 2: Load-aware dispatch, no migration",
        "short": "Load no migration",
        "family": "baseline",
        "path": ROOT / "enhanced_sim_opt-1.3b_20260701_212246" / "load_no_migration",
        "color": "#2563EB",
    },
    {
        "id": "load_migration_defrag",
        "label": "Baseline 3: Load-aware + migration + defrag",
        "short": "Load + migration + defrag",
        "family": "baseline",
        "path": ROOT / "enhanced_sim_opt-1.3b_20260701_212246" / "load_migration_defrag",
        "color": "#D97706",
    },
    {
        "id": "llumnix_load_migration",
        "label": "Llumnix method: simulator migration run",
        "short": "Llumnix sim migration",
        "family": "llumnix",
        "path": ROOT / "enhanced_sim_opt-1.3b_20260701_212246" / "llumnix_sim_migration",
        "color": "#059669",
    },
]


def percentile(values, p):
    if not values:
        return None
    values = sorted(values)
    k = math.ceil(p * len(values)) - 1
    return values[max(0, min(k, len(values) - 1))]


def parse_array(text, name):
    match = re.search(name + r"=(\[.*?\])", text, re.S)
    if not match:
        return []
    try:
        return ast.literal_eval(match.group(1))
    except Exception:
        return []


def parse_benchmark(path):
    log_path = path / "benchmark.log"
    data = {"valid": False, "error": None, "benchmark_exists": log_path.exists()}
    if not log_path.exists():
        data["error"] = "benchmark.log missing"
        return data

    text = log_path.read_text(encoding="utf-8", errors="replace")
    if "Connect to" in text:
        match = re.search(r"Connect to .*? failed with: (.*)", text)
        data["error"] = match.group(1).strip() if match else "connection failed"
    if "successful_responses" in text and not data["error"]:
        data["valid"] = True

    match = re.search(
        r"backend \S+ dur_s ([0-9.]+) tokens_per_s ([0-9.]+) qps ([0-9.]+)",
        text,
    )
    if match:
        data["duration_s"] = float(match.group(1))
        data["tokens_per_s"] = float(match.group(2))
        data["qps"] = float(match.group(3))

    match = re.search(
        r"successful_responses (\d+) prompt_token_count (\d+) response_token_count (\d+)",
        text,
    )
    if match:
        data["successful_responses"] = int(match.group(1))
        data["prompt_tokens"] = int(match.group(2))
        data["response_tokens"] = int(match.group(3))

    match = re.search(
        r"median_token_latency=([0-9.]+), median_e2e_latency=([0-9.]+), "
        r"median_inference_latency=([0-9.]+)",
        text,
    )
    if match:
        data["median_token_latency_s"] = float(match.group(1))
        data["median_e2e_latency_s"] = float(match.group(2))
        data["median_inference_latency_s"] = float(match.group(3))

    arrays = {
        "e2e_latency_s": parse_array(text, "all_e2e_latencies"),
        "per_token_latency_s": parse_array(text, "all_per_token_latencies"),
        "waiting_latency_s": parse_array(text, "all_waiting_latencies"),
        "decode_token_latency": parse_array(text, "all_decode_token_latencies"),
        "prompt_lens": parse_array(text, "all_prompt_lens"),
        "response_lens": parse_array(text, "all_response_lens"),
        "total_tokens": parse_array(text, "all_total_tokens"),
    }
    for name, values in arrays.items():
        data[name] = values
        if values and all(isinstance(value, (int, float)) for value in values):
            data[f"{name}_count"] = len(values)
            data[f"{name}_mean"] = statistics.mean(values)
            data[f"{name}_p50"] = statistics.median(values)
            data[f"{name}_p95"] = percentile(values, 0.95)
            data[f"{name}_p99"] = percentile(values, 0.99)
            data[f"{name}_max"] = max(values)
    return data


def parse_migration(path):
    log_path = path / "migration.log"
    data = {
        "migration_log_exists": log_path.exists(),
        "attempts": 0,
        "finished": 0,
        "aborted": 0,
        "cost_ms": [],
        "finished_cost_ms": [],
        "blocks": [],
        "finished_blocks": [],
        "recv_mb": [],
        "recv_speed_gbps": [],
    }
    if not log_path.exists():
        return data

    text = log_path.read_text(encoding="utf-8", errors="replace")
    for match in re.finditer(
        r"migration status: MigrationStatus\.([A-Z_]+), len: (\d+) blocks, "
        r"cost: ([0-9.]+) ms",
        text,
    ):
        status = match.group(1)
        blocks = int(match.group(2))
        cost = float(match.group(3))
        data["attempts"] += 1
        data["cost_ms"].append(cost)
        data["blocks"].append(blocks)
        if status == "FINISHED":
            data["finished"] += 1
            data["finished_cost_ms"].append(cost)
            data["finished_blocks"].append(blocks)
        else:
            data["aborted"] += 1

    for match in re.finditer(
        r"total_kv_cache_size: ([0-9.]+) MB, time: ([0-9.]+)s, "
        r"speed: ([0-9.]+)GB/s",
        text,
    ):
        data["recv_mb"].append(float(match.group(1)))
        data["recv_speed_gbps"].append(float(match.group(3)))

    values = data["finished_cost_ms"]
    if values:
        data["finished_cost_ms_mean"] = statistics.mean(values)
        data["finished_cost_ms_p50"] = statistics.median(values)
        data["finished_cost_ms_p95"] = percentile(values, 0.95)
        data["finished_cost_ms_p99"] = percentile(values, 0.99)
        data["finished_cost_ms_max"] = max(values)

    values = data["finished_blocks"]
    if values:
        data["finished_blocks_mean"] = statistics.mean(values)
        data["finished_blocks_p50"] = statistics.median(values)
        data["finished_blocks_max"] = max(values)

    values = data["recv_speed_gbps"]
    if values:
        data["recv_speed_gbps_mean"] = statistics.mean(values)
        data["recv_speed_gbps_p50"] = statistics.median(values)
        data["recv_speed_gbps_p95"] = percentile(values, 0.95)

    values = data["recv_mb"]
    if values:
        data["recv_mb_mean"] = statistics.mean(values)
        data["recv_mb_p50"] = statistics.median(values)
        data["recv_mb_max"] = max(values)
    return data


def parse_server_instance(path):
    csv_path = path / "server_instance.csv"
    data = {"server_instance_exists": csv_path.exists(), "rows": 0, "series": {}}
    if not csv_path.exists():
        return data
    try:
        frame = pd.read_csv(csv_path)
    except Exception as exc:
        data["error"] = repr(exc)
        return data
    data["rows"] = len(frame)
    if len(frame) == 0:
        return data

    columns = [
        "gpu_cache_usage",
        "num_available_gpu_blocks",
        "num_running_requests",
        "num_waiting_requests",
        "kv_blocks_ratio",
        "remaining_steps",
        "migration_load_metric",
    ]
    for column in columns:
        if column not in frame.columns:
            continue
        values = pd.to_numeric(frame[column], errors="coerce").dropna().tolist()
        if not values:
            continue
        data[f"{column}_mean"] = statistics.mean(values)
        data[f"{column}_p50"] = statistics.median(values)
        data[f"{column}_p95"] = percentile(values, 0.95)
        data[f"{column}_max"] = max(values)
        data[f"{column}_min"] = min(values)
        if len(values) > 250:
            step = math.ceil(len(values) / 250)
            values = values[::step]
        data["series"][column] = values
    return data


def bar_chart(title, labels, values, colors, ylabel=""):
    numeric_values = [value for value in values if value is not None and not pd.isna(value)]
    max_value = max(numeric_values) if numeric_values else 1
    rows = []
    for label, value, color in zip(labels, values, colors):
        if value is None or pd.isna(value):
            width = 0
            value_text = "无有效数据"
        else:
            width = max(1, value / max_value * 100)
            value_text = f"{value:.3f}" if isinstance(value, float) else str(value)
        rows.append(
            f"""
        <div class="bar-row">
          <div class="bar-label">{html.escape(label)}</div>
          <div class="bar-track"><div class="bar-fill" style="width:{width:.2f}%; background:{color}"></div></div>
          <div class="bar-value">{html.escape(value_text)}</div>
        </div>"""
        )
    return (
        f'<section class="panel"><h2>{html.escape(title)}</h2>'
        f'<div class="ylabel">{html.escape(ylabel)}</div><div class="bars">'
        f'{"".join(rows)}</div></section>'
    )


def line_chart_svg(title, series_defs, ylabel="", height=300):
    width = 900
    pad_l, pad_r, pad_t, pad_b = 54, 18, 34, 36
    plot_w = width - pad_l - pad_r
    plot_h = height - pad_t - pad_b
    all_values = []
    for series in series_defs:
        all_values.extend(value for value in series["values"] if value is not None)
    if not all_values:
        return (
            f'<section class="panel wide"><h2>{html.escape(title)}</h2>'
            '<p class="muted">无可绘制数据。</p></section>'
        )
    ymin, ymax = min(all_values), max(all_values)
    if ymin == ymax:
        ymax = ymin + 1

    def xy(index, count, value):
        x = pad_l + (index / max(1, count - 1)) * plot_w
        y = pad_t + (1 - (value - ymin) / (ymax - ymin)) * plot_h
        return x, y

    paths = []
    legend = []
    for series in series_defs:
        values = series["values"]
        if not values:
            continue
        points = [xy(index, len(values), value) for index, value in enumerate(values)]
        path_data = " ".join(
            ("M" if index == 0 else "L") + f"{x:.1f},{y:.1f}"
            for index, (x, y) in enumerate(points)
        )
        paths.append(
            f'<path d="{path_data}" fill="none" stroke="{series["color"]}" stroke-width="2" />'
        )
        legend.append(
            f'<span><i style="background:{series["color"]}"></i>{html.escape(series["label"])}</span>'
        )

    grid = ""
    for tick in range(5):
        y = pad_t + tick / 4 * plot_h
        value = ymax - tick / 4 * (ymax - ymin)
        grid += (
            f'<line x1="{pad_l}" y1="{y:.1f}" x2="{width-pad_r}" y2="{y:.1f}" stroke="#e5e7eb"/>'
            f'<text x="8" y="{y+4:.1f}" font-size="11" fill="#6b7280">{value:.2f}</text>'
        )
    return f"""
    <section class="panel wide"><h2>{html.escape(title)}</h2><div class="ylabel">{html.escape(ylabel)}</div>
      <svg viewBox="0 0 {width} {height}" role="img">
        {grid}
        <line x1="{pad_l}" y1="{pad_t}" x2="{pad_l}" y2="{height-pad_b}" stroke="#9ca3af"/>
        <line x1="{pad_l}" y1="{height-pad_b}" x2="{width-pad_r}" y2="{height-pad_b}" stroke="#9ca3af"/>
        {''.join(paths)}
      </svg>
      <div class="legend">{''.join(legend)}</div>
    </section>"""


def histogram_svg(title, datasets, bins=24, unit="", height=300):
    values = []
    for dataset in datasets:
        values.extend(value for value in dataset["values"] if value is not None)
    if not values:
        return (
            f'<section class="panel wide"><h2>{html.escape(title)}</h2>'
            '<p class="muted">无可绘制数据。</p></section>'
        )
    low, high = min(values), max(values)
    if low == high:
        high = low + 1
    groups = []
    max_count = 1
    for dataset in datasets:
        counts = [0] * bins
        for value in dataset["values"]:
            index = min(bins - 1, int((value - low) / (high - low) * bins))
            counts[index] += 1
        max_count = max(max_count, max(counts))
        groups.append((dataset, counts))

    width = 900
    pad_l, pad_r, pad_t, pad_b = 48, 18, 34, 44
    plot_w = width - pad_l - pad_r
    plot_h = height - pad_t - pad_b
    bar_group_w = plot_w / bins
    bar_w = bar_group_w / max(1, len(groups)) * 0.82
    rects = []
    for group_index, (dataset, counts) in enumerate(groups):
        for index, count in enumerate(counts):
            bar_h = count / max_count * plot_h
            x = pad_l + index * bar_group_w + group_index * (bar_group_w / max(1, len(groups)))
            y = pad_t + plot_h - bar_h
            rects.append(
                f'<rect x="{x:.1f}" y="{y:.1f}" width="{bar_w:.1f}" height="{bar_h:.1f}" '
                f'fill="{dataset["color"]}" opacity="0.72"/>'
            )
    legend = "".join(
        f'<span><i style="background:{dataset["color"]}"></i>{html.escape(dataset["label"])}</span>'
        for dataset, _ in groups
    )
    return f"""
    <section class="panel wide"><h2>{html.escape(title)}</h2>
      <svg viewBox="0 0 {width} {height}" role="img">
        <line x1="{pad_l}" y1="{height-pad_b}" x2="{width-pad_r}" y2="{height-pad_b}" stroke="#9ca3af"/>
        <line x1="{pad_l}" y1="{pad_t}" x2="{pad_l}" y2="{height-pad_b}" stroke="#9ca3af"/>
        {''.join(rects)}
        <text x="{pad_l}" y="{height-14}" font-size="11" fill="#6b7280">{low:.2f}{unit}</text>
        <text x="{width-pad_r-70}" y="{height-14}" font-size="11" fill="#6b7280">{high:.2f}{unit}</text>
      </svg>
      <div class="legend">{legend}</div>
    </section>"""


def table_html(frame):
    columns = [
        "method",
        "valid_benchmark",
        "successful_responses",
        "tokens_per_s",
        "qps",
        "e2e_p50_s",
        "e2e_p95_s",
        "e2e_p99_s",
        "migration_attempts",
        "migration_finished",
        "migration_cost_p50_ms",
        "migration_cost_p95_ms",
        "server_instance_rows",
        "error",
    ]
    labels = {
        "method": "实验组",
        "valid_benchmark": "benchmark有效",
        "successful_responses": "成功请求",
        "tokens_per_s": "tokens/s",
        "qps": "QPS",
        "e2e_p50_s": "E2E P50(s)",
        "e2e_p95_s": "E2E P95(s)",
        "e2e_p99_s": "E2E P99(s)",
        "migration_attempts": "迁移尝试",
        "migration_finished": "迁移成功",
        "migration_cost_p50_ms": "迁移P50(ms)",
        "migration_cost_p95_ms": "迁移P95(ms)",
        "server_instance_rows": "实例采样行",
        "error": "错误",
    }
    head = "".join(f"<th>{labels[column]}</th>" for column in columns)
    body = ""
    for _, row in frame.iterrows():
        cells = []
        for column in columns:
            value = row.get(column)
            if pd.isna(value):
                value = ""
            elif isinstance(value, float):
                value = f"{value:.3f}"
            cells.append(f"<td>{html.escape(str(value))}</td>")
        body += "<tr>" + "".join(cells) + "</tr>"
    return (
        '<section class="panel wide"><h2>关键指标表</h2><div class="table-wrap">'
        f"<table><thead><tr>{head}</tr></thead><tbody>{body}</tbody></table>"
        "</div></section>"
    )


def main():
    records = []
    for exp in EXPS:
        benchmark = parse_benchmark(exp["path"])
        migration = parse_migration(exp["path"])
        server_instance = parse_server_instance(exp["path"])
        exp["benchmark"] = benchmark
        exp["migration"] = migration
        exp["server_instance"] = server_instance
        records.append(
            {
                "method_id": exp["id"],
                "method": exp["short"],
                "valid_benchmark": benchmark.get("valid"),
                "error": benchmark.get("error"),
                "successful_responses": benchmark.get("successful_responses"),
                "duration_s": benchmark.get("duration_s"),
                "tokens_per_s": benchmark.get("tokens_per_s"),
                "qps": benchmark.get("qps"),
                "e2e_p50_s": benchmark.get("e2e_latency_s_p50")
                or benchmark.get("median_e2e_latency_s"),
                "e2e_mean_s": benchmark.get("e2e_latency_s_mean"),
                "e2e_p95_s": benchmark.get("e2e_latency_s_p95"),
                "e2e_p99_s": benchmark.get("e2e_latency_s_p99"),
                "e2e_max_s": benchmark.get("e2e_latency_s_max"),
                "per_token_p50_s": benchmark.get("per_token_latency_s_p50")
                or benchmark.get("median_token_latency_s"),
                "per_token_p95_s": benchmark.get("per_token_latency_s_p95"),
                "migration_attempts": migration.get("attempts"),
                "migration_finished": migration.get("finished"),
                "migration_aborted": migration.get("aborted"),
                "migration_cost_p50_ms": migration.get("finished_cost_ms_p50"),
                "migration_cost_mean_ms": migration.get("finished_cost_ms_mean"),
                "migration_cost_p95_ms": migration.get("finished_cost_ms_p95"),
                "migration_cost_max_ms": migration.get("finished_cost_ms_max"),
                "recv_speed_mean_gbps": migration.get("recv_speed_gbps_mean"),
                "server_instance_rows": server_instance.get("rows"),
                "waiting_requests_p95": server_instance.get("num_waiting_requests_p95"),
                "gpu_cache_usage_p95": server_instance.get("gpu_cache_usage_p95"),
            }
        )

    metrics_frame = pd.DataFrame(records)
    metrics_frame.to_csv(METRICS_CSV, index=False, encoding="utf-8-sig")

    labels = [exp["short"] for exp in EXPS]
    colors = [exp["color"] for exp in EXPS]
    successful = [exp["benchmark"].get("successful_responses") for exp in EXPS]
    throughput = [exp["benchmark"].get("tokens_per_s") for exp in EXPS]
    qps = [exp["benchmark"].get("qps") for exp in EXPS]
    e2e_p50 = [
        exp["benchmark"].get("e2e_latency_s_p50")
        or exp["benchmark"].get("median_e2e_latency_s")
        for exp in EXPS
    ]
    e2e_p95 = [exp["benchmark"].get("e2e_latency_s_p95") for exp in EXPS]
    e2e_p99 = [exp["benchmark"].get("e2e_latency_s_p99") for exp in EXPS]
    migration_attempts = [exp["migration"].get("attempts") for exp in EXPS]
    migration_finished = [exp["migration"].get("finished") for exp in EXPS]
    migration_p50 = [exp["migration"].get("finished_cost_ms_p50") for exp in EXPS]
    migration_p95 = [exp["migration"].get("finished_cost_ms_p95") for exp in EXPS]

    latency_datasets = [
        {
            "label": exp["short"],
            "values": exp["benchmark"].get("e2e_latency_s") or [],
            "color": exp["color"],
        }
        for exp in EXPS
        if exp["benchmark"].get("e2e_latency_s")
    ]
    migration_datasets = [
        {
            "label": exp["short"],
            "values": exp["migration"].get("finished_cost_ms") or [],
            "color": exp["color"],
        }
        for exp in EXPS
        if exp["migration"].get("finished_cost_ms")
    ]
    waiting_series = []
    cache_series = []
    for exp in EXPS:
        series = exp["server_instance"].get("series", {})
        if "num_waiting_requests" in series:
            waiting_series.append(
                {
                    "label": exp["short"],
                    "values": series["num_waiting_requests"],
                    "color": exp["color"],
                }
            )
        if "gpu_cache_usage" in series:
            cache_series.append(
                {
                    "label": exp["short"],
                    "values": series["gpu_cache_usage"],
                    "color": exp["color"],
                }
            )

    valid_count = sum(1 for exp in EXPS if exp["benchmark"].get("valid"))
    invalid_count = len(EXPS) - valid_count
    cards = f"""
<div class="cards">
  <div class="card"><span>实验组数量</span><strong>{len(EXPS)}</strong></div>
  <div class="card"><span>有效 benchmark</span><strong>{valid_count}</strong></div>
  <div class="card"><span>无效/未连上服务</span><strong>{invalid_count}</strong></div>
  <div class="card"><span>原始总表</span><strong>86,560 行</strong></div>
</div>
"""

    status_items = ""
    for exp in EXPS:
        benchmark = exp["benchmark"]
        status = "有效" if benchmark.get("valid") else "无效"
        css_class = "ok" if benchmark.get("valid") else "bad"
        detail = (
            "成功请求 " + str(benchmark.get("successful_responses"))
            if benchmark.get("valid")
            else benchmark.get("error") or "没有有效 benchmark"
        )
        status_items += (
            f'<li><span class="badge {css_class}">{status}</span>'
            f"<b>{html.escape(exp['short'])}</b><em>{html.escape(detail)}</em></li>"
        )

    html_doc = f"""<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width, initial-scale=1" />
<title>Llumnix 实验数据可视化报告</title>
<style>
:root {{ --bg:#f7f8fb; --panel:#ffffff; --text:#111827; --muted:#6b7280; --line:#e5e7eb; }}
* {{ box-sizing:border-box; }}
body {{ margin:0; font-family:"Microsoft YaHei", "Segoe UI", Arial, sans-serif; background:var(--bg); color:var(--text); }}
header {{ padding:28px 32px 18px; background:#ffffff; border-bottom:1px solid var(--line); }}
h1 {{ margin:0 0 8px; font-size:28px; }}
.sub {{ color:var(--muted); line-height:1.6; max-width:1100px; }}
main {{ padding:24px 32px 40px; }}
.cards {{ display:grid; grid-template-columns:repeat(4,minmax(0,1fr)); gap:14px; margin-bottom:18px; }}
.card {{ background:var(--panel); border:1px solid var(--line); border-radius:8px; padding:16px; }}
.card span {{ display:block; color:var(--muted); font-size:13px; margin-bottom:8px; }}
.card strong {{ font-size:24px; }}
.grid {{ display:grid; grid-template-columns:repeat(2,minmax(0,1fr)); gap:18px; }}
.panel {{ background:var(--panel); border:1px solid var(--line); border-radius:8px; padding:18px; min-width:0; }}
.panel.wide {{ grid-column:1 / -1; }}
h2 {{ margin:0 0 12px; font-size:18px; }}
.ylabel {{ color:var(--muted); font-size:12px; margin-bottom:10px; }}
.bars {{ display:flex; flex-direction:column; gap:12px; }}
.bar-row {{ display:grid; grid-template-columns:190px 1fr 94px; align-items:center; gap:10px; }}
.bar-label {{ font-size:13px; color:#374151; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }}
.bar-track {{ height:18px; background:#eef2f7; border-radius:4px; overflow:hidden; }}
.bar-fill {{ height:100%; border-radius:4px; }}
.bar-value {{ text-align:right; font-variant-numeric:tabular-nums; font-size:13px; color:#374151; }}
.status-list {{ list-style:none; padding:0; margin:0; display:grid; gap:10px; }}
.status-list li {{ display:grid; grid-template-columns:74px 230px 1fr; gap:10px; align-items:center; border-bottom:1px solid #f1f5f9; padding-bottom:10px; }}
.badge {{ display:inline-flex; justify-content:center; padding:4px 8px; border-radius:999px; font-size:12px; }}
.badge.ok {{ background:#dcfce7; color:#166534; }}
.badge.bad {{ background:#fee2e2; color:#991b1b; }}
em {{ color:var(--muted); font-style:normal; font-size:13px; }}
.legend {{ display:flex; flex-wrap:wrap; gap:14px; margin-top:8px; color:#374151; font-size:13px; }}
.legend span {{ display:inline-flex; align-items:center; gap:6px; }}
.legend i {{ width:12px; height:12px; display:inline-block; border-radius:2px; }}
.table-wrap {{ overflow:auto; }}
table {{ border-collapse:collapse; width:100%; font-size:13px; }}
th,td {{ border-bottom:1px solid #eef2f7; text-align:left; padding:9px 10px; vertical-align:top; }}
th {{ background:#f8fafc; position:sticky; top:0; }}
.muted {{ color:var(--muted); }}
.note {{ background:#fff7ed; border:1px solid #fed7aa; color:#7c2d12; border-radius:8px; padding:12px 14px; margin-bottom:18px; line-height:1.6; }}
footer {{ color:var(--muted); padding:20px 32px 40px; font-size:13px; }}
@media (max-width: 900px) {{ .cards,.grid {{ grid-template-columns:1fr; }} .bar-row {{ grid-template-columns:1fr; }} .bar-value {{ text-align:left; }} .status-list li {{ grid-template-columns:1fr; }} }}
</style>
</head>
<body>
<header>
<h1>Llumnix 实验数据可视化报告</h1>
<div class="sub">围绕论文关注的服务性能、迁移效率、负载/碎片状态进行可视化。数据来源为当前目录下三组 baseline 与 Llumnix 方法实验日志；原始数据总表为 <code>all_experiment_raw_data.csv</code>。</div>
</header>
<main>
{cards}
<div class="note">注意：图表不会把失败的 baseline 伪装成有效数据。若 benchmark 没连上 <code>127.0.0.1:1234</code>，该组在性能图中显示为“无有效数据”。</div>
<section class="panel wide"><h2>实验有效性</h2><ul class="status-list">{status_items}</ul></section>
<div class="grid">
{bar_chart("成功请求数", labels, successful, colors, "successful_responses")}
{bar_chart("吞吐量对比", labels, throughput, colors, "tokens/s")}
{bar_chart("QPS 对比", labels, qps, colors, "requests/s")}
{bar_chart("端到端延迟 P50", labels, e2e_p50, colors, "seconds")}
{bar_chart("端到端延迟 P95", labels, e2e_p95, colors, "seconds")}
{bar_chart("端到端延迟 P99", labels, e2e_p99, colors, "seconds")}
{histogram_svg("端到端延迟分布", latency_datasets, bins=28, unit="s")}
{bar_chart("迁移尝试次数", labels, migration_attempts, colors, "count")}
{bar_chart("成功迁移次数", labels, migration_finished, colors, "count")}
{bar_chart("成功迁移耗时 P50", labels, migration_p50, colors, "ms")}
{bar_chart("成功迁移耗时 P95", labels, migration_p95, colors, "ms")}
{histogram_svg("成功迁移耗时分布", migration_datasets, bins=24, unit="ms")}
{line_chart_svg("等待队列变化", waiting_series, "num_waiting_requests")}
{line_chart_svg("GPU cache usage 变化", cache_series, "gpu_cache_usage")}
{table_html(metrics_frame)}
</div>
</main>
<footer>生成文件：<code>{html.escape(str(OUT))}</code>；指标 CSV：<code>{html.escape(str(METRICS_CSV))}</code>。</footer>
</body>
</html>"""
    OUT.write_text(html_doc, encoding="utf-8")
    print(OUT)
    print(METRICS_CSV)
    print(metrics_frame.to_string(index=False))


if __name__ == "__main__":
    main()
