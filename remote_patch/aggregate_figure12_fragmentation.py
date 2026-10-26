#!/usr/bin/env python3
import html
import json
import re
import shutil
import statistics
from datetime import datetime
from pathlib import Path

ROOT = Path("/data/home/zylu/llumnix-v018-main/results/5090_paper_supplement_20260821_170651")
OUT_RUNS = ROOT / "02_fig12_fragmentation"
FINAL = ROOT / "05_final"
STEP = ROOT / "04_step_summaries"

SOURCES = {
    "no_migration": [ROOT / f"03_runs/figure12/fragmentation_medium/no_migration/run_{i}" for i in range(1, 4)],
    "load_sr": [ROOT / f"03_runs/figure12/fragmentation_medium/load_sr/run_{i}" for i in range(1, 4)],
    "tpot_tail": [
        ROOT / "03_runs/figure12_endpoint_guard_retry/fragmentation_medium/tpot_tail/run_1",
        ROOT / "03_runs/figure12/fragmentation_medium/tpot_tail/run_2",
        ROOT / "03_runs/figure12/fragmentation_medium/tpot_tail/run_3",
    ],
    "composite_proposed": [ROOT / f"03_runs/figure12/fragmentation_medium/composite_proposed/run_{i}" for i in range(1, 4)],
}

LABELS = {
    "no_migration": "不迁移",
    "load_sr": "Load-SR（KV 小优先）",
    "tpot_tail": "TPOT 尾延迟感知",
    "composite_proposed": "综合策略 Proposed",
}

STRICT_ZERO = [
    "EngineCore_fatal_count", "Scheduler_fatal_count", "Traceback_count",
    "KVTResp_code_500_count", "abort_count", "finish_reason_abort_count",
    "CUDA_illegal_memory_access_count",
]

def percentile(values, p):
    values = sorted(values)
    if not values:
        return None
    pos = (len(values) - 1) * p / 100
    lo, hi = int(pos), min(int(pos) + 1, len(values) - 1)
    return values[lo] + (values[hi] - values[lo]) * (pos - lo)

def true_error_lines(run_dir):
    found = []
    for name in ["redis.log", "scheduler.log", *[p.name for p in run_dir.glob("runtime_*.log")], *[p.name for p in run_dir.glob("vllm_*.log")]]:
        path = run_dir / name
        if not path.exists():
            continue
        for lineno, line in enumerate(path.read_text(errors="replace").splitlines(), 1):
            if re.search(r"(?:^|\))\s*ERROR(?:\s|:)", line) or re.match(r"^ERROR:", line):
                if "ERROR:blade_kvt.hybrid_connector.kvtbackend:disagg kvt done." not in line:
                    found.append(f"{name}:{lineno}:{line}")
    return found

def mean(rows, key):
    vals = [r.get(key) for r in rows if isinstance(r.get(key), (int, float))]
    return statistics.mean(vals) if vals else None

def total(rows, key):
    return sum((r.get(key) or 0) for r in rows)

def fmt(value, digits=3):
    if value is None or value == "NA":
        return "NA"
    if isinstance(value, float):
        return f"{value:.{digits}f}"
    return str(value)

OUT_RUNS.mkdir(parents=True, exist_ok=True)
FINAL.mkdir(parents=True, exist_ok=True)
STEP.mkdir(parents=True, exist_ok=True)

runs_by_policy = {}
validation_errors = []
for policy, dirs in SOURCES.items():
    rows = []
    for repeat_id, run_dir in enumerate(dirs, 1):
        src = run_dir / "run_summary.json"
        if not src.exists():
            validation_errors.append(f"missing: {src}")
            continue
        data = json.loads(src.read_text())
        actual_errors = true_error_lines(run_dir)
        derived = dict(data)
        derived.update({
            "official_figure12_policy": policy,
            "official_figure12_repeat_id": repeat_id,
            "official_source_path": str(run_dir),
            "raw_ERROR_count_from_original_parser": data.get("ERROR_count"),
            "actual_service_ERROR_count": len(actual_errors),
            "actual_service_ERROR_lines": actual_errors,
            "http_200_text_nonempty": data.get("http_200_text_nonempty", 0),
        })
        exact = data.get("exact_instance_status_sample_count") or 0
        frag = data.get("estimated_fragmentation_sample_count") or 0
        derived["estimated_fragmentation_proportion"] = frag / exact if exact else None
        derived["long_input_TTFT_mean_s"] = None
        derived["long_input_TTFT_p95_s"] = None
        derived["long_input_TTFT_p99_s"] = None
        derived["prefill_mean_s"] = None
        derived["prefill_p95_s"] = None
        derived["prefill_p99_s"] = None
        derived["avg_queue_delay_s"] = None
        out_dir = OUT_RUNS / policy / f"run_{repeat_id}"
        out_dir.mkdir(parents=True, exist_ok=True)
        (out_dir / "run_summary.json").write_text(json.dumps(derived, ensure_ascii=False, indent=2))
        rows.append(derived)

        if data.get("successful_responses") != 500:
            validation_errors.append(f"{policy}/run_{repeat_id}: successful_responses={data.get('successful_responses')}")
        if data.get("http_200_text_nonempty") != 500 or data.get("empty_text_count") != 0:
            validation_errors.append(f"{policy}/run_{repeat_id}: nonempty={data.get('http_200_text_nonempty')}")
        for key in STRICT_ZERO:
            if (data.get(key) or 0) != 0:
                validation_errors.append(f"{policy}/run_{repeat_id}: {key}={data.get(key)}")
        if actual_errors:
            validation_errors.append(f"{policy}/run_{repeat_id}: actual_service_ERROR_count={len(actual_errors)}")
    runs_by_policy[policy] = rows

aggregates = {}
for policy, rows in runs_by_policy.items():
    frag_props = [r["estimated_fragmentation_proportion"] for r in rows if r["estimated_fragmentation_proportion"] is not None]
    aggregates[policy] = {
        "label": LABELS[policy],
        "run_count": len(rows),
        "successful_responses_total": total(rows, "successful_responses"),
        "http_200_text_nonempty_total": total(rows, "http_200_text_nonempty"),
        "QPS_mean": mean(rows, "QPS"),
        "throughput_tok_s_mean": mean(rows, "output_throughput_tok_s"),
        "E2E_median_mean_s": mean(rows, "median_e2e_latency_s"),
        "E2E_p95_mean_s": mean(rows, "P95_e2e_latency_s"),
        "E2E_p99_mean_s": mean(rows, "P99_e2e_latency_s"),
        "long_input_E2E_median_mean_s": mean(rows, "large_arrival_median_e2e_s"),
        "long_input_E2E_p95_mean_s": mean(rows, "large_arrival_p95_e2e_s"),
        "long_input_E2E_p99_mean_s": mean(rows, "large_arrival_p99_e2e_s"),
        "long_input_TTFT_mean_s": None,
        "long_input_TTFT_p95_s": None,
        "long_input_TTFT_p99_s": None,
        "prefill_mean_s": None,
        "prefill_p95_s": None,
        "prefill_p99_s": None,
        "fragmentation_proportion_mean": statistics.mean(frag_props) if frag_props else None,
        "fragmentation_proportion_p95": percentile(frag_props, 95),
        "fragmentation_proportion_p99": percentile(frag_props, 99),
        "blocked_HOL_request_count_total": total(rows, "estimated_HOL_sample_count"),
        "avg_queue_delay_s": None,
        "migration_count_total": total(rows, "triggered_migration_count"),
        "successful_migration_total": total(rows, "successful_migration_count"),
        "failed_migration_total": total(rows, "failed_migration_count"),
        "stale_migration_update_total": total(rows, "stale_migration_update_count"),
        "invalid_migration_endpoint_total": total(rows, "invalid_migration_endpoint_count"),
        "useless_migration_total": total(rows, "useless_migration_count"),
        "KVTResp_code_0_total": total(rows, "KVTResp_code_0_count"),
        "KVTResp_code_404_total": total(rows, "KVTResp_code_404_count"),
        "KVTResp_code_500_total": total(rows, "KVTResp_code_500_count"),
        "FinishedReq_total": total(rows, "FinishedReq_lines"),
        "SendStubMetrics_total": total(rows, "SendStubMetrics_count"),
        "EngineCore_fatal_total": total(rows, "EngineCore_fatal_count"),
        "Scheduler_fatal_total": total(rows, "Scheduler_fatal_count"),
        "Traceback_total": total(rows, "Traceback_count"),
        "raw_ERROR_total": total(rows, "raw_ERROR_count_from_original_parser"),
        "actual_service_ERROR_total": total(rows, "actual_service_ERROR_count"),
        "abort_total": total(rows, "abort_count"),
        "runs": [{k: r.get(k) for k in [
            "official_figure12_repeat_id", "official_source_path", "fairness_status", "gpu_contention",
            "successful_responses", "http_200_text_nonempty", "QPS", "output_throughput_tok_s",
            "median_e2e_latency_s", "P95_e2e_latency_s", "P99_e2e_latency_s",
            "large_arrival_median_e2e_s", "large_arrival_p95_e2e_s", "large_arrival_p99_e2e_s",
            "estimated_fragmentation_proportion", "estimated_HOL_sample_count", "triggered_migration_count",
            "successful_migration_count", "failed_migration_count", "stale_migration_update_count",
            "invalid_migration_endpoint_count", "KVTResp_code_0_count", "KVTResp_code_404_count",
            "KVTResp_code_500_count", "FinishedReq_lines", "SendStubMetrics_count", "EngineCore_fatal_count",
            "Scheduler_fatal_count", "Traceback_count", "raw_ERROR_count_from_original_parser",
            "actual_service_ERROR_count", "abort_count"
        ]} for r in rows],
    }

best_fragmentation = min(aggregates, key=lambda p: aggregates[p]["fragmentation_proportion_mean"] if aggregates[p]["fragmentation_proportion_mean"] is not None else float("inf"))
best_p99 = min(aggregates, key=lambda p: aggregates[p]["E2E_p99_mean_s"] if aggregates[p]["E2E_p99_mean_s"] is not None else float("inf"))

all_runs_completed = all(len(v) == 3 for v in runs_by_policy.values())
strict_validation_passed = all_runs_completed and not validation_errors
result = {
    "experiment": "Figure 12 fragmentation",
    "generated_at": datetime.now().isoformat(timespec="seconds"),
    "result_root": str(ROOT),
    "all_runs_completed": all_runs_completed,
    "strict_validation_passed": strict_validation_passed,
    "complete": strict_validation_passed,
    "validation_errors": validation_errors,
    "metric_availability": {
        "fragmentation": "estimated from exported free-token capacity; not exact block layout",
        "long_input_TTFT": "NA: response records do not expose TTFT",
        "prefill": "NA: response records do not expose prefill duration",
        "avg_queue_delay": "NA: no per-request queue timestamps",
        "raw_ERROR": "original parser includes model response payload; actual_service_ERROR excludes gateway payload",
    },
    "best": {
        "fragmentation": best_fragmentation,
        "long_input_TTFT": None,
        "E2E_P99": best_p99,
    },
    "policies": aggregates,
}

json_path = FINAL / "fig12_fragmentation_aggregated.json"
json_path.write_text(json.dumps(result, ensure_ascii=False, indent=2))

md = [
    "# Figure 12 碎片化实验汇总", "",
    f"- 执行状态：{'12/12 轮已执行' if result['all_runs_completed'] else '轮次不完整'}",
    f"- 严格验收：{'通过' if result['strict_validation_passed'] else '未通过'}",
    f"- 有效轮次：{sum(len(v) for v in runs_by_policy.values())}/12",
    "- 每轮配置：500 请求，concurrency=50，4 实例",
    "- 真实 long-input TTFT / prefill / queue delay：未记录，未使用 E2E 冒充",
    "- 碎片化：由 exported free-token capacity 估算，不代表精确 block layout",
    "", "## 聚合结果", "",
    "| 策略 | 成功请求 | QPS | 吞吐 tok/s | E2E P99 s | 长输入 E2E P99 s | 碎片化比例 | 迁移成功/失败 | KVT 0/404/500 | 真实 ERROR |",
    "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
]
for policy, a in aggregates.items():
    md.append(f"| {a['label']} | {a['successful_responses_total']}/1500 | {fmt(a['QPS_mean'])} | {fmt(a['throughput_tok_s_mean'])} | {fmt(a['E2E_p99_mean_s'])} | {fmt(a['long_input_E2E_p99_mean_s'])} | {fmt(a['fragmentation_proportion_mean'])} | {a['successful_migration_total']}/{a['failed_migration_total']} | {a['KVTResp_code_0_total']}/{a['KVTResp_code_404_total']}/{a['KVTResp_code_500_total']} | {a['actual_service_ERROR_total']} |")
md += ["", "## 最优项", "", f"- 碎片化比例最低：{LABELS[best_fragmentation]}（本组所有策略均为 0，实际并列）", "- long-input TTFT：无法判定（原始记录未提供 TTFT）", f"- E2E P99 最低：{LABELS[best_p99]}", "", "## 验收", "", f"- 验收问题：{validation_errors or '无'}", "- 原始 ERROR 误计说明：gateway 响应载荷中的模型文本包含 `ERROR`，已与真实服务 ERROR 分开统计。"]
md_path = STEP / "fig12_fragmentation_summary.md"
md_path.write_text("\n".join(md))

def td(v, cls=""):
    return f'<td class="{cls}">{html.escape(fmt(v))}</td>'

agg_rows = []
for policy, a in aggregates.items():
    agg_rows.append("<tr>" + td(a["label"], "policy") + td(a["successful_responses_total"]) + td(a["QPS_mean"]) + td(a["throughput_tok_s_mean"]) + td(a["E2E_median_mean_s"]) + td(a["E2E_p95_mean_s"]) + td(a["E2E_p99_mean_s"]) + td(a["long_input_E2E_p99_mean_s"]) + td(a["fragmentation_proportion_mean"]) + "</tr>")
mig_rows = []
for policy, a in aggregates.items():
    mig_rows.append("<tr>" + td(a["label"], "policy") + td(a["migration_count_total"]) + td(a["successful_migration_total"]) + td(a["failed_migration_total"]) + td(a["stale_migration_update_total"]) + td(a["invalid_migration_endpoint_total"]) + td(a["KVTResp_code_0_total"]) + td(a["KVTResp_code_404_total"]) + td(a["KVTResp_code_500_total"]) + td(a["FinishedReq_total"]) + td(a["SendStubMetrics_total"]) + "</tr>")
run_rows = []
for policy, a in aggregates.items():
    for r in a["runs"]:
        run_rows.append("<tr>" + td(a["label"], "policy") + td(r["official_figure12_repeat_id"]) + td(r["successful_responses"]) + td(r["QPS"]) + td(r["output_throughput_tok_s"]) + td(r["median_e2e_latency_s"]) + td(r["P95_e2e_latency_s"]) + td(r["P99_e2e_latency_s"]) + td(r["actual_service_ERROR_count"]) + td(r["fairness_status"]) + "</tr>")

css = """body{font-family:Arial,'Microsoft YaHei',sans-serif;margin:24px;color:#1e293b;background:#f8fafc}h1{font-size:25px}h2{font-size:18px;margin-top:28px}.meta{color:#475569;margin-bottom:18px}.panel{background:white;border:1px solid #dbe3ec;border-radius:6px;padding:16px;margin:14px 0}table{width:100%;border-collapse:collapse;font-size:13px;table-layout:fixed}th,td{border:1px solid #dbe3ec;padding:7px;text-align:center;overflow-wrap:anywhere}th{background:#e8eef5}.policy{text-align:left;font-weight:700}.ok{color:#166534;font-weight:700}.na{background:#f1f5f9;color:#64748b}.note{font-size:13px;color:#475569;line-height:1.7}code{font-size:12px}"""
html_doc = f"""<!doctype html><html lang="zh-CN"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Llumnix Figure 12 碎片化实验</title><style>{css}</style></head><body>
<h1>Llumnix RTX 5090 Figure 12 碎片化实验</h1><div class="meta">结果目录：{html.escape(str(ROOT))}</div>
<div class="panel"><b>执行：{'12/12 轮完成' if result['all_runs_completed'] else '轮次不完整'}；严格验收：{'通过' if result['strict_validation_passed'] else '未通过（Load-SR 历史三轮存在 load fail abort）'}</b><div class="note">每轮 500 请求、并发 50、4 实例。真实 TTFT、prefill 与 queue delay 未记录，表中不以 E2E 替代。碎片化为 exported free-token capacity 估算。</div></div>
<h2>性能与碎片化</h2><div class="panel"><table><thead><tr><th>策略</th><th>成功请求</th><th>QPS</th><th>吞吐 tok/s</th><th>E2E 中位数 s</th><th>E2E P95 s</th><th>E2E P99 s</th><th>长输入 E2E P99 s</th><th>碎片化比例</th></tr></thead><tbody>{''.join(agg_rows)}</tbody></table></div>
<h2>迁移证据</h2><div class="panel"><table><thead><tr><th>策略</th><th>触发迁移</th><th>成功迁移</th><th>失败迁移</th><th>stale update</th><th>invalid endpoint</th><th>KVT 0</th><th>KVT 404</th><th>KVT 500</th><th>FinishedReq</th><th>SendStubMetrics</th></tr></thead><tbody>{''.join(mig_rows)}</tbody></table></div>
<h2>逐轮结果</h2><div class="panel"><table><thead><tr><th>策略</th><th>轮次</th><th>成功请求</th><th>QPS</th><th>吞吐 tok/s</th><th>E2E 中位数 s</th><th>E2E P95 s</th><th>E2E P99 s</th><th>真实 ERROR</th><th>公平性</th></tr></thead><tbody>{''.join(run_rows)}</tbody></table></div>
<div class="note">生成时间：{html.escape(result['generated_at'])}<br>原始 ERROR 统计可能命中模型响应文本；“真实 ERROR”仅匹配服务日志级错误并排除 gateway 响应载荷。</div></body></html>"""
html_path = FINAL / "fig12_fragmentation_table.html"
html_path.write_text(html_doc)

print(json.dumps({
    "all_runs_completed": result["all_runs_completed"],
    "strict_validation_passed": result["strict_validation_passed"],
    "complete": result["complete"], "validation_errors": validation_errors,
    "summary": str(md_path), "html": str(html_path), "json": str(json_path),
    "best": result["best"], "labels": LABELS,
}, ensure_ascii=False, indent=2))
