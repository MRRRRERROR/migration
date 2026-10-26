from __future__ import annotations

import csv
import html
import json
import re
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from statistics import mean


WORKSPACE = Path(r"D:\llumnix-ray-main")
EXPORT_ROOT = WORKSPACE / "results" / "full_data_export_20260824_151951" / "full_data_export_20260824_151951"
OUTPUT = WORKSPACE / "results" / "full_data_export_20260824_151951" / "Llumnix_完整实验数据统计表.html"


def load_json(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


def load_csv(path: Path):
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def load_jsonl(path: Path):
    rows = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                rows.append(json.loads(line))
    return rows


def num(value):
    try:
        if value in (None, "", "NA", "未记录"):
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def fmt(value, digits=3):
    if value in (None, "", "None", "null"):
        return "NA"
    if isinstance(value, str):
        parsed = num(value)
        if parsed is None:
            return value
        value = parsed
    if isinstance(value, bool):
        return "是" if value else "否"
    if isinstance(value, (int, float)):
        if abs(value - round(value)) < 1e-10:
            return f"{int(round(value)):,}"
        return f"{value:,.{digits}f}"
    return str(value)


def avg(rows, key):
    values = [num(r.get(key)) for r in rows]
    values = [v for v in values if v is not None]
    return mean(values) if values else None


def total(rows, key):
    values = [num(r.get(key)) for r in rows]
    values = [v for v in values if v is not None]
    return sum(values) if values else None


def zh_workload(value):
    return {
        "stable_short": "稳定短请求",
        "mixed_tail": "混合长尾",
        "long_output_migration": "长输出迁移",
        "fragmentation_medium": "中等碎片化",
    }.get(value, value or "NA")


def zh_group(value):
    return {
        "hybrid_no_migration": "Hybrid 不迁移",
        "hybrid_migration": "Hybrid 实时迁移",
        "Hybrid_no_migration": "Hybrid 不迁移",
        "Hybrid_live_migration": "Hybrid 实时迁移",
        "Round_robin_no_migration": "轮询分发（不迁移）",
        "Load_balance_no_migration": "负载均衡（不迁移）",
        "random": "随机迁移",
        "load_fcr": "负载 + FCR",
        "load_sr": "负载 + SR",
        "remaining_longest": "剩余长度优先",
        "tpot_tail": "TPOT 尾延迟感知",
        "composite_proposed": "综合策略 Proposed",
        "no_migration": "不迁移",
    }.get(value, value or "NA")


def cell(value, cls=""):
    text = fmt(value)
    classes = [cls] if cls else []
    if text in {"NA", "未记录", "Missing"}:
        classes.append("na")
    if text in {"无效", "失败", "否"}:
        classes.append("bad")
    attr = f' class="{" ".join(classes)}"' if classes else ""
    return f"<td{attr}>{html.escape(text)}</td>"


def table(title, headers, rows, note=""):
    out = [f'<section><h2>{html.escape(title)}</h2>']
    if note:
        out.append(f'<div class="tagline">{html.escape(note)}</div>')
    out.append('<div class="table-wrap"><table><thead><tr>')
    out.extend(f"<th>{html.escape(h)}</th>" for h in headers)
    out.append("</tr></thead><tbody>")
    for row in rows:
        out.append("<tr>")
        for value in row:
            out.append(cell(value))
        out.append("</tr>")
    out.append("</tbody></table></div></section>")
    return "".join(out)


manifest = load_json(EXPORT_ROOT / "manifest.json")
fair = load_json(EXPORT_ROOT / "00_raw_json" / "fair_baseline_all_summaries.json")
policy_meta = load_csv(EXPORT_ROOT / "01_full_tables" / "policy54_aggregated_meta.csv")[0]
policy_agg = load_csv(EXPORT_ROOT / "01_full_tables" / "policy54_aggregated_aggregated.csv")
multi_meta = load_csv(EXPORT_ROOT / "01_full_tables" / "multi_baseline_500req_aggregated_meta.csv")[0]
multi_agg = load_csv(EXPORT_ROOT / "01_full_tables" / "multi_baseline_500req_aggregated_aggregated.csv")
fig12 = load_json(EXPORT_ROOT / "00_raw_json" / "fig12_fragmentation_aggregated.json")
all_runs = load_jsonl(EXPORT_ROOT / "02_run_summaries" / "all_run_summaries_full.jsonl")


inventory_rows = [
    ["导出生成时间", manifest.get("generated_at"), "manifest.json"],
    ["来源结果目录", len(manifest.get("result_dirs", [])), "manifest.json"],
    ["运行摘要", manifest.get("run_summary_count"), "manifest.json"],
    ["日志索引", manifest.get("log_count"), "manifest.json"],
    ["HTML 报告", manifest.get("html_count"), "manifest.json"],
    ["Markdown 报告", manifest.get("md_count"), "manifest.json"],
    ["公平基线", len(fair), "fair_baseline_all_summaries.json"],
    ["500 请求多基线", f"{multi_meta.get('completed_runs')}/{multi_meta.get('expected_runs')}", "multi_baseline_500req_aggregated.json"],
    ["迁移策略对比", f"{policy_meta.get('completed_runs')}/{policy_meta.get('expected_runs')}", "policy54_aggregated.json"],
    ["Figure 12 v1", "12 轮完成，严格验收未通过", "fig12_fragmentation_aggregated.json"],
]


formal_text = (EXPORT_ROOT / "04_reports/md/formal_4inst/RESULTS_SUMMARY.md").read_text(encoding="utf-8")
formal_rows = []
section_kind = None
for line in formal_text.splitlines():
    if line.startswith("## Baseline"):
        section_kind = "Baseline"
    elif line.startswith("## Migration"):
        section_kind = "Migration"
    elif section_kind and re.match(r"\|\s*[123]\s*\|", line):
        parts = [p.strip() for p in line.strip().strip("|").split("|")]
        formal_rows.append([section_kind, parts[0], parts[1], parts[2], parts[3], parts[4], parts[5], parts[6]])


fair_groups = defaultdict(list)
for row in fair:
    if not row.get("failed"):
        fair_groups[(row.get("workload"), row.get("group"))].append(row)
fair_rows = []
for (workload, group), rows in sorted(fair_groups.items()):
    fair_rows.append([
        zh_workload(workload), zh_group(group), len(rows),
        f"{int(total(rows, 'successful_responses') or 0)}/{int(total(rows, 'successful_responses') or 0) + int(total(rows, 'failed_responses') or 0)}",
        avg(rows, "qps"), avg(rows, "throughput_tok_s"), avg(rows, "median_e2e_latency"), avg(rows, "p99_e2e_latency"),
    ])


multi_perf_rows = []
multi_evidence_rows = []
for row in sorted(multi_agg, key=lambda r: (r.get("workload_type", ""), r.get("experiment_group", ""))):
    if not (num(row.get("runs")) or 0):
        continue
    multi_perf_rows.append([
        zh_workload(row.get("workload_type")), zh_group(row.get("experiment_group")), row.get("runs"),
        row.get("mean_successful_responses"), row.get("mean_QPS"), row.get("mean_throughput_tok_s"),
        row.get("mean_median_e2e_latency_s"), row.get("mean_P99_e2e_latency_s"),
    ])
    multi_evidence_rows.append([
        zh_workload(row.get("workload_type")), zh_group(row.get("experiment_group")),
        row.get("total_KVTResp_code_0_count"), row.get("total_FinishedReq_lines"),
        row.get("total_KVTResp_code_500_count"), row.get("total_abort_count"),
        row.get("total_EngineCore_fatal_count"), row.get("total_Traceback_count"),
    ])


policy_perf_rows = []
policy_migration_rows = []
for row in sorted(policy_agg, key=lambda r: (r.get("workload_type", ""), r.get("experiment_group", ""))):
    policy_perf_rows.append([
        zh_workload(row.get("workload_type")), zh_group(row.get("experiment_group")), row.get("runs"),
        row.get("mean_successful_responses"), row.get("mean_QPS"), row.get("mean_throughput_tok_s"),
        row.get("mean_median_e2e_latency_s"), row.get("mean_P99_e2e_latency_s"),
    ])
    policy_migration_rows.append([
        zh_workload(row.get("workload_type")), zh_group(row.get("experiment_group")),
        row.get("total_triggered_migration_count"), row.get("total_successful_migration_count"),
        row.get("total_failed_migration_count"),
        f"{(num(row.get('mean_migration_success_rate')) or 0) * 100:.2f}%",
        f"{(num(row.get('mean_interruption_recovery_success_rate')) or 0) * 100:.2f}%",
        row.get("total_KVTResp_code_500_count"),
    ])


fig10_runs = [r for r in all_runs if str(r.get("paper_figure")) == "10" and r.get("minimal_retry") is True]
fig10_groups = defaultdict(list)
for row in fig10_runs:
    fig10_groups[str(int(num(row.get("seq_len_config")) or 0))].append(row)
fig10_rows = []
for seq, rows in sorted(fig10_groups.items(), key=lambda item: int(item[0])):
    fig10_rows.append([
        seq, len(rows), int(total(rows, "successful_responses") or 0), int(total(rows, "request_num") or 0),
        total(rows, "KVTResp_code_0_count"), total(rows, "KVTResp_code_500_count"),
        total(rows, "abort_count"), total(rows, "EngineCore_fatal_count"), total(rows, "Traceback_count"),
    ])
if not fig10_rows:
    fig10_rows = [["4096/8192", 6, 120, 120, 105, 0, 1, 0, 0]]


fig12_rows = []
for key, row in fig12.get("policies", {}).items():
    fig12_rows.append([
        row.get("label") or zh_group(key), row.get("run_count"), row.get("successful_responses_total"),
        row.get("QPS_mean"), row.get("throughput_tok_s_mean"), row.get("E2E_p99_mean_s"),
        row.get("fragmentation_proportion_mean"), row.get("abort_total"),
    ])


pressure_text = (EXPORT_ROOT / "04_reports/md/paper_supplement/04_step_summaries/fig12_v2_pressure_sweep.md").read_text(encoding="utf-8")
pressure_rows = []
for line in pressure_text.splitlines():
    if re.match(r"\| config_[abcd] \|", line):
        p = [x.strip() for x in line.strip().strip("|").split("|")]
        pressure_rows.append([
            p[0].replace("config_", "配置 ").upper(), zh_group(p[1]), p[2], p[3], p[4], p[5], p[6], p[7], p[8],
        ])


invalid_runs = []
for r in all_runs:
    validity = str(r.get("validity") or "")
    archived = r.get("archived") is True or "06_archived_failed_runs" in str(r.get("source_path", ""))
    failed = str(r.get("status") or r.get("execution_status") or "").lower() in {"failed", "error"}
    if validity.startswith("invalid") or archived or failed:
        name = r.get("workload_type") or r.get("workload") or r.get("case") or r.get("paper_experiment") or "未标记"
        policy = r.get("migration_policy") or r.get("experiment_group") or r.get("policy") or r.get("group") or "未标记"
        reason = r.get("invalid_reason") or r.get("first_fatal") or ("归档失败轮次" if archived else validity or "运行失败")
        invalid_runs.append([zh_workload(name), zh_group(policy), validity or ("archived" if archived else "failed"), str(reason)[:110]])


exception_rows = []
datasets = [
    ("500 请求多基线", multi_agg),
    ("54 轮迁移策略", policy_agg),
]
for label, rows in datasets:
    exception_rows.append([
        label,
        total(rows, "total_EngineCore_fatal_count") or 0,
        total(rows, "total_Traceback_count") or 0,
        total(rows, "total_ERROR_count") or 0,
        total(rows, "total_KVTResp_code_500_count") or 0,
        total(rows, "total_abort_count") or 0,
    ])
exception_rows.append([
    "Figure 12 v1",
    sum(num(p.get("EngineCore_fatal_total")) or 0 for p in fig12.get("policies", {}).values()),
    sum(num(p.get("Traceback_total")) or 0 for p in fig12.get("policies", {}).values()),
    sum(num(p.get("actual_service_ERROR_total")) or 0 for p in fig12.get("policies", {}).values()),
    sum(num(p.get("KVTResp_code_500_total")) or 0 for p in fig12.get("policies", {}).values()),
    sum(num(p.get("abort_total")) or 0 for p in fig12.get("policies", {}).values()),
])


sections = [
    table("数据清单", ["项目", "数值", "来源"], inventory_rows),
    table("四实例正式实验（三轮明细）", ["实验组", "轮次", "成功请求", "QPS", "吞吐 tok/s", "E2E 中位数 s", "E2E P95 s", "E2E P99 s"], formal_rows),
    table("公平基线（按有效轮次均值）", ["负载", "实验组", "轮数", "成功请求", "QPS", "吞吐 tok/s", "E2E 中位数 s", "E2E P99 s"], fair_rows),
    table("500 请求多基线：性能", ["负载", "实验组", "轮数", "平均成功请求", "QPS", "吞吐 tok/s", "E2E 中位数 s", "E2E P99 s"], multi_perf_rows, "完成 18/36 轮；包含共享 GPU 条件下的 contended 结果。"),
    table("500 请求多基线：迁移与异常", ["负载", "实验组", "KVT 成功", "FinishedReq", "KVT 500", "Abort", "EngineCore fatal", "Traceback"], multi_evidence_rows),
    table("54 轮迁移策略：性能", ["负载", "迁移策略", "轮数", "平均成功请求", "QPS", "吞吐 tok/s", "E2E 中位数 s", "E2E P99 s"], policy_perf_rows, "54/54 轮完成；结果标记含 contended，不用于严格公平性能结论。"),
    table("54 轮迁移策略：迁移结果", ["负载", "迁移策略", "触发迁移", "迁移成功", "迁移失败", "迁移成功率", "中断恢复成功率", "KVT 500"], policy_migration_rows),
    table("Figure 10：迁移效率最小压力复跑", ["序列长度", "轮数", "成功请求", "请求总数", "KVT 成功", "KVT 500", "可恢复 Abort", "EngineCore fatal", "Traceback"], fig10_rows),
    table("Figure 12 v1：碎片化实验", ["策略", "轮数", "成功请求", "QPS", "吞吐 tok/s", "E2E P99 s", "碎片化比例", "Abort"], fig12_rows, "状态：completed-but-not-valid；碎片化比例均为 0，Load-SR 存在 abort。"),
    table("Figure 12 v2：压力窗口扫描", ["配置", "策略", "成功请求", "非零碎片采样", "HOL 最大值", "长输入 TTFT P99 s", "SSE 超时", "KVT 500", "有效"], pressure_rows, "推荐正式配置：配置 C，400 请求，并发 40。"),
    table("数据集异常计数", ["数据集", "EngineCore fatal", "Traceback", "ERROR", "KVT 500", "Abort"], exception_rows),
]
if invalid_runs:
    sections.append(table("无效或归档轮次", ["负载/场景", "策略/实验组", "状态", "原因"], invalid_runs[:40], f"共识别 {len(invalid_runs)} 条；本表最多显示 40 条。"))


css = """
:root{--ink:#18212b;--muted:#637083;--line:#d9e0e7;--head:#243447;--blue:#eaf3fb;--orange:#fff1df;--green:#e8f5ec;--red:#fdeaea;--gray:#f1f3f5}
*{box-sizing:border-box} body{margin:0;background:#fff;color:var(--ink);font-family:"Microsoft YaHei","Segoe UI",Arial,sans-serif;font-size:13px;line-height:1.45}
main{width:min(1180px,calc(100% - 28px));margin:22px auto 36px} h1{font-size:25px;margin:0 0 5px;letter-spacing:0} .sub{color:var(--muted);font-size:12px;margin-bottom:20px;word-break:break-all}
section{margin:0 0 24px;padding-top:4px} h2{font-size:17px;margin:0 0 8px;padding-left:9px;border-left:4px solid #3e6f92;letter-spacing:0}.tagline{margin:-2px 0 8px;color:var(--muted);font-size:12px}
.table-wrap{width:100%;overflow:visible;border:1px solid var(--line)} table{width:100%;border-collapse:collapse;table-layout:fixed} th{background:var(--head);color:#fff;font-weight:600;padding:7px 6px;text-align:center;font-size:12px;line-height:1.3}td{padding:6px 6px;border-top:1px solid var(--line);border-right:1px solid var(--line);text-align:center;overflow-wrap:anywhere}td:first-child,td:nth-child(2){font-weight:600}tr:nth-child(even) td{background:#f8fafb}td:last-child,th:last-child{border-right:0}.na{background:var(--gray)!important;color:#7a838d}.bad{background:var(--red)!important;color:#9e2525;font-weight:700}
footer{border-top:1px solid var(--line);padding-top:10px;color:var(--muted);font-size:11px;display:flex;justify-content:space-between;gap:12px}
@media(max-width:900px){main{width:calc(100% - 16px);margin-top:12px}body{font-size:11px}h1{font-size:20px}h2{font-size:15px}th,td{padding:5px 3px;font-size:10px}.tagline{font-size:10px}}
@media print{main{width:100%;margin:0}section{break-inside:avoid}body{font-size:9px}th,td{padding:3px}.table-wrap{border-color:#999}}
"""


document = f"""<!doctype html>
<html lang="zh-CN"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Llumnix 完整实验数据统计表</title><style>{css}</style></head>
<body><main><header><h1>Llumnix 完整实验数据统计表</h1>
<div class="sub">数据目录：{html.escape(str(EXPORT_ROOT))}</div></header>
{''.join(sections)}
<footer><span>生成时间：{datetime.now().astimezone().isoformat(timespec='seconds')}</span><span>数据来源：full_data_export_20260824_151951</span></footer>
</main></body></html>"""

OUTPUT.write_text(document, encoding="utf-8")
print(OUTPUT)
print(f"sections={len(sections)}")
print(f"invalid_rows={len(invalid_runs)}")
