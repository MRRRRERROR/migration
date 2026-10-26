import html
import json
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
INPUT = ROOT / "multi_baseline_500req_aggregated_current.json"
OUTPUT = ROOT / "llumnix_5090_current_results.html"

GROUP_LABELS = {
    "Hybrid_no_migration": "Hybrid（无迁移）",
    "Hybrid_live_migration": "Hybrid（实时迁移）",
    "Round_robin_no_migration": "轮询（无迁移）",
    "Load_balance_no_migration": "负载均衡（无迁移）",
}
WORKLOAD_LABELS = {
    "stable_short": "稳定短请求",
    "mixed_tail": "混合长尾请求",
    "long_output_migration": "长输出迁移请求",
}


def esc(value):
    return html.escape(str(value), quote=True)


def num(value, digits=2, suffix=""):
    if value is None:
        return "未记录"
    return f"{float(value):.{digits}f}{suffix}"


def td(value, cls=""):
    class_attr = f' class="{cls}"' if cls else ""
    return f"<td{class_attr}>{esc(value)}</td>"


def aggregate_completed(summaries):
    grouped = defaultdict(list)
    for item in summaries:
        if item.get("status") == "completed":
            grouped[(item.get("workload_type"), item.get("experiment_group"))].append(item)
    result = {}
    for key, rows in grouped.items():
        def mean(field):
            values = [float(r[field]) for r in rows if r.get(field) is not None]
            return sum(values) / len(values) if values else None

        result[key] = {
            "runs": len(rows),
            "success": sum(int(r.get("successful_responses", 0) or 0) for r in rows),
            "success_rate": mean("success_rate"),
            "qps": mean("QPS"),
            "throughput": mean("throughput_tok_s"),
            "median": mean("median_e2e_latency_s"),
            "p95": mean("P95_e2e_latency_s"),
            "p99": mean("P99_e2e_latency_s"),
        }
    return result


def performance_table(workload, aggregates):
    rows = []
    for group in GROUP_LABELS:
        item = aggregates.get((workload, group))
        if not item:
            continue
        rows.append(
            "<tr>"
            + td(GROUP_LABELS[group], "group")
            + td(item["runs"])
            + td(item["success"])
            + td(num(item["success_rate"] * 100, 2, "%"), "ok")
            + td(num(item["qps"], 2))
            + td(num(item["throughput"], 1))
            + td(num(item["median"], 3))
            + td(num(item["p95"], 3))
            + td(num(item["p99"], 3))
            + "</tr>"
        )
    return f"""
    <section>
      <h2>{esc(WORKLOAD_LABELS[workload])}</h2>
      <table>
        <thead><tr><th>实验组</th><th>完成轮次</th><th>成功请求</th><th>成功率</th><th>QPS<br><small>req/s</small></th><th>输出吞吐<br><small>tok/s</small></th><th>Median<br><small>s</small></th><th>P95<br><small>s</small></th><th>P99<br><small>s</small></th></tr></thead>
        <tbody>{''.join(rows)}</tbody>
      </table>
    </section>"""


def bar_chart(workload, aggregates, metric, title, unit):
    values = []
    for group in GROUP_LABELS:
        item = aggregates.get((workload, group))
        if item and item.get(metric) is not None:
            values.append((GROUP_LABELS[group], float(item[metric])))
    maximum = max((value for _, value in values), default=1)
    bars = []
    for label, value in values:
        width = max(2, value / maximum * 100)
        bars.append(
            f'<div class="bar-row"><span>{esc(label)}</span><div class="track"><i style="width:{width:.2f}%"></i></div><b>{value:.2f} {esc(unit)}</b></div>'
        )
    return f'<div class="chart"><h3>{esc(title)}</h3>{"".join(bars)}</div>'


def main():
    data = json.loads(INPUT.read_text(encoding="utf-8"))
    summaries = data["summaries"]
    aggregates = aggregate_completed(summaries)
    completed = [item for item in summaries if item.get("status") == "completed"]
    failed = [item for item in summaries if item.get("status") != "completed"]
    contended = [item for item in summaries if item.get("fairness_status") == "contended"]
    successful_requests = sum(int(item.get("successful_responses", 0) or 0) for item in completed)

    evidence = [
        item
        for item in completed
        if item.get("experiment_group") == "Hybrid_live_migration"
    ]
    evidence.sort(key=lambda x: (x.get("workload_type", ""), x.get("repeat_id", 0)))
    evidence_rows = []
    for item in evidence:
        evidence_rows.append(
            "<tr>"
            + td(WORKLOAD_LABELS.get(item.get("workload_type"), item.get("workload_type")), "group")
            + td(item.get("repeat_id"))
            + td(item.get("KVTResp_code_0_count", 0), "ok" if item.get("KVTResp_code_0_count", 0) else "muted")
            + td(item.get("FinishedReq_lines", 0))
            + td(item.get("unique_FinishedReq_ids", 0))
            + td(item.get("SendStubMetrics_count", 0))
            + td(item.get("migration_end_count", 0))
            + td(item.get("KVTResp_code_500_count", 0), "ok" if not item.get("KVTResp_code_500_count", 0) else "bad")
            + td(item.get("finish_reason_abort_count", 0), "ok" if not item.get("finish_reason_abort_count", 0) else "bad")
            + "</tr>"
        )

    anomaly_keys = [
        ("EngineCore fatal", "EngineCore_fatal_count"),
        ("Traceback", "Traceback_count"),
        ("ERROR", "ERROR_count"),
        ("RuntimeError", "RuntimeError_count"),
        ("CUDA illegal memory access", "CUDA_illegal_memory_access_count"),
        ("KVT code=500", "KVTResp_code_500_count"),
        ("请求 abort", "finish_reason_abort_count"),
    ]
    anomaly_rows = []
    for label, key in anomaly_keys:
        value = sum(int(item.get(key, 0) or 0) for item in summaries)
        anomaly_rows.append("<tr>" + td(label, "group") + td(value, "ok" if value == 0 else "bad") + "</tr>")

    run_rows = []
    for item in sorted(summaries, key=lambda x: (x.get("workload_type", ""), x.get("experiment_group", ""), x.get("repeat_id", 0))):
        status = item.get("status", "未知")
        fair = "共享试跑" if item.get("fairness_status") == "contended" else "早期保留结果"
        run_rows.append(
            "<tr>"
            + td(WORKLOAD_LABELS.get(item.get("workload_type"), item.get("workload_type")), "group")
            + td(GROUP_LABELS.get(item.get("experiment_group"), item.get("experiment_group")))
            + td(item.get("repeat_id"))
            + td("完成" if status == "completed" else "失败", "ok" if status == "completed" else "bad")
            + td(fair, "warn" if fair == "共享试跑" else "")
            + td(item.get("successful_responses", "-"))
            + td(num(item.get("QPS"), 2))
            + td(num(item.get("throughput_tok_s"), 1))
            + td(num(item.get("P99_e2e_latency_s"), 3))
            + "</tr>"
        )

    group_counts = Counter(item.get("experiment_group") for item in completed)
    failure_text = "无"
    if failed:
        item = failed[0]
        failure_text = (
            f'{WORKLOAD_LABELS.get(item.get("workload_type"), item.get("workload_type"))} / '
            f'{GROUP_LABELS.get(item.get("experiment_group"), item.get("experiment_group"))} / '
            f'第 {item.get("repeat_id")} 轮：EngineCore 启动端口冲突（EADDRINUSE，port 39351）'
        )

    generated = datetime.now().astimezone().isoformat(timespec="seconds")
    html_doc = f"""<!doctype html>
<html lang="zh-CN"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Llumnix RTX 5090 四实例阶段性实验结果</title>
<style>
:root{{--ink:#17212b;--muted:#65717d;--line:#d7dde3;--paper:#fff;--bg:#f3f5f7;--blue:#2f6f9f;--orange:#d17a22;--green:#1b7f55;--red:#b42318;--amber:#9a6700}}
*{{box-sizing:border-box}}body{{margin:0;background:var(--bg);color:var(--ink);font-family:Arial,"Microsoft YaHei",sans-serif;letter-spacing:0}}main{{max-width:1160px;margin:0 auto;padding:24px 20px 40px}}header{{border-bottom:2px solid var(--ink);padding-bottom:14px}}h1{{font-size:25px;margin:0 0 8px}}h2{{font-size:18px;margin:26px 0 10px}}h3{{font-size:14px;margin:0 0 12px}}p{{margin:5px 0;color:var(--muted);font-size:13px}}.notice{{margin-top:14px;padding:10px 12px;border-left:4px solid var(--amber);background:#fff8df;color:#634b00}}.stats{{display:grid;grid-template-columns:repeat(4,1fr);gap:10px;margin:18px 0}}.stat{{background:var(--paper);border:1px solid var(--line);padding:14px}}.stat span{{display:block;color:var(--muted);font-size:12px}}.stat b{{display:block;font-size:23px;margin-top:5px}}.charts{{display:grid;grid-template-columns:1fr 1fr;gap:16px}}.chart{{background:var(--paper);border-top:3px solid var(--blue);padding:14px}}.bar-row{{display:grid;grid-template-columns:145px 1fr 95px;align-items:center;gap:8px;margin:9px 0;font-size:12px}}.bar-row b{{font-weight:500;text-align:right}}.track{{height:12px;background:#e7ebef}}.track i{{display:block;height:100%;background:var(--blue)}}table{{width:100%;border-collapse:collapse;background:var(--paper);font-size:12px;table-layout:fixed}}th,td{{border:1px solid var(--line);padding:7px 6px;text-align:center;vertical-align:middle;overflow-wrap:anywhere}}th{{background:#263746;color:#fff;font-weight:500}}th small{{font-weight:400;color:#dce5ec}}td.group{{font-weight:500;text-align:left}}tbody tr:nth-child(even){{background:#f7f9fa}}.ok{{color:var(--green);font-weight:500}}.bad{{color:var(--red);font-weight:500;background:#fff1f0}}.warn{{color:var(--amber);font-weight:500}}.muted{{color:var(--muted)}}.two{{display:grid;grid-template-columns:1.15fr .85fr;gap:16px}}.compact th,.compact td{{padding:6px 5px}}footer{{border-top:1px solid var(--line);margin-top:28px;padding-top:12px;color:var(--muted);font-size:12px}}@media(max-width:800px){{main{{padding:16px 10px}}.stats{{grid-template-columns:1fr 1fr}}.charts,.two{{grid-template-columns:1fr}}.bar-row{{grid-template-columns:120px 1fr 82px}}table{{font-size:11px}}th,td{{padding:5px 3px}}}}
</style></head><body><main>
<header><h1>Llumnix RTX 5090 四实例阶段性实验结果</h1><p>500 请求多基线对比 · 当前已记录 19/36 轮</p><p>结果目录：{esc(data.get('result_root'))}</p><div class="notice">GPU 共享环境下的非公平试跑。数据可用于验证流程、baseline 可运行性和迁移证据，不用于最终公平性能结论。</div></header>
<div class="stats"><div class="stat"><span>成功完成轮次</span><b>{len(completed)} / 36</b></div><div class="stat"><span>成功请求总数</span><b>{successful_requests:,}</b></div><div class="stat"><span>已记录迁移完成事件</span><b>{sum(int(x.get('migration_end_count',0) or 0) for x in evidence)}</b></div><div class="stat"><span>失败轮次</span><b>{len(failed)}</b></div></div>
<section><h2>实验配置与进度</h2><table class="compact"><thead><tr><th>实例数</th><th>每轮请求数</th><th>并发</th><th>计划轮次</th><th>已完成</th><th>共享试跑标记</th><th>尚未开始</th></tr></thead><tbody><tr><td>4</td><td>500</td><td>50</td><td>36</td><td>{len(completed)}</td><td>{len(contended)} 份 summary</td><td>长输出迁移 workload</td></tr></tbody></table><p>失败记录：{esc(failure_text)}</p></section>
<section><h2>性能概览</h2><div class="charts">{bar_chart('stable_short', aggregates, 'qps', '稳定短请求：平均 QPS', 'req/s')}{bar_chart('mixed_tail', aggregates, 'qps', '混合长尾请求：平均 QPS', 'req/s')}</div></section>
{performance_table('stable_short', aggregates)}
{performance_table('mixed_tail', aggregates)}
<section><h2>实时迁移证据</h2><table><thead><tr><th>Workload</th><th>轮次</th><th>KVT 成功</th><th>FinishedReq</th><th>唯一迁移请求</th><th>发送指标</th><th>迁移完成</th><th>KVT 500</th><th>Abort</th></tr></thead><tbody>{''.join(evidence_rows)}</tbody></table><p>有迁移证据的轮次共记录 KVT 成功 {sum(int(x.get('KVTResp_code_0_count',0) or 0) for x in evidence)} 次；部分实时迁移轮次未触发迁移，因此事件数为 0。</p></section>
<div class="two"><section><h2>异常复核</h2><table class="compact"><thead><tr><th>异常类型</th><th>数量</th></tr></thead><tbody>{''.join(anomaly_rows)}</tbody></table></section><section><h2>各组完成进度</h2><table class="compact"><thead><tr><th>实验组</th><th>完成轮次</th></tr></thead><tbody>{''.join('<tr>'+td(GROUP_LABELS[g],'group')+td(group_counts.get(g,0))+'</tr>' for g in GROUP_LABELS)}</tbody></table></section></div>
<section><h2>逐轮结果</h2><table><thead><tr><th>Workload</th><th>实验组</th><th>轮次</th><th>状态</th><th>数据属性</th><th>成功请求</th><th>QPS</th><th>吞吐<br><small>tok/s</small></th><th>P99<br><small>s</small></th></tr></thead><tbody>{''.join(run_rows)}</tbody></table></section>
<footer>整理时间：{esc(generated)}<br>仅整理当前已有结果，未新增或重跑实验。原始 JSON 与日志保持不变。</footer>
</main></body></html>"""
    OUTPUT.write_text(html_doc, encoding="utf-8")
    print(OUTPUT)


if __name__ == "__main__":
    main()
