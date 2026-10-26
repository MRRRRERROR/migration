import ast
import html
import math
import re
import statistics
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parent
EXP = ROOT / "opt-1.3b_20260628_153807"
OUT = ROOT / "real_migration_experiment_report.html"
METRICS = ROOT / "real_migration_experiment_metrics.csv"


def percentile(values, p):
    if not values:
        return None
    values = sorted(values)
    index = math.ceil(p * len(values)) - 1
    return values[max(0, min(index, len(values) - 1))]


def parse_array(text, name):
    match = re.search(name + r"=(\[.*?\])", text, re.S)
    if not match:
        return []
    try:
        return ast.literal_eval(match.group(1))
    except Exception:
        return []


def stats(values):
    values = [v for v in values if isinstance(v, (int, float)) and v == v]
    if not values:
        return {
            "样本数": 0,
            "平均值": None,
            "P50": None,
            "P95": None,
            "P99": None,
            "最小值": None,
            "最大值": None,
        }
    return {
        "样本数": len(values),
        "平均值": statistics.mean(values),
        "P50": statistics.median(values),
        "P95": percentile(values, 0.95),
        "P99": percentile(values, 0.99),
        "最小值": min(values),
        "最大值": max(values),
    }


def fmt(value, digits=4):
    if value is None:
        return "无数据"
    if isinstance(value, float):
        return f"{value:.{digits}f}"
    return str(value)


def parse_benchmark():
    text = (EXP / "benchmark.log").read_text(encoding="utf-8", errors="replace")
    data = {"是否有效": "successful_responses" in text and "Connect to" not in text}

    match = re.search(
        r"backend \S+ dur_s ([0-9.]+) tokens_per_s ([0-9.]+) qps ([0-9.]+)",
        text,
    )
    if match:
        data["实验总耗时_秒"] = float(match.group(1))
        data["吞吐量_tokens每秒"] = float(match.group(2))
        data["实际QPS"] = float(match.group(3))

    match = re.search(
        r"successful_responses (\d+) prompt_token_count (\d+) response_token_count (\d+)",
        text,
    )
    if match:
        data["成功请求数"] = int(match.group(1))
        data["输入Token总数"] = int(match.group(2))
        data["输出Token总数"] = int(match.group(3))

    data["端到端延迟_秒"] = parse_array(text, "all_e2e_latencies")
    data["每Token延迟_秒"] = parse_array(text, "all_per_token_latencies")
    data["等待延迟_秒"] = parse_array(text, "all_waiting_latencies")
    data["DecodeToken延迟"] = parse_array(text, "all_decode_token_latencies")
    data["输入长度"] = parse_array(text, "all_prompt_lens")
    data["输出长度"] = parse_array(text, "all_response_lens")
    data["总Token数"] = parse_array(text, "all_total_tokens")
    return data


def parse_migration():
    text = (EXP / "migration.log").read_text(encoding="utf-8", errors="replace")
    migrations = []
    recv_events = []

    for match in re.finditer(
        r"Instance ([^ ]+)->([^ ]+) migrate done, migration type: ([A-Z_]+), "
        r"migrate request \[(.*?)\], migration status: MigrationStatus\.([A-Z_]+), "
        r"len: (\d+) blocks, cost: ([0-9.]+) ms",
        text,
    ):
        migrations.append(
            {
                "源实例": match.group(1),
                "目标实例": match.group(2),
                "迁移类型": match.group(3),
                "请求ID": match.group(4).replace("'", ""),
                "状态": match.group(5),
                "迁移Block数": int(match.group(6)),
                "迁移耗时_毫秒": float(match.group(7)),
            }
        )

    for match in re.finditer(
        r"total_kv_cache_size: ([0-9.]+) MB, time: ([0-9.]+)s, speed: ([0-9.]+)GB/s",
        text,
    ):
        recv_events.append(
            {
                "KV大小_MB": float(match.group(1)),
                "传输时间_秒": float(match.group(2)),
                "传输速度_GB每秒": float(match.group(3)),
            }
        )
    return migrations, recv_events


def parse_instance_stats():
    frame = pd.read_csv(EXP / "server_instance.csv")
    columns = {
        "gpu_cache_usage": "GPU缓存使用率",
        "num_available_gpu_blocks": "可用GPU Block数",
        "num_running_requests": "运行中请求数",
        "num_waiting_requests": "等待请求数",
        "kv_blocks_ratio": "KV Block使用比例",
        "remaining_steps": "剩余步数",
        "migration_load_metric": "迁移负载指标",
    }
    rows = []
    for column, name in columns.items():
        if column not in frame.columns:
            continue
        values = pd.to_numeric(frame[column], errors="coerce").dropna().tolist()
        s = stats(values)
        rows.append(
            {
                "指标": name,
                "样本数": s["样本数"],
                "平均值": s["平均值"],
                "P50": s["P50"],
                "P95": s["P95"],
                "P99": s["P99"],
                "最小值": s["最小值"],
                "最大值": s["最大值"],
            }
        )
    return frame, rows


def make_table(headers, rows, class_name=""):
    thead = "".join(f"<th>{html.escape(str(h))}</th>" for h in headers)
    body = ""
    for row in rows:
        body += "<tr>"
        for header in headers:
            body += f"<td>{html.escape(fmt(row.get(header)))}</td>"
        body += "</tr>"
    return f'<div class="table-wrap"><table class="{class_name}"><thead><tr>{thead}</tr></thead><tbody>{body}</tbody></table></div>'


def metric_table_rows(benchmark, migrations, recv_events, instance_frame):
    finished = [m for m in migrations if m["状态"] == "FINISHED"]
    aborted = [m for m in migrations if m["状态"] != "FINISHED"]
    e2e = stats(benchmark["端到端延迟_秒"])
    token = stats(benchmark["每Token延迟_秒"])
    migration_cost = stats([m["迁移耗时_毫秒"] for m in finished])
    migration_blocks = stats([m["迁移Block数"] for m in finished])
    recv_size = stats([e["KV大小_MB"] for e in recv_events])
    recv_speed = stats([e["传输速度_GB每秒"] for e in recv_events])

    rows = [
        ("实验类型", "真实迁移实验", "", "真实启动 Llumnix/vLLM，在 RTX 3090 上运行"),
        ("模型", "facebook/opt-1.3b", "", "本轮真实迁移实验使用的模型"),
        ("GPU", "2 x RTX 3090", "", "实验室服务器硬件"),
        ("Benchmark是否有效", benchmark["是否有效"], "", "是否成功解析到完整 benchmark 结果"),
        ("成功请求数", benchmark.get("成功请求数"), "个", "成功完成响应的请求数"),
        ("实验总耗时", benchmark.get("实验总耗时_秒"), "秒", "benchmark 总运行时间"),
        ("吞吐量", benchmark.get("吞吐量_tokens每秒"), "tokens/s", "单位时间处理的 token 数"),
        ("实际QPS", benchmark.get("实际QPS"), "requests/s", "单位时间完成的请求数"),
        ("输入Token总数", benchmark.get("输入Token总数"), "tokens", "所有请求的 prompt token 总量"),
        ("输出Token总数", benchmark.get("输出Token总数"), "tokens", "所有请求的 response token 总量"),
        ("端到端延迟P50", e2e["P50"], "秒", "50%请求完成时间不超过该值"),
        ("端到端延迟P95", e2e["P95"], "秒", "95%请求完成时间不超过该值"),
        ("端到端延迟P99", e2e["P99"], "秒", "99%请求完成时间不超过该值"),
        ("端到端延迟最大值", e2e["最大值"], "秒", "最慢请求完成时间"),
        ("每Token延迟P50", token["P50"], "秒/token", "生成阶段单 token 延迟中位数"),
        ("每Token延迟P95", token["P95"], "秒/token", "生成阶段单 token 延迟P95"),
        ("迁移尝试次数", len(migrations), "次", "迁移日志中记录的迁移事件总数"),
        ("迁移成功次数", len(finished), "次", "状态为FINISHED的迁移次数"),
        ("迁移中止次数", len(aborted), "次", "状态不是FINISHED的迁移次数"),
        ("迁移成功率", len(finished) / len(migrations) if migrations else None, "比例", "成功迁移次数/迁移尝试次数"),
        ("成功迁移耗时P50", migration_cost["P50"], "毫秒", "成功迁移总耗时中位数"),
        ("成功迁移耗时P95", migration_cost["P95"], "毫秒", "成功迁移总耗时P95"),
        ("成功迁移耗时最大值", migration_cost["最大值"], "毫秒", "最慢一次成功迁移耗时"),
        ("成功迁移Block数P50", migration_blocks["P50"], "blocks", "成功迁移的KV cache block数中位数"),
        ("成功迁移Block数最大值", migration_blocks["最大值"], "blocks", "单次成功迁移最多block数"),
        ("KV接收事件数", len(recv_events), "次", "日志中KV cache接收事件数量"),
        ("KV接收大小P50", recv_size["P50"], "MB", "单次接收KV cache大小中位数"),
        ("KV接收大小最大值", recv_size["最大值"], "MB", "单次接收KV cache最大大小"),
        ("KV接收速度P50", recv_speed["P50"], "GB/s", "KV cache接收速度中位数"),
        ("KV接收速度P95", recv_speed["P95"], "GB/s", "KV cache接收速度P95"),
        ("实例状态采样行数", len(instance_frame), "行", "server_instance.csv中的采样记录数"),
    ]
    return [
        {"指标": name, "数值": value, "单位": unit, "说明": desc}
        for name, value, unit, desc in rows
    ]


def main():
    benchmark = parse_benchmark()
    migrations, recv_events = parse_migration()
    instance_frame, instance_rows = parse_instance_stats()
    metric_rows = metric_table_rows(benchmark, migrations, recv_events, instance_frame)

    pd.DataFrame(metric_rows).to_csv(METRICS, index=False, encoding="utf-8-sig")

    migration_rows = migrations
    recv_rows = recv_events

    status_counts = {}
    for migration in migrations:
        status_counts[migration["状态"]] = status_counts.get(migration["状态"], 0) + 1
    status_rows = [
        {"迁移状态": status, "次数": count} for status, count in sorted(status_counts.items())
    ]

    html_doc = f"""<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<title>Llumnix 真实迁移实验表格报告</title>
<style>
body {{ margin:0; font-family:"Microsoft YaHei","Segoe UI",Arial,sans-serif; background:#f7f8fb; color:#111827; }}
header {{ background:white; border-bottom:1px solid #e5e7eb; padding:28px 34px 18px; }}
h1 {{ margin:0 0 8px; font-size:28px; }}
.sub {{ color:#6b7280; line-height:1.6; }}
main {{ padding:24px 34px 42px; display:grid; gap:18px; }}
.panel {{ background:white; border:1px solid #e5e7eb; border-radius:8px; padding:18px; }}
h2 {{ margin:0 0 12px; font-size:18px; }}
.note {{ background:#fff7ed; border:1px solid #fed7aa; color:#7c2d12; padding:12px 14px; border-radius:8px; line-height:1.6; }}
.table-wrap {{ overflow:auto; }}
table {{ border-collapse:collapse; width:100%; font-size:13px; }}
th, td {{ border-bottom:1px solid #eef2f7; padding:9px 10px; text-align:left; vertical-align:top; }}
th {{ background:#f8fafc; position:sticky; top:0; }}
td {{ font-variant-numeric:tabular-nums; }}
footer {{ padding:18px 34px 36px; color:#6b7280; font-size:13px; }}
</style>
</head>
<body>
<header>
<h1>Llumnix 真实迁移实验表格报告</h1>
<div class="sub">数据来源：<code>opt-1.3b_20260628_153807</code>。本报告只展示真实迁移实验，不混入仿真 baseline。</div>
</header>
<main>
<div class="note">说明：这里的“迁移耗时”来自日志中的 <code>migration status ... cost: xx ms</code>，表示迁移总耗时，不等同于论文严格定义的 downtime。</div>
<section class="panel">
<h2>指标明细</h2>
{make_table(["指标", "数值", "单位", "说明"], metric_rows)}
</section>
<section class="panel">
<h2>迁移状态统计</h2>
{make_table(["迁移状态", "次数"], status_rows)}
</section>
<section class="panel">
<h2>每次迁移记录</h2>
{make_table(["源实例", "目标实例", "迁移类型", "请求ID", "状态", "迁移Block数", "迁移耗时_毫秒"], migration_rows)}
</section>
<section class="panel">
<h2>KV Cache 接收记录</h2>
{make_table(["KV大小_MB", "传输时间_秒", "传输速度_GB每秒"], recv_rows)}
</section>
<section class="panel">
<h2>实例状态统计</h2>
{make_table(["指标", "样本数", "平均值", "P50", "P95", "P99", "最小值", "最大值"], instance_rows)}
</section>
</main>
<footer>生成文件：<code>{html.escape(str(OUT))}</code>；指标 CSV：<code>{html.escape(str(METRICS))}</code>。</footer>
</body>
</html>"""
    OUT.write_text(html_doc, encoding="utf-8")
    print(OUT)
    print(METRICS)


if __name__ == "__main__":
    main()
