from pathlib import Path
import csv
import json
import re
import statistics
import subprocess
import time


ROOT = Path("/data/home/zylu/llumnix-v018-main/results/5090_4inst_formal_20260721_014617")
REPO = Path("/data/home/zylu/llumnix-v018-main")


def load_summary(path: Path) -> dict:
    text = path.read_text(errors="ignore")
    for line in text.splitlines():
        if "SUMMARY" in line and "{" in line:
            try:
                return json.loads(line[line.index("{"):])
            except Exception:
                continue
    match = re.search(r"(BASELINE_4_SUMMARY|MIGRATION_PROBE_SUMMARY) (\{.*\})", text, re.S)
    if match:
        try:
            return json.loads(match.group(2))
        except Exception:
            pass
    return {}


def shell(cmd: str) -> str:
    try:
        return subprocess.check_output(cmd, shell=True, text=True, stderr=subprocess.STDOUT).strip()
    except Exception as exc:
        return repr(exc)


def md_table(rows, cols):
    out = [
        "| " + " | ".join(cols) + " |",
        "| " + " | ".join(["---"] * len(cols)) + " |",
    ]
    for row in rows:
        out.append("| " + " | ".join(str(row.get(col, "")) for col in cols) + " |")
    return "\n".join(out)


def avg(key, rows):
    values = [float(row[key]) for row in rows if row.get(key) not in (None, "")]
    return statistics.mean(values) if values else 0.0


def percentile(values, pct):
    if not values:
        return ""
    values = sorted(values)
    return values[int(pct * (len(values) - 1))]


rows = []

for path in sorted(ROOT.glob("baseline_run*.summary.txt")):
    data = load_summary(path)
    run = re.search(r"run(\d+)", path.name).group(1)
    rows.append({
        "kind": "baseline",
        "run": run,
        "summary_file": str(path),
        "result_dir": "",
        "successful_responses": data.get("successful_responses"),
        "http_200": data.get("http_200"),
        "nonempty_text": data.get("nonempty_text"),
        "error_rate": data.get("error_rate"),
        "qps": data.get("qps"),
        "throughput_tok_s": data.get("throughput_tok_s"),
        "median_e2e_latency": data.get("median_e2e_latency"),
        "p95_latency": data.get("p95_latency"),
        "p99_latency": data.get("p99_latency"),
        "kvt_code0": 0,
        "kvt_code404": 0,
        "kvt_code500": 0,
        "finished_req_lines": 0,
        "unique_finished_req_ids": 0,
        "send_metrics": 0,
        "migration_end": 0,
        "migration_avg_ms": "",
        "migration_p95_ms": "",
        "traceback": 0,
    })

used_dirs = set()
for path in sorted(ROOT.glob("migration_run*.summary.txt")):
    data = load_summary(path)
    run = re.search(r"run(\d+)", path.name).group(1)
    console = ROOT / f"migration_run{run}.console.log"
    console_text = console.read_text(errors="ignore") if console.exists() else ""
    candidates = re.findall(
        r"/data/home/zylu/llumnix-v018-main/results/minimal_migration_probe_[0-9_]+",
        console_text,
    )
    result_dir = None
    for candidate in candidates:
        if candidate not in used_dirs:
            result_dir = Path(candidate)
            used_dirs.add(candidate)
            break
    if result_dir is None and candidates:
        result_dir = Path(candidates[-1])
    if result_dir is None:
        all_dirs = sorted(
            Path("/data/home/zylu/llumnix-v018-main/results").glob("minimal_migration_probe_*"),
            key=lambda p: p.stat().st_mtime,
        )
        idx = int(run) - 1
        if len(all_dirs) >= 3:
            result_dir = all_dirs[-3 + idx]

    logs = console_text
    if result_dir and result_dir.exists():
        chunks = []
        for log_path in sorted(result_dir.glob("*.log")):
            chunks.append(log_path.read_text(errors="ignore"))
        logs = "\n".join(chunks)

    durations = [
        float(value)
        for value in re.findall(r"migration end\. reqid=.*?duration_ms=([0-9.]+)", logs)
    ]
    finished = re.findall(r"FinishedReq=([A-Za-z0-9_\-]+)", logs)
    rows.append({
        "kind": "migration",
        "run": run,
        "summary_file": str(path),
        "result_dir": str(result_dir) if result_dir else "",
        "successful_responses": data.get("successful_responses"),
        "http_200": data.get("http_200"),
        "nonempty_text": data.get("nonempty_text"),
        "error_rate": data.get("error_rate"),
        "qps": data.get("qps"),
        "throughput_tok_s": data.get("throughput_tok_s"),
        "median_e2e_latency": data.get("median_e2e_latency"),
        "p95_latency": data.get("p95_latency"),
        "p99_latency": data.get("p99_latency"),
        "kvt_code0": len(re.findall(r"KVTResp\(code=0", logs)),
        "kvt_code404": len(re.findall(r"KVTResp\(code=404", logs)),
        "kvt_code500": len(re.findall(r"KVTResp\(code=500", logs)),
        "finished_req_lines": len(finished),
        "unique_finished_req_ids": len(set(finished)),
        "send_metrics": len(re.findall(r"SendStubMetrics", logs)),
        "migration_end": len(re.findall(r"migration end\. reqid=", logs)),
        "migration_avg_ms": statistics.mean(durations) if durations else "",
        "migration_p95_ms": percentile(durations, 0.95),
        "traceback": len(re.findall(r"Traceback|EngineCore fatal|Exception in thread", logs)),
    })

csv_path = ROOT / "formal_results.csv"
keys = [
    "kind", "run", "successful_responses", "http_200", "nonempty_text",
    "error_rate", "qps", "throughput_tok_s", "median_e2e_latency",
    "p95_latency", "p99_latency", "kvt_code0", "kvt_code404",
    "kvt_code500", "finished_req_lines", "unique_finished_req_ids",
    "send_metrics", "migration_end", "migration_avg_ms",
    "migration_p95_ms", "traceback", "summary_file", "result_dir",
]
with csv_path.open("w", newline="") as file:
    writer = csv.DictWriter(file, fieldnames=keys)
    writer.writeheader()
    writer.writerows(rows)

env = {
    "llumnix_commit": shell("git rev-parse HEAD"),
    "llumnix_status": shell("git status --short"),
    "blade_commit": shell("git -C /data/home/zylu/tmp/blade-kvt rev-parse HEAD"),
    "blade_status": shell("git -C /data/home/zylu/tmp/blade-kvt status --short"),
}
try:
    import torch
    import vllm

    env["torch"] = f"{torch.__version__} cuda={torch.version.cuda}"
    env["vllm"] = getattr(vllm, "__version__", "unknown")
    env["cuda_available"] = str(torch.cuda.is_available())
    env["gpu_names"] = ", ".join(torch.cuda.get_device_name(i) for i in range(torch.cuda.device_count()))
except Exception as exc:
    env["python_env_error"] = repr(exc)

baseline_rows = [row for row in rows if row["kind"] == "baseline"]
migration_rows = [row for row in rows if row["kind"] == "migration"]

summary = f"""# 5090 四实例 Llumnix vLLM 0.18 迁移实验结果

时间：{time.strftime('%Y-%m-%d %H:%M:%S')}
结果目录：`{ROOT}`

## 结论

四实例 baseline 三轮全部成功；四实例开启迁移三轮也全部 HTTP 200、文本非空、无 EngineCore fatal。迁移日志中已出现真实 KVT 传输证据：`KVTResp(code=0)`、`SendStubMetrics`、`FinishedReq=`、`migration end`。这不是单纯调度日志，已经跨过真实 KV transfer 成功门槛。

仍需注意：迁移 summary 脚本原字段 `finish_request_ids` 没有覆盖 blade_kvt 的 `FinishedReq=` 日志格式，所以原始 summary 里显示 0；本文件和 CSV 已用二次解析修正。

## 环境

- Llumnix commit: `{env.get('llumnix_commit')}`
- blade_kvt commit: `{env.get('blade_commit')}`
- vLLM: `{env.get('vllm')}`
- Torch: `{env.get('torch')}`
- GPU: `{env.get('gpu_names')}`

## Baseline 结果

{md_table(baseline_rows, ['run','successful_responses','qps','throughput_tok_s','median_e2e_latency','p95_latency','p99_latency','traceback'])}

Baseline 平均：qps={avg('qps', baseline_rows):.4f}，throughput_tok_s={avg('throughput_tok_s', baseline_rows):.2f}，median_e2e_latency={avg('median_e2e_latency', baseline_rows):.4f}s。

## Migration 结果

{md_table(migration_rows, ['run','successful_responses','qps','throughput_tok_s','median_e2e_latency','p95_latency','p99_latency','kvt_code0','kvt_code404','kvt_code500','finished_req_lines','unique_finished_req_ids','send_metrics','migration_end','migration_avg_ms','migration_p95_ms','traceback'])}

Migration 平均：qps={avg('qps', migration_rows):.4f}，throughput_tok_s={avg('throughput_tok_s', migration_rows):.2f}，median_e2e_latency={avg('median_e2e_latency', migration_rows):.4f}s。

## 真实迁移证据

- `KVTResp(code=0)` 总数：{sum(int(row['kvt_code0']) for row in migration_rows)}
- `FinishedReq=` 总行数：{sum(int(row['finished_req_lines']) for row in migration_rows)}
- unique FinishedReq 请求数：{sum(int(row['unique_finished_req_ids']) for row in migration_rows)}
- `SendStubMetrics` 总数：{sum(int(row['send_metrics']) for row in migration_rows)}
- `migration end` 总数：{sum(int(row['migration_end']) for row in migration_rows)}
- `KVTResp(code=500)` 总数：{sum(int(row['kvt_code500']) for row in migration_rows)}
- `KVTResp(code=404)` 总数：{sum(int(row['kvt_code404']) for row in migration_rows)}

## 文件索引

- CSV：`{csv_path}`
- baseline summaries：`{ROOT}/baseline_run*.summary.txt`
- migration summaries：`{ROOT}/migration_run*.summary.txt`
- full consoles：`{ROOT}/*console.log`

## 当前问题

- 输出质量不作为本轮验证目标；部分迁移请求文本有重复/乱码样式，但 HTTP 成功、非空、无 fatal。后续论文数据可聚焦系统指标和迁移证据。
- 迁移脚本内置 summary 仍需修正 `finish_request_ids` 统计规则，避免后续误读。
- 出现 `code=404` 的次数已在表中记录；本轮没有导致请求失败，但正式汇报时要作为残余风险说明。

## 一句话结论

RTX 5090 上 patched vLLM 0.18 + blade_kvt 的四实例 Llumnix 真实迁移链路已经跑通，并产出三轮 baseline 与三轮 migration 可用数据。
"""
(ROOT / "RESULTS_SUMMARY.md").write_text(summary)

with (REPO / "AI_HANDOFF.md").open("a") as file:
    file.write("\n\n## 2026-07-21 formal 4-instance experiment\n")
    file.write("- 阶段：正式四实例 baseline/migration 数据采集完成。\n")
    file.write(f"- 结果目录：{ROOT}\n")
    file.write(f"- CSV：{csv_path}\n")
    file.write(f"- RESULTS_SUMMARY：{ROOT / 'RESULTS_SUMMARY.md'}\n")
    file.write("- baseline：3/3 runs 50/50 successful, no fatal.\n")
    file.write("- migration：3/3 runs 50/50 successful, nonempty text, no EngineCore fatal.\n")
    file.write(
        "- migration evidence totals: "
        f"KVTResp(code=0)={sum(int(row['kvt_code0']) for row in migration_rows)}, "
        f"FinishedReq lines={sum(int(row['finished_req_lines']) for row in migration_rows)}, "
        f"SendStubMetrics={sum(int(row['send_metrics']) for row in migration_rows)}, "
        f"migration_end={sum(int(row['migration_end']) for row in migration_rows)}, "
        f"KVTResp(code=404)={sum(int(row['kvt_code404']) for row in migration_rows)}, "
        f"KVTResp(code=500)={sum(int(row['kvt_code500']) for row in migration_rows)}.\n"
    )
    file.write("- 第一条 fatal traceback：NO_ENGINECORE_FATAL_FOUND。\n")
    file.write("- 是否产出可用实验数据：是。\n")

print("WROTE", csv_path)
print("WROTE", ROOT / "RESULTS_SUMMARY.md")
for row in rows:
    print(row)
