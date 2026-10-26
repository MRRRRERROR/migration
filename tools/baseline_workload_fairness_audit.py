#!/usr/bin/env python3
import csv
import re
from datetime import datetime
from pathlib import Path


FORMAL_DIR = Path("/data/home/zylu/llumnix-v018-main/results/5090_4inst_formal_20260721_014617")
OUT_DIR = Path("/data/home/zylu/llumnix-v018-main/results/5090_baselines_20260721_172757")
OUT = OUT_DIR / "WORKLOAD_FAIRNESS_AUDIT.md"
REPO = Path("/data/home/zylu/llumnix-v018-main")


def read(path):
    try:
        return path.read_text(errors="replace")
    except FileNotFoundError:
        return ""


def extract(pattern, text, default="未记录"):
    m = re.search(pattern, text, re.M)
    return m.group(1).strip() if m else default


def csv_rows():
    rows = []
    with (FORMAL_DIR / "formal_results.csv").open(newline="", errors="replace") as f:
        rows.extend(csv.DictReader(f))
    return rows


def avg(rows, kind, key):
    vals = []
    for row in rows:
        if row.get("kind") == kind:
            try:
                vals.append(float(row[key]))
            except Exception:
                pass
    return sum(vals) / len(vals) if vals else None


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    rows = csv_rows()
    env = read(FORMAL_DIR / "ENV_AND_PATCH_SUMMARY.md")
    formal = read(FORMAL_DIR / "RESULTS_SUMMARY.md")
    baseline_sh = read(REPO / "run_remote_no_migration_baseline.sh")
    baseline_py = read(REPO / "remote_no_migration_baseline.py")
    migration_sh = read(REPO / "run_remote_minimal_migration_probe.sh")
    migration_py = read(REPO / "remote_minimal_migration_probe.py")

    baseline = {
        "请求数": "50",
        "并发数": "20",
        "max_tokens": "32",
        "实例数": "4",
        "prompt": extract(r'"prompt":\s*f"([^"]+)"', baseline_py),
        "模型路径": "/data/models/Qwen2.5-7B-Instruct",
        "调度策略": extract(r"get_gateway_command\(policy=\"([^\"]+)\"", baseline_py),
        "connector": "HybridConnector",
        "迁移阈值/触发策略": "不启用迁移",
        "max_model_len": extract(r"--max-model-len\s+([0-9]+)", baseline_py),
        "max_num_batched_tokens": extract(r"--max-num-batched-tokens\s+([0-9]+)", baseline_py),
        "gpu_memory_utilization": extract(r"--gpu-memory-utilization\s+([0-9.]+)", baseline_py),
    }
    migration = {
        "请求数": "50",
        "并发数": "20",
        "max_tokens": "256",
        "实例数": "4",
        "prompt": extract(r'prompt\s*=\s*\(f"([^"]+)"', migration_py),
        "模型路径": "/data/models/Qwen2.5-7B-Instruct",
        "调度策略": extract(r"get_scheduler_command\(\s*policy=\"([^\"]+)\"", migration_py),
        "connector": "HybridConnector",
        "迁移阈值/触发策略": "enable_migration=True, rescheduling thresholds=0, trigger_policy=neutral_load",
        "max_model_len": extract(r"--max-model-len\s+([0-9]+)", migration_py),
        "max_num_batched_tokens": extract(r"--max-num-batched-tokens\s+([0-9]+)", migration_py),
        "gpu_memory_utilization": extract(r"--gpu-memory-utilization\s+([0-9.]+)", migration_py),
    }

    keys = [
        "请求数", "prompt", "max_tokens", "并发数", "实例数", "模型路径",
        "max_model_len", "max_num_batched_tokens", "gpu_memory_utilization",
        "connector", "迁移阈值/触发策略", "调度策略",
    ]
    consistent = []
    inconsistent = []
    for key in keys:
        if baseline[key] == migration[key]:
            consistent.append(key)
        else:
            inconsistent.append(key)

    bqps = avg(rows, "baseline", "qps")
    mqps = avg(rows, "migration", "qps")
    blat = avg(rows, "baseline", "median_e2e_latency")
    mlat = avg(rows, "migration", "median_e2e_latency")

    lines = []
    lines.append("# WORKLOAD_FAIRNESS_AUDIT")
    lines.append("")
    lines.append(f"生成时间：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append(f"正式结果目录：`{FORMAL_DIR}`")
    lines.append("")
    lines.append("## 审计结论")
    lines.append("")
    lines.append("旧正式结果中的 baseline 与 migration 不是同 workload，QPS/latency 不能直接作为公平性能对比。")
    lines.append("")
    lines.append("## 配置对照")
    lines.append("")
    lines.append("| 配置项 | Baseline | Migration | 是否一致 |")
    lines.append("| --- | --- | --- | --- |")
    for key in keys:
        ok = "一致" if baseline[key] == migration[key] else "不一致"
        lines.append(f"| {key} | {baseline[key]} | {migration[key]} | {ok} |")
    lines.append("")
    lines.append("## 一致配置")
    lines.append("")
    for key in consistent:
        lines.append(f"- {key}")
    lines.append("")
    lines.append("## 不一致配置")
    lines.append("")
    for key in inconsistent:
        lines.append(f"- {key}: baseline=`{baseline[key]}`，migration=`{migration[key]}`")
    lines.append("")
    lines.append("## 指标是否可直接比较")
    lines.append("")
    lines.append(f"- baseline 平均 QPS：{bqps:.4f}" if bqps else "- baseline 平均 QPS：未记录")
    lines.append(f"- migration 平均 QPS：{mqps:.4f}" if mqps else "- migration 平均 QPS：未记录")
    lines.append(f"- baseline median e2e latency 平均：{blat:.4f}s" if blat else "- baseline median e2e latency 平均：未记录")
    lines.append(f"- migration median e2e latency 平均：{mlat:.4f}s" if mlat else "- migration median e2e latency 平均：未记录")
    lines.append("- 结论：由于 max_tokens、prompt 构造、调度/迁移触发策略不同，这些 QPS/latency 不能直接说明 migration 比 no-migration 差。")
    lines.append("")
    lines.append("## 后续公平实验必须固定的变量")
    lines.append("")
    for item in [
        "同一份 workload 文件",
        "同一 request_num",
        "同一 prompt 内容或 prompt 文件",
        "同一 max_tokens",
        "同一并发数 / request_rate",
        "同一 4 实例数量",
        "同一模型路径",
        "同一 max_model_len",
        "同一 max_num_batched_tokens",
        "同一 gpu_memory_utilization",
        "同一连接器版本和环境，仅切换是否开启迁移",
    ]:
        lines.append(f"- {item}")
    lines.append("")
    lines.append("## 数据来源")
    lines.append("")
    lines.append("- formal_results.csv")
    lines.append("- run_remote_no_migration_baseline.sh")
    lines.append("- remote_no_migration_baseline.py")
    lines.append("- run_remote_minimal_migration_probe.sh")
    lines.append("- remote_minimal_migration_probe.py")
    lines.append("- ENV_AND_PATCH_SUMMARY.md")
    OUT.write_text("\n".join(lines), encoding="utf-8")
    print(OUT)


if __name__ == "__main__":
    main()
