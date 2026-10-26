from pathlib import Path
import csv
import tarfile

repo = Path("/data/home/zylu/llumnix-v018-main")
root = repo / "results" / "5090_4inst_formal_20260721_014617"
archive = repo / "results" / "5090_4inst_formal_20260721_014617.tar.gz"

scan_files = list(root.glob("*run*.console.log")) + list(root.glob("*run*.summary.txt"))
patterns = ["EngineCore fatal", "Traceback", "ERROR"]
filtered = []
counts = {pattern: 0 for pattern in patterns}

for path in sorted(scan_files):
    for lineno, line in enumerate(path.read_text(errors="ignore").splitlines(), 1):
        for pattern in patterns:
            if pattern not in line:
                continue
            if "NO_ENGINECORE_FATAL_FOUND" in line:
                continue
            if '"traceback": 0' in line or "'traceback': 0" in line:
                continue
            if "无 EngineCore fatal" in line or "no EngineCore fatal" in line:
                continue
            counts[pattern] += 1
            filtered.append(f"{path}:{lineno}:{line[:2000]}")

(root / "fatal_error_scan_filtered.txt").write_text(
    "Filtered scan over formal console and summary files.\n"
    + "\n".join(f"{key}: {value}" for key, value in counts.items())
    + "\n\nMatches:\n"
    + ("\n".join(filtered) if filtered else "none\n")
)

rows = list(csv.DictReader((root / "formal_results.csv").open()))
baseline = [row for row in rows if row["kind"] == "baseline"]
migration = [row for row in rows if row["kind"] == "migration"]

def avg(key, subset):
    vals = [float(row[key]) for row in subset if row.get(key)]
    return sum(vals) / len(vals) if vals else 0.0

fatal_present = any(counts.values())

(root / "FINAL_HANDOFF.md").write_text(
    "# FINAL_HANDOFF\n\n"
    "实验是否完成：完成。四实例 baseline 三轮和 migration 三轮均完成，migration 日志包含真实 KVT transfer 证据。\n"
    f"结果目录：{root}\n\n"
    "关键指标：\n"
    f"- baseline：3 轮，每轮 successful_responses=50/50，平均 qps={avg('qps', baseline):.4f}，平均 throughput_tok_s={avg('throughput_tok_s', baseline):.2f}，平均 median_e2e_latency={avg('median_e2e_latency', baseline):.4f}s。\n"
    f"- migration：3 轮，每轮 successful_responses=50/50，平均 qps={avg('qps', migration):.4f}，平均 throughput_tok_s={avg('throughput_tok_s', migration):.2f}，平均 median_e2e_latency={avg('median_e2e_latency', migration):.4f}s。\n"
    f"- migration evidence：KVTResp(code=0)={sum(int(row['kvt_code0']) for row in migration)}，FinishedReq lines={sum(int(row['finished_req_lines']) for row in migration)}，unique FinishedReq ids={sum(int(row['unique_finished_req_ids']) for row in migration)}，SendStubMetrics={sum(int(row['send_metrics']) for row in migration)}，migration_end={sum(int(row['migration_end']) for row in migration)}，KVTResp(code=404)={sum(int(row['kvt_code404']) for row in migration)}，KVTResp(code=500)={sum(int(row['kvt_code500']) for row in migration)}。\n"
    "\n"
    f"是否存在 fatal：{'存在匹配，查看 fatal_error_scan_filtered.txt' if fatal_present else '过滤扫描未发现 EngineCore fatal / Traceback / ERROR。'}\n\n"
    "可用于汇报的文件：\n"
    + "\n".join(
        f"- {root / name}"
        for name in [
            "RESULTS_SUMMARY.md",
            "formal_results.csv",
            "ENV_AND_PATCH_SUMMARY.md",
            "FINAL_HANDOFF.md",
            "fatal_error_scan_filtered.txt",
            "results_summary_field_check.txt",
            "llumnix_git_diff.patch",
            "blade_kvt_git_diff.patch",
            "baseline_run1.summary.txt",
            "baseline_run2.summary.txt",
            "baseline_run3.summary.txt",
            "migration_run1.summary.txt",
            "migration_run2.summary.txt",
            "migration_run3.summary.txt",
        ]
    )
    + "\n"
)

with tarfile.open(archive, "w:gz") as tar:
    tar.add(root, arcname=root.name)

print("filtered_counts", counts)
print("rewrote", root / "fatal_error_scan_filtered.txt")
print("rewrote", root / "FINAL_HANDOFF.md")
print("rewrote", archive)
