from pathlib import Path

root = Path("/data/home/zylu/llumnix-v018-main/results/5090_4inst_formal_20260721_014617")
summary = root / "RESULTS_SUMMARY.md"
csv = root / "formal_results.csv"
handoff = Path("/data/home/zylu/llumnix-v018-main/AI_HANDOFF.md")

print("summary_exists", summary.exists())
print("csv_exists", csv.exists())
print("csv_lines", sum(1 for _ in csv.open()) if csv.exists() else 0)

text = summary.read_text(errors="ignore") if summary.exists() else ""
checks = [
    "四实例 baseline 三轮全部成功",
    "四实例开启迁移三轮也全部 HTTP 200",
    "KVTResp(code=0) 总数：57",
    "FinishedReq=",
    "真实迁移链路已经跑通",
]
for check in checks:
    print("check", check, check in text)

print("handoff_has_formal", "formal 4-instance experiment" in handoff.read_text(errors="ignore"))
print("handoff_tail")
print(handoff.read_text(errors="ignore")[-1200:])
